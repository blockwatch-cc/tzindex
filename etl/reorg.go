// Copyright (c) 2018 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"container/list"
	"context"
	"fmt"

	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
)

func (c *Crawler) Rollback(ctx context.Context, height int64, ignoreErrors bool) error {
	tip := c.Tip()

	// negative height is treated as offset
	if height < 0 {
		height = util.Max64(tip.BestHeight+height, 0)
	}

	// check height against current tip
	if th := tip.BestHeight; th <= height && !ignoreErrors {
		return fmt.Errorf("invalid height %d < current tip %d", height, th)
	}

	// find block at target height
	target, err := c.indexer.BlockByHeight(ctx, height)
	if err != nil {
		return err
	}

	return c.reorganize(ctx, c.builder.parent, target, ignoreErrors, true)
}

func (c *Crawler) reorganize(ctx context.Context, formerBest, newBest *model.Block, ignoreErrors, rollbackOnly bool) error {
	// prepare a list of blocks to reorganize
	forkBlock, detach, attach, err := c.getReorganizeBlocks(ctx, formerBest, newBest, rollbackOnly)
	if err != nil {
		return err
	}

	// empty lists may be result of a node / proxy error, reorganize is called when
	// a broken chain is detected
	if attach.Len() == 0 && detach.Len() == 0 {
		return fmt.Errorf("REORGANIZE empty orphan/main chain: %d/%d", detach.Len(), attach.Len())
	}

	// Ensure the provided blocks match the current best chain
	if detach.Len() != 0 {
		firstDetachBlock := detach.Front().Value.(*model.Block)
		if firstDetachBlock.Hash.String() != formerBest.Hash.String() {
			return fmt.Errorf("REORGANIZE blocks to detach are "+
				"not for the current best chain -- first detach block %s, "+
				"current chain %s", firstDetachBlock.Hash, formerBest.Hash)
		}
	}

	// Ensure the provided blocks are for the same fork point.
	if attach.Len() != 0 && detach.Len() != 0 {
		firstAttachBlock := attach.Front().Value.(*model.Block)
		lastDetachBlock := detach.Back().Value.(*model.Block)
		firstParentHash := firstAttachBlock.TZ.ParentHash()
		lastParentHash := lastDetachBlock.TZ.ParentHash()
		if firstParentHash != lastParentHash {
			return fmt.Errorf("REORGANIZE blocks do not have the "+
				"same fork point -- first attach parent %s, last detach "+
				"parent %s", firstParentHash, lastParentHash)
		}
	}

	// start reorg by flushing all tables
	if err := c.indexer.Flush(ctx); err != nil {
		return fmt.Errorf("flushing tables: %w", err)
	}

	// Disconnect all of the blocks back to the fork point.
	tip := c.Tip()

	log.Infof("REORGANIZE: %d blocks to detach, %d blocks to attach.",
		detach.Len(), attach.Len())

	// detach orphaned blocks from indexes first
	if detach.Len() > 0 {
		// guaranteed not to fail
		e := detach.Front()
		for block, parent := e.Value.(*model.Block), (*model.Block)(nil); block != nil; block = parent {
			// stop after last block has been detached
			if e == nil {
				break
			}

			// peek next block to detach, use fork block when list is empty
			if e = e.Next(); e != nil {
				parent = e.Value.(*model.Block)
			} else {
				parent = forkBlock
			}

			log.Infof("REORGANIZE: detaching block %d %s", block.Height, block.Hash)

			// we need resolved accounts to rebuild the previous balance set state
			// so we keep identity data and rpc bundle and rebuild the current block
			tz, bid, pid := block.TZ, block.RowId, block.ParentId

			// BuildReorg() uses previous parent or fork block as parent data!
			block, err = c.builder.BuildReorg(ctx, tz, parent)
			if err != nil {
				log.Errorf("REORGANIZE: failed resolving account set for block %d %s",
					tz.Height(), tz.Hash())
				return err
			}
			if bid > 0 {
				block.RowId = bid
			}
			if pid > 0 {
				block.ParentId = pid
			}

			// update indexes to rollback block

			// disconnect block from indexes
			log.Infof("REORGANIZE: dropping indexes for block %d %s", block.Height, block.Hash)
			if err = c.indexer.DisconnectBlock(ctx, block, c.builder, ignoreErrors); err != nil {
				return err
			}

			// flush after each detached block to make all delete/update ops durable
			log.Infof("REORGANIZE: flushing databases")
			if err := c.indexer.Flush(ctx); err != nil {
				return fmt.Errorf("REORGANIZE: flushing tables failed for %d: %w", block.Height, err)
			}

			// rollback chain state to parent block
			newTip := &model.ChainTip{
				Name:        tip.Name,
				Symbol:      tip.Symbol,
				ChainId:     tip.ChainId,
				BestHash:    parent.Hash,
				BestId:      parent.RowId,
				BestHeight:  parent.Height,
				BestTime:    parent.Timestamp,
				GenesisTime: tip.GenesisTime,
				NYEveBlocks: tip.NYEveBlocks,
				Deployments: tip.Deployments,
			}

			if err := c.db.Update(func(dbTx store.Tx) error {
				if err := c.indexer.storeTips(dbTx); err != nil {
					return err
				}
				return dbStoreChainTip(dbTx, newTip)
			}); err != nil {
				return fmt.Errorf("REORGANIZE: updating statedb failed for %d: %w", block.Height, err)
			}

			// update chain tip
			c.updateTip(newTip)
			tip = newTip

			// cleanup, do not touch parent because we need it during next iteration
			c.builder.CleanReorg()
		}
		log.Infof("REORGANIZE: rollback to fork point %s (height %d) "+
			"completed successfully.", tip.BestHash, tip.BestHeight)
	}

	// setup builder for attaching
	// on forward reorg use forkblock as initial parent
	// when no block will be attached, this sets the correct parent block as well
	c.builder.Purge()
	if err := c.builder.Init(ctx, tip, c.rpc); err != nil {
		log.Errorf("REORGANIZE: failed builder re-init at fork point %s %d: %v", tip.BestHash, tip.BestHeight, err)
		return err
	}

	for e := attach.Front(); e != nil; e = e.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		// build from RPC block on forward reorg
		block := e.Value.(*model.Block)
		log.Infof("REORGANIZE: attaching block %d %s", block.Height, block.Hash)

		// reuse ids when a block existed before (as orphan side chain from previous reorg)
		tz, bid, pid := block.TZ, block.RowId, block.ParentId

		// perform regular build, will generate a clean block
		block, err := c.builder.Build(ctx, tz)
		if err != nil {
			log.Errorf("REORGANIZE: failed resolving block %d %s: %v", tz.Height(), tz.Hash(), err)
			return err
		}

		// reuse ids to keep existing links
		if bid > 0 {
			block.RowId = bid
		}
		if pid > 0 {
			block.ParentId = pid
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// update indexes; this will also generate a unique block id
		// when the connected block is not yet known
		log.Infof("REORGANIZE: indexing block %d %s", block.Height, block.Hash)
		if err = c.indexer.ConnectBlock(ctx, block, c.builder); err != nil {
			return err
		}

		// foreward chain tip
		newTip := &model.ChainTip{
			Name:        tip.Name,
			Symbol:      tip.Symbol,
			ChainId:     tip.ChainId,
			BestHash:    block.Hash,
			BestId:      block.RowId,
			BestHeight:  block.Height,
			BestTime:    block.Timestamp,
			GenesisTime: tip.GenesisTime,
			NYEveBlocks: tip.NYEveBlocks,
			Deployments: tip.Deployments,
		}

		// update blockchain years
		if newTip.GenesisTime.AddDate(len(newTip.NYEveBlocks)+1, 0, 0).Before(block.Timestamp) {
			newTip.NYEveBlocks = append(newTip.NYEveBlocks, block.Height)
			log.Infof("Happy New Blockchain Year %d at block %d!", len(newTip.NYEveBlocks)+1, block.Height)
		}
		// update deployments on protocol upgrade
		if block.IsProtocolUpgrade() {
			newTip.AddDeployment(block.Params)
		}

		// flush after each attached block to make all insert/update ops durable
		log.Infof("REORGANIZE: flushing database journals")
		if err := c.indexer.FlushJournals(ctx); err != nil {
			return fmt.Errorf("REORGANIZE: flushing table journals failed for %d: %w", block.Height, err)
		}

		err = c.db.Update(func(dbTx store.Tx) error {
			if err := c.indexer.storeTips(dbTx); err != nil {
				return err
			}
			return dbStoreChainTip(dbTx, newTip)
		})
		if err != nil {
			return fmt.Errorf("REORGANIZE: updating block database failed for %d: %w", block.Height, err)
		}

		// update chainstate with new version
		c.updateTip(newTip)
		tip = newTip

		// cleanup and prepare for next block (forward attach keeps parent relation in builder)
		c.builder.Clean()
	}

	// flush again when done
	log.Infof("REORGANIZE: flushing databases")
	if err := c.indexer.Flush(ctx); err != nil {
		return fmt.Errorf("flushing tables: %w", err)
	}

	log.Infof("REORGANIZE: completed successfully at %s (height %d).",
		tip.BestHash, tip.BestHeight)

	return nil
}

// getReorganizeNodes finds the fork point between the main chain and the previous
// tip and returns a list of blocks that would need to be detached from
// the main chain and a list of blocks that would need to be attached to
// the fork point (which will be the end of the main chain after detaching the
// returned list of blocks) in order to reorganize the chain such that the
// passed best block is the new end of the main chain. The lists will be empty if the
// passed tip is not on a side chain.
//
//	   to detach
//	 |-------------|
//	 [ ] - [ ] - [ ] <- tip
//	/
//
// [ ] - [ ] - [ ] - [ ] - [ ] - [ ]  <-best
//
//	      |    |-------------------|
//	fork point        to attach (Note: all but last may exist from previous reorg)
//
// This function MUST be called with the chain state lock held (for reads).
func (c *Crawler) getReorganizeBlocks(ctx context.Context, tip *model.Block, best *model.Block, rollbackOnly bool) (*model.Block, *list.List, *list.List, error) {
	// Nothing to detach or attach if there is no node.
	attachBlocks := list.New()
	detachBlocks := list.New()
	if tip == nil || best == nil {
		log.Warn("REORGANIZE called with nil tip/best block")
		return nil, detachBlocks, attachBlocks, nil
	}

	log.Infof("REORGANIZE: searching fork point side=%s main=%s", tip.Hash, best.Hash)
	// identify fork point
	maxreorg := 100
	sidechain, err := c.rpc.GetTips(ctx, maxreorg, tip.Hash)
	if err != nil || len(sidechain) == 0 {
		return nil, nil, nil, fmt.Errorf("empty tip chain")
	}
	mainchain, err := c.rpc.GetTips(ctx, maxreorg, best.Hash)
	if err != nil || len(mainchain) == 0 {
		return nil, nil, nil, fmt.Errorf("empty main chain")
	}

	var forkDepthSide, forkDepthMain int = -1, -1
findfork:
	for i, side := range sidechain[0] {
		for j, main := range mainchain[0] {
			if side == main {
				// discount the best block (will be appended after reorg finishes
				forkDepthMain = j - 1
				forkDepthSide = i
				break findfork
			}
		}
	}
	if rollbackOnly {
		forkDepthMain = 0
	}

	if forkDepthSide < 0 || forkDepthMain < 0 {
		return nil, nil, nil, fmt.Errorf("cannot find fork point in last %d blocks", maxreorg)
	}

	log.Infof("REORGANIZE: will assemble %d blocks for detach and %d for attach",
		forkDepthSide, forkDepthMain)

	// walk side chain starting at tip and register blocks for detach
	ancestor := tip.Clone()
	for ; forkDepthSide > 0; forkDepthSide-- {
		log.Infof("REORGANIZE: will detach %d, %s", ancestor.Height, ancestor.Hash)

		// make sure rpc info exists
		if err := ancestor.FetchRPC(ctx, c.rpc); err != nil {
			log.Errorf("REORGANIZE refetch block %d: %s", ancestor.Height, err)
			return nil, nil, nil, err
		}

		// keep block for removal
		detachBlocks.PushBack(ancestor)

		// load parent block from db
		parent, err := c.indexer.BlockByID(ctx, ancestor.ParentId)
		if err != nil {
			log.Errorf("REORGANIZE loading sidechain parent id %d for block %d %s: %s",
				ancestor.ParentId, ancestor.Height, ancestor.Hash, err)
			return nil, nil, nil, err
		}

		ancestor = parent
	}

	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	// Now start from the end of the valid main chain and work backwards
	// until the common ancestor adding each block to the list of blocks to attach
	// to the main chain. Some of these blocks may already be in the local block DB
	// from a previous reorg and others may not be in the DB.

	// make sure rpc info exists
	if err := best.FetchRPC(ctx, c.rpc); err != nil {
		log.Errorf("REORGANIZE refetch block %d: %s", best.Height, err)
		return nil, nil, nil, err
	}
	for block := best; forkDepthMain > 0; forkDepthMain-- {
		// try loading parent block from db, but don't fail if it does not exist
		h := block.TZ.ParentHash()
		// log.Debugf("REORGANIZE: looking for parent block %d %s", ancestor.Height-1, h)
		if parent, err := c.indexer.BlockByHash(ctx, h, 0, 0); err != nil {
			// when block is missing from DB, resolve as new block via RPC
			if tz, err := c.fetchBlock(ctx, h); err != nil {
				log.Errorf("REORGANIZE failed fetching main chain block %s: %s", h, err)
				return nil, nil, nil, err
			} else {
				// parent may be unknown, so leave empty and handle this case later
				if block, err = model.NewBlock(tz, nil); err != nil {
					log.Errorf("REORGANIZE failed building main chain block from %s: %s", tz.Hash(), err)
					return nil, nil, nil, err
				}
			}
		} else {
			// block is known, so we only need to resolve the RPC data
			if err := parent.FetchRPC(ctx, c.rpc); err != nil {
				log.Errorf("REORGANIZE failed fetching main chain parent block: %s", err)
				return nil, nil, nil, err
			}
			block = parent
		}

		// keep for attachement in reverse order
		log.Infof("REORGANIZE: will attach %d, %s", block.Height, block.Hash)
		attachBlocks.PushFront(block)
	}

	// make sure rpc info exists for fork block
	if err := ancestor.FetchRPC(ctx, c.rpc); err != nil {
		log.Errorf("REORGANIZE refetch block %d: %s", ancestor.Height, err)
		return nil, nil, nil, err
	}

	// make sure chain state and parent are loaded for fork block
	ancestor.Chain, err = c.indexer.ChainByHeight(ctx, ancestor.Height)
	if err != nil {
		return nil, nil, nil, err
	}
	ancestor.Supply, err = c.indexer.SupplyByHeight(ctx, ancestor.Height)
	if err != nil {
		return nil, nil, nil, err
	}
	ancestor.Parent, err = c.indexer.BlockByID(ctx, ancestor.ParentId)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := ancestor.Parent.FetchRPC(ctx, c.rpc); err != nil {
		return nil, nil, nil, err
	}

	return ancestor, detachBlocks, attachBlocks, nil
}
