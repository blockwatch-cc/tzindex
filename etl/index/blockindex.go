// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/etl/model"
)

const (
	BlockPackSizeLog2    = 15 // 32k packs
	BlockJournalSizeLog2 = 16 // 64k - search only on rollback and lookup
	BlockCacheSize       = 2
	BlockFillLevel       = 100
	BlockIndexKey        = "block"
	BlockTableKey        = "block"
)

var (
	// ErrNoBlockEntry is an error that indicates a requested entry does
	// not exist in the block bucket.
	ErrNoBlockEntry = errors.New("block not indexed")

	// ErrInvalidBlockHeight
	ErrInvalidBlockHeight = errors.New("invalid block height")

	// ErrInvalidBlockHash
	ErrInvalidBlockHash = errors.New("invalid block hash")
)

type BlockIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
}

var _ model.BlockIndexer = (*BlockIndex)(nil)

func NewBlockIndex(opts pack.Options) *BlockIndex {
	return &BlockIndex{opts: opts}
}

func (idx *BlockIndex) DB() *pack.DB {
	return idx.db
}

func (idx *BlockIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *BlockIndex) Key() string {
	return BlockIndexKey
}

func (idx *BlockIndex) Name() string {
	return BlockIndexKey + " index"
}

func (idx *BlockIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(model.Block{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		BlockTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BlockPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BlockJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BlockCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, BlockFillLevel),
		})
	if err != nil {
		return err
	}
	return nil
}

func (idx *BlockIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		BlockTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BlockJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BlockCacheSize),
		},
	)
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *BlockIndex) FinalizeSync(ctx context.Context) error {
	return nil
}

func (idx *BlockIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %s", idx.Name(), err)
		}
		idx.table = nil
	}
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *BlockIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
	// update parent block to write blocks endorsed bitmap
	if block.Parent != nil && block.Parent.RowId > 0 {
		if err := idx.table.Update(ctx, []pack.Item{block.Parent}); err != nil {
			return fmt.Errorf("parent update: %w", err)
		}
	}

	// fetch and update snapshot block
	if snap := block.TZ.Snapshot; snap != nil {
		// protocol upgrades happen 1 block before cycle end. when changes to
		// cycle lenth happen, we miss snapshot 15
		p := block.Params
		if block.Parent != nil {
			p = block.Parent.Params
		}
		snapHeight := p.SnapshotBlock(snap.Cycle, snap.Index)
		log.Debugf("Marking block %d [%d] index %d as roll snapshot for cycle %d",
			snapHeight, block.Params.CycleFromHeight(snapHeight), snap.Index, snap.Cycle)
		snapBlock := &model.Block{}
		err := pack.NewQuery("block_height.search", idx.table).
			WithoutCache().
			WithLimit(1).
			AndEqual("height", snapHeight).
			Execute(ctx, snapBlock)
		if err != nil {
			return fmt.Errorf("snapshot index block %d for cycle %d index %d: %w", snapHeight, snap.Cycle, snap.Index, err)
		}
		if snapBlock.RowId == 0 {
			return fmt.Errorf("missing snapshot index block %d for cycle %d index %d", snapHeight, snap.Cycle, snap.Index)
		}
		snapBlock.IsCycleSnapshot = true
		if err := idx.table.Update(ctx, []pack.Item{snapBlock}); err != nil {
			return fmt.Errorf("snapshot index block %d: %w", snapHeight, err)
		}
	}

	// Note: during reorg some blocks may already exist (have a valid row id)
	// we assume insert will update such rows instead of creating new rows
	return idx.table.Insert(ctx, []pack.Item{block})
}

func (idx *BlockIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	// parent update will be done on next connect
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BlockIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting block at height %d", height)
	_, err := pack.NewQuery("etl.block.delete", idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *BlockIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting block cycle %d", cycle)
	_, err := pack.NewQuery("etl.block.delete", idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *BlockIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
