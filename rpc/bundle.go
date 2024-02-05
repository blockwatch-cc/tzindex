// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
)

type Bundle struct {
	Block         *Block
	Params        *Params
	Baking        [][]BakingRight    // all blocks from one or more cycle
	Endorsing     [][]EndorsingRight // all blocks from one or more cycles
	PrevEndorsing []EndorsingRight   // last block from previous cycle
	Snapshot      *SnapshotIndex
	SnapInfo      *SnapshotInfo
	Issuance      []Issuance // future cycles where rights exist
}

func (b Bundle) IsValid() bool {
	return b.Block != nil && b.Params != nil
}

func (b Bundle) IsFull() bool {
	return b.Snapshot != nil && b.SnapInfo != nil
}

func (b Bundle) Height() int64 {
	return b.Block.GetLevel()
}

func (b Bundle) Cycle() int64 {
	return b.Block.GetCycle()
}

func (b Bundle) Hash() (h tezos.BlockHash) {
	h = b.Block.Hash
	return
}

func (b Bundle) ParentHash() (h tezos.BlockHash) {
	h = b.Block.Header.Predecessor
	return
}

func (b Bundle) Protocol() (h tezos.ProtocolHash) {
	h = b.Block.Protocol
	return
}

func (b Bundle) IsCycleStart() bool {
	return b.Height() > 0 && b.GetCyclePosition() == 0
}

func (b Bundle) IsCycleEnd() bool {
	// FIX granada early start
	if b.Height() == 1589248 && b.Params.IsMainnet() {
		return true
	}
	return b.GetCyclePosition()+1 == b.Params.BlocksPerCycle
}

func (b Bundle) GetCycleStart() int64 {
	return b.Block.GetLevel() - b.GetCyclePosition()
}

func (b Bundle) GetCyclePosition() int64 {
	return b.Block.GetLevelInfo().CyclePosition
}

func (b Bundle) IsSeedRequired() bool {
	return b.Block.GetLevelInfo().ExpectedCommitment
}

func (b Bundle) IsVoteStart() bool {
	// Bugs and Quirks
	//
	// 1/ Edo v008 added a 5th vote period and decreased period from 32,768
	// blocks to 20,480. They have also introduced a bug into vote block
	// alignment during the switch from Delphi to Edo (Granada is supposed
	// to fix this). We use an extra offset for tracking this bug. Note
	// that it only starts to appear from cycle end of the first Edo vote
	// epoch.
	//
	// 2/ In an attempt to fix vote/cycle alignment Granada offsets
	// voting period by +1 on activation which apparently fails on
	// Granadanet or any other network that is not mainnet because the
	// problem did not exist there. Anyways, vote start and cycle start
	// should be the same again, but there is one problem:
	//
	// The last Florence vote epoch ends on 1,589,247 (due to the Edo bug),
	// one block short of cycle end like all epochs since Edo started.
	// Since Granada will activate at block 1,589,248 == at cycle end
	// of the last Florence cycle, this block will also become the first
	// voting block in Granada. A great start for re-alignement, isn't it!
	// The sane choice would have been to skip one block and let vote start
	// at cycle start 1,589,249. However, in tyical Tezos manner, things
	// have to be more complicated and instead we make the first voting epoch
	// in Granada 1 block longer (on top of that all RPC voting counters
	// are broken).
	//
	// See
	// https://tezos.gitlab.io/protocols/010_granada.html#bogus-rpc-results
	//
	// Block      Proto     Cycle Start   Cycle End   Vote Start   Vote End
	// --------------------------------------------------------------------
	// 1,343,488  Delphi                     x             x !!       x (both end & start)
	// 1,343,489  Edo           x                          - !!
	// ...
	// 1,466,367  Edo                                                 x
	// 1,466,368  Florence                   x             x
	// ...
	// 1,589,247  Florence                                            x
	// 1,589,248  Granada                    x                        x (ends twice)
	// 1,589,249  Granada       x                          x
	//
	switch b.Height() {
	case 0, 1: // block 0 and 1 have no level or vote info
		return false
	case 2: // block 2 starts the vote
		return true
	case 1343489, 1589248: // Edo bug
		return b.Params.IsMainnet()
	case 1589249: // Edo bug in Granada
		return !b.Params.IsMainnet()
	}
	return b.Block.GetVotingInfo().Position == 0
}

func (b Bundle) IsVoteEnd() bool {
	switch b.Height() {
	case 0, 1: // block 0 and 1 have no level or vote info
		return false
	case 1589248: // fix Granada double close bug
		if b.Params.IsMainnet() {
			return false
		}
	}
	return b.Block.GetVotingInfo().Remaining == 0
}

func (b Bundle) IsSnapshotBlock() bool {
	return (b.GetCyclePosition()+1)%b.Params.BlocksPerSnapshot == 0
}

func (b Bundle) GetSnapshotIndex() int {
	// FIX granada early start
	if b.Height() == 1589248 && b.Params.IsMainnet() {
		return 15
	}
	return int((b.GetCyclePosition()+1)/b.Params.BlocksPerSnapshot) - 1
}

func (c *Client) GetLightBundle(ctx context.Context, id BlockID, p *Params) (b *Bundle, err error) {
	b = &Bundle{}
	if b.Block, err = c.GetBlock(ctx, id); err != nil {
		return
	}
	if err = b.Block.UpdateAllOriginatedScripts(ctx, c); err != nil {
		return
	}
	if b.Height() > 0 && !b.Protocol().IsValid() {
		return nil, fmt.Errorf("fetch: empty metadata in RPC response (maybe you are not using an archive node)")
	}
	b.Params, err = c.fetchParamsForBlock(ctx, b.Block, p)
	if err != nil {
		return
	}
	return
}

func (c *Client) GetFullBundle(ctx context.Context, id BlockID, p *Params) (b *Bundle, err error) {
	b, err = c.GetLightBundle(ctx, id, p)
	if err != nil {
		return
	}

	height := b.Height()
	switch height {
	case 0:
		// skip on genesis
		return
	case 1:
		// on first block after genesis, fetch rights for first 6 cycles [0..5]
		// cycle 6 bootstrap rights are then processed at start of cycle 1
		// cycle 7+ rights from snapshots are then processed at start of cycle 2
		log.Infof("Fetching bootstrap rights for %d(+1) preserved cycles", b.Params.PreservedCycles)
		for cycle := int64(0); cycle <= b.Params.PreservedCycles; cycle++ {
			// fetch using current height (context stores from [n-5, n+5])
			if err = c.FetchRightsByCycle(ctx, height, cycle, b); err != nil {
				err = fmt.Errorf("fetch: rights for cycle %d: %v", cycle, err)
				return
			}
			b.PrevEndorsing = nil

			// fetch issuance params
			if err = c.FetchIssuanceByCycle(ctx, height, cycle, b); err != nil {
				err = fmt.Errorf("fetch: issuance for cycle %d: %v", cycle, err)
				return
			}
		}
	default:
		// Fetch snapshot index and rights at the start of a cycle
		if !b.IsCycleStart() {
			return
		}

		// snapshot index and rights for future cycle N; the snapshot index
		// refers to a snapshot block taken in cycle N-7 (6 post-Ithaca) and
		// randomness collected from seed_nonce_revelations during cycle N-6
		// (5 post-Ithaca); N is the farthest future cycle that exists.
		cycle := b.Cycle() + b.Params.PreservedCycles
		if err = c.FetchRightsByCycle(ctx, height, cycle, b); err != nil {
			err = fmt.Errorf("fetch: rights for cycle %d: %v", cycle, err)
			return
		}
		// fetch issuance params
		if err = c.FetchIssuanceByCycle(ctx, height, cycle, b); err != nil {
			err = fmt.Errorf("fetch: issuance for cycle %d: %v", cycle, err)
			return
		}
	}
	return
}

func (c *Client) fetchParamsForBlock(ctx context.Context, block *Block, p *Params) (*Params, error) {
	height, cycle := block.GetLevel(), block.GetCycle()
	isCycleStart := block.GetLevelInfo().CyclePosition == 0
	isProtoUpdate := p == nil || !p.Protocol.Equal(block.Protocol)
	if !isCycleStart && !isProtoUpdate {
		return p, nil
	}
	log.Debugf("Need new params for block %d", height)

	// fetch params from chain
	next := NewParams()
	if height > 0 {
		cons, err := c.GetConstants(ctx, BlockLevel(height))
		if err != nil {
			return nil, fmt.Errorf("fetch: %v", err)
		}
		next = cons.Params()
	}

	next = next.
		WithChainId(block.ChainId).
		WithProtocol(block.Metadata.Protocol).
		WithDeployment(block.Header.Proto)

	// v18 has adaptive issuance
	if next.Version >= 18 {
		if issue, _ := c.GetIssuance(ctx, BlockLevel(height)); len(issue) > 0 {
			next.WithIssuance(issue[0])
		}
	}

	// adjust deployment number for genesis & bootstrap blocks
	if height <= 1 {
		next.Deployment--
	}

	// copy tracking info from previous params or block when protocol changed
	// calculate start offset for correct cycle start block calculations
	// (some protocols start at different points) so that
	//
	//   cycle start = proto start block - start offset
	//
	if p == nil || p.Version < next.Version {
		next.StartHeight = height
		next.StartCycle = cycle
		next.StartOffset = block.GetLevelInfo().CyclePosition
		switch next.Version {
		// v009 and v010 start one block too early due to a bug in v008
		case 9:
			next.StartOffset = 4095 // will add +1 to protocol start level
		case 10:
			next.StartOffset = -1 // shift into correct cycle
			next.StartCycle++
		}
	} else {
		next.StartHeight = p.StartHeight
		next.StartCycle = p.StartCycle
		next.StartOffset = p.StartOffset
	}
	return next, nil
}
