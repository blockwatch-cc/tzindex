// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

// caches rights for up to 6 cycles (current + future preserved_cycles)
// for fast lookup of next right by account id + current height
// for about 500 active bakers that is
// - 2 * ((64 uint64 per cycle * 6 cycles) + const slice+map overhead 48b) * 500 = 3MB
type RightsCache struct {
	startHeight    int64
	fillHeight     int64
	blocksPerCycle int64
	numCycles      int64

	baking    map[model.AccountID]*vec.BitSet // account id -> bitmap of prio 0 block heights
	endorsing map[model.AccountID]*vec.BitSet // account id -> bitmap of endorse block heights
}

func NewRightsCache(blocksPerCycle, numCycles int64, startHeight int64) *RightsCache {
	cache := &RightsCache{
		startHeight:    startHeight,
		blocksPerCycle: blocksPerCycle,
		numCycles:      numCycles,
		baking:         make(map[model.AccountID]*vec.BitSet),
		endorsing:      make(map[model.AccountID]*vec.BitSet),
	}
	return cache
}

func (c *RightsCache) BitmapSize() int {
	return int(c.numCycles * c.blocksPerCycle)
}

func (c *RightsCache) Start() int64 {
	return c.startHeight
}

func (c *RightsCache) End() int64 {
	return c.fillHeight
}

func (c *RightsCache) Len() int64 {
	return c.fillHeight - c.startHeight
}

func (c *RightsCache) SetBakeBit(id model.AccountID, height int64) {
	bitmap, ok := c.baking[id]
	if !ok {
		bitmap = vec.NewBitSet(c.BitmapSize())
		c.baking[id] = bitmap
	}
	bitmap.Set(int(height - c.startHeight))
	c.fillHeight = util.Max64(c.fillHeight, height)
}

func (c *RightsCache) SetEndorseBit(id model.AccountID, height int64) {
	bitmap, ok := c.endorsing[id]
	if !ok {
		bitmap = vec.NewBitSet(c.BitmapSize())
		c.endorsing[id] = bitmap
	}
	bitmap.Set(int(height - c.startHeight))
	c.fillHeight = util.Max64(c.fillHeight, height)
}

func (c *RightsCache) Lookup(id model.AccountID, height int64) (int64, int64) {
	pos := int(height-c.startHeight) + 1
	if pos < 0 || height > c.fillHeight {
		return 0, 0
	}
	var nextBakeHeight, nextEndorseHeight int64
	if bake, ok := c.baking[id]; ok {
		if next, _ := bake.Run(pos); next >= 0 {
			nextBakeHeight = c.startHeight + int64(next)
		}
	}
	if endorse, ok := c.endorsing[id]; ok {
		if next, _ := endorse.Run(pos); next >= 0 {
			nextEndorseHeight = c.startHeight + int64(next)
		}
	}
	return nextBakeHeight, nextEndorseHeight
}

func (m *Indexer) GetRights(ctx context.Context, height int64) (*RightsCache, error) {
	// lazy-load on first call
	rights := m.rights.Load()
	if rights == nil || rights.(*RightsCache).End() < height {
		// grab lock
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again
		rights = m.rights.Load()
		// build if still not updated by other goroutine
		if rights == nil {
			var err error
			rights, err = m.BuildRights(ctx, height)
			if err != nil {
				return nil, err
			}
			m.rights.Store(rights)
		}
	}
	return rights.(*RightsCache), nil
}

func (m *Indexer) UpdateRights(ctx context.Context, height int64) error {
	rights, err := m.BuildRights(ctx, height)
	if err != nil {
		return err
	}
	m.rights.Store(rights)
	return nil
}

func (m *Indexer) BuildRights(ctx context.Context, height int64) (*RightsCache, error) {
	startTime := time.Now()
	rightsTable, err := m.Table(index.RightsTableKey)
	if err != nil {
		return nil, err
	}
	params := m.ParamsByHeight(height)
	startCycle := params.CycleFromHeight(height)
	next := NewRightsCache(params.BlocksPerCycle, params.PreservedCycles+1, params.CycleStartHeight(startCycle))

	q := pack.Query{
		Name:    "build_rights_cache",
		Fields:  rightsTable.Fields().Select("h", "t", "A", "p"),
		NoCache: true,
		Conditions: pack.ConditionList{pack.Condition{
			Field: rightsTable.Fields().Find("c"), // from cycle
			Mode:  pack.FilterModeGte,
			Value: startCycle,
		}, pack.Condition{
			Field: rightsTable.Fields().Find("p"), // priority for bake & endorse
			Mode:  pack.FilterModeLte,
			Value: int64(31),
		}},
	}
	right := &model.Right{}
	var nBake, nEndorse int
	err = rightsTable.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(right); err != nil {
			return err
		}
		switch right.Type {
		case chain.RightTypeBaking:
			// store only prio zero blocks
			if right.Priority == 0 {
				next.SetBakeBit(right.AccountId, right.Height)
				nBake++
			}
		case chain.RightTypeEndorsing:
			// store all endorsements (cannot skip because we need to store for
			// different validators)
			next.SetEndorseBit(right.AccountId, right.Height)
			nEndorse++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Infof("Rights cache built in %s", time.Since(startTime))

	return next, nil
}
