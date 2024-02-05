// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"context"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/etl/model"
)

// caches rights for up to 6 cycles (current + future preserved_cycles)
// for fast lookup of next right by account id + current height
// for about 500 active bakers that is
// - 2 * ((1024 byte per cycle * 6 cycles) + const slice+map overhead 48b) * 500 = 6MB
type RightsCache struct {
	startHeight    int64
	startCycle     int64
	fillHeight     int64
	blocksPerCycle int64
	numCycles      int64
	stats          Stats

	baking    map[model.AccountID]*vec.BitSet // account id -> bitmap of prio 0 block heights
	endorsing map[model.AccountID]*vec.BitSet // account id -> bitmap of endorse block heights
}

func NewRightsCache(blocksPerCycle, numCycles int64, startHeight, startCycle int64) *RightsCache {
	cache := &RightsCache{
		startHeight:    startHeight,
		startCycle:     startCycle,
		fillHeight:     startHeight + numCycles*blocksPerCycle - 1,
		blocksPerCycle: blocksPerCycle,
		numCycles:      numCycles,
		baking:         make(map[model.AccountID]*vec.BitSet),
		endorsing:      make(map[model.AccountID]*vec.BitSet),
	}
	return cache
}

func (c RightsCache) Cap() int {
	return c.BitmapSize() / 8
}

func (c RightsCache) Size() int {
	return (len(c.baking) + len(c.endorsing)) * c.BitmapSize() / 8
}

func (c RightsCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = int(c.Len())
	s.Bytes = int64(c.Size())
	return s
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
	return c.fillHeight - c.startHeight + 1
}

func (c *RightsCache) Add(r *model.Right) {
	// baking
	bitmap, ok := c.baking[r.AccountId]
	if !ok {
		bitmap = vec.NewBitSet(c.BitmapSize())
		c.baking[r.AccountId] = bitmap
	}
	ins := int((r.Cycle - c.startCycle) * c.blocksPerCycle)
	bitmap.Replace(&r.Bake, 0, int(c.blocksPerCycle), ins)

	// endorsing
	bitmap, ok = c.endorsing[r.AccountId]
	if !ok {
		bitmap = vec.NewBitSet(c.BitmapSize())
		c.endorsing[r.AccountId] = bitmap
	}
	bitmap.Replace(&r.Endorse, 0, int(c.blocksPerCycle), ins)
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
			c.stats.CountHits(1)
		}
	}
	if endorse, ok := c.endorsing[id]; ok {
		if next, _ := endorse.Run(pos); next >= 0 {
			nextEndorseHeight = c.startHeight + int64(next)
			c.stats.CountHits(1)
		}
	}
	return nextBakeHeight, nextEndorseHeight
}

func (c *RightsCache) Build(ctx context.Context, startCycle int64, table *pack.Table) error {
	c.stats.CountUpdates(1)
	right := &model.Right{}
	return pack.NewQuery("cache.init").
		WithTable(table).
		WithoutCache().
		WithFields("account_id", "cycle", "baking_rights", "endorsing_rights").
		AndGte("cycle", startCycle). // from cycle
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(right); err != nil {
				return err
			}
			c.Add(right)
			return nil
		})
}
