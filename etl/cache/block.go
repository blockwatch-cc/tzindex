// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"context"
	"sort"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

// NOTE: simple read-mostly cache for timestamps and block hashes
//
// Timestamps
// - 4-8 MB (2021)
// - block height ->> unix seconds (uint32)
// - first timestamp is actual time in unix secs, remainder are offsets in seconds
// - each entry uses 4 bytes per block, safe until June 2017 + 66 years;
//   by then Tezos may have reached 34M blocks and the cache is 132MB in size
//
// Block hashes
// - 32 byte per hash, growth rate is 16MB per year
//
type BlockCache struct {
	times  []uint32 // all block timestamps
	hashes []byte   // all block hashes
	stats  Stats
}

const defaultBlockCacheSize = 1 << 21 // 4M blocks = 150MB

var (
	blockHashLen = tezos.HashTypeBlock.Len()
)

func NewBlockCache(size int) *BlockCache {
	if size < defaultBlockCacheSize {
		size = defaultBlockCacheSize
	}
	size = roundUpPow2(size, 1<<defaultBucketSizeLog2)
	return &BlockCache{
		times:  make([]uint32, 0, size),
		hashes: make([]byte, 0, blockHashLen*size),
	}
}

func (c BlockCache) Cap() int {
	return cap(c.times)
}

func (c BlockCache) Len() int {
	return len(c.times)
}

func (c BlockCache) Size() int {
	return len(c.times) * (4 + blockHashLen)
}

func (c BlockCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.Len()
	s.Bytes = int64(c.Size())
	return s
}

func (c *BlockCache) GetHash(height int64) tezos.BlockHash {
	if c.Len() > int(height) {
		offs := int(height) * blockHashLen
		c.stats.CountHits(1)
		return tezos.NewBlockHash(c.hashes[offs : offs+blockHashLen])
	}
	c.stats.CountMisses(1)
	return tezos.BlockHash{}
}

func (c *BlockCache) GetTime(height int64) time.Time {
	l := c.Len()
	h := int(height)
	if l <= h {
		c.stats.CountMisses(1)
		return time.Time{} // future block
	}
	c.stats.CountHits(1)
	if h <= 0 {
		return time.Unix(int64(c.times[0]), 0).UTC() // genesis
	}
	return time.Unix(int64(c.times[h])+int64(c.times[0]), 0).UTC()
}

func (c *BlockCache) GetHeight(tm time.Time) int64 {
	l := c.Len()
	if l == 0 || !tm.After(time.Unix(int64(c.times[0]), 0)) {
		c.stats.CountMisses(1)
		return 0
	}
	c.stats.CountHits(1)
	tsdiff := uint32(tm.Unix() - int64(c.times[0]))
	i := sort.Search(l, func(i int) bool { return c.times[i] >= tsdiff })
	if i == l {
		return int64(l - 1)
	}
	if c.times[i] == tsdiff {
		return int64(i)
	}
	return int64(i - 1)
}

func (c *BlockCache) Build(ctx context.Context, table *pack.Table) error {
	c.times = c.times[:0]
	c.hashes = c.hashes[:0]
	type XBlock struct {
		Timestamp time.Time `pack:"T"`
		Hash      []byte    `pack:"H"`
	}
	c.stats.CountUpdates(1)
	b := XBlock{}
	return pack.NewQuery("init_cache").
		WithTable(table).
		WithoutCache().
		WithFields("time", "hash").
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(&b); err != nil {
				return err
			}
			if len(c.times) == 0 {
				// safe, it's before 2036
				c.times = append(c.times, uint32(b.Timestamp.Unix()))
			} else {
				// make sure there's no rounding error
				tsdiff := b.Timestamp.Unix() - int64(c.times[0])
				c.times = append(c.times, uint32(tsdiff))
			}
			c.hashes = append(c.hashes, b.Hash...)
			return nil
		})
}

// only called from single thread in crawler, no locking required
func (c *BlockCache) Update(block *model.Block) error {
	if block == nil {
		return nil
	}

	// extend or shrink (after reorg) slices and patch time-diff into position
	tsdiff := uint32(block.Timestamp.Unix() - int64(c.times[0]))
	if len(c.times) > int(block.Height) {
		// overwrite time slice element
		c.times[int(block.Height)] = tsdiff
		c.times = c.times[:int(block.Height+1)]

		// overwrite block hashes element
		copy(c.hashes[int(block.Height)*blockHashLen:], block.Hash.Hash.Hash)
		c.hashes = c.hashes[:int(block.Height)*blockHashLen+blockHashLen]
	} else {
		// creates an implicit copy when capacity is reached
		newTimes := append(c.times, tsdiff)
		newHashes := append(c.hashes, block.Hash.Hash.Hash...)

		// lock-free overwrite (this is safe since we have a single writer only)
		c.times = newTimes
		c.hashes = newHashes
	}
	c.stats.CountUpdates(1)
	return nil
}
