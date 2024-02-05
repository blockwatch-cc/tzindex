// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"bytes"
	"context"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

// a cache of on-chain addresses id->hash
type AddressCache struct {
	hashes []byte
	stats  Stats
}

const (
	defaultAddrCacheSize = 1 << 22 // 4M addresses = 88MB
	addrLen              = 21      // efficient binary encoding for addresses plus type
)

func NewAddressCache(size int) *AddressCache {
	if size < defaultAddrCacheSize {
		size = defaultAddrCacheSize
	}
	size = roundUpPow2(size, 1<<defaultBucketSizeLog2)
	return &AddressCache{
		hashes: make([]byte, 0, addrLen*size),
	}
}

func (c AddressCache) Cap() int {
	return cap(c.hashes) / addrLen
}

func (c AddressCache) Len() int {
	return len(c.hashes) / addrLen
}

func (c AddressCache) Size() int {
	return len(c.hashes)
}

func (c AddressCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.Len()
	s.Bytes = int64(c.Size())
	return s
}

func (c *AddressCache) GetAddress(id model.AccountID) tezos.Address {
	offs := int(id.U64()-1) * addrLen
	if len(c.hashes) > offs {
		c.stats.CountHits(1)
		return tezos.Address(c.hashes[offs:]).Clone()
	}
	c.stats.CountMisses(1)
	return tezos.InvalidAddress
}

func (c *AddressCache) Build(ctx context.Context, table *pack.Table) error {
	c.hashes = c.hashes[:0]
	type XAccount struct {
		RowId   model.AccountID `pack:"I"`
		Address tezos.Address   `pack:"H"`
	}
	a := XAccount{}
	c.stats.CountUpdates(1)
	return pack.NewQuery("cache.init").
		WithTable(table).
		WithoutCache().
		WithFields("row_id", "address").
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(&a); err != nil {
				return err
			}
			diff := int64(a.RowId.U64()) - int64(c.Len())
			if diff <= 0 {
				log.Warnf("AddressCache: Skipping %d duplicates/out-of-order ids detected before id %d", -(diff - 1), a.RowId)
				return nil
			} else if diff > 1 {
				log.Warnf("AddressCache: gap size %d before id %d", diff, a.RowId)
				// pad with empty bytes when we detect a gap in account ids
				c.hashes = append(c.hashes, bytes.Repeat([]byte{0}, int(diff-1)*addrLen)...)
			}
			c.hashes = append(c.hashes, a.Address[:]...)
			return nil
		})
}

// only called from single thread in crawler, no locking required
func (c *AddressCache) Update(accounts map[model.AccountID]*model.Account) error {
	if len(accounts) == 0 {
		return nil
	}

	// collect all NEW and UPDATED addresses from this block
	type XAccount struct {
		RowId   model.AccountID
		Address tezos.Address
	}
	ins := make([]XAccount, 0)
	upd := make([]XAccount, 0)
	for _, v := range accounts {
		// insert address when new
		if v.IsNew {
			ins = append(ins, XAccount{RowId: v.RowId, Address: v.Address})
			continue
		}
		// replace address when account was just activated
		if v.IsActivated && v.FirstSeen == v.LastSeen {
			upd = append(upd, XAccount{RowId: v.RowId, Address: v.Address})
		}
	}

	// update
	for _, v := range upd {
		pos := int(v.RowId.U64()-1) * addrLen
		if pos > len(c.hashes) {
			// safety skip, should not happen
			continue
		}
		copy(c.hashes[pos:], v.Address[:])
	}

	if len(ins) == 0 {
		return nil
	}

	// insert sorted
	sort.Slice(ins, func(i, j int) bool { return ins[i].RowId < ins[j].RowId })

	// copy hashes if capacity is insufficient
	dest := c.hashes
	if c.Cap()-c.Len() < len(ins) {
		size := roundUpPow2(c.Len()+len(ins), 1<<defaultBucketSizeLog2)
		dest = make([]byte, len(c.hashes), addrLen*size)
		copy(dest, c.hashes)
	}

	// append new address data
	for _, v := range ins {
		// pad with empty bytes when we detect a gap in account ids
		if pad := int64(v.RowId.U64()) - int64(len(dest)/addrLen) - 1; pad > 0 {
			dest = append(dest, bytes.Repeat([]byte{0}, int(pad)*addrLen)...)
		}
		dest = append(dest, v.Address[:]...)
	}

	// replace after append, grow (safe for concurrent use due to single writer)
	c.hashes = dest
	c.stats.CountInserts(int64(len(ins)))
	return nil
}
