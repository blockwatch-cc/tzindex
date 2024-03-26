// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"fmt"
	"sync/atomic"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"github.com/cespare/xxhash/v2"
	lru "github.com/hashicorp/golang-lru/v2"
)

var AccountCacheSizeMaxSize = 16384 // entries

type AccountCache struct {
	idmap map[uint32]uint64               // short account id -> cache key (switch to uint64 when > 4Bn accounts)
	cache *lru.TwoQueueCache[uint64, any] // key := xxhash64(typ:hash)
	size  int64
	stats Stats
}

func NewAccountCache(sz int) *AccountCache {
	if sz <= 0 {
		sz = AccountCacheSizeMaxSize
	}
	c := &AccountCache{
		idmap: make(map[uint32]uint64),
	}
	c.cache, _ = lru.New2Q[uint64, any](sz)
	return c
}

func (c *AccountCache) AccountHashKey(a *model.Account) uint64 {
	return c.hashKey(a.Address)
}

func (c *AccountCache) AddressHashKey(a tezos.Address) uint64 {
	return c.hashKey(a)
}

func (c *AccountCache) hashKey(a tezos.Address) uint64 {
	return xxhash.Sum64(a[:])
}

func (c *AccountCache) Add(a *model.Account) {
	// skip bakers
	if a.IsBaker {
		return
	}
	key := c.AccountHashKey(a)
	c.idmap[a.RowId.U32()] = key
	buf, _ := a.MarshalBinary()
	c.cache.Add(key, buf)
}

func (c *AccountCache) Drop(a *model.Account) {
	// log.Infof("Cache drop %s %s revealed=%t", a, a.Key(), a.IsRevealed)
	c.cache.Remove(c.AccountHashKey(a))
}

func (c *AccountCache) Purge() {
	c.cache.Purge()
	c.size = int64(len(c.idmap) * 4)
	for n := range c.idmap {
		delete(c.idmap, n)
	}
}

func (c *AccountCache) GetAddress(addr tezos.Address) (uint64, *model.Account, bool) {
	key := c.AddressHashKey(addr)
	val, ok := c.cache.Get(key)
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	acc := model.AllocAccount()
	_ = acc.UnmarshalBinary(val.([]byte))
	// cross-check for hash collisions
	if acc.RowId == 0 || acc.Address != addr {
		acc.Free()
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	atomic.AddInt64(&c.stats.Hits, 1)
	// log.Infof("Cache lookup %s %s revealed=%t", acc, acc.Key(), acc.IsRevealed)
	return key, acc, true
}

func (c *AccountCache) GetId(id model.AccountID) (uint64, *model.Account, bool) {
	key, ok := c.idmap[id.U32()]
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	val, ok := c.cache.Get(key)
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	acc := model.AllocAccount()
	_ = acc.UnmarshalBinary(val.([]byte))
	// cross-check for hash collisions
	if acc.RowId != id {
		acc.Free()
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	atomic.AddInt64(&c.stats.Hits, 1)
	return key, acc, true
}

func (c AccountCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.cache.Len()
	// correct size
	if c.size < 0 {
		c.size = 0
		for _, key := range c.cache.Keys() {
			val, ok := c.cache.Peek(key)
			if !ok {
				continue
			}
			c.size += int64(len(val.([]byte)))
		}
		c.size += int64(len(c.idmap) * 4)
	}
	s.Bytes = c.size
	return s
}

func (c *AccountCache) Walk(fn func(key uint64, acc *model.Account) error) error {
	acc := model.AllocAccount()
	for _, key := range c.cache.Keys() {
		val, ok := c.cache.Peek(key)
		if !ok {
			return fmt.Errorf("missing cache key %v", key)
		}
		if err := acc.UnmarshalBinary(val.([]byte)); err != nil {
			return err
		}
		if err := fn(key, acc); err != nil {
			return err
		}
	}
	return nil
}
