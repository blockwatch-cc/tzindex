// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"

	"blockwatch.cc/packdb/cache/lru"
	"github.com/cespare/xxhash"
)

var AccountCacheSizeLog2 = 17 // 128k

type AccountCache struct {
	idmap map[model.AccountID]uint64 // account id -> cache key
	cache *lru.TwoQueueCache         // key := xxhash64(typ:hash)
	size  int64
	stats Stats
}

func NewAccountCache(sz int) *AccountCache {
	c := &AccountCache{
		idmap: make(map[model.AccountID]uint64),
	}
	c.cache, _ = lru.New2QWithEvict(1<<uint(sz), func(_, v interface{}) {
		buf := v.([]byte)
		// first 8 bytes is row id
		delete(c.idmap, model.AccountID(binary.LittleEndian.Uint64(buf)))
		c.size -= int64(len(buf))
		atomic.AddInt64(&c.stats.Evictions, 1)
	})
	return c
}

func (c *AccountCache) AccountHashKey(a *model.Account) uint64 {
	return c.hashKey(a.Address.Type, a.Address.Hash)
}

func (c *AccountCache) AddressHashKey(a tezos.Address) uint64 {
	return c.hashKey(a.Type, a.Hash)
}

var hashKeyPool = &sync.Pool{
	// we always only need 20 + 1 bytes for address hash plus type
	New: func() interface{} { return make([]byte, 0, 21) },
}

func (c *AccountCache) hashKey(typ tezos.AddressType, h []byte) uint64 {
	bufIf := hashKeyPool.Get()
	buf := bufIf.([]byte)[:1+len(h)]
	buf[0] = byte(typ)
	copy(buf[1:], h)
	sum := xxhash.Sum64(buf)
	buf = buf[:0]
	hashKeyPool.Put(bufIf)
	return sum
}

func (c *AccountCache) Add(a *model.Account) {
	// skip bakers
	if a.IsBaker {
		return
	}
	key := c.AccountHashKey(a)
	c.idmap[a.RowId] = key
	buf, _ := a.MarshalBinary()
	updated, _ := c.cache.Add(key, buf)
	if updated {
		// log.Infof("Cache update %s %s revealed=%t", a, a.Key(), a.IsRevealed)
		// can't count the difference in size
		atomic.AddInt64(&c.stats.Updates, 1)
	} else {
		// log.Infof("Cache insert %s %s revealed=%t", a, a.Key(), a.IsRevealed)
		c.size += int64(len(buf))
		atomic.AddInt64(&c.stats.Inserts, 1)
	}
}

func (c *AccountCache) Drop(a *model.Account) {
	// log.Infof("Cache drop %s %s revealed=%t", a, a.Key(), a.IsRevealed)
	c.cache.Remove(c.AccountHashKey(a))
}

func (c *AccountCache) Purge() {
	c.cache.Purge()
	c.size = 0
	c.idmap = make(map[model.AccountID]uint64)
}

func (c *AccountCache) GetAddress(addr tezos.Address) (uint64, *model.Account, bool) {
	key := c.AddressHashKey(addr)
	val, ok := c.cache.Get(key)
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	acc := model.AllocAccount()
	acc.UnmarshalBinary(val.([]byte))
	// cross-check for hash collisions
	if acc.RowId == 0 || !acc.Address.Equal(addr) {
		acc.Free()
		atomic.AddInt64(&c.stats.Misses, 1)
		return key, nil, false
	}
	atomic.AddInt64(&c.stats.Hits, 1)
	// log.Infof("Cache lookup %s %s revealed=%t", acc, acc.Key(), acc.IsRevealed)
	return key, acc, true
}

func (c *AccountCache) GetId(id model.AccountID) (uint64, *model.Account, bool) {
	key, ok := c.idmap[id]
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
	acc.UnmarshalBinary(val.([]byte))
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
	}
	s.Bytes = c.size
	return s
}

func (c *AccountCache) Walk(fn func(key uint64, acc *model.Account) error) error {
	acc := model.AllocAccount()
	for _, key := range c.cache.Keys() {
		val, ok := c.cache.Peek(key)
		if !ok {
			return fmt.Errorf("missing cache key %w", key)
		}
		if err := acc.UnmarshalBinary(val.([]byte)); err != nil {
			return err
		}
		if err := fn(key.(uint64), acc); err != nil {
			return err
		}
	}
	return nil
}
