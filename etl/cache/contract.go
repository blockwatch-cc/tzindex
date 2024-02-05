// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	ContractMaxCacheSize     = 16384 // entries
	ContractTypeMaxCacheSize = 16384 // entries
)

type ContractCache struct {
	cache *lru.TwoQueueCache[model.AccountID, *model.Contract] // key := account_id
	size  int64
	stats Stats
}

func NewContractCache(sz int) *ContractCache {
	if sz <= 0 {
		sz = ContractMaxCacheSize
	}
	c := &ContractCache{}
	c.cache, _ = lru.New2Q[model.AccountID, *model.Contract](sz)
	return c
}

func (c ContractCache) Size() int64 {
	return c.size
}

func (c ContractCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.cache.Len()
	s.Bytes = c.Size()
	return s
}

func (c *ContractCache) Get(id model.AccountID) (*model.Contract, bool) {
	cc, ok := c.cache.Get(id)
	if ok {
		atomic.AddInt64(&c.stats.Hits, 1)
		return cc, ok
	} else {
		atomic.AddInt64(&c.stats.Misses, 1)
		return nil, false
	}
}

func (c *ContractCache) Add(cc *model.Contract) {
	c.cache.Add(cc.AccountId, cc)
}

func (c *ContractCache) Drop(cc *model.Contract) {
	c.cache.Remove(cc.AccountId)
}

func (c *ContractCache) Purge() {
	c.cache.Purge()
	c.size = 0
}

type ContractTypeCache struct {
	cache *lru.TwoQueueCache[uint64, any] // key := account_id
	size  int64
	stats Stats
}

type ContractTypeElem struct {
	ParamType   micheline.Type
	StorageType micheline.Type
	CodeHash    uint64
}

func (e ContractTypeElem) Size() int64 {
	return int64(e.ParamType.Size()+e.StorageType.Size()) + 8
}

func NewContractTypeCache(sz int) *ContractTypeCache {
	if sz <= 0 {
		sz = ContractTypeMaxCacheSize
	}
	c := &ContractTypeCache{}
	c.cache, _ = lru.New2Q[uint64, any](sz)
	return c
}

func (c *ContractTypeCache) Add(cc *model.Contract) *ContractTypeElem {
	elem := &ContractTypeElem{CodeHash: cc.CodeHash}
	elem.ParamType, elem.StorageType, _ = cc.LoadType()
	c.cache.Add(cc.AccountId.U64(), elem)
	return elem
}

func (c *ContractTypeCache) Drop(cc *model.Contract) {
	c.cache.Remove(cc.AccountId.U64())
}

func (c *ContractTypeCache) Purge() {
	c.cache.Purge()
	c.size = 0
}

func (c *ContractTypeCache) Get(id model.AccountID) (*ContractTypeElem, bool) {
	val, ok := c.cache.Get(id.U64())
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.stats.Hits, 1)
	return val.(*ContractTypeElem), true
}

func (c ContractTypeCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.cache.Len()
	s.Bytes = c.size
	return s
}
