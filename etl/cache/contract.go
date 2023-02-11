// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
    "sync/atomic"

    "blockwatch.cc/packdb/cache/lru"

    "blockwatch.cc/tzgo/micheline"
    "blockwatch.cc/tzindex/etl/model"
)

var (
    ContractTypeMaxCacheSize = 16384 // entries
)

type ContractTypeCache struct {
    cache *lru.TwoQueueCache // key := account_id
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
    c.cache, _ = lru.New2QWithEvict(sz, func(_, v interface{}) {
        c.size -= v.(*ContractTypeElem).Size()
        atomic.AddInt64(&c.stats.Evictions, 1)
    })
    return c
}

func (c *ContractTypeCache) Add(cc *model.Contract) *ContractTypeElem {
    elem := &ContractTypeElem{CodeHash: cc.CodeHash}
    elem.ParamType, elem.StorageType, _ = cc.LoadType()
    updated, _ := c.cache.Add(cc.AccountId, elem)
    if updated {
        atomic.AddInt64(&c.stats.Updates, 1)
    } else {
        c.size += elem.Size()
        atomic.AddInt64(&c.stats.Inserts, 1)
    }
    return elem
}

func (c *ContractTypeCache) Drop(cc *model.Contract) {
    c.cache.Remove(cc.AccountId)
}

func (c *ContractTypeCache) Purge() {
    c.cache.Purge()
    c.size = 0
}

func (c *ContractTypeCache) Get(id model.AccountID) (*ContractTypeElem, bool) {
    val, ok := c.cache.Get(id)
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
