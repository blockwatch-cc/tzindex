// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
    "sync/atomic"

    "blockwatch.cc/packdb/cache/lru"
    "blockwatch.cc/tzindex/etl/model"
)

const (
    TicketTypeMaxCacheSize = 16384 // entries
)

type TicketTypeCache struct {
    cache *lru.TwoQueueCache // key := TicketTypeID
    size  int
    stats Stats
}

func NewTicketTypeCache(sz int) *TicketTypeCache {
    if sz <= 0 {
        sz = TicketTypeMaxCacheSize
    }
    c := &TicketTypeCache{}
    c.cache, _ = lru.New2QWithEvict(sz, func(_, v interface{}) {
        c.size -= v.(*model.TicketType).Size()
        atomic.AddInt64(&c.stats.Evictions, 1)
    })
    return c
}

func (c *TicketTypeCache) Add(t *model.TicketType) {
    updated, _ := c.cache.Add(t.Id, t)
    if updated {
        atomic.AddInt64(&c.stats.Updates, 1)
    } else {
        c.size += t.Size()
        atomic.AddInt64(&c.stats.Inserts, 1)
    }
}

func (c *TicketTypeCache) Drop(t *model.TicketType) {
    c.cache.Remove(t.Id)
}

func (c *TicketTypeCache) Purge() {
    c.cache.Purge()
    c.size = 0
}

func (c *TicketTypeCache) Get(id model.TicketID) (*model.TicketType, bool) {
    val, ok := c.cache.Get(id)
    if !ok {
        atomic.AddInt64(&c.stats.Misses, 1)
        return nil, false
    }
    atomic.AddInt64(&c.stats.Hits, 1)
    return val.(*model.TicketType), true
}

func (c TicketTypeCache) Stats() Stats {
    s := c.stats.Get()
    s.Size = c.cache.Len()
    s.Bytes = int64(c.size)
    return s
}
