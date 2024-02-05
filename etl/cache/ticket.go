// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"sync/atomic"

	"blockwatch.cc/tzindex/etl/model"
	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	TicketTypeMaxCacheSize = 16384 // entries
)

type TicketTypeCache struct {
	cache *lru.TwoQueueCache[model.TicketID, *model.TicketType] // key := TicketTypeID
	size  int
	stats Stats
}

func NewTicketTypeCache(sz int) *TicketTypeCache {
	if sz <= 0 {
		sz = TicketTypeMaxCacheSize
	}
	c := &TicketTypeCache{}
	c.cache, _ = lru.New2Q[model.TicketID, *model.TicketType](sz)
	return c
}

func (c *TicketTypeCache) Add(t *model.TicketType) {
	c.cache.Add(t.Id, t)
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
	return val, true
}

func (c TicketTypeCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.cache.Len()
	s.Bytes = int64(c.size)
	return s
}
