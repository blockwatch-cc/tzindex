// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"sync/atomic"

	"blockwatch.cc/tzindex/etl/model"
	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	TicketMaxCacheSize = 16384 // entries
)

type TicketCache struct {
	cache *lru.TwoQueueCache[model.TicketID, *model.Ticket] // key := TicketID
	size  int
	stats Stats
}

func NewTicketCache(sz int) *TicketCache {
	if sz <= 0 {
		sz = TicketMaxCacheSize
	}
	c := &TicketCache{}
	c.cache, _ = lru.New2Q[model.TicketID, *model.Ticket](sz)
	return c
}

func (c *TicketCache) Add(t *model.Ticket) {
	c.cache.Add(t.Id, t)
}

func (c *TicketCache) Drop(t *model.Ticket) {
	c.cache.Remove(t.Id)
}

func (c *TicketCache) Purge() {
	c.cache.Purge()
	c.size = 0
}

func (c *TicketCache) Get(id model.TicketID) (*model.Ticket, bool) {
	val, ok := c.cache.Get(id)
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.stats.Hits, 1)
	return val, true
}

func (c TicketCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.cache.Len()
	s.Bytes = int64(c.size)
	return s
}
