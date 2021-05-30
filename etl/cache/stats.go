// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"sync/atomic"
)

type Stats struct {
	Size      int   `json:"size"`
	Bytes     int64 `json:"bytes"`
	Inserts   int64 `json:"inserts"`
	Updates   int64 `json:"updates"`
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
}

func (s Stats) Get() Stats {
	return Stats{
		Size:      s.Size,
		Bytes:     s.Bytes,
		Inserts:   atomic.LoadInt64(&s.Inserts),
		Updates:   atomic.LoadInt64(&s.Updates),
		Hits:      atomic.LoadInt64(&s.Hits),
		Misses:    atomic.LoadInt64(&s.Misses),
		Evictions: atomic.LoadInt64(&s.Evictions),
	}
}

func (s *Stats) CountInserts(n int64) {
	atomic.AddInt64(&s.Inserts, n)
}

func (s *Stats) CountUpdates(n int64) {
	atomic.AddInt64(&s.Updates, n)
}

func (s *Stats) CountHits(n int64) {
	atomic.AddInt64(&s.Hits, n)
}

func (s *Stats) CountMisses(n int64) {
	atomic.AddInt64(&s.Misses, n)
}

func (s *Stats) CountEvictions(n int64) {
	atomic.AddInt64(&s.Evictions, n)
}
