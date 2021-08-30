// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/packdb/cache/lru"
	"blockwatch.cc/packdb/pack"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

var BigmapHistoryMaxCacheSize = 32 // entries

type BigmapHistory struct {
	BigmapId     int64
	Height       int64
	KeyOffsets   []uint32
	ValueOffsets []uint32
	Data         []byte
}

func (h BigmapHistory) Size() int64 {
	return int64(len(h.KeyOffsets) + len(h.ValueOffsets) + len(h.Data))
}

func (h BigmapHistory) Len() int {
	return len(h.KeyOffsets)
}

func (h BigmapHistory) Get(key tezos.ExprHash) *model.BigmapKV {
	var found int = -1
	for i, v := range h.KeyOffsets {
		kStart, kEnd := v, h.ValueOffsets[i]
		if !key.Equal(micheline.KeyHash(h.Data[kStart:kEnd])) {
			continue
		}
		found = i
		break
	}
	if found < 0 {
		return nil
	}
	kStart, vStart := int(h.KeyOffsets[found]), int(h.ValueOffsets[found])
	kEnd, vEnd := vStart, len(h.Data)
	if found < h.Len()-1 {
		vEnd = int(h.KeyOffsets[found+1])
	}
	return &model.BigmapKV{
		RowId:    uint64(found + 1),
		BigmapId: h.BigmapId,
		KeyId:    model.GetKeyId(h.BigmapId, micheline.KeyHash(h.Data[kStart:kEnd])),
		Key:      h.Data[kStart:kEnd],
		Value:    h.Data[vStart:vEnd],
	}
}

func (h BigmapHistory) Range(from, to int) []*model.BigmapKV {
	if to < 0 || to >= h.Len() {
		to = h.Len() - 1
	}
	if to <= from {
		return nil
	}
	items := make([]*model.BigmapKV, to-from)
	for i := 0; i < len(items); i++ {
		kStart, vStart := int(h.KeyOffsets[i+from]), int(h.ValueOffsets[i+from])
		kEnd, vEnd := vStart, len(h.Data)
		if i+from < len(h.KeyOffsets) {
			vEnd = int(h.KeyOffsets[i+from+1])
		}

		// log.Infof("Item %d: key [%d:%d] value[%d:%d] max=%d", i, kStart, kEnd, vStart, vEnd, len(h.Data))
		items[i] = &model.BigmapKV{
			RowId:    uint64(i + from + 1),
			BigmapId: h.BigmapId,
			KeyId:    model.GetKeyId(h.BigmapId, micheline.KeyHash(h.Data[kStart:kEnd])),
			Key:      h.Data[kStart:kEnd],
			Value:    h.Data[vStart:vEnd],
		}
	}
	return items
}

type BigmapHistoryCache struct {
	cache *lru.TwoQueueCache // key := int64(bigmap_id<<32 & height)
	size  int64
	stats Stats
}

func NewBigmapHistoryCache() *BigmapHistoryCache {
	c := &BigmapHistoryCache{}
	c.cache, _ = lru.New2QWithEvict(BigmapHistoryMaxCacheSize, func(_, v interface{}) {
		atomic.AddInt64(&c.size, -v.(*BigmapHistory).Size())
		atomic.AddInt64(&c.stats.Evictions, 1)
	})
	return c
}

func (c BigmapHistoryCache) makeKey(id, height int64) int64 {
	return id<<32 | height
}

func (c BigmapHistoryCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.cache.Len()
	s.Bytes = c.size
	return s
}

func (c *BigmapHistoryCache) Purge() {
	c.cache.Purge()
	c.size = 0
}

func (c *BigmapHistoryCache) Get(id, height int64) (*BigmapHistory, bool) {
	hist, ok := c.cache.Get(c.makeKey(id, height))
	if ok {
		c.stats.CountHits(1)
		return hist.(*BigmapHistory), ok
	}
	c.stats.CountMisses(1)
	return nil, false
}

func (c *BigmapHistoryCache) Build(ctx context.Context, updates *pack.Table, id, height int64) (*BigmapHistory, error) {
	kvStore := make(map[uint64]*model.BigmapKV)
	upd := &model.BigmapUpdate{}
	var count int
	err := pack.NewQuery("build_history_cache", updates).
		WithoutCache().
		WithFields("a", "k", "v").
		AndEqual("bigmap_id", id).
		AndLte("height", height).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(upd); err != nil {
				return err
			}
			count++
			switch upd.Action {
			case micheline.DiffActionAlloc, micheline.DiffActionCopy:
				// ignore
			case micheline.DiffActionUpdate:
				kvStore[upd.KeyId] = upd.ToKV()
			case micheline.DiffActionRemove:
				delete(kvStore, upd.KeyId)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	log.Debugf("Processed %d updates, found %d live keys", count, len(kvStore))

	// compile into compact cacheable form
	hist := &BigmapHistory{
		BigmapId:     id,
		Height:       height,
		KeyOffsets:   make([]uint32, len(kvStore)),
		ValueOffsets: make([]uint32, len(kvStore)),
		Data:         make([]byte, 0, len(kvStore)*16), // guess
	}
	count = 0
	for _, v := range kvStore {
		hist.KeyOffsets[count] = uint32(len(hist.Data))
		hist.Data = append(hist.Data, v.Key...)
		hist.ValueOffsets[count] = uint32(len(hist.Data))
		hist.Data = append(hist.Data, v.Value...)
		count++
	}
	c.cache.Add(c.makeKey(id, height), hist)
	c.stats.CountInserts(1)
	atomic.AddInt64(&c.size, hist.Size())
	return hist, nil
}
