// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"io"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
)

// always read fresh data with updated counters
func (m *Indexer) LookupBigmapAlloc(ctx context.Context, id int64) (*model.BigmapAlloc, error) {
	table, err := m.Table(model.BigmapAllocTableKey)
	if err != nil {
		return nil, err
	}
	alloc := &model.BigmapAlloc{}
	err = pack.NewQuery("api.search_bigmap").
		WithTable(table).
		AndEqual("bigmap_id", id).
		Execute(ctx, alloc)
	if err != nil {
		return nil, err
	}
	if alloc.RowId == 0 {
		return nil, model.ErrNoBigmap
	}
	return alloc, nil
}

// only type info is relevant
func (m *Indexer) LookupBigmapType(ctx context.Context, id int64) (*model.BigmapAlloc, error) {
	alloc, ok := m.bigmap_types.GetType(id)
	if ok {
		return alloc, nil
	}
	table, err := m.Table(model.BigmapAllocTableKey)
	if err != nil {
		return nil, err
	}
	alloc = &model.BigmapAlloc{}
	err = pack.NewQuery("api.search_bigmap").
		WithTable(table).
		AndEqual("bigmap_id", id).
		Execute(ctx, alloc)
	if err != nil {
		return nil, err
	}
	if alloc.RowId == 0 {
		return nil, model.ErrNoBigmap
	}
	m.bigmap_types.Add(alloc)
	return alloc, nil
}

func (m *Indexer) ListHistoricBigmapKeys(ctx context.Context, r ListRequest) ([]*model.BigmapValue, error) {
	hist, ok := m.bigmap_values.Get(r.BigmapId, r.Since)
	if !ok {
		start := time.Now()
		table, err := m.Table(model.BigmapUpdateTableKey)
		if err != nil {
			return nil, err
		}

		// check if we have any previous bigmap state cached
		prev, ok := m.bigmap_values.GetBest(r.BigmapId, r.Since)
		if ok {
			// update from existing cache
			hist, err = m.bigmap_values.Update(ctx, prev, table, r.Since)
			if err != nil {
				return nil, err
			}
			log.Debugf("Updated history cache for bigmap %d from height %d to height %d with %d entries in %s",
				r.BigmapId, prev.Height, r.Since, hist.Len(), time.Since(start))

		} else {
			// build a new cache
			hist, err = m.bigmap_values.Build(ctx, table, r.BigmapId, r.Since)
			if err != nil {
				return nil, err
			}
			log.Debugf("Built history cache for bigmap %d at height %d with %d entries in %s",
				r.BigmapId, r.Since, hist.Len(), time.Since(start))
		}
	}

	// cursor and offset are mutually exclusive, we use offset below
	// Note that cursor starts at 1
	if r.Cursor > 0 {
		r.Offset = uint(r.Cursor - 1)
	}
	var from, to int
	if r.Order == pack.OrderAsc {
		from, to = int(r.Offset), int(r.Offset+r.Limit)
	} else {
		l := hist.Len()
		from = util.Max(0, l-int(r.Offset-r.Limit))
		to = from + int(r.Limit)
	}

	// get from cache
	var items []*model.BigmapValue
	if r.BigmapKey.IsValid() {
		if item := hist.Get(r.BigmapKey); item != nil {
			items = []*model.BigmapValue{item}
		}
	} else {
		items = hist.Range(from, to)
	}

	// maybe reverse order
	if r.Order == pack.OrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	return items, nil
}

func (m *Indexer) ListBigmapKeys(ctx context.Context, r ListRequest) ([]*model.BigmapValue, error) {
	table, err := m.Table(model.BigmapValueTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("api.list_bigmap").
		WithTable(table).
		WithOrder(r.Order).
		AndEqual("bigmap_id", r.BigmapId)
	if r.Cursor > 0 {
		r.Offset = 0
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.BigmapKey.IsValid() {
		// assume hash collisions
		q = q.WithDesc().AndEqual("key_id", model.GetKeyId(r.BigmapId, r.BigmapKey))
	}
	items := make([]*model.BigmapValue, 0)
	err = q.Stream(ctx, func(row pack.Row) error {
		// skip before decoding
		if r.Offset > 0 {
			r.Offset--
			return nil
		}
		b := &model.BigmapValue{}
		if err := row.Decode(b); err != nil {
			return err
		}

		// skip hash collisions on key_id
		if r.BigmapKey.IsValid() && !r.BigmapKey.Equal(b.GetKeyHash()) {
			// log.Infof("Skip hash collision for item %s %d %d key %x", b.Action, b.BigmapId, b.RowId, b.Key)
			return nil
		}

		// log.Infof("Found item %s %d %d key %x", b.Action, b.BigmapId, b.RowId, b.Key)
		items = append(items, b)
		if len(items) == int(r.Limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}

func (m *Indexer) ListBigmapUpdates(ctx context.Context, r ListRequest) ([]model.BigmapUpdate, error) {
	table, err := m.Table(model.BigmapUpdateTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("api.list_bigmap").
		WithTable(table).
		WithOrder(r.Order)
	if r.BigmapId > 0 {
		q = q.AndEqual("bigmap_id", r.BigmapId)
	}
	if r.OpId > 0 {
		q = q.AndEqual("op_id", r.OpId)
	}
	if r.Cursor > 0 {
		r.Offset = 0
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.Since > 0 {
		q = q.AndGte("height", r.Since)
	}
	if r.Until > 0 {
		q = q.AndLte("height", r.Until)
	}
	if r.BigmapKey.IsValid() {
		q = q.AndEqual("key_id", model.GetKeyId(r.BigmapId, r.BigmapKey))
	}
	items := make([]model.BigmapUpdate, 0)
	err = table.Stream(ctx, q, func(row pack.Row) error {
		if r.Offset > 0 {
			r.Offset--
			return nil
		}
		b := model.BigmapUpdate{}
		if err := row.Decode(&b); err != nil {
			return err
		}
		// skip hash collisions on key_id
		if r.BigmapKey.IsValid() && !r.BigmapKey.Equal(b.GetKeyHash()) {
			return nil
		}
		items = append(items, b)
		if len(items) == int(r.Limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}
