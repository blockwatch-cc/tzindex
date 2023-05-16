// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) ChainByHeight(ctx context.Context, height int64) (*model.Chain, error) {
	table, err := m.Table(model.ChainTableKey)
	if err != nil {
		return nil, err
	}
	c := &model.Chain{}
	err = pack.NewQuery("api.chain_by_height").
		WithTable(table).
		AndEqual("height", height).
		WithLimit(1).
		Execute(ctx, c)
	if err != nil {
		return nil, err
	}
	if c.RowId == 0 {
		return nil, model.ErrNoChain
	}
	return c, nil
}

func (m *Indexer) SupplyByHeight(ctx context.Context, height int64) (*model.Supply, error) {
	table, err := m.Table(model.SupplyTableKey)
	if err != nil {
		return nil, err
	}
	s := &model.Supply{}
	err = pack.NewQuery("api.supply_by_height").
		WithTable(table).
		AndEqual("height", height).
		WithLimit(1).
		Execute(ctx, s)
	if err != nil {
		return nil, err
	}
	if s.RowId == 0 {
		return nil, model.ErrNoSupply
	}
	return s, nil
}

func (m *Indexer) SupplyByTime(ctx context.Context, t time.Time) (*model.Supply, error) {
	table, err := m.Table(model.SupplyTableKey)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	from, to := t, now
	if from.After(to) {
		from, to = to, from
	}
	s := &model.Supply{}
	err = pack.NewQuery("api.supply_by_time").
		WithTable(table).
		AndRange("time", from, to). // search for timestamp
		AndGte("height", 1).        // height larger than supply init block 1
		WithLimit(1).
		Execute(ctx, s)
	if err != nil {
		return nil, err
	}
	if s.RowId == 0 {
		return nil, model.ErrNoSupply
	}
	return s, nil
}
