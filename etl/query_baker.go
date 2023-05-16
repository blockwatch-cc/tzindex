// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) LookupBaker(ctx context.Context, addr tezos.Address) (*model.Baker, error) {
	if !addr.IsValid() {
		return nil, model.ErrInvalidAddress
	}
	table, err := m.Table(model.BakerTableKey)
	if err != nil {
		return nil, err
	}
	bkr := &model.Baker{}
	err = pack.NewQuery("api.baker_by_hash").
		WithTable(table).
		AndEqual("address", addr[:]).
		Execute(ctx, bkr)
	if bkr.RowId == 0 {
		err = model.ErrNoBaker
	}
	if err != nil {
		return nil, err
	}
	acc, err := m.LookupAccountId(ctx, bkr.AccountId)
	if err != nil {
		return nil, err
	}
	bkr.Account = acc
	return bkr, nil
}

func (m *Indexer) LookupBakerId(ctx context.Context, id model.AccountID) (*model.Baker, error) {
	acc, err := m.LookupAccountId(ctx, id)
	if err != nil {
		return nil, err
	}
	table, err := m.Table(model.BakerTableKey)
	if err != nil {
		return nil, err
	}
	bkr := &model.Baker{}
	err = pack.NewQuery("api.baker_by_id").
		WithTable(table).
		AndEqual("account_id", id).
		Execute(ctx, bkr)
	if bkr.RowId == 0 {
		err = model.ErrNoAccount
	}
	if err != nil {
		return nil, err
	}
	bkr.Account = acc
	return bkr, nil
}

func (m *Indexer) ListBakers(ctx context.Context, activeOnly bool) ([]*model.Baker, error) {
	table, err := m.Table(model.BakerTableKey)
	if err != nil {
		return nil, err
	}
	bkrs := make([]*model.Baker, 0)
	q := pack.NewQuery("api.list_bakers").WithTable(table)
	if activeOnly {
		q = q.AndEqual("is_active", true)
	}
	err = q.Execute(ctx, &bkrs)
	if err != nil {
		return nil, err
	}
	bkrMap := make(map[model.AccountID]*model.Baker)
	accIds := make([]uint64, 0)
	for _, v := range bkrs {
		bkrMap[v.AccountId] = v
		accIds = append(accIds, v.AccountId.Value())
	}
	accounts, err := m.Table(model.AccountTableKey)
	if err != nil {
		return nil, err
	}
	err = accounts.StreamLookup(ctx, accIds, func(r pack.Row) error {
		acc := &model.Account{}
		if err := r.Decode(acc); err != nil {
			return err
		}
		bkr, ok := bkrMap[acc.RowId]
		if ok {
			bkr.Account = acc
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return bkrs, err
}
