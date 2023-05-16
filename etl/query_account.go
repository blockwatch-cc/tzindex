// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"io"
	"strings"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) LookupAccount(ctx context.Context, addr tezos.Address) (*model.Account, error) {
	if !addr.IsValid() {
		return nil, model.ErrInvalidAddress
	}
	table, err := m.Table(model.AccountTableKey)
	if err != nil {
		return nil, err
	}
	acc := &model.Account{}
	err = pack.NewQuery("api.account_by_hash").
		WithTable(table).
		AndEqual("address", addr[:]).
		Execute(ctx, acc)
	if acc.RowId == 0 {
		err = model.ErrNoAccount
	}
	if err != nil {
		return nil, err
	}
	return acc, nil
}

func (m *Indexer) LookupAccountId(ctx context.Context, id model.AccountID) (*model.Account, error) {
	table, err := m.Table(model.AccountTableKey)
	if err != nil {
		return nil, err
	}
	acc := &model.Account{}
	err = pack.NewQuery("api.account_by_id").
		WithTable(table).
		AndEqual("row_id", id).
		Execute(ctx, acc)
	if err != nil {
		return nil, err
	}
	if acc.RowId == 0 {
		return nil, model.ErrNoAccount
	}
	return acc, nil
}

func (m *Indexer) LookupAccountIds(ctx context.Context, ids []uint64) ([]*model.Account, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	table, err := m.Table(model.AccountTableKey)
	if err != nil {
		return nil, err
	}
	accs := make([]*model.Account, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(accs) {
			return io.EOF
		}
		a := &model.Account{}
		if err := r.Decode(a); err != nil {
			return err
		}
		accs[count] = a
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, model.ErrNoAccount
	}
	accs = accs[:count]
	return accs, nil
}

func (m *Indexer) FindActivatedAccount(ctx context.Context, addr tezos.Address) (*model.Account, error) {
	table, err := m.Table(model.OpTableKey)
	if err != nil {
		return nil, err
	}
	type Xop struct {
		SenderId  model.AccountID `pack:"S"`
		CreatorId model.AccountID `pack:"M"`
		Data      string          `pack:"a"`
	}
	var o Xop
	err = pack.NewQuery("api.find_activation").
		WithTable(table).
		WithFields("sender_id", "creator_id", "data").
		WithoutCache().
		AndEqual("type", model.OpTypeActivation).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(&o); err != nil {
				return err
			}
			// data contains hex(secret),blinded_address
			data := strings.Split(o.Data, ",")
			if len(data) != 2 {
				// skip broken records
				return nil
			}
			ba, err := tezos.DecodeBlindedAddress(data[1])
			if err != nil {
				// skip broken records
				return nil
			}
			if addr.Equal(ba) {
				return io.EOF // found
			}
			return nil
		})
	if err != io.EOF {
		if err == nil {
			err = model.ErrNoAccount
		}
		return nil, err
	}
	// lookup account by id
	if o.CreatorId != 0 {
		return m.LookupAccountId(ctx, o.CreatorId)
	}
	return m.LookupAccountId(ctx, o.SenderId)
}
