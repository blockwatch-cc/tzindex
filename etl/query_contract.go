// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) LookupContract(ctx context.Context, addr tezos.Address) (*model.Contract, error) {
	if !addr.IsValid() {
		return nil, model.ErrInvalidAddress
	}
	table, err := m.Table(model.ContractTableKey)
	if err != nil {
		return nil, err
	}
	cc := &model.Contract{}
	err = pack.NewQuery("api.contract_by_hash").
		WithTable(table).
		AndEqual("address", addr[:]).
		Execute(ctx, cc)
	if err != nil {
		return nil, err
	}
	if cc.RowId == 0 {
		return nil, model.ErrNoContract
	}
	return cc, nil
}

func (m *Indexer) LookupContractId(ctx context.Context, id model.AccountID) (*model.Contract, error) {
	table, err := m.Table(model.ContractTableKey)
	if err != nil {
		return nil, err
	}
	cc := &model.Contract{}
	err = pack.NewQuery("api.contract_by_id").
		WithTable(table).
		AndEqual("account_id", id).
		Execute(ctx, cc)
	if err != nil {
		return nil, err
	}
	if cc.RowId == 0 {
		return nil, model.ErrNoContract
	}
	return cc, nil
}

func (m *Indexer) LookupContractType(ctx context.Context, id model.AccountID) (micheline.Type, micheline.Type, uint64, error) {
	elem, ok := m.contract_types.Get(id)
	if !ok {
		cc, err := m.LookupContractId(ctx, id)
		if err != nil {
			return micheline.Type{}, micheline.Type{}, 0, err
		}
		elem = m.contract_types.Add(cc)
	}
	return elem.ParamType, elem.StorageType, elem.CodeHash, nil
}

func (m *Indexer) LookupTicket(ctx context.Context, id model.TicketID) (*model.TicketType, error) {
	tt, ok := m.ticket_types.Get(id)
	if !ok {
		table, err := m.Table(model.TicketTypeTableKey)
		if err != nil {
			return nil, err
		}
		tt = model.NewTicketType()
		err = pack.NewQuery("api.ticket_type_by_id").
			WithTable(table).
			AndEqual("row_id", id).
			Execute(ctx, tt)
		if err != nil {
			return nil, err
		}
		if tt.Id == 0 {
			return nil, model.ErrNoTicketType
		}
		m.ticket_types.Add(tt)
	}
	return tt, nil
}

func (m *Indexer) ListContracts(ctx context.Context, r ListRequest) ([]*model.Contract, error) {
	table, err := m.Table(model.ContractTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}

	if !r.Account.IsContract {
		// regular accounts are set is creator in contract table
		q := pack.NewQuery("api.list_created_contracts").
			WithTable(table).
			AndEqual("creator_id", r.Account.RowId). // manager/creator id
			WithOrder(r.Order).
			WithLimit(int(r.Limit)).
			WithOffset(int(r.Offset))
		if r.Cursor > 0 {
			if r.Order == pack.OrderDesc {
				q = q.AndLt("I", r.Cursor)
			} else {
				q = q.AndGt("I", r.Cursor)
			}
		}
		ccs := make([]*model.Contract, 0)
		err = q.Execute(ctx, &ccs)
		if err != nil {
			return nil, err
		}
		return ccs, nil
	} else {
		// contract factories (1: list ids from op table, 2: list contracts)
		ops, err := m.Table(model.OpTableKey)
		if err != nil {
			return nil, err
		}
		q := pack.NewQuery("api.list_factory_originations").
			WithTable(ops).
			WithFields("receiver_id").
			AndGte("height", r.Account.FirstSeen).
			AndLte("height", r.Account.LastSeen).
			AndEqual("type", model.OpTypeOrigination).
			AndEqual("is_success", true).
			AndEqual("creator_id", r.Account.RowId). // direct sender is stored as creator
			WithOrder(r.Order).
			WithLimit(int(r.Limit)).
			WithOffset(int(r.Offset))
		// cursor is deployed contract, which is stored as receiver
		if r.Cursor > 0 {
			if r.Order == pack.OrderDesc {
				q = q.AndLt("receiver_id", r.Cursor)
			} else {
				q = q.AndGt("receiver_id", r.Cursor)
			}
		}
		res, err := q.Run(ctx)
		if err != nil {
			return nil, err
		}
		defer res.Close()
		ids, err := res.Column("receiver_id")
		if err != nil {
			return nil, err
		}
		ccs := make([]*model.Contract, 0)
		err = pack.NewQuery("api.load_contracts").
			WithTable(table).
			AndIn("account_id", ids.([]uint32)). // stored as U32 in ops table!
			Execute(ctx, &ccs)
		if err != nil {
			return nil, err
		}
		return ccs, nil
	}
}

func (m *Indexer) LookupConstant(ctx context.Context, hash tezos.ExprHash) (*model.Constant, error) {
	if !hash.IsValid() {
		return nil, model.ErrInvalidExprHash
	}
	table, err := m.Table(model.ConstantTableKey)
	if err != nil {
		return nil, err
	}
	cc := &model.Constant{}
	err = pack.NewQuery("api.constant_by_hash").
		WithTable(table).
		AndEqual("address", hash.Bytes()).
		Execute(ctx, cc)
	if err != nil {
		return nil, err
	}
	if cc.RowId == 0 {
		return nil, model.ErrNoConstant
	}
	return cc, nil
}

func (m *Indexer) FindPreviousStorage(ctx context.Context, id model.AccountID, since, until int64) (*model.Storage, error) {
	table, err := m.Table(model.StorageTableKey)
	if err != nil {
		return nil, err
	}
	store := &model.Storage{}
	err = pack.NewQuery("api.storage.find").
		WithTable(table).
		WithLimit(1). // there should only be one match anyways
		WithDesc().   // search in reverse order to find latest update
		AndGte("height", since).
		AndLte("height", until).
		AndEqual("account_id", id).
		Execute(ctx, store)
	if err != nil {
		return nil, err
	}
	if store.RowId == 0 {
		return nil, model.ErrNoStorage
	}
	return store, nil
}

func (m *Indexer) LookupStorage(ctx context.Context, id model.AccountID, h uint64, since, until int64) (*model.Storage, error) {
	table, err := m.Table(model.StorageTableKey)
	if err != nil {
		return nil, err
	}
	store := &model.Storage{}
	err = pack.NewQuery("api.storage.lookup").
		WithTable(table).
		WithLimit(1). // there should only be one match anyways
		WithDesc().   // search in reverse order to find latest update
		AndGte("height", since).
		AndLte("height", until).
		AndEqual("account_id", id).
		AndEqual("hash", h).
		// WithStatsAfter(10).
		Execute(ctx, store)
	if err != nil {
		return nil, err
	}
	if store.RowId == 0 {
		return nil, model.ErrNoStorage
	}
	return store, nil
}

func (m *Indexer) ListContractBigmaps(ctx context.Context, acc model.AccountID, height int64) ([]*model.BigmapAlloc, error) {
	table, err := m.Table(model.BigmapAllocTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("api.list_bigmaps").
		WithTable(table).
		AndEqual("account_id", acc)
	if height > 0 {
		// bigmap must exist at this height
		q = q.AndLte("alloc_height", height).
			// and not be deleted
			OrCondition(
				pack.Equal("delete_height", 0),
				pack.Gt("delete_height", height),
			)
	} else {
		q = q.AndEqual("delete_height", 0)
	}
	allocs := make([]*model.BigmapAlloc, 0)
	err = q.Stream(ctx, func(r pack.Row) error {
		a := &model.BigmapAlloc{}
		if err := r.Decode(a); err != nil {
			return err
		}
		allocs = append(allocs, a)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(allocs, func(i, j int) bool { return allocs[i].BigmapId < allocs[j].BigmapId })
	return allocs, nil
}

// id is the external op id (not row_id)!
func (m *Indexer) ListOpEvents(ctx context.Context, id uint64, sndr model.AccountID) ([]*model.Event, error) {
	table, err := m.Table(model.EventTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("api.list_events").
		WithTable(table).
		AndEqual("op_id", id).
		AndEqual("account_id", sndr)
	events := make([]*model.Event, 0)
	err = q.Execute(ctx, &events)
	if err != nil {
		return nil, err
	}
	return events, nil
}
