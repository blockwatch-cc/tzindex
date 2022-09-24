// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "sort"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzgo/micheline"
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) LookupContract(ctx context.Context, addr tezos.Address) (*model.Contract, error) {
    if !addr.IsValid() {
        return nil, ErrInvalidHash
    }
    table, err := m.Table(index.ContractTableKey)
    if err != nil {
        return nil, err
    }
    cc := &model.Contract{}
    err = pack.NewQuery("contract_by_hash").
        WithTable(table).
        AndEqual("address", addr.Bytes22()).
        Execute(ctx, cc)
    if err != nil {
        return nil, err
    }
    if cc.RowId == 0 {
        return nil, index.ErrNoContractEntry
    }
    return cc, nil
}

func (m *Indexer) LookupContractId(ctx context.Context, id model.AccountID) (*model.Contract, error) {
    table, err := m.Table(index.ContractTableKey)
    if err != nil {
        return nil, err
    }
    cc := &model.Contract{}
    err = pack.NewQuery("contract_by_id").
        WithTable(table).
        AndEqual("account_id", id).
        Execute(ctx, cc)
    if err != nil {
        return nil, err
    }
    if cc.RowId == 0 {
        return nil, index.ErrNoContractEntry
    }
    return cc, nil
}

func (m *Indexer) LookupContractType(ctx context.Context, id model.AccountID) (micheline.Type, micheline.Type, error) {
    elem, ok := m.contract_types.Get(id)
    if !ok {
        cc, err := m.LookupContractId(ctx, id)
        if err != nil {
            return micheline.Type{}, micheline.Type{}, err
        }
        elem = m.contract_types.Add(cc)
    }
    return elem.ParamType, elem.StorageType, nil
}

func (m *Indexer) ListContracts(ctx context.Context, r ListRequest) ([]*model.Contract, error) {
    table, err := m.Table(index.ContractTableKey)
    if err != nil {
        return nil, err
    }
    // cursor and offset are mutually exclusive
    if r.Cursor > 0 {
        r.Offset = 0
    }
    q := pack.NewQuery("list_created_contracts").
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
}

func (m *Indexer) LookupConstant(ctx context.Context, hash tezos.ExprHash) (*model.Constant, error) {
    if !hash.IsValid() {
        return nil, ErrInvalidHash
    }
    table, err := m.Table(index.ConstantTableKey)
    if err != nil {
        return nil, err
    }
    cc := &model.Constant{}
    err = pack.NewQuery("constant_by_hash").
        WithTable(table).
        AndEqual("address", hash.Bytes()).
        Execute(ctx, cc)
    if err != nil {
        return nil, err
    }
    if cc.RowId == 0 {
        return nil, index.ErrNoConstantEntry
    }
    return cc, nil
}

func (m *Indexer) FindPreviousStorage(ctx context.Context, id model.AccountID, since, until int64) (*model.Storage, error) {
    table, err := m.Table(index.StorageTableKey)
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
        return nil, index.ErrNoStorageEntry
    }
    return store, nil
}

func (m *Indexer) LookupStorage(ctx context.Context, id model.AccountID, h uint64, since, until int64) (*model.Storage, error) {
    table, err := m.Table(index.StorageTableKey)
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
        return nil, index.ErrNoStorageEntry
    }
    return store, nil
}

func (m *Indexer) ListContractBigmaps(ctx context.Context, acc model.AccountID, height int64) ([]*model.BigmapAlloc, error) {
    table, err := m.Table(index.BigmapAllocTableKey)
    if err != nil {
        return nil, err
    }
    q := pack.NewQuery("list_bigmaps").
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
    table, err := m.Table(index.EventTableKey)
    if err != nil {
        return nil, err
    }
    q := pack.NewQuery("list_events").
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
