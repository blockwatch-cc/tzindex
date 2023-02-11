// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
    "context"
    "errors"
    "fmt"
    "sort"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/packdb/util"
    "blockwatch.cc/tzindex/etl/model"
)

const (
    BalancePackSizeLog2    = 15 // =32k packs
    BalanceJournalSizeLog2 = 16 // =64k entries
    BalanceCacheSize       = 128
    BalanceFillLevel       = 100
    BalanceIndexKey        = "balance"
    BalanceTableKey        = "balance"
)

var (
    ErrNoBalanceEntry = errors.New("balance not indexed")
)

type BalanceIndex struct {
    db    *pack.DB
    opts  pack.Options
    table *pack.Table
}

var _ model.BlockIndexer = (*BalanceIndex)(nil)

func NewBalanceIndex(opts pack.Options) *BalanceIndex {
    return &BalanceIndex{opts: opts}
}

func (idx *BalanceIndex) DB() *pack.DB {
    return idx.db
}

func (idx *BalanceIndex) Tables() []*pack.Table {
    return []*pack.Table{
        idx.table,
    }
}

func (idx *BalanceIndex) Key() string {
    return BalanceIndexKey
}

func (idx *BalanceIndex) Name() string {
    return BalanceIndexKey + " index"
}

func (idx *BalanceIndex) Create(path, label string, opts interface{}) error {
    fields, err := pack.Fields(model.Balance{})
    if err != nil {
        return err
    }
    db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
    if err != nil {
        return fmt.Errorf("creating %s database: %w", idx.Key(), err)
    }
    defer db.Close()

    _, err = db.CreateTableIfNotExists(
        BalanceTableKey,
        fields,
        pack.Options{
            PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BalancePackSizeLog2),
            JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BalanceJournalSizeLog2),
            CacheSize:       util.NonZero(idx.opts.CacheSize, BalanceCacheSize),
            FillLevel:       util.NonZero(idx.opts.FillLevel, BalanceFillLevel),
        })
    return err
}

func (idx *BalanceIndex) Init(path, label string, opts interface{}) error {
    var err error
    idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
    if err != nil {
        return err
    }
    idx.table, err = idx.db.Table(
        BalanceTableKey,
        pack.Options{
            JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BalanceJournalSizeLog2),
            CacheSize:       util.NonZero(idx.opts.CacheSize, BalanceCacheSize),
        })
    if err != nil {
        idx.Close()
        return err
    }
    return nil
}

func (idx *BalanceIndex) FinalizeSync(_ context.Context) error {
    return nil
}

func (idx *BalanceIndex) Close() error {
    for _, v := range idx.Tables() {
        if v != nil {
            if err := v.Close(); err != nil {
                log.Errorf("Closing %s table: %s", v.Name(), err)
            }
        }
    }
    idx.table = nil
    if idx.db != nil {
        if err := idx.db.Close(); err != nil {
            return err
        }
        idx.db = nil
    }
    return nil
}

func (idx *BalanceIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
    ins := make([]pack.Item, 0)

    // handle bakers
    for _, bkr := range builder.Bakers() {
        acc := bkr.Account
        if !acc.IsDirty || acc.PrevBalance == acc.Balance() {
            continue
        }
        bal := &model.Balance{
            AccountId: acc.RowId,
            Balance:   acc.Balance(),
            ValidFrom: block.Height,
        }
        ins = append(ins, bal)
    }

    // handle regular accounts
    for _, acc := range builder.Accounts() {
        if !acc.IsDirty || acc.PrevBalance == acc.Balance() {
            continue
        }
        bal := &model.Balance{
            AccountId: acc.RowId,
            Balance:   acc.Balance(),
            ValidFrom: block.Height,
        }
        ins = append(ins, bal)
    }

    // insert sorted values
    sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Balance).AccountId < ins[j].(*model.Balance).AccountId })
    return idx.table.Insert(ctx, ins)
}

func (idx *BalanceIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
    return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BalanceIndex) DeleteBlock(ctx context.Context, height int64) error {
    _, err := pack.NewQuery("etl.balance.delete").
        WithTable(idx.table).
        AndEqual("valid_from", height).
        Delete(ctx)
    return err
}

func (idx *BalanceIndex) DeleteCycle(ctx context.Context, cycle int64) error {
    return nil
}

func (idx *BalanceIndex) Flush(ctx context.Context) error {
    for _, v := range idx.Tables() {
        if err := v.Flush(ctx); err != nil {
            return err
        }
    }
    return nil
}
