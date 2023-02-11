// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
    "context"
    "errors"
    "fmt"

    "blockwatch.cc/packdb/cache"
    "blockwatch.cc/packdb/cache/lru"
    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

const (
    TicketIndexKey       = "ticket"
    TicketTypeTableKey   = "ticket_types"
    TicketUpdateTableKey = "ticket_updates"
)

var (
    ticketTypeOpts = pack.Options{
        PackSizeLog2:    13,  // 8k pack size
        JournalSizeLog2: 14,  // 16k journal size
        CacheSize:       128, // max MB
        FillLevel:       100, // boltdb fill level to limit reallocations
    }
    ticketUpdateOpts = pack.Options{
        PackSizeLog2:    13,  // 8k pack size
        JournalSizeLog2: 14,  // 16k journal size
        CacheSize:       128, // max MB
        FillLevel:       100, // boltdb fill level to limit reallocations
    }

    ErrNoTicketType   = errors.New("ticket type not indexed")
    ErrNoTicketUpdate = errors.New("ticket update not indexed")
)

type TicketIndex struct {
    db     *pack.DB
    opts   pack.Options
    tables map[string]*pack.Table
    cache  cache.Cache
}

var _ model.BlockIndexer = (*TicketIndex)(nil)

func NewTicketIndex(opts pack.Options) *TicketIndex {
    c, _ := lru.New(1 << 15) // 32k
    return &TicketIndex{
        tables: make(map[string]*pack.Table),
        opts:   opts,
        cache:  c,
    }
}

func (idx *TicketIndex) DB() *pack.DB {
    return idx.db
}

func (idx *TicketIndex) Tables() []*pack.Table {
    t := []*pack.Table{}
    for _, v := range idx.tables {
        t = append(t, v)
    }
    return t
}

func (idx *TicketIndex) Key() string {
    return TicketIndexKey
}

func (idx *TicketIndex) Name() string {
    return TicketIndexKey + " index"
}

func (idx *TicketIndex) Create(path, label string, opts interface{}) error {
    typFields, err := pack.Fields(model.TicketType{})
    if err != nil {
        return err
    }
    updateFields, err := pack.Fields(model.TicketUpdate{})
    if err != nil {
        return err
    }

    db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
    if err != nil {
        return fmt.Errorf("creating %s database: %w", idx.Key(), err)
    }
    defer db.Close()

    _, err = db.CreateTableIfNotExists(
        TicketTypeTableKey,
        typFields,
        ticketTypeOpts.Merge(idx.opts),
    )
    if err != nil {
        return err
    }
    _, err = db.CreateTableIfNotExists(
        TicketUpdateTableKey,
        updateFields,
        ticketUpdateOpts.Merge(idx.opts),
    )
    if err != nil {
        return err
    }
    return nil
}

func (idx *TicketIndex) Init(path, label string, opts interface{}) error {
    db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
    if err != nil {
        return err
    }
    idx.db = db

    t, err := idx.db.Table(TicketTypeTableKey, ticketTypeOpts.Merge(idx.opts))
    if err != nil {
        idx.Close()
        return err
    }
    idx.tables[TicketTypeTableKey] = t
    t, err = idx.db.Table(TicketUpdateTableKey, ticketUpdateOpts.Merge(idx.opts))
    if err != nil {
        idx.Close()
        return err
    }
    idx.tables[TicketUpdateTableKey] = t

    return nil
}

func (idx *TicketIndex) FinalizeSync(_ context.Context) error {
    return nil
}

func (idx *TicketIndex) Close() error {
    for n, v := range idx.tables {
        if err := v.Close(); err != nil {
            log.Errorf("Closing %s table: %s", n, err)
        }
        delete(idx.tables, n)
    }
    if idx.db != nil {
        if err := idx.db.Close(); err != nil {
            return err
        }
        idx.db = nil
    }
    return nil
}

func (idx *TicketIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
    ins := make([]pack.Item, 0)

    for _, op := range block.Ops {
        if op.Type != model.OpTypeTransaction && op.Type != model.OpTypeOrigination {
            continue
        }

        for _, up := range op.RawTicketUpdates {
            // load or create type
            tick, err := idx.findOrCreateTicketType(ctx, up.Ticket)
            if err != nil {
                return fmt.Errorf("ticket: load/create type for %s: %v", op.Hash, err)
            }

            // process balance changes
            for _, bal := range up.Updates {
                acc, ok := b.AccountByAddress(bal.Account)
                if !ok {
                    return fmt.Errorf("ticket: missing owner account %s in %s", bal.Account, op.Hash)
                }

                tu := model.NewTicketUpdate()
                tu.TicketId = tick.Id
                tu.AccountId = acc.RowId
                tu.Amount = bal.Amount
                tu.Height = op.Height
                tu.Time = op.Timestamp
                tu.OpId = op.Id() // unique external id
                ins = append(ins, tu)
            }
        }
    }

    // batch insert all updates
    if len(ins) > 0 {
        if err := idx.tables[TicketUpdateTableKey].Insert(ctx, ins); err != nil {
            return fmt.Errorf("ticket: insert: %w", err)
        }
    }

    return nil
}

func (idx *TicketIndex) findOrCreateTicketType(ctx context.Context, t rpc.Ticket) (*model.TicketType, error) {
    key := t.Hash()
    ityp, ok := idx.cache.Get(key.String())
    if ok {
        return ityp.(*model.TicketType), nil
    }
    tt := model.NewTicketType()
    err := pack.NewQuery("etl.find_ticket_type").
        WithTable(idx.tables[TicketTypeTableKey]).
        AndEqual("hash", key).
        Execute(ctx, tt)
    if err != nil {
        return nil, err
    }
    if tt.Id == 0 {
        tt.Ticketer = t.Ticketer
        tt.Type = t.Type
        tt.Content = t.Content
        tt.Hash = key
        if err := idx.tables[TicketTypeTableKey].Insert(ctx, tt); err != nil {
            return nil, err
        }
    }
    idx.cache.Add(key.String(), tt)
    return tt, nil
}

func (idx *TicketIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
    return idx.DeleteBlock(ctx, block.Height)
}

func (idx *TicketIndex) DeleteBlock(ctx context.Context, height int64) error {
    _, err := pack.NewQuery("etl.delete").
        WithTable(idx.tables[TicketUpdateTableKey]).
        AndEqual("height", height).
        Delete(ctx)
    return err
}

func (idx *TicketIndex) DeleteCycle(ctx context.Context, cycle int64) error {
    // _, err := pack.NewQuery("etl.delete").
    //     WithTable(idx.table).
    //     AndRange("height", params.CycleStartHeight(cycle), params.CycleEndHeight(cycle),).
    //     Delete(ctx)
    return nil
}

func (idx *TicketIndex) Flush(ctx context.Context) error {
    for _, v := range idx.Tables() {
        if err := v.Flush(ctx); err != nil {
            return err
        }
    }
    return nil
}
