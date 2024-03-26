// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
    "context"
    "errors"
    "sync"
    "time"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzgo/tezos"
)

const (
    TicketUpdateTableKey = "ticket_updates"
)

var (
    ErrNoTicketUpdate = errors.New("ticket update not indexed")

    ticketUpdatePool = &sync.Pool{
        New: func() any { return new(TicketUpdate) },
    }
)

type TicketUpdateID uint64

// TicketUpdate tracks low-level updates issued in operation receipts.
type TicketUpdate struct {
    Id        TicketUpdateID `pack:"I,pk"      json:"row_id"`
    TicketId  TicketID       `pack:"T"         json:"ticket"`
    AccountId AccountID      `pack:"S"         json:"account"`
    Amount    tezos.Z        `pack:"A,snappy"  json:"amount"`
    Height    int64          `pack:"h,i32"     json:"height"`
    Time      time.Time      `pack:"t"         json:"time"`
    OpId      OpID           `pack:"d"         json:"op_id"`
}

// Ensure TicketUpdate implements the pack.Item interface.
var _ pack.Item = (*TicketUpdate)(nil)

func (t TicketUpdate) ID() uint64 {
    return uint64(t.Id)
}

func (t *TicketUpdate) SetID(id uint64) {
    t.Id = TicketUpdateID(id)
}

func NewTicketUpdate() *TicketUpdate {
    return ticketUpdatePool.Get().(*TicketUpdate)
}

func (m *TicketUpdate) Reset() {
    *m = TicketUpdate{}
}

func (m *TicketUpdate) Free() {
    m.Reset()
    ticketUpdatePool.Put(m)
}

func (_ TicketUpdate) TableKey() string {
    return TicketUpdateTableKey
}

func (_ TicketUpdate) TableOpts() pack.Options {
    return pack.Options{
        PackSizeLog2:    13,  // 8k pack size
        JournalSizeLog2: 14,  // 16k journal size
        CacheSize:       128, // max MB
        FillLevel:       100, // boltdb fill level to limit reallocations
    }
}

func (_ TicketUpdate) IndexOpts(_ string) pack.Options {
    return pack.NoOptions
}

func (_ TicketUpdate) Indexes() []string {
    return nil
}

func (t *TicketUpdate) Store(ctx context.Context, table *pack.Table) error {
    if t.Id > 0 {
        return table.Update(ctx, t)
    }
    return table.Insert(ctx, t)
}

func ListTicketUpdates(ctx context.Context, t *pack.Table, q pack.Query) ([]*TicketUpdate, error) {
    list := make([]*TicketUpdate, 0)
    err := q.WithTable(t).Execute(ctx, &list)
    if err != nil {
        list = list[:0]
        return nil, err
    }
    return list, nil
}
