// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
)

const (
	TicketEventTableKey = "ticket_events"
)

var (
	ErrNoTicketEvent = errors.New("ticket event not indexed")

	ticketEventPool = &sync.Pool{
		New: func() any { return new(TicketEvent) },
	}
)

type TicketEventType byte

const (
	TicketEventTypeInvalid TicketEventType = iota
	TicketEventTypeTransfer
	TicketEventTypeMint
	TicketEventTypeBurn
)

var (
	ticketEventTypeString         = "invalid_transfer_mint_burn"
	ticketEventTypeIdx            = [6][2]int{{0, 7}, {8, 16}, {17, 21}, {22, 26}}
	ticketEventTypeReverseStrings = map[string]TicketEventType{}
)

func init() {
	for i, v := range ticketEventTypeIdx {
		ticketEventTypeReverseStrings[ticketEventTypeString[v[0]:v[1]]] = TicketEventType(i)
	}
}

func (t TicketEventType) IsValid() bool {
	return t > TicketEventTypeInvalid
}

func (t TicketEventType) String() string {
	idx := ticketEventTypeIdx[t]
	return ticketEventTypeString[idx[0]:idx[1]]
}

func ParseTicketEventType(s string) TicketEventType {
	return ticketEventTypeReverseStrings[s]
}

func (t *TicketEventType) UnmarshalText(data []byte) error {
	v := ParseTicketEventType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid token event type %q", string(data))
	}
	*t = v
	return nil
}

func (t TicketEventType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

type TicketEventID uint64

func (i TicketEventID) U64() uint64 {
	return uint64(i)
}

// TicketEvent tracks all ticket events such as transfers, mints, burns.
type TicketEvent struct {
	Id       TicketEventID   `pack:"I,pk"      json:"row_id"`
	Type     TicketEventType `pack:"y"         json:"type"`
	Ticket   TicketID        `pack:"T,bloom=3" json:"ticket"`
	Ticketer AccountID       `pack:"E,bloom=3" json:"ticketer"`
	Sender   AccountID       `pack:"S"         json:"sender"`
	Receiver AccountID       `pack:"R"         json:"receiver"`
	Amount   tezos.Z         `pack:"A,snappy"  json:"amount"`
	Height   int64           `pack:"h,i32"     json:"height"`
	Time     time.Time       `pack:"t"         json:"time"`
	OpId     uint64          `pack:"d"         json:"op_id"`
}

// Ensure TicketEvent implements the pack.Item interface.
var _ pack.Item = (*TicketEvent)(nil)

func (t TicketEvent) ID() uint64 {
	return uint64(t.Id)
}

func (t *TicketEvent) SetID(id uint64) {
	t.Id = TicketEventID(id)
}

func (_ TicketEvent) TableKey() string {
	return TicketEventTableKey
}

func (_ TicketEvent) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    13,  // 8k pack size
		JournalSizeLog2: 14,  // 16k journal size
		CacheSize:       128, // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (_ TicketEvent) IndexOpts(_ string) pack.Options {
	return pack.NoOptions
}

func NewTicketEvent() *TicketEvent {
	return ticketEventPool.Get().(*TicketEvent)
}

func (e *TicketEvent) Reset() {
	*e = TicketEvent{}
}

func (e *TicketEvent) Free() {
	e.Reset()
	ticketEventPool.Put(e)
}

func (e *TicketEvent) Store(ctx context.Context, t *pack.Table) error {
	if e.Id > 0 {
		return t.Update(ctx, e)
	}
	return t.Insert(ctx, e)
}

func StoreTicketEvents(ctx context.Context, t *pack.Table, events []*TicketEvent) error {
	if len(events) == 0 {
		return nil
	}
	ins := make([]pack.Item, len(events))
	for i, v := range events {
		ins[i] = v
	}
	return t.Insert(ctx, ins)
}

func ListTicketEvents(ctx context.Context, t *pack.Table, q pack.Query) ([]*TicketEvent, error) {
	list := make([]*TicketEvent, 0)
	err := q.WithTable(t).Execute(ctx, &list)
	if err != nil {
		list = list[:0]
		return nil, err
	}
	return list, nil
}
