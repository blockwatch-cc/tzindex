// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"context"
	"errors"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"github.com/cespare/xxhash/v2"
)

const (
	TicketTableKey = "ticket"
)

var (
	ErrNoTicket = errors.New("ticket type not indexed")

	ticketTypePool = &sync.Pool{
		New: func() any { return new(Ticket) },
	}
)

type TicketID uint64

func (i TicketID) U64() uint64 {
	return uint64(i)
}

// Ticket tracks all ticket instances. A Ticket is uniquely defined by
// a ticketer (the issuing contract), type and contents matching this type.
type Ticket struct {
	Id           TicketID       `pack:"I,pk"      json:"row_id"`
	Address      tezos.Address  `pack:"A,bloom=3" json:"address"`
	Ticketer     AccountID      `pack:"X,bloom=3" json:"ticketer"`
	Type         micheline.Prim `pack:"T,snappy"  json:"type"`
	Content      micheline.Prim `pack:"C,snappy"  json:"content"`
	Hash         uint64         `pack:"H,bloom"   json:"hash"`
	Creator      AccountID      `pack:"c"         json:"creator"`
	FirstBlock   int64          `pack:"<,i32"     json:"first_block"`
	FirstTime    time.Time      `pack:"f"         json:"first_time"`
	LastBlock    int64          `pack:">,i32"     json:"last_block"`
	LastTime     time.Time      `pack:"t"         json:"last_time"`
	Supply       tezos.Z        `pack:"S,snappy"  json:"total_supply"`
	TotalMint    tezos.Z        `pack:"m,snappy"  json:"total_mint"`
	TotalBurn    tezos.Z        `pack:"b,snappy"  json:"total_burn"`
	NumTransfers int            `pack:"x,i32"     json:"num_transfers"`
	NumHolders   int            `pack:"y,i32"     json:"num_holders"`
}

func (t Ticket) ID() uint64 {
	return uint64(t.Id)
}

func (t *Ticket) SetID(id uint64) {
	t.Id = TicketID(id)
}

func (_ Ticket) TableKey() string {
	return TicketTableKey
}

func (_ Ticket) TableOpts() pack.Options {
	return pack.NoOptions
}

func (_ Ticket) IndexOpts(_ string) pack.Options {
	return pack.NoOptions
}

func TicketHash(a tezos.Address, typ, content micheline.Prim) uint64 {
	key := micheline.NewPair(
		micheline.NewBytes(a.EncodePadded()),
		micheline.NewPair(typ, content),
	)
	buf, _ := key.MarshalBinary()
	return xxhash.Sum64(buf)
}

func NewTicket() *Ticket {
	return ticketTypePool.Get().(*Ticket)
}

func (t *Ticket) Reset() {
	*t = Ticket{}
}

func (t *Ticket) Free() {
	t.Reset()
	ticketTypePool.Put(t)
}

func (t Ticket) Size() int {
	// address size is 1 + 24 + 22 = 47
	// hash size is 1 + 24 + 32 = 57
	return 8 + 47 + 57 + t.Type.Size() + t.Content.Size()
}

func (t *Ticket) Store(ctx context.Context, s *pack.Table) error {
	if t.Id > 0 {
		return s.Update(ctx, t)
	}
	return s.Insert(ctx, t)
}

func GetTicketId(ctx context.Context, s *pack.Table, id TicketID) (*Ticket, error) {
	ty := NewTicket()
	err := pack.NewQuery("find.ticket_by_id").
		WithTable(s).
		AndEqual("row_id", id).
		Execute(ctx, ty)
	if err != nil {
		return nil, err
	}
	if ty.Id == 0 {
		return nil, ErrNoTicket
	}
	return ty, nil
}

func LookupTicket(ctx context.Context, s *pack.Table, addr tezos.Address, hash uint64) (*Ticket, error) {
	var tick Ticket
	err := pack.NewQuery("find.ticket_by_addr").
		WithTable(s).
		AndEqual("address", addr).
		AndEqual("hash", hash).
		Execute(ctx, &tick)
	if err != nil {
		return nil, err
	}
	if tick.Id == 0 {
		return nil, ErrNoTicket
	}
	return &tick, nil
}

func LookupTicketId(ctx context.Context, t *pack.Table, addr tezos.Address, hash uint64) (TicketID, error) {
	tick, err := LookupTicket(ctx, t, addr, hash)
	if err != nil {
		return 0, err
	}
	return tick.Id, nil
}

func ListTickets(ctx context.Context, s *pack.Table, q pack.Query) ([]*Ticket, error) {
	list := make([]*Ticket, 0)
	err := q.WithTable(s).Execute(ctx, &list)
	if err != nil {
		list = list[:0]
		return nil, err
	}
	return list, nil
}
