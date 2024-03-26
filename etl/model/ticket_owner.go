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
	TicketOwnerTableKey = "ticket_owners"
)

var (
	ErrNoTicketOwner = errors.New("ticket owner not indexed")

	ticketOwnerPool = &sync.Pool{
		New: func() any { return new(TicketOwner) },
	}
)

type TicketOwnerID uint64

func (i TicketOwnerID) U64() uint64 {
	return uint64(i)
}

// TicketOwner tracks owner info.
type TicketOwner struct {
	Id           TicketOwnerID `pack:"I,pk"      json:"row_id"`
	Ticket       TicketID      `pack:"T,bloom=3" json:"ticket"`
	Ticketer     AccountID     `pack:"E,bloom=3" json:"ticketer"`
	Account      AccountID     `pack:"A,bloom=3" json:"account"`
	Balance      tezos.Z       `pack:"B,snappy"  json:"balance"`       // current balance
	FirstBlock   int64         `pack:"<,i32"     json:"first_block"`   // block height
	FirstTime    time.Time     `pack:"f"         json:"first_time"`    // block time
	LastBlock    int64         `pack:">,i32"     json:"last_block"`    // block height
	LastTime     time.Time     `pack:"t"         json:"last_time"`     // block time
	NumTransfers int           `pack:"x,i32"     json:"num_transfers"` // #xfers this owner/ticket combi sent or recv
	NumMints     int           `pack:"y,i32"     json:"num_mints"`     // #mints this owner/ticket combi
	NumBurns     int           `pack:"z,i32"     json:"num_burns"`     // #burns this owner/ticket combi
	VolSent      tezos.Z       `pack:"s,snappy"  json:"vol_sent"`      // running total
	VolRecv      tezos.Z       `pack:"r,snappy"  json:"vol_recv"`      // running total
	VolMint      tezos.Z       `pack:"m,snappy"  json:"vol_mint"`      // running total
	VolBurn      tezos.Z       `pack:"b,snappy"  json:"vol_burn"`      // running total

	// internal, used for stats
	WasZero bool `pack:"-"  json:"-"`
}

// Ensure TicketOwner implements the pack.Item interface.
var _ pack.Item = (*TicketOwner)(nil)

func (t TicketOwner) ID() uint64 {
	return uint64(t.Id)
}

func (t *TicketOwner) SetID(id uint64) {
	t.Id = TicketOwnerID(id)
}

func (_ TicketOwner) TableKey() string {
	return TicketOwnerTableKey
}

func (_ TicketOwner) TableOpts() pack.Options {
	return pack.NoOptions
}

func (_ TicketOwner) IndexOpts(_ string) pack.Options {
	return pack.NoOptions
}

func NewTicketOwner() *TicketOwner {
	return ticketOwnerPool.Get().(*TicketOwner)
}

func (o *TicketOwner) Reset() {
	*o = TicketOwner{}
}

func (o *TicketOwner) Free() {
	o.Reset()
	ticketOwnerPool.Put(o)
}

func (o *TicketOwner) Store(ctx context.Context, t *pack.Table) error {
	if o.Id > 0 {
		return t.Update(ctx, o)
	}
	return t.Insert(ctx, o)
}

func GetTicketOwner(ctx context.Context, t *pack.Table, accountId AccountID, id TicketID) (*TicketOwner, error) {
	o := NewTicketOwner()
	err := pack.NewQuery("find").
		WithTable(t).
		AndEqual("account", accountId).
		AndEqual("ticket", id).
		Execute(ctx, o)
	if err != nil {
		o.Free()
		return nil, err
	}
	if o.Id == 0 {
		o.Free()
		return nil, ErrNoTicketOwner
	}
	o.WasZero = o.Balance.IsZero()
	return o, nil
}

func GetOrCreateTicketOwner(ctx context.Context, t *pack.Table, accountId, issuerId AccountID, id TicketID, height int64, tm time.Time) (*TicketOwner, error) {
	o := NewTicketOwner()
	err := pack.NewQuery("find.by_owner").
		WithTable(t).
		AndEqual("account", accountId).
		AndEqual("ticket", id).
		Execute(ctx, o)
	if err != nil {
		o.Free()
		return nil, err
	}
	o.WasZero = o.Balance.IsZero()
	if o.Id == 0 {
		o.Account = accountId
		o.Ticket = id
		o.Ticketer = issuerId
		o.FirstBlock = height
		o.FirstTime = tm
		if err := t.Insert(ctx, o); err != nil {
			o.Free()
			return nil, err
		}
	}
	return o, nil
}

func GetTicketOwnerId(ctx context.Context, t *pack.Table, id TicketOwnerID) (*TicketOwner, error) {
	o := NewTicketOwner()
	err := pack.NewQuery("find.by_id").
		WithTable(t).
		AndEqual("row_id", id).
		Execute(ctx, o)
	if err != nil {
		o.Free()
		return nil, err
	}
	o.WasZero = o.Balance.IsZero()
	return o, nil
}

func ListTicketOwners(ctx context.Context, t *pack.Table, q pack.Query) ([]*TicketOwner, error) {
	list := make([]*TicketOwner, 0)
	err := q.WithTable(t).Execute(ctx, &list)
	if err != nil {
		list = list[:0]
		return nil, err
	}
	return list, nil
}
