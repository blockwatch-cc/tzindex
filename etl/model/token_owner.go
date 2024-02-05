// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"context"
	"errors"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
)

const (
	TokenOwnerTableKey = "token_owners"
)

var (
	ErrNoTokenOwner = errors.New("token owner not indexed")

	tokenOwnerPool = &sync.Pool{
		New: func() any { return new(TokenOwner) },
	}
)

type TokenOwnerID uint64

// TokenOwner tracks current token ownership balances for each account and
// lifetime running totals. First/last and counters are useful as app-level
// stats and to limit db query range scans.
type TokenOwner struct {
	Id           TokenOwnerID `pack:"I,pk"      json:"row_id"`
	Account      AccountID    `pack:"A,bloom=3" json:"account"`
	Ledger       AccountID    `pack:"L,bloom=3" json:"ledger"`
	Token        TokenID      `pack:"T,bloom=3" json:"token"`
	Balance      tezos.Z      `pack:"B,snappy"  json:"balance"`       // current balance
	FirstBlock   int64        `pack:"<,i32"     json:"first_seen"`    // height
	LastBlock    int64        `pack:">,i32"     json:"last_seen"`     // height
	NumTransfers int          `pack:"x,i32"     json:"num_transfers"` // #xfers this owner/token combi sent or recv
	NumMints     int          `pack:"y,i32"     json:"num_mints"`     // #mints this owner/token combi
	NumBurns     int          `pack:"z,i32"     json:"num_burns"`     // #burns this owner/token combi
	VolSent      tezos.Z      `pack:"s,snappy"  json:"vol_sent"`      // running total
	VolRecv      tezos.Z      `pack:"r,snappy"  json:"vol_recv"`      // running total
	VolMint      tezos.Z      `pack:"m,snappy"  json:"vol_mint"`      // running total
	VolBurn      tezos.Z      `pack:"b,snappy"  json:"vol_burn"`      // running total

	// internal, used for stats
	WasZero bool `pack:"-"  json:"-"`
}

// Ensure TokenOwner items implement the pack.Item interface.
var _ pack.Item = (*TokenOwner)(nil)

func (m *TokenOwner) ID() uint64 {
	return uint64(m.Id)
}

func (m *TokenOwner) SetID(id uint64) {
	m.Id = TokenOwnerID(id)
}

func NewTokenOwner() *TokenOwner {
	return tokenOwnerPool.Get().(*TokenOwner)
}

func (m *TokenOwner) Reset() {
	*m = TokenOwner{}
}

func (m *TokenOwner) Free() {
	m.Reset()
	tokenOwnerPool.Put(m)
}

func (m TokenOwner) TableKey() string {
	return TokenOwnerTableKey
}

func (m TokenOwner) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    13,  // 8k pack size
		JournalSizeLog2: 14,  // 16k journal size
		CacheSize:       128, // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m TokenOwner) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func GetTokenOwner(ctx context.Context, t *pack.Table, accountId AccountID, tokenId TokenID) (*TokenOwner, error) {
	o := NewTokenOwner()
	err := pack.NewQuery("find").
		WithTable(t).
		AndEqual("account", accountId).
		AndEqual("token", tokenId).
		WithDesc().
		Execute(ctx, o)
	if err != nil {
		o.Free()
		return nil, err
	}
	if o.Id == 0 {
		o.Free()
		return nil, ErrNoTokenOwner
	}
	o.WasZero = o.Balance.IsZero()
	return o, nil
}

func GetOrCreateOwner(ctx context.Context, t *pack.Table, accountId AccountID, tokenId TokenID, ledgerId AccountID) (*TokenOwner, error) {
	o := NewTokenOwner()
	err := pack.NewQuery("find").
		WithTable(t).
		AndEqual("account", accountId).
		AndEqual("token", tokenId).
		Execute(ctx, o)
	if err != nil {
		o.Free()
		return nil, err
	}
	o.WasZero = o.Balance.IsZero()
	if o.Id == 0 {
		o.Account = accountId
		o.Token = tokenId
		o.Ledger = ledgerId
		if err := t.Insert(ctx, o); err != nil {
			o.Free()
			return nil, err
		}
		// ctx.Log.Debugf("creating new owner for A_%d / T_%d => O_%d", accountId, tokenId, o.Id)
	}
	return o, nil
}

func GetOwnerId(ctx context.Context, t *pack.Table, id TokenOwnerID) (*TokenOwner, error) {
	o := NewTokenOwner()
	err := pack.NewQuery("find").
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

func GetCurrentTokenOwner(ctx context.Context, t *pack.Table, tokenId TokenID) (*TokenOwner, error) {
	o := NewTokenOwner()
	err := pack.NewQuery("find").
		WithTable(t).
		WithDesc().
		AndEqual("token", tokenId).
		Execute(ctx, o)
	if err != nil {
		o.Free()
		return nil, err
	}
	o.WasZero = o.Balance.IsZero()
	return o, nil
}

func ListOwners(ctx context.Context, t *pack.Table, q pack.Query) ([]*TokenOwner, error) {
	list := make([]*TokenOwner, 0)
	err := q.WithTable(t).Execute(ctx, &list)
	if err != nil {
		for _, v := range list {
			v.Free()
		}
		list = list[:0]
		return nil, err
	}
	return list, nil
}

func IsLedgerOwner(ctx context.Context, t *pack.Table, ownerId, ledgerId AccountID) (bool, error) {
	cnt, err := pack.NewQuery("is_owner").
		WithTable(t).
		WithDesc().
		WithLimit(1).
		AndEqual("account", ownerId).
		AndEqual("ledger", ledgerId).
		AndNotEqual("balance", tezos.Zero).
		Count(ctx)
	return cnt > 0, err
}
