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
	TokenEventTableKey = "token_events"
)

var (
	ErrNoTokenEvent = errors.New("token event not indexed")

	tokenEventPool = &sync.Pool{
		New: func() any { return new(TokenEvent) },
	}
)

type TokenEventType byte

const (
	TokenEventTypeInvalid TokenEventType = iota
	TokenEventTypeTransfer
	TokenEventTypeMint
	TokenEventTypeBurn
)

var (
	tokenEventTypeString         = "invalid_transfer_mint_burn"
	tokenEventTypeIdx            = [6][2]int{{0, 7}, {8, 16}, {17, 21}, {22, 26}}
	tokenEventTypeReverseStrings = map[string]TokenEventType{}
)

func init() {
	for i, v := range tokenEventTypeIdx {
		tokenEventTypeReverseStrings[tokenEventTypeString[v[0]:v[1]]] = TokenEventType(i)
	}
}

func (t TokenEventType) IsValid() bool {
	return t > TokenEventTypeInvalid
}

func (t TokenEventType) String() string {
	idx := tokenEventTypeIdx[t]
	return tokenEventTypeString[idx[0]:idx[1]]
}

func ParseTokenEventType(s string) TokenEventType {
	return tokenEventTypeReverseStrings[s]
}

func (t *TokenEventType) UnmarshalText(data []byte) error {
	v := ParseTokenEventType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid token event type %q", string(data))
	}
	*t = v
	return nil
}

func (t TokenEventType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

type TokenEventID uint64

// TokenEvent tracks all token events such as transfers, mints, burns.
type TokenEvent struct {
	Id       TokenEventID   `pack:"I,pk"      json:"row_id"`
	Ledger   AccountID      `pack:"l,bloom=3" json:"ledger"`
	Token    TokenID        `pack:"T,bloom=3" json:"token"`
	Type     TokenEventType `pack:"y"         json:"type"`
	Signer   AccountID      `pack:"Z"         json:"signer"`
	Sender   AccountID      `pack:"S"         json:"sender"`
	Receiver AccountID      `pack:"R"         json:"receiver"`
	Amount   tezos.Z        `pack:"A,snappy"  json:"amount"`
	Height   int64          `pack:"h"         json:"height"`
	Time     time.Time      `pack:"t"         json:"time"`
	OpId     OpID           `pack:"d"         json:"op_id"`

	TokenRef *Token `pack:"-" json:"-"`
}

// Ensure TokenEvent items implement the pack.Item interface.
var _ pack.Item = (*TokenEvent)(nil)

func (m *TokenEvent) ID() uint64 {
	return uint64(m.Id)
}

func (m *TokenEvent) SetID(id uint64) {
	m.Id = TokenEventID(id)
}

func NewTokenEvent() *TokenEvent {
	return tokenEventPool.Get().(*TokenEvent)
}

func (m *TokenEvent) Reset() {
	*m = TokenEvent{}
}

func (m *TokenEvent) Free() {
	m.Reset()
	tokenEventPool.Put(m)
}

func (m TokenEvent) TableKey() string {
	return TokenEventTableKey
}

func (m TokenEvent) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    13,  // 8k pack size
		JournalSizeLog2: 14,  // 16k journal size
		CacheSize:       16,  // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m TokenEvent) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func StoreTokenEvents(ctx context.Context, t *pack.Table, events []*TokenEvent) error {
	if len(events) == 0 {
		return nil
	}
	items := make([]pack.Item, len(events))
	for i := range events {
		items[i] = events[i]
	}
	return t.Insert(ctx, items)
}
