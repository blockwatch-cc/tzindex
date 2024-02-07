// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Implements the following models
//
// TicketType    unique identity (ticketer + content_type + content)
// TicketUpdate  copy of operation receipts

const (
	TicketTypeTableKey   = "ticket_types"
	TicketUpdateTableKey = "ticket_updates"
)

var (
	ErrNoTicketType   = errors.New("ticket type not indexed")
	ErrNoTicketUpdate = errors.New("ticket update not indexed")

	ticketTypePool = &sync.Pool{
		New: func() interface{} { return new(TicketType) },
	}
	ticketUpdatePool = &sync.Pool{
		New: func() interface{} { return new(TicketUpdate) },
	}
)

type TicketID uint64

// TicketType tracks all ticket types
type TicketType struct {
	Id       TicketID       `pack:"I,pk"      json:"row_id"`
	Ticketer tezos.Address  `pack:"A,bloom"   json:"ticketer"`
	Type     micheline.Prim `pack:"T,snappy"  json:"type"`
	Content  micheline.Prim `pack:"C,snappy"  json:"content"`
	Hash     tezos.ExprHash `pack:"H,bloom"   json:"hash"`
}

// Ensure TicketType items implement the pack.Item interface.
var _ pack.Item = (*TicketType)(nil)

func (m *TicketType) ID() uint64 {
	return uint64(m.Id)
}

func (m *TicketType) SetID(id uint64) {
	m.Id = TicketID(id)
}

func NewTicketType() *TicketType {
	return ticketTypePool.Get().(*TicketType)
}

func (m *TicketType) Reset() {
	*m = TicketType{}
}

func (m *TicketType) Free() {
	m.Reset()
	ticketTypePool.Put(m)
}

func (m TicketType) TableKey() string {
	return TicketTypeTableKey
}

func (m TicketType) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    13,  // 8k pack size
		JournalSizeLog2: 14,  // 16k journal size
		CacheSize:       128, // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m TicketType) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (m TicketType) Size() int {
	// address size is 1 + 24 + 22 = 47
	// hash size is 1 + 24 + 32 = 57
	return 8 + 47 + 57 + m.Type.Size() + m.Content.Size()
}

type TicketUpdateID uint64

// TicketUpdate tracks low-level updates issued in operation receipts.
type TicketUpdate struct {
	Id        TicketUpdateID `pack:"I,pk"      json:"row_id"`
	TicketId  TicketID       `pack:"T"         json:"ticket"`
	AccountId AccountID      `pack:"S"         json:"account"`
	Amount    tezos.Z        `pack:"A,snappy"  json:"amount"`
	Height    int64          `pack:"h"         json:"height"`
	Time      time.Time      `pack:"t"         json:"time"`
	OpId      uint64         `pack:"d"         json:"op_id"` // unique external operation id
}

// Ensure TicketUpdate items implement the pack.Item interface.
var _ pack.Item = (*TicketUpdate)(nil)

func (m *TicketUpdate) ID() uint64 {
	return uint64(m.Id)
}

func (m *TicketUpdate) SetID(id uint64) {
	m.Id = TicketUpdateID(id)
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

func (m TicketUpdate) TableKey() string {
	return TicketUpdateTableKey
}

func (m TicketUpdate) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    13,  // 8k pack size
		JournalSizeLog2: 14,  // 16k journal size
		CacheSize:       128, // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m TicketUpdate) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}
