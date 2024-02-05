// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/rpc"
)

const EventTableKey = "event"

var (
	ErrNoEvent = errors.New("event not indexed")

	eventPool = &sync.Pool{
		New: func() interface{} { return new(Event) },
	}
)

type EventID uint64

func (id EventID) U64() uint64 {
	return uint64(id)
}

// Event holds location, type and content of on-chain events
type Event struct {
	RowId     EventID   `pack:"I,pk"      json:"row_id"`
	Height    int64     `pack:"h,i32"     json:"height"`
	OpId      uint64    `pack:"o"         json:"op_id"` // unique external operation id
	AccountId AccountID `pack:"C,bloom=3" json:"account_id"`
	Type      []byte    `pack:"t,snappy"  json:"type"`
	Payload   []byte    `pack:"p,snappy"  json:"payload"`
	Tag       string    `pack:"a,snappy"  json:"tag"`
	TypeHash  uint64    `pack:"H,bloom"   json:"type_hash"`
}

// Ensure Event implements the pack.Item interface.
var _ pack.Item = (*Event)(nil)

// assuming the op was successful!
func NewEventWithData(ev rpc.InternalResult, src AccountID, op *Op) *Event {
	e := NewEvent()
	payload, _ := ev.Payload.MarshalBinary()
	typ, _ := ev.Type.MarshalBinary()
	e.Height = op.Height
	e.OpId = op.Id()
	e.AccountId = src
	e.Type = typ
	e.Payload = payload
	e.Tag = ev.Tag
	e.TypeHash = ev.Type.CloneNoAnnots().Hash64()
	return e
}

func (e *Event) ID() uint64 {
	return uint64(e.RowId)
}

func (e *Event) SetID(id uint64) {
	e.RowId = EventID(id)
}

func NewEvent() *Event {
	return eventPool.Get().(*Event)
}

func (m *Event) Reset() {
	*m = Event{}
}

func (m *Event) Free() {
	m.Reset()
	eventPool.Put(m)
}

func (m Event) TableKey() string {
	return EventTableKey
}

func (m Event) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    12,
		JournalSizeLog2: 12,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Event) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}
