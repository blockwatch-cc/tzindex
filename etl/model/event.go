// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzindex/rpc"
)

type EventID uint64

func (id EventID) Value() uint64 {
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
func NewEvent(ev rpc.InternalResult, src AccountID, op *Op) *Event {
    payload, _ := ev.Payload.MarshalBinary()
    typ, _ := ev.Type.MarshalBinary()
    e := &Event{
        Height:    op.Height,
        OpId:      op.Id(),
        AccountId: src,
        Type:      typ,
        Payload:   payload,
        Tag:       ev.Tag,
        TypeHash:  ev.Type.CloneNoAnnots().Hash64(),
    }
    return e
}

func (e *Event) ID() uint64 {
    return uint64(e.RowId)
}

func (e *Event) SetID(id uint64) {
    e.RowId = EventID(id)
}
