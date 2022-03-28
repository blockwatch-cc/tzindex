// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	"time"

	"github.com/cespare/xxhash"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// /tables/bigmaps
type BigmapAlloc struct {
	RowId     uint64    `pack:"I,pk,snappy" json:"row_id"`        // internal: id
	BigmapId  int64     `pack:"B,snappy"    json:"bigmap_id"`     // unique bigmap id
	AccountId AccountID `pack:"A,snappy"    json:"account_id"`    // account table id for contract
	Height    int64     `pack:"h,snappy"    json:"alloc_height"`  // allocation height
	NUpdates  int64     `pack:"n,snappy"    json:"n_updates"`     // running update counter
	NKeys     int64     `pack:"k,snappy"    json:"n_keys"`        // current number of active keys
	Updated   int64     `pack:"u,snappy"    json:"update_height"` // last update height
	Data      []byte    `pack:"d,snappy"    json:"-"`             // micheline encoded type tree (key/val pair)

	// internal, not stored
	KeyType   micheline.Type `pack:"-" json:"-"`
	ValueType micheline.Type `pack:"-" json:"-"`
}

// Ensure bigmap items implement the pack.Item interface.
var _ pack.Item = (*BigmapAlloc)(nil)

func (m *BigmapAlloc) ID() uint64 {
	return m.RowId
}

func (m *BigmapAlloc) SetID(id uint64) {
	m.RowId = id
}

func GetKeyId(bigmapid int64, kh tezos.ExprHash) uint64 {
	var buf [40]byte
	binary.BigEndian.PutUint64(buf[:], uint64(bigmapid))
	copy(buf[8:], kh.Hash.Hash)
	return xxhash.Sum64(buf[:])
}

func (b *BigmapAlloc) GetKeyType() micheline.Type {
	if !b.KeyType.IsValid() {
		b.decodeType()
	}
	return b.KeyType
}

func (b *BigmapAlloc) GetValueType() micheline.Type {
	if !b.ValueType.IsValid() {
		b.decodeType()
	}
	return b.ValueType
}

func (b *BigmapAlloc) decodeType() {
	var prim micheline.Prim
	_ = prim.UnmarshalBinary(b.Data)
	if prim.IsValid() {
		b.KeyType = micheline.NewType(prim.Args[0])
		b.ValueType = micheline.NewType(prim.Args[1])
	}
}

func (b *BigmapAlloc) GetKeyTypeBytes() []byte {
	if !b.KeyType.IsValid() {
		b.decodeType()
	}
	buf, _ := b.KeyType.MarshalBinary()
	return buf
}

func (b *BigmapAlloc) GetValueTypeBytes() []byte {
	if !b.ValueType.IsValid() {
		b.decodeType()
	}
	buf, _ := b.ValueType.MarshalBinary()
	return buf
}

func NewBigmapAlloc(op *Op, b micheline.BigmapDiffElem) *BigmapAlloc {
	if b.Action != micheline.DiffActionAlloc {
		return nil
	}
	m := &BigmapAlloc{
		BigmapId:  b.Id,
		AccountId: op.ReceiverId,
		Height:    op.Height,
		Updated:   op.Height,
	}
	m.Data, _ = micheline.NewPairType(b.KeyType, b.ValueType).MarshalBinary()
	return m
}

func CopyBigmapAlloc(b *BigmapAlloc, op *Op, dst int64) *BigmapAlloc {
	m := &BigmapAlloc{
		BigmapId:  dst,
		AccountId: b.AccountId,
		Height:    op.Height,
		Updated:   op.Height,
		Data:      make([]byte, len(b.Data)),
	}
	if op.Type == OpTypeOrigination && b.BigmapId < 0 {
		m.AccountId = op.ReceiverId
	}
	copy(m.Data, b.Data)
	return m
}

func (b *BigmapAlloc) ToUpdate(op *Op) *BigmapUpdate {
	m := &BigmapUpdate{
		BigmapId:  b.BigmapId,
		KeyId:     0,
		Action:    micheline.DiffActionAlloc,
		OpId:      op.RowId,
		Height:    op.Height,
		Timestamp: op.Timestamp,
	}
	m.Key, _ = b.GetKeyType().MarshalBinary()
	m.Value, _ = b.GetValueType().MarshalBinary()
	return m
}

func (b *BigmapAlloc) ToUpdateCopy(op *Op, srcid int64) *BigmapUpdate {
	m := &BigmapUpdate{
		BigmapId:  b.BigmapId,
		KeyId:     uint64(srcid),
		Action:    micheline.DiffActionCopy,
		OpId:      op.RowId,
		Height:    op.Height,
		Timestamp: op.Timestamp,
	}
	m.Key, _ = b.GetKeyType().MarshalBinary()
	m.Value, _ = b.GetValueType().MarshalBinary()

	return m
}

func (m *BigmapAlloc) Reset() {
	m.RowId = 0
	m.BigmapId = 0
	m.AccountId = 0
	m.NUpdates = 0
	m.NKeys = 0
	m.Updated = 0
	m.Data = nil
	m.KeyType = micheline.Type{}
	m.ValueType = micheline.Type{}
}

// /tables/bigmap_values
type BigmapKV struct {
	RowId    uint64 `pack:"I,pk,snappy"    json:"row_id"`    // internal: id
	BigmapId int64  `pack:"B,snappy,bloom" json:"bigmap_id"` // unique bigmap id
	Height   int64  `pack:"h,snappy"       json:"height"`    // update height
	KeyId    uint64 `pack:"K,snappy"       json:"key_id"`    // xxhash(BigmapId, KeyHash)
	Key      []byte `pack:"k,snappy"       json:"key"`       // key/value bytes: binary encoded micheline.Prim Pair
	Value    []byte `pack:"v,snappy"       json:"value"`     // key/value bytes: binary encoded micheline.Prim Pair
}

var _ pack.Item = (*BigmapKV)(nil)

func (m *BigmapKV) ID() uint64 {
	return m.RowId
}

func (m *BigmapKV) SetID(id uint64) {
	m.RowId = id
}

func (b *BigmapKV) GetKey(typ micheline.Type) (micheline.Key, error) {
	return micheline.DecodeKey(typ, b.Key)
}

func (b *BigmapKV) GetValue(typ micheline.Type) micheline.Value {
	prim := micheline.Prim{}
	prim.UnmarshalBinary(b.Value)
	return micheline.NewValue(typ, prim)
}

func (b *BigmapKV) GetKeyHash() tezos.ExprHash {
	return micheline.KeyHash(b.Key)
}

func NewBigmapKV(b micheline.BigmapDiffElem, height int64) *BigmapKV {
	if b.Action != micheline.DiffActionUpdate {
		return nil
	}
	m := &BigmapKV{
		BigmapId: b.Id,
		KeyId:    GetKeyId(b.Id, b.KeyHash),
		Height:   height,
	}
	m.Key, _ = b.Key.MarshalBinary()
	m.Value, _ = b.Value.MarshalBinary()
	return m
}

func CopyBigmapKV(b *BigmapKV, dst, height int64) *BigmapKV {
	m := &BigmapKV{
		BigmapId: dst,
		Height:   height,
		KeyId:    GetKeyId(dst, b.GetKeyHash()),
		Key:      make([]byte, len(b.Key)),
		Value:    make([]byte, len(b.Value)),
	}
	copy(m.Key, b.Key)
	copy(m.Value, b.Value)
	return m
}

func (b *BigmapKV) ToUpdateCopy(op *Op) *BigmapUpdate {
	m := &BigmapUpdate{
		BigmapId:  b.BigmapId,
		KeyId:     b.KeyId,
		Action:    micheline.DiffActionUpdate,
		OpId:      op.RowId,
		Height:    op.Height,
		Timestamp: op.Timestamp,
		Key:       make([]byte, len(b.Key)),
		Value:     make([]byte, len(b.Value)),
	}
	copy(m.Key, b.Key)
	copy(m.Value, b.Value)
	return m
}

func (b *BigmapKV) ToUpdateRemove(op *Op) *BigmapUpdate {
	m := &BigmapUpdate{
		BigmapId:  b.BigmapId,
		KeyId:     b.KeyId,
		Action:    micheline.DiffActionRemove,
		OpId:      op.RowId,
		Height:    op.Height,
		Timestamp: op.Timestamp,
		Key:       make([]byte, len(b.Key)),
		Value:     nil,
	}
	copy(m.Key, b.Key)
	return m
}

func (m *BigmapKV) Reset() {
	m.RowId = 0
	m.BigmapId = 0
	m.Height = 0
	m.KeyId = 0
	m.Key = nil
	m.Value = nil
}

// /tables/bigmap_updates
type BigmapUpdate struct {
	RowId     uint64               `pack:"I,pk,snappy"     json:"row_id"`    // internal: id
	BigmapId  int64                `pack:"B,snappy,bloom"  json:"bigmap_id"` // unique bigmap id
	KeyId     uint64               `pack:"K,snappy"        json:"key_id"`    // xxhash(BigmapId, KeyHash)
	Action    micheline.DiffAction `pack:"a,snappy"        json:"action"`    // action (alloc, copy, update, remove)
	OpId      OpID                 `pack:"o,snappy"        json:"op_id"`     // operation id
	Height    int64                `pack:"h,snappy"        json:"height"`    // creation time
	Timestamp time.Time            `pack:"t,snappy"        json:"time"`      // creation height
	Key       []byte               `pack:"k,snappy"        json:"key"`       // key/value bytes: binary encoded micheline.Prim
	Value     []byte               `pack:"v,snappy"        json:"value"`     // key/value bytes: binary encoded micheline.Prim, (Pair(int,int) on copy)
}

var _ pack.Item = (*BigmapUpdate)(nil)

func (m *BigmapUpdate) ID() uint64 {
	return m.RowId
}

func (m *BigmapUpdate) SetID(id uint64) {
	m.RowId = id
}

func (b *BigmapUpdate) GetKey(typ micheline.Type) (micheline.Key, error) {
	return micheline.DecodeKey(typ, b.Key)
}

func (b *BigmapUpdate) GetKeyHash() tezos.ExprHash {
	return micheline.KeyHash(b.Key)
}

func (b *BigmapUpdate) GetValue(typ micheline.Type) micheline.Value {
	prim := micheline.Prim{}
	prim.UnmarshalBinary(b.Value)
	return micheline.NewValue(typ, prim)
}

func (b *BigmapUpdate) GetKeyType() micheline.Type {
	var prim micheline.Prim
	_ = prim.UnmarshalBinary(b.Key)
	return micheline.NewType(prim)
}

func (b *BigmapUpdate) GetValueType() micheline.Type {
	var prim micheline.Prim
	_ = prim.UnmarshalBinary(b.Value)
	return micheline.NewType(prim)
}

func NewBigmapUpdate(op *Op, b micheline.BigmapDiffElem) *BigmapUpdate {
	m := &BigmapUpdate{
		BigmapId:  b.Id,
		KeyId:     GetKeyId(b.Id, b.KeyHash),
		Action:    b.Action,
		OpId:      op.RowId,
		Height:    op.Height,
		Timestamp: op.Timestamp,
	}
	switch b.Action {
	case micheline.DiffActionAlloc:
		m.Key, _ = b.KeyType.MarshalBinary()
		m.Value, _ = b.ValueType.MarshalBinary()
	case micheline.DiffActionUpdate:
		m.Key, _ = b.Key.MarshalBinary()
		m.Value, _ = b.Value.MarshalBinary()
	case micheline.DiffActionCopy:
		m.BigmapId = b.DestId
		m.KeyId = uint64(b.SourceId)
		m.Key, _ = micheline.NewInt64(b.SourceId).MarshalBinary()
	case micheline.DiffActionRemove:
		if b.Key.IsValid() {
			m.Key, _ = b.Key.MarshalBinary()
		}
	}
	return m
}

func (b *BigmapUpdate) ToKV() *BigmapKV {
	m := &BigmapKV{
		BigmapId: b.BigmapId,
		KeyId:    b.KeyId,
		Height:   b.Height,
		Key:      make([]byte, len(b.Key)),
		Value:    make([]byte, len(b.Value)),
	}
	copy(m.Key, b.Key)
	copy(m.Value, b.Value)
	return m
}

func (b *BigmapUpdate) ToAlloc() *BigmapAlloc {
	return &BigmapAlloc{
		BigmapId:  b.BigmapId,
		KeyType:   b.GetKeyType(),
		ValueType: b.GetValueType(),
	}
}

func (m *BigmapUpdate) Reset() {
	m.RowId = 0
	m.BigmapId = 0
	m.KeyId = 0
	m.Action = 0
	m.OpId = 0
	m.Height = 0
	m.Timestamp = time.Time{}
	m.Key = nil
	m.Value = nil
}
