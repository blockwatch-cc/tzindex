// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	"errors"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"github.com/cespare/xxhash/v2"
)

var (
	ErrNoBigmap        = errors.New("bigmap not indexed")
	ErrInvalidExprHash = errors.New("invalid expr hash")
)

const (
	BigmapAllocTableKey  = "bigmap_types"
	BigmapUpdateTableKey = "bigmap_updates"
	BigmapValueTableKey  = "bigmap_values"
)

// /tables/bigmaps
type BigmapAlloc struct {
	RowId     uint64    `pack:"I,pk"     json:"row_id"`        // internal: id
	BigmapId  int64     `pack:"B,i32"    json:"bigmap_id"`     // unique bigmap id
	AccountId AccountID `pack:"A,u32"    json:"account_id"`    // account table id for contract
	Height    int64     `pack:"h,i32"    json:"alloc_height"`  // allocation height
	NUpdates  int64     `pack:"n,i32"    json:"n_updates"`     // running update counter
	NKeys     int64     `pack:"k,i32"    json:"n_keys"`        // current number of active keys
	Updated   int64     `pack:"u,i32"    json:"update_height"` // last update height
	Deleted   int64     `pack:"D,i32"    json:"delete_height"` // block when bigmap was removed
	Data      []byte    `pack:"d,snappy" json:"-"`             // micheline encoded type tree (key/val pair)

	// internal, not stored
	KeyType   micheline.Type `pack:"-" json:"-"`
	ValueType micheline.Type `pack:"-" json:"-"`

	// nice to have: howto resolve ambigous types?
	// Name      string    `pack:"N,snappy" json:"name"`           // bigmap name used in contract annots
}

// Ensure bigmap items implement the pack.Item interface.
var _ pack.Item = (*BigmapAlloc)(nil)

func (m *BigmapAlloc) ID() uint64 {
	return m.RowId
}

func (m *BigmapAlloc) SetID(id uint64) {
	m.RowId = id
}

func (m BigmapAlloc) TableKey() string {
	return BigmapAllocTableKey
}

func (m BigmapAlloc) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    13,  // 8k pack size
		JournalSizeLog2: 12,  // 4k journal size
		CacheSize:       128, // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m BigmapAlloc) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func GetKeyId(bigmapid int64, kh tezos.ExprHash) uint64 {
	var buf [40]byte
	binary.BigEndian.PutUint64(buf[:], uint64(bigmapid))
	copy(buf[8:], kh[:])
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
	var prim micheline.Prim
	_ = prim.UnmarshalBinary(b.Data)
	buf, _ := prim.Args[0].MarshalBinary()
	return buf
}

func (b *BigmapAlloc) GetValueTypeBytes() []byte {
	var prim micheline.Prim
	_ = prim.UnmarshalBinary(b.Data)
	buf, _ := prim.Args[1].MarshalBinary()
	return buf
}

func NewBigmapAlloc(op *Op, b micheline.BigmapEvent) *BigmapAlloc {
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

func (b *BigmapAlloc) ToRemove(op *Op) *BigmapUpdate {
	return &BigmapUpdate{
		BigmapId:  b.BigmapId,
		Action:    micheline.DiffActionRemove,
		OpId:      op.RowId,
		Height:    op.Height,
		Timestamp: op.Timestamp,
	}
}

func (m *BigmapAlloc) Reset() {
	*m = BigmapAlloc{}
}

// /tables/bigmap_values
type BigmapValue struct {
	RowId    uint64 `pack:"I,pk"             json:"row_id"`    // internal: id
	BigmapId int64  `pack:"B,i32,bloom"      json:"bigmap_id"` // unique bigmap id
	Height   int64  `pack:"h,i32"            json:"height"`    // update height
	KeyId    uint64 `pack:"K,bloom=3,snappy" json:"key_id"`    // xxhash(BigmapId, KeyHash)
	Key      []byte `pack:"k,snappy"         json:"key"`       // key/value bytes: binary encoded micheline.Prim Pair
	Value    []byte `pack:"v,snappy"         json:"value"`     // key/value bytes: binary encoded micheline.Prim Pair
}

var _ pack.Item = (*BigmapValue)(nil)

func (m *BigmapValue) ID() uint64 {
	return m.RowId
}

func (m *BigmapValue) SetID(id uint64) {
	m.RowId = id
}

func (m BigmapValue) TableKey() string {
	return BigmapValueTableKey
}

func (m BigmapValue) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    14,   // 16k pack size
		JournalSizeLog2: 17,   // 128k journal size
		CacheSize:       2048, // max MB
		FillLevel:       100,  // boltdb fill level to limit reallocations
	}
}

func (m BigmapValue) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (b *BigmapValue) GetKey(typ micheline.Type) (micheline.Key, error) {
	return micheline.DecodeKey(typ, b.Key)
}

func (b *BigmapValue) GetValue(typ micheline.Type) micheline.Value {
	prim := micheline.Prim{}
	_ = prim.UnmarshalBinary(b.Value)
	return micheline.NewValue(typ, prim)
}

func (b *BigmapValue) GetKeyHash() tezos.ExprHash {
	return micheline.KeyHash(b.Key)
}

func NewBigmapValue(b micheline.BigmapEvent, height int64) *BigmapValue {
	if b.Action != micheline.DiffActionUpdate {
		return nil
	}
	m := &BigmapValue{
		BigmapId: b.Id,
		KeyId:    GetKeyId(b.Id, b.KeyHash),
		Height:   height,
	}
	m.Key, _ = b.Key.MarshalBinary()
	m.Value, _ = b.Value.MarshalBinary()
	return m
}

func CopyBigmapValue(b *BigmapValue, dst, height int64) *BigmapValue {
	m := &BigmapValue{
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

func (b *BigmapValue) ToUpdateCopy(op *Op) *BigmapUpdate {
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

func (b *BigmapValue) ToUpdateRemove(op *Op) *BigmapUpdate {
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

func (m *BigmapValue) Reset() {
	*m = BigmapValue{}
}

// /tables/bigmap_updates
type BigmapUpdate struct {
	RowId     uint64               `pack:"I,pk"             json:"row_id"`    // internal: id
	BigmapId  int64                `pack:"B,i32,bloom"      json:"bigmap_id"` // unique bigmap id
	KeyId     uint64               `pack:"K,bloom=3,snappy" json:"key_id"`    // xxhash(BigmapId, KeyHash)
	Action    micheline.DiffAction `pack:"a,u8"             json:"action"`    // action (alloc, copy, update, remove)
	OpId      OpID                 `pack:"o"                json:"op_id"`     // operation id
	Height    int64                `pack:"h,i32"            json:"height"`    // creation time
	Timestamp time.Time            `pack:"t"                json:"time"`      // creation height
	Key       []byte               `pack:"k,snappy"         json:"key"`       // key/value bytes: binary encoded micheline.Prim
	Value     []byte               `pack:"v,snappy"         json:"value"`     // key/value bytes: binary encoded micheline.Prim, (Pair(int,int) on copy)
}

var _ pack.Item = (*BigmapUpdate)(nil)

func (m *BigmapUpdate) ID() uint64 {
	return m.RowId
}

func (m *BigmapUpdate) SetID(id uint64) {
	m.RowId = id
}

func (m BigmapUpdate) TableKey() string {
	return BigmapUpdateTableKey
}

func (m BigmapUpdate) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,  // 32k pack size
		JournalSizeLog2: 15,  // 32k journal size
		CacheSize:       128, // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m BigmapUpdate) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (b *BigmapUpdate) GetKey(typ micheline.Type) (micheline.Key, error) {
	return micheline.DecodeKey(typ, b.Key)
}

func (b *BigmapUpdate) GetKeyHash() tezos.ExprHash {
	return micheline.KeyHash(b.Key)
}

func (b *BigmapUpdate) GetValue(typ micheline.Type) micheline.Value {
	prim := micheline.Prim{}
	_ = prim.UnmarshalBinary(b.Value)
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

func NewBigmapUpdate(op *Op, b micheline.BigmapEvent) *BigmapUpdate {
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
	case micheline.DiffActionRemove:
		if b.Key.IsValid() {
			m.Key, _ = b.Key.MarshalBinary()
		}
	}
	return m
}

func (b *BigmapUpdate) ToKV() *BigmapValue {
	m := &BigmapValue{
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

func (b *BigmapUpdate) ToEvent() micheline.BigmapEvent {
	ev := micheline.BigmapEvent{
		Action: b.Action,
		Id:     b.BigmapId,
	}
	switch b.Action {
	case micheline.DiffActionAlloc:
		_ = ev.KeyType.UnmarshalBinary(b.Key)
		_ = ev.ValueType.UnmarshalBinary(b.Value)
	case micheline.DiffActionUpdate:
		_ = ev.Key.UnmarshalBinary(b.Key)
		_ = ev.Value.UnmarshalBinary(b.Value)
		ev.KeyHash = micheline.KeyHash(b.Key)
	case micheline.DiffActionCopy:
		ev.DestId = b.BigmapId
		ev.SourceId = int64(b.KeyId)
	case micheline.DiffActionRemove:
		if len(b.Key) > 0 {
			_ = ev.Key.UnmarshalBinary(b.Key)
		}
	}
	return ev
}

func (m *BigmapUpdate) Reset() {
	*m = BigmapUpdate{}
}
