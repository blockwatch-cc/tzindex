// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/cespare/xxhash"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

var ENoBigmapAlloc = errors.New("bigmap item is not an allocation")

var bigmapPool = &sync.Pool{
	New: func() interface{} { return new(BigmapItem) },
}

type BigmapItem struct {
	RowId      uint64               `pack:"I,pk,snappy"   json:"row_id"`      // internal: id
	KeyId      uint64               `pack:"K,snappy"      json:"key_id"`      // xxhash(BigmapId, KeyHash)
	PrevId     uint64               `pack:"P,snappy"      json:"prev_id"`     // row_id of previous value that's updated
	AccountId  AccountID            `pack:"A,snappy"      json:"account_id"`  // account table entry for contract
	ContractId ContractID           `pack:"C,snappy"      json:"contract_id"` // contract table entry
	OpId       OpID                 `pack:"O,snappy"      json:"op_id"`       // operation id that created/updated/deleted the entry
	Height     int64                `pack:"h,snappy"      json:"height"`      // creation/update/deletion time
	Timestamp  time.Time            `pack:"T,snappy"      json:"time"`        // creation/update/deletion height
	BigmapId   int64                `pack:"B,snappy"      json:"bigmap_id"`   // id of the bigmap
	Action     micheline.DiffAction `pack:"a,snappy"      json:"action"`      // action
	Key        []byte               `pack:"k,snappy"      json:"key"`         // key bytes: int: big.Int, string or []byte
	Value      []byte               `pack:"v,snappy"      json:"value"`       // value bytes: binary encoded micheline.Prim
	IsReplaced bool                 `pack:"r,snappy"      json:"is_replaced"` // flag to indicate this entry has been replaced by a newer entry
	IsDeleted  bool                 `pack:"d,snappy"      json:"is_deleted"`  // flag to indicate this key has been deleted
	IsCopied   bool                 `pack:"c,snappy"      json:"is_copied"`   // flag to indicate this key has been copied
	Counter    int64                `pack:"n,snappy"      json:"-"`           // running update counter
	NKeys      int64                `pack:"z,snappy"      json:"-"`           // current number of active keys
	Updated    int64                `pack:"u,snappy"      json:"-"`           // height at which this entry was replaced
}

// Ensure BigmapItem implements the pack.Item interface.
var _ pack.Item = (*BigmapItem)(nil)

func (m *BigmapItem) ID() uint64 {
	return m.RowId
}

func (m *BigmapItem) SetID(id uint64) {
	m.RowId = id
}

func AllocBigmapItem() *BigmapItem {
	return bigmapPool.Get().(*BigmapItem)
}

func GetKeyId(bigmapid int64, kh tezos.ExprHash) uint64 {
	var buf [40]byte
	binary.BigEndian.PutUint64(buf[:], uint64(bigmapid))
	copy(buf[8:], kh.Hash.Hash)
	return xxhash.Sum64(buf[:])
}

func (b *BigmapItem) GetKey(typ micheline.Type) (micheline.Key, error) {
	return micheline.DecodeKey(typ, b.Key)
}

func (b *BigmapItem) GetKeyHash() tezos.ExprHash {
	switch b.Action {
	case micheline.DiffActionAlloc, micheline.DiffActionCopy:
		return tezos.ExprHash{}
	}
	return micheline.KeyHash(b.Key)
}

func (b *BigmapItem) GetKeyType() (micheline.Type, error) {
	var typ micheline.Type
	if b.Action != micheline.DiffActionAlloc {
		return typ, ENoBigmapAlloc
	}
	err := typ.UnmarshalBinary(b.Key)
	return typ, err
}

func (b *BigmapItem) GetValueType() (micheline.Type, error) {
	var typ micheline.Type
	if b.Action != micheline.DiffActionAlloc {
		return typ, ENoBigmapAlloc
	}
	err := typ.UnmarshalBinary(b.Value)
	return typ, err
}

// assuming BigMapDiffElem.Action is update or remove (copy & alloc are handled below)
func NewBigmapItem(o *Op, cc *Contract, b micheline.BigmapDiffElem, prev uint64, counter, nkeys int64) *BigmapItem {
	m := AllocBigmapItem()
	m.PrevId = prev
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigmapId = b.Id
	m.Action = b.Action
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	switch b.Action {
	case micheline.DiffActionUpdate, micheline.DiffActionRemove:
		m.Key, _ = b.Key.MarshalBinary()
		m.IsDeleted = b.Action == micheline.DiffActionRemove
		if !m.IsDeleted {
			m.Value, _ = b.Value.MarshalBinary()
		}
		m.KeyId = GetKeyId(b.Id, b.KeyHash)

	case micheline.DiffActionAlloc:
		m.Key, _ = b.KeyType.MarshalBinary()
		m.Value, _ = b.ValueType.MarshalBinary()

	case micheline.DiffActionCopy:
		// handled outside
	}
	return m
}

// assuming BigMapDiffElem.Action is alloc
func CopyBigMapAlloc(b *BigmapItem, o *Op, cc *Contract, dst, counter, nkeys int64) *BigmapItem {
	m := AllocBigmapItem()
	m.PrevId = b.RowId
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigmapId = dst
	m.Action = micheline.DiffActionAlloc
	m.Key = make([]byte, len(b.Key))
	copy(m.Key, b.Key)
	m.Value = make([]byte, len(b.Value))
	copy(m.Value, b.Value)
	m.IsCopied = true
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	return m
}

// assuming BigMapDiffElem.Action is copy
func CopyBigMapValue(b *BigmapItem, o *Op, cc *Contract, dst, counter, nkeys int64) *BigmapItem {
	m := AllocBigmapItem()
	m.PrevId = b.RowId
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigmapId = dst
	m.Action = micheline.DiffActionUpdate
	m.Key = make([]byte, len(b.Key))
	copy(m.Key, b.Key)
	m.Value = make([]byte, len(b.Value))
	copy(m.Value, b.Value)
	m.IsCopied = true
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	return m
}

func (m *BigmapItem) BigmapDiff() micheline.BigmapDiffElem {
	var d micheline.BigmapDiffElem
	d.Action = m.Action
	d.Id = m.BigmapId

	// unpack action-specific fields
	switch m.Action {
	case micheline.DiffActionUpdate, micheline.DiffActionRemove:
		d.KeyHash = m.GetKeyHash()
		d.Key = micheline.Prim{}
		_ = d.Key.UnmarshalBinary(m.Key)
		if m.Action != micheline.DiffActionRemove {
			d.Value = micheline.Prim{}
			_ = d.Value.UnmarshalBinary(m.Value)
		}

	case micheline.DiffActionAlloc:
		d.KeyType = micheline.Prim{}
		_ = d.KeyType.UnmarshalBinary(m.Key) // encoded prim is stored as []byte
		d.ValueType = micheline.Prim{}
		_ = d.ValueType.UnmarshalBinary(m.Value)

	case micheline.DiffActionCopy:
		d.DestId = m.BigmapId
		var z micheline.Z
		_ = z.UnmarshalBinary(m.Value)
		d.SourceId = z.Int64()
	}
	return d
}

func (m *BigmapItem) Free() {
	m.Reset()
	bigmapPool.Put(m)
}

func (m *BigmapItem) Reset() {
	m.RowId = 0
	m.AccountId = 0
	m.ContractId = 0
	m.OpId = 0
	m.Height = 0
	m.Timestamp = time.Time{}
	m.BigmapId = 0
	m.Key = nil
	m.Value = nil
	m.IsReplaced = false
	m.IsDeleted = false
	m.IsCopied = false
	m.Counter = 0
	m.NKeys = 0
	m.Updated = 0
}
