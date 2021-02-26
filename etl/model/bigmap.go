// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/micheline"
)

var ENoBigMapAlloc = errors.New("bigmap item is not an allocation")

var bigmapPool = &sync.Pool{
	New: func() interface{} { return new(BigMapItem) },
}

type BigMapItem struct {
	RowId       uint64               `pack:"I,pk,snappy"   json:"row_id"`       // internal: id
	PrevId      uint64               `pack:"P,snappy"      json:"prev_id"`      // row_id of previous value that's updated
	AccountId   AccountID            `pack:"A,snappy"      json:"account_id"`   // account table entry for contract
	ContractId  uint64               `pack:"C,snappy"      json:"contract_id"`  // contract table entry
	OpId        OpID                 `pack:"O,snappy"      json:"op_id"`        // operation id that created/updated/deleted the entry
	Height      int64                `pack:"h,snappy"      json:"height"`       // creation/update/deletion time
	Timestamp   time.Time            `pack:"T,snappy"      json:"time"`         // creation/update/deletion height
	BigMapId    int64                `pack:"B,snappy"      json:"bigmap_id"`    // id of the bigmap
	Action      micheline.DiffAction `pack:"a,snappy"      json:"action"`       // action
	KeyHash     []byte               `pack:"H"             json:"key_hash"`     // not compressedn because random
	KeyEncoding micheline.PrimType   `pack:"e,snappy"      json:"key_encoding"` // type of the key encoding
	KeyType     micheline.OpCode     `pack:"t,snappy"      json:"key_type"`     // type of the key encoding
	Key         []byte               `pack:"k,snappy"      json:"key"`          // key bytes: int: big.Int, string or []byte
	Value       []byte               `pack:"v,snappy"      json:"value"`        // value bytes: binary encoded micheline.Prim
	IsReplaced  bool                 `pack:"r,snappy"      json:"is_replaced"`  // flag to indicate this entry has been replaced by a newer entry
	IsDeleted   bool                 `pack:"d,snappy"      json:"is_deleted"`   // flag to indicate this key has been deleted
	IsCopied    bool                 `pack:"c,snappy"      json:"is_copied"`    // flag to indicate this key has been copied
	Counter     int64                `pack:"n,snappy"      json:"-"`            // running update counter
	NKeys       int64                `pack:"z,snappy"      json:"-"`            // current number of active keys
	Updated     int64                `pack:"u,snappy"      json:"-"`            // height at which this entry was replaced
}

// Ensure BigMapItem implements the pack.Item interface.
var _ pack.Item = (*BigMapItem)(nil)

func (m *BigMapItem) ID() uint64 {
	return m.RowId
}

func (m *BigMapItem) SetID(id uint64) {
	m.RowId = id
}

func AllocBigMapItem() *BigMapItem {
	return bigmapPool.Get().(*BigMapItem)
}

func (b *BigMapItem) GetKeyAs(typ *micheline.Prim) (*micheline.BigMapKey, error) {
	return micheline.DecodeBigMapKey(typ, b.Key)
}

func (b *BigMapItem) GetKeyType() (*micheline.Prim, error) {
	if b.Action != micheline.DiffActionAlloc {
		return nil, ENoBigMapAlloc
	}
	p := &micheline.Prim{}
	if err := p.UnmarshalBinary(b.Key); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *BigMapItem) GetValueType() (*micheline.Prim, error) {
	if b.Action != micheline.DiffActionAlloc {
		return nil, ENoBigMapAlloc
	}
	p := &micheline.Prim{}
	if err := p.UnmarshalBinary(b.Value); err != nil {
		return nil, err
	}
	return p, nil
}

// assuming BigMapDiffElem.Action is update or remove (copy & alloc are handled below)
func NewBigMapItem(o *Op, cc *Contract, b micheline.BigMapDiffElem, keyType micheline.OpCode, prev uint64, counter, nkeys int64) *BigMapItem {
	m := AllocBigMapItem()
	m.PrevId = prev
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigMapId = b.Id
	m.Action = b.Action
	m.Counter = counter
	m.NKeys = nkeys
	m.Updated = 0
	switch b.Action {
	case micheline.DiffActionUpdate, micheline.DiffActionRemove:
		m.KeyHash = b.KeyHash.Hash.Hash
		m.KeyType = keyType
		m.KeyEncoding = b.Encoding()
		m.Key, _ = b.Key.MarshalBinary()
		m.IsDeleted = b.Action == micheline.DiffActionRemove
		if !m.IsDeleted {
			m.Value, _ = b.Value.MarshalBinary()
		}

	case micheline.DiffActionAlloc:
		m.Key, _ = b.KeyType.MarshalBinary()
		m.KeyType = b.KeyType.OpCode
		m.KeyEncoding = b.Encoding()
		m.Value, _ = b.ValueType.MarshalBinary()

	case micheline.DiffActionCopy:
		// handled outside
	}
	return m
}

// assuming BigMapDiffElem.Action is alloc
func CopyBigMapAlloc(b *BigMapItem, o *Op, cc *Contract, dst, counter, nkeys int64) *BigMapItem {
	m := AllocBigMapItem()
	m.PrevId = b.RowId
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigMapId = dst
	m.Action = micheline.DiffActionAlloc
	m.KeyType = b.KeyType
	m.KeyEncoding = b.KeyEncoding
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
func CopyBigMapValue(b *BigMapItem, o *Op, cc *Contract, dst, counter, nkeys int64) *BigMapItem {
	m := AllocBigMapItem()
	m.PrevId = b.RowId
	m.AccountId = cc.AccountId
	m.ContractId = cc.RowId
	m.OpId = o.RowId
	m.Height = o.Height
	m.Timestamp = o.Timestamp
	m.BigMapId = dst
	m.Action = micheline.DiffActionUpdate
	m.KeyType = b.KeyType
	m.KeyEncoding = b.KeyEncoding
	m.KeyHash = make([]byte, len(b.KeyHash))
	copy(m.KeyHash, b.KeyHash)
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

func (m *BigMapItem) BigMapDiff() micheline.BigMapDiffElem {
	var d micheline.BigMapDiffElem
	d.Action = m.Action
	d.Id = m.BigMapId

	// unpack action-specific fields
	switch m.Action {
	case micheline.DiffActionUpdate, micheline.DiffActionRemove:
		d.KeyHash = chain.NewExprHash(m.KeyHash)
		d.Key = &micheline.Prim{}
		_ = d.Key.UnmarshalBinary(m.Key)
		if m.Action != micheline.DiffActionRemove {
			d.Value = &micheline.Prim{}
			_ = d.Value.UnmarshalBinary(m.Value)
		}

	case micheline.DiffActionAlloc:
		d.KeyType = &micheline.Prim{}
		_ = d.KeyType.UnmarshalBinary(m.Key) // encoded prim is stored as []byte
		d.ValueType = &micheline.Prim{}
		_ = d.ValueType.UnmarshalBinary(m.Value)

	case micheline.DiffActionCopy:
		d.DestId = m.BigMapId
		var z micheline.Z
		_ = z.UnmarshalBinary(m.Value)
		d.SourceId = z.Int64()
	}
	return d
}

func (m *BigMapItem) Free() {
	m.Reset()
	bigmapPool.Put(m)
}

func (m *BigMapItem) Reset() {
	m.RowId = 0
	m.AccountId = 0
	m.ContractId = 0
	m.OpId = 0
	m.Height = 0
	m.Timestamp = time.Time{}
	m.BigMapId = 0
	m.KeyHash = nil
	m.KeyType = 0
	m.KeyEncoding = 0
	m.Key = nil
	m.Value = nil
	m.IsReplaced = false
	m.IsDeleted = false
	m.IsCopied = false
	m.Counter = 0
	m.NKeys = 0
	m.Updated = 0
}
