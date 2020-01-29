// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
)

var snapshotPool = &sync.Pool{
	New: func() interface{} { return new(Snapshot) },
}

// Snapshot is an account balance snapshot made at a snapshot block.
type Snapshot struct {
	RowId        uint64    `pack:"I,pk,snappy" json:"row_id"`
	Height       int64     `pack:"h,snappy"    json:"height"`
	Cycle        int64     `pack:"c,snappy"    json:"cycle"`
	IsSelected   bool      `pack:"s,snappy"    json:"is_selected"`
	Timestamp    time.Time `pack:"T,snappy"    json:"time"`
	Index        int64     `pack:"i,snappy"    json:"index"`
	Rolls        int64     `pack:"r,snappy"    json:"rolls"`
	AccountId    AccountID `pack:"a,snappy"    json:"account_id"`
	DelegateId   AccountID `pack:"d,snappy"    json:"delegate_id"`
	IsDelegate   bool      `pack:"?,snappy"    json:"is_delegate"`
	IsActive     bool      `pack:"v,snappy"    json:"is_active"`
	Balance      int64     `pack:"B,snappy"    json:"balance"`
	Delegated    int64     `pack:"D,snappy"    json:"delegated"`
	NDelegations int64     `pack:"n,snappy"    json:"n_delegations"`
	Since        int64     `pack:"S,snappy"    json:"since"`
}

// Ensure Snapshot implements the pack.Item interface.
var _ pack.Item = (*Snapshot)(nil)

func NewSnapshot() *Snapshot {
	return allocSnapshot()
}

func allocSnapshot() *Snapshot {
	return snapshotPool.Get().(*Snapshot)
}

func (s *Snapshot) Free() {
	s.Reset()
	snapshotPool.Put(s)
}

func (s Snapshot) ID() uint64 {
	return uint64(s.RowId)
}

func (s *Snapshot) SetID(id uint64) {
	s.RowId = id
}

func (s *Snapshot) Reset() {
	s.RowId = 0
	s.Height = 0
	s.Cycle = 0
	s.IsSelected = false
	s.Timestamp = time.Time{}
	s.Index = 0
	s.Rolls = 0
	s.AccountId = 0
	s.DelegateId = 0
	s.IsDelegate = false
	s.IsActive = false
	s.Balance = 0
	s.Delegated = 0
	s.NDelegations = 0
	s.Since = 0
}
