// Copyright (c) 2020-2022 Blockwatch Data Inc.
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
	RowId        uint64    `pack:"I,pk"     json:"row_id"`
	Height       int64     `pack:"h"        json:"height"`
	Cycle        int64     `pack:"c"        json:"cycle"`
	IsSelected   bool      `pack:"s"        json:"is_selected"`
	Timestamp    time.Time `pack:"T"        json:"time"`
	Index        int       `pack:"i"        json:"index"`
	Rolls        int64     `pack:"r"        json:"rolls"`
	ActiveStake  int64     `pack:"K"        json:"active_stake"`
	AccountId    AccountID `pack:"a,bloom"  json:"account_id"`
	BakerId      AccountID `pack:"d"        json:"baker_id"`
	IsBaker      bool      `pack:"?"        json:"is_baker"`
	IsActive     bool      `pack:"v"        json:"is_active"`
	Balance      int64     `pack:"B"        json:"balance"`
	Delegated    int64     `pack:"D"        json:"delegated"`
	NDelegations int64     `pack:"n"        json:"n_delegations"`
	Since        int64     `pack:"S"        json:"since"`
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
	s.ActiveStake = 0
	s.AccountId = 0
	s.BakerId = 0
	s.IsBaker = false
	s.IsActive = false
	s.Balance = 0
	s.Delegated = 0
	s.NDelegations = 0
	s.Since = 0
}
