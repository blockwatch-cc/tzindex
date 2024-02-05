// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
)

const (
	SnapshotTableKey        = "snapshot"
	SnapshotStagingTableKey = "snapstage"
)

var (
	snapshotPool = &sync.Pool{
		New: func() interface{} { return new(Snapshot) },
	}

	ErrNoSnapshot = errors.New("snapshot not indexed")
)

// Snapshot is an account balance snapshot made at a snapshot block.
type Snapshot struct {
	RowId          uint64    `pack:"I,pk"         json:"row_id"`
	Height         int64     `pack:"h,i32"        json:"height"`
	Cycle          int64     `pack:"c,i16"        json:"cycle"`
	Timestamp      time.Time `pack:"T"            json:"time"`
	Index          int       `pack:"i,i8"         json:"index"`
	OwnStake       int64     `pack:"O"            json:"own_stake"`
	StakingBalance int64     `pack:"K"            json:"staking_balance"`
	AccountId      AccountID `pack:"a,u32,bloom"  json:"account_id"`
	BakerId        AccountID `pack:"d,u32"        json:"baker_id"`
	IsBaker        bool      `pack:"?,snappy"     json:"is_baker"`
	IsActive       bool      `pack:"v,snappy"     json:"is_active"`
	Balance        int64     `pack:"B"            json:"balance"`
	Delegated      int64     `pack:"D"            json:"delegated"`
	NDelegations   int64     `pack:"n,i32"        json:"n_delegations"`
	NStakers       int64     `pack:"#,i32"        json:"n_stakers"`
	Since          int64     `pack:"S,i32"        json:"since"`
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
	return s.RowId
}

func (s *Snapshot) SetID(id uint64) {
	s.RowId = id
}

func (m Snapshot) TableKey() string {
	return SnapshotTableKey
}

func (m Snapshot) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 18,
		CacheSize:       128,
		FillLevel:       100,
	}
}

func (m Snapshot) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (s *Snapshot) Reset() {
	*s = Snapshot{}
}
