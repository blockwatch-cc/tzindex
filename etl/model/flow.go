// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
)

const FlowTableKey = "flow"

var (
	flowPool = &sync.Pool{
		New: func() interface{} { return new(Flow) },
	}

	ErrNoFlow = errors.New("flow not indexed")
)

// Flow captures individual account balance updates.
//
// Example 1
//
// A transaction that pays fees and moves funds from one account to another generates
// four distinct balance updates (1) fees paid, (2) fees received by baker, (3) funds sent,
// (4) funds received.
//
// If sender and/or receiver delegate their balance, 2 more updates adjusting the delegated
// amounts for the respective delegate accounts are created.
//
//
// Example 2
//
// A block baker receives (frozen) rewards and fees, and pays a deposit, generating four
// flows: (1) frozen rewards received, (2) frozen fees received, (3) deposit paid from
// balance, (4) deposit paid to frozen deposits.
//
// Example 2
//
// Unfreeze of deposits, fees and rewards flows from a freezer category to the balance
// category on the same account (i.e. an internal operation). This creates 2 distinct
// flows for each freezer category, one out-flow and a second in-flow to the balance category.

type Flow struct {
	RowId          uint64    `pack:"I,pk"               json:"row_id"`
	Height         int64     `pack:"h,i32,snappy"       json:"height"`
	Cycle          int64     `pack:"c,i16,snappy"       json:"cycle"`
	Timestamp      time.Time `pack:"T,snappy"           json:"time"`
	OpN            int       `pack:"1,i16,snappy"       json:"op_n"`
	OpC            int       `pack:"2,i16,snappy"       json:"op_c"`
	OpI            int       `pack:"3,i16,snappy"       json:"op_i"`
	AccountId      AccountID `pack:"A,u32,snappy,bloom" json:"account_id"`
	CounterPartyId AccountID `pack:"R,u32,snappy"       json:"counterparty_id"` // account that initiated the flow
	Kind           FlowKind  `pack:"C,u8,snappy,bloom"  json:"kind"`            // sub-account that received the update
	Type           FlowType  `pack:"O,u8,snappy,bloom"  json:"type"`            // op type that caused this update
	AmountIn       int64     `pack:"i,snappy"           json:"amount_in"`       // sum flowing in to the account
	AmountOut      int64     `pack:"o,snappy"           json:"amount_out"`      // sum flowing out of the account
	IsFee          bool      `pack:"e,snappy"           json:"is_fee"`          // flag: out-flow paid a fee
	IsBurned       bool      `pack:"b,snappy"           json:"is_burned"`       // flag: out-flow was burned
	IsFrozen       bool      `pack:"f,snappy"           json:"is_frozen"`       // flag: in-flow is frozen
	IsUnfrozen     bool      `pack:"u,snappy"           json:"is_unfrozen"`     // flag: out-flow was unfrozen (rewards -> balance)
	IsShielded     bool      `pack:"y,snappy"           json:"is_shielded"`     // flag: in-flow was shielded (Sapling)
	IsUnshielded   bool      `pack:"Y,snappy"           json:"is_unshielded"`   // flag: out-flow was unshielded (Sapling)
	TokenAge       int64     `pack:"a,i32,snappy"       json:"token_age"`       // time since last transfer in seconds
}

// Ensure Flow implements the pack.Item interface.
var _ pack.Item = (*Flow)(nil)

func (f *Flow) ID() uint64 {
	return f.RowId
}

func (f *Flow) SetID(id uint64) {
	f.RowId = id
}

func (m Flow) TableKey() string {
	return FlowTableKey
}

func (m Flow) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 15,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Flow) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

// be compatible with time series interface
func (f Flow) Time() time.Time {
	return f.Timestamp
}

func NewFlow(b *Block, acc *Account, cntr *Account, id OpRef) *Flow {
	f := flowPool.Get().(*Flow)
	f.Height = b.Height
	f.Cycle = b.Cycle
	f.Timestamp = b.Timestamp
	f.AccountId = acc.RowId
	f.OpN = id.N
	f.OpC = id.C
	f.OpI = id.I
	if cntr != nil {
		f.CounterPartyId = cntr.RowId
	}
	return f
}

func (f *Flow) Free() {
	f.Reset()
	flowPool.Put(f)
}

func (f *Flow) Reset() {
	*f = Flow{}
}
