// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
)

var flowPool = &sync.Pool{
	New: func() interface{} { return new(Flow) },
}

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
	RowId          uint64       `pack:"I,pk,snappy"    json:"row_id"`
	Height         int64        `pack:"h,snappy"       json:"height"`
	Cycle          int64        `pack:"c,snappy"       json:"cycle"`
	Timestamp      time.Time    `pack:"T,snappy"       json:"time"`
	OpN            int          `pack:"1,snappy"       json:"op_n"`
	OpC            int          `pack:"2,snappy"       json:"op_c"`
	OpI            int          `pack:"3,snappy"       json:"op_i"`
	AccountId      AccountID    `pack:"A,snappy,bloom" json:"account_id"`
	CounterPartyId AccountID    `pack:"R,snappy"       json:"counterparty_id"` // account that initiated the flow
	Category       FlowCategory `pack:"C,snappy,bloom" json:"category"`        // sub-account that received the update
	Operation      FlowType     `pack:"O,snappy,bloom" json:"operation"`       // op type that caused this update
	AmountIn       int64        `pack:"i,snappy"       json:"amount_in"`       // sum flowing in to the account
	AmountOut      int64        `pack:"o,snappy"       json:"amount_out"`      // sum flowing out of the account
	IsFee          bool         `pack:"e,snappy"       json:"is_fee"`          // flag: out-flow paid a fee
	IsBurned       bool         `pack:"b,snappy"       json:"is_burned"`       // flag: out-flow was burned
	IsFrozen       bool         `pack:"f,snappy"       json:"is_frozen"`       // flag: in-flow is frozen
	IsUnfrozen     bool         `pack:"u,snappy"       json:"is_unfrozen"`     // flag: out-flow was unfrozen (rewards -> balance)
	IsShielded     bool         `pack:"y,snappy"       json:"is_shielded"`     // flag: in-flow was shielded (Sapling)
	IsUnshielded   bool         `pack:"Y,snappy"       json:"is_unshielded"`   // flag: out-flow was unshielded (Sapling)
	TokenAge       int64        `pack:"a,snappy"       json:"token_age"`       // time since last transfer in seconds

	TokenGenMin int64 `pack:"-"  json:"-"`
	TokenGenMax int64 `pack:"-"  json:"-"`
}

// Ensure Flow implements the pack.Item interface.
var _ pack.Item = (*Flow)(nil)

func (f *Flow) ID() uint64 {
	return f.RowId
}

func (f *Flow) SetID(id uint64) {
	f.RowId = id
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
	f.RowId = 0
	f.Height = 0
	f.Cycle = 0
	f.Timestamp = time.Time{}
	f.OpN = 0
	f.OpC = 0
	f.OpI = 0
	f.AccountId = 0
	f.CounterPartyId = 0
	f.Category = 0
	f.Operation = 0
	f.AmountIn = 0
	f.AmountOut = 0
	f.IsFee = false
	f.IsBurned = false
	f.IsFrozen = false
	f.IsUnfrozen = false
	f.IsShielded = false
	f.IsUnshielded = false
	f.TokenGenMin = 0
	f.TokenGenMax = 0
	f.TokenAge = 0
}
