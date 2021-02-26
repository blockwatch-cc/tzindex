// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
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
	RowId          uint64            `pack:"I,pk,snappy"   json:"row_id"`          // internal: id, not height!
	Height         int64             `pack:"h,snappy"      json:"height"`          // bc: block height (also for orphans)
	Cycle          int64             `pack:"c,snappy"      json:"cycle"`           // bc: block cycle (tezos specific)
	Timestamp      time.Time         `pack:"T,snappy"      json:"time"`            // bc: block creation time
	OpId           OpID              `pack:"D,snappy"      json:"op_id"`           // unique row id from op table
	OpN            int               `pack:"0,snappy"      json:"op_n"`            // bc: position in block (block.Operations.([][]*OperationHeader) list position)
	OpL            int               `pack:"L,snappy"      json:"op_l"`            // bc: operation list (i.e. 0 for endorsements, etc corresponding to validation pass)
	OpP            int               `pack:"P,snappy"      json:"op_p"`            // bc: operation list position (use in combination with op_l to lookup op on RPC)
	OpC            int               `pack:"1,snappy"      json:"op_c"`            // bc: position in OperationHeader.Contents.([]Operation) list
	OpI            int               `pack:"2,snappy"      json:"op_i"`            // bc: position in internal operation result list
	AccountId      AccountID         `pack:"A,snappy"      json:"account_id"`      // unique account id
	AddressType    chain.AddressType `pack:"t,snappy"      json:"address_type"`    // address type, usable as filter
	CounterPartyId AccountID         `pack:"R,snappy"      json:"counterparty_id"` // counter account that initiated the flow
	Category       FlowCategory      `pack:"C,snappy"      json:"category"`        // sub-account that received the update
	Operation      FlowType          `pack:"O,snappy"      json:"operation"`       // op type that caused this update
	AmountIn       int64             `pack:"i,snappy"      json:"amount_in"`       // sum flowing in to the account
	AmountOut      int64             `pack:"o,snappy"      json:"amount_out"`      // sum flowing out of the account
	IsFee          bool              `pack:"e,snappy"      json:"is_fee"`          // flag to indicate this out-flow paid a fee
	IsBurned       bool              `pack:"b,snappy"      json:"is_burned"`       // flag to indicate this out-flow was burned
	IsFrozen       bool              `pack:"f,snappy"      json:"is_frozen"`       // flag to indicate this in-flow is frozen
	IsUnfrozen     bool              `pack:"u,snappy"      json:"is_unfrozen"`     // flag to indicate this flow (rewards -> balance) was unfrozen
	TokenGenMin    int64             `pack:"g,snappy"      json:"token_gen_min"`   // hops
	TokenGenMax    int64             `pack:"G,snappy"      json:"token_gen_max"`   // hops
	TokenAge       int64             `pack:"a,snappy"      json:"token_age"`       // time since last move in seconds
}

// Ensure Flow implements the pack.Item interface.
var _ pack.Item = (*Flow)(nil)

func (f *Flow) ID() uint64 {
	return f.RowId
}

func (f *Flow) SetID(id uint64) {
	f.RowId = id
}

func NewFlow(b *Block, acc *Account, cntr *Account, n, l, p, c, i int) *Flow {
	f := flowPool.Get().(*Flow)
	f.Height = b.Height
	f.Cycle = b.Cycle
	f.Timestamp = b.Timestamp
	f.AccountId = acc.RowId
	f.AddressType = acc.Type
	f.OpN = n
	f.OpL = l
	f.OpP = p
	f.OpC = c
	f.OpI = i
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
	f.OpId = 0
	f.OpN = 0
	f.OpL = 0
	f.OpP = 0
	f.OpC = 0
	f.OpI = 0
	f.AccountId = 0
	f.AddressType = 0
	f.CounterPartyId = 0
	f.Category = 0
	f.Operation = 0
	f.AmountIn = 0
	f.AmountOut = 0
	f.IsFee = false
	f.IsBurned = false
	f.IsFrozen = false
	f.IsUnfrozen = false
	f.TokenGenMin = 0
	f.TokenGenMax = 0
	f.TokenAge = 0
}
