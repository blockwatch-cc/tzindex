// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
)

const (
	DustLimit int64 = 1_000_000
)

var accountPool = &sync.Pool{
	New: func() interface{} { return new(Account) },
}

type AccountID uint64

func (id AccountID) Value() uint64 {
	return uint64(id)
}

type AccountRank struct {
	AccountId    AccountID
	Balance      int64 // total balance
	TxVolume24h  int64 // tx volume in+out
	TxTraffic24h int64 // number of tx in+out
	RichRank     int   // assigned rank based on balance
	VolumeRank   int   // assigned rank based on volume
	TrafficRank  int   // assigned rank based on traffic
}

// Account is an up-to-date snapshot of the current status of all known accounts.
// For history look at Op and Flow (balance updates) for baker info look at Baker,
// for smart contract info at Contract.
type Account struct {
	RowId            AccountID         `pack:"I,pk,snappy" json:"row_id"`
	Address          tezos.Address     `pack:"H,snappy"    json:"address"`
	Type             tezos.AddressType `pack:"t,snappy"    json:"address_type"`
	Pubkey           []byte            `pack:"k,snappy"    json:"pubkey"`
	Counter          int64             `pack:"j,snappy"    json:"counter"`
	BakerId          AccountID         `pack:"D,snappy"    json:"baker_id"`
	CreatorId        AccountID         `pack:"C,snappy"    json:"creator_id"`
	FirstIn          int64             `pack:"i,snappy"    json:"first_in"`
	FirstOut         int64             `pack:"o,snappy"    json:"first_out"`
	LastIn           int64             `pack:"J,snappy"    json:"last_in"`
	LastOut          int64             `pack:"O,snappy"    json:"last_out"`
	FirstSeen        int64             `pack:"0,snappy"    json:"first_seen"`
	LastSeen         int64             `pack:"l,snappy"    json:"last_seen"`
	DelegatedSince   int64             `pack:"+,snappy"    json:"delegated_since"`
	TotalReceived    int64             `pack:"R,snappy"    json:"total_received"`
	TotalSent        int64             `pack:"S,snappy"    json:"total_sent"`
	TotalBurned      int64             `pack:"B,snappy"    json:"total_burned"`
	TotalFeesPaid    int64             `pack:"F,snappy"    json:"total_fees_paid"`
	UnclaimedBalance int64             `pack:"U,snappy"    json:"unclaimed_balance"`
	SpendableBalance int64             `pack:"s,snappy"    json:"spendable_balance"`
	IsFunded         bool              `pack:"f,snappy"    json:"is_funded"`
	IsActivated      bool              `pack:"A,snappy"    json:"is_activated"`
	IsDelegated      bool              `pack:"=,snappy"    json:"is_delegated"`
	IsRevealed       bool              `pack:"r,snappy"    json:"is_revealed"`
	IsBaker          bool              `pack:"d,snappy"    json:"is_baker"`
	IsContract       bool              `pack:"c,snappy"    json:"is_contract"`
	NOps             int               `pack:"1,snappy"    json:"n_ops"`
	NOpsFailed       int               `pack:"2,snappy"    json:"n_ops_failed"`
	NTx              int               `pack:"3,snappy"    json:"n_tx"`
	NDelegation      int               `pack:"4,snappy"    json:"n_delegation"`
	NOrigination     int               `pack:"5,snappy"    json:"n_origination"`
	NConstants       int               `pack:"6,snappy"    json:"n_constants"`
	TokenGenMin      int64             `pack:"m,snappy"    json:"token_gen_min"`
	TokenGenMax      int64             `pack:"M,snappy"    json:"token_gen_max"`

	// used during block processing, not stored in DB
	IsNew       bool  `pack:"-" json:"-"` // first seen this block
	WasFunded   bool  `pack:"-" json:"-"` // true if account was funded before processing this block
	WasDust     bool  `pack:"-" json:"-"` // true if account had 0 < balance < 1tez at start of block
	IsDirty     bool  `pack:"-" json:"-"` // indicates an update happened
	MustDelete  bool  `pack:"-" json:"-"` // indicates the account should be deleted (during rollback)
	PrevBalance int64 `pack:"-" json:"-"` // previous balance before update
	PrevSeen    int64 `pack:"-" json:"-"` // previous balance height
}

// Ensure Account implements the pack.Item interface.
var _ pack.Item = (*Account)(nil)

func NewAccount(addr tezos.Address) *Account {
	acc := AllocAccount()
	acc.Type = addr.Type
	acc.Address = addr.Clone()
	acc.IsNew = true
	acc.IsDirty = true
	return acc
}

func AllocAccount() *Account {
	return accountPool.Get().(*Account)
}

func (a *Account) Equal(b *Account) bool {
	return a != nil && b != nil && a.RowId == b.RowId
}

func (a *Account) Free() {
	a.Reset()
	accountPool.Put(a)
}

func (a Account) ID() uint64 {
	return uint64(a.RowId)
}

func (a *Account) SetID(id uint64) {
	a.RowId = AccountID(id)
}

func (a Account) String() string {
	return a.Address.String()
}

func (a Account) Key() tezos.Key {
	key := tezos.Key{}
	if len(a.Pubkey) == 0 {
		return key
	}
	_ = key.UnmarshalBinary(a.Pubkey)
	return key
}

func (a Account) IsDust() bool {
	return a.SpendableBalance > 0 && a.SpendableBalance < DustLimit
}

func (a Account) Balance() int64 {
	return a.SpendableBalance
}

func (a *Account) Reset() {
	a.RowId = 0
	a.Address = tezos.Address{}
	a.Type = 0
	a.Pubkey = nil
	a.Counter = 0
	a.BakerId = 0
	a.CreatorId = 0
	a.FirstIn = 0
	a.FirstOut = 0
	a.LastIn = 0
	a.LastOut = 0
	a.FirstSeen = 0
	a.LastSeen = 0
	a.DelegatedSince = 0
	a.TotalReceived = 0
	a.TotalSent = 0
	a.TotalBurned = 0
	a.TotalFeesPaid = 0
	a.UnclaimedBalance = 0
	a.SpendableBalance = 0
	a.IsFunded = false
	a.IsActivated = false
	a.IsDelegated = false
	a.IsRevealed = false
	a.IsBaker = false
	a.IsContract = false
	a.NOps = 0
	a.NOpsFailed = 0
	a.NTx = 0
	a.NDelegation = 0
	a.NOrigination = 0
	a.NConstants = 0
	a.TokenGenMin = 1
	a.TokenGenMax = 1

	a.IsNew = false
	a.WasFunded = false
	a.WasDust = false
	a.IsDirty = false
	a.MustDelete = false
	a.PrevBalance = 0
	a.PrevSeen = 0
}

func (a *Account) UpdateBalanceN(flows []*Flow) error {
	for _, f := range flows {
		if err := a.UpdateBalance(f); err != nil {
			return err
		}
	}
	return nil
}

func (a *Account) UpdateBalance(f *Flow) error {
	a.IsDirty = true

	switch f.Category {
	case FlowCategoryBalance:
		if a.SpendableBalance < f.AmountOut-f.AmountIn {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.SpendableBalance, f.AmountOut-f.AmountIn)
		}
		switch f.Operation {
		case FlowTypeAirdrop, FlowTypeInvoice, FlowTypeSubsidy:
			a.TotalReceived += f.AmountIn

		case FlowTypeTransaction, FlowTypeOrigination, FlowTypeDelegation,
			FlowTypeReveal, FlowTypeRegisterConstant, FlowTypeDepositsLimit:
			// can pay fee, can pay burn, can send and receive
			if !f.IsBurned && !f.IsFee {
				// count send/received only for non-fee and non-burn flows
				a.TotalReceived += f.AmountIn
				a.TotalSent += f.AmountOut
			}
			if f.IsFee {
				// fee payments from balance
				a.TotalFeesPaid += f.AmountOut
			}
			if f.IsBurned {
				// Ithaca has balance-neutral burns (!)
				a.TotalBurned += f.AmountOut - f.AmountIn
			}

		case FlowTypeActivation:
			if a.UnclaimedBalance < f.AmountIn {
				return fmt.Errorf("acc.update id %d %s unclaimed balance %d is smaller than "+
					"activated amount %d", a.RowId, a, a.UnclaimedBalance, f.AmountIn)
			}
			a.UnclaimedBalance -= f.AmountIn
		}

		// for all types adjust spendable balance
		a.SpendableBalance += f.AmountIn - f.AmountOut

		// update token generation on in-flows only
		if f.AmountIn > 0 {
			// add +1 more hop
			a.TokenGenMin = util.Min64(a.TokenGenMin, f.TokenGenMin+1)
			a.TokenGenMax = util.Max64(a.TokenGenMax, f.TokenGenMax+1)
		}
	}

	// update in/out events for balance updates
	if f.Category == FlowCategoryBalance {
		if f.AmountIn > 0 {
			a.LastIn = f.Height
			if a.FirstIn == 0 {
				a.FirstIn = a.LastIn
			}
		}
		if f.AmountOut > 0 {
			a.LastOut = f.Height
			if a.FirstOut == 0 {
				a.FirstOut = a.LastOut
			}
		}
	}

	// any flow contributes to `last seen`
	a.IsFunded = a.IsBaker || a.SpendableBalance > 0
	a.LastSeen = util.Max64(a.LastSeen, f.Height)

	// reset token generation
	if !a.IsFunded {
		if !a.IsBaker {
			a.IsRevealed = false
		}
		a.TokenGenMin = 0
		a.TokenGenMax = 0
	}

	return nil
}

func (a *Account) RollbackBalanceN(flows []*Flow) error {
	for _, f := range flows {
		if err := a.RollbackBalance(f); err != nil {
			return err
		}
	}
	return nil
}

func (a *Account) RollbackBalance(f *Flow) error {
	a.IsDirty = true
	a.IsNew = a.FirstSeen == f.Height

	switch f.Category {

	case FlowCategoryBalance:
		if a.SpendableBalance < f.AmountIn-f.AmountOut {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.SpendableBalance, f.AmountIn-f.AmountOut)
		}

		switch f.Operation {
		case FlowTypeInvoice, FlowTypeAirdrop, FlowTypeSubsidy:
			a.TotalReceived -= f.AmountIn

		case FlowTypeTransaction, FlowTypeOrigination, FlowTypeDelegation,
			FlowTypeReveal, FlowTypeRegisterConstant, FlowTypeDepositsLimit:
			// can pay fee, can pay burn, can send and receive
			if !f.IsBurned && !f.IsFee {
				// count send/received only for non-fee and non-burn flows
				a.TotalReceived -= f.AmountIn
				a.TotalSent -= f.AmountOut
			}
			if f.IsFee {
				// fee payments from balance
				a.TotalFeesPaid -= f.AmountOut
			}
			if f.IsBurned {
				// Ithaca has balance-neutral burns (!)
				a.TotalBurned -= f.AmountOut - f.AmountIn
			}

		case FlowTypeActivation:
			a.UnclaimedBalance += f.AmountIn
		}

		// for all types adjust spendable balance
		a.SpendableBalance -= f.AmountIn - f.AmountOut

		// cannot update token age
	}

	// skip activity updates (too complex to track previous in/out heights)
	// and rely on subsequent block updates, we still set LastSeen to the current block
	a.IsFunded = a.IsBaker || a.SpendableBalance > 0
	a.LastSeen = util.Min64(f.Height, util.Max64(a.LastIn, a.LastOut))
	return nil
}

func (a Account) MarshalBinary() ([]byte, error) {
	le := binary.LittleEndian
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	binary.Write(buf, le, a.RowId)
	binary.Write(buf, le, int32(len(a.Address.Hash)))
	buf.Write(a.Address.Hash)
	binary.Write(buf, le, a.Type)
	binary.Write(buf, le, int32(len(a.Pubkey)))
	buf.Write(a.Pubkey)
	binary.Write(buf, le, a.Counter)
	binary.Write(buf, le, a.BakerId)
	binary.Write(buf, le, a.CreatorId)
	binary.Write(buf, le, a.FirstIn)
	binary.Write(buf, le, a.FirstOut)
	binary.Write(buf, le, a.LastIn)
	binary.Write(buf, le, a.LastOut)
	binary.Write(buf, le, a.FirstSeen)
	binary.Write(buf, le, a.LastSeen)
	binary.Write(buf, le, a.DelegatedSince)
	binary.Write(buf, le, a.TotalReceived)
	binary.Write(buf, le, a.TotalSent)
	binary.Write(buf, le, a.TotalBurned)
	binary.Write(buf, le, a.TotalFeesPaid)
	binary.Write(buf, le, a.UnclaimedBalance)
	binary.Write(buf, le, a.SpendableBalance)
	binary.Write(buf, le, a.IsFunded)
	binary.Write(buf, le, a.IsActivated)
	binary.Write(buf, le, a.IsDelegated)
	binary.Write(buf, le, a.IsRevealed)
	binary.Write(buf, le, a.IsBaker)
	binary.Write(buf, le, a.IsContract)
	binary.Write(buf, le, int32(a.NOps))
	binary.Write(buf, le, int32(a.NOpsFailed))
	binary.Write(buf, le, int32(a.NTx))
	binary.Write(buf, le, int32(a.NDelegation))
	binary.Write(buf, le, int32(a.NOrigination))
	binary.Write(buf, le, int32(a.NConstants))
	binary.Write(buf, le, a.TokenGenMin)
	binary.Write(buf, le, a.TokenGenMax)
	return buf.Bytes(), nil
}

func (a *Account) UnmarshalBinary(data []byte) error {
	le := binary.LittleEndian
	buf := bytes.NewBuffer(data)
	binary.Read(buf, le, &a.RowId)
	var l int32
	binary.Read(buf, le, &l)
	if l > 0 {
		if cap(a.Address.Hash) < int(l) {
			a.Address.Hash = make([]byte, int(l))
		}
		a.Address.Hash = a.Address.Hash[:int(l)]
		buf.Read(a.Address.Hash)
	}
	binary.Read(buf, le, &a.Type)
	a.Address.Type = a.Type
	binary.Read(buf, le, &l)
	if l > 0 {
		a.Pubkey = make([]byte, int(l))
		buf.Read(a.Pubkey)
	}
	binary.Read(buf, le, &a.Counter)
	binary.Read(buf, le, &a.BakerId)
	binary.Read(buf, le, &a.CreatorId)
	binary.Read(buf, le, &a.FirstIn)
	binary.Read(buf, le, &a.FirstOut)
	binary.Read(buf, le, &a.LastIn)
	binary.Read(buf, le, &a.LastOut)
	binary.Read(buf, le, &a.FirstSeen)
	binary.Read(buf, le, &a.LastSeen)
	binary.Read(buf, le, &a.DelegatedSince)
	binary.Read(buf, le, &a.TotalReceived)
	binary.Read(buf, le, &a.TotalSent)
	binary.Read(buf, le, &a.TotalBurned)
	binary.Read(buf, le, &a.TotalFeesPaid)
	binary.Read(buf, le, &a.UnclaimedBalance)
	binary.Read(buf, le, &a.SpendableBalance)
	binary.Read(buf, le, &a.IsFunded)
	binary.Read(buf, le, &a.IsActivated)
	binary.Read(buf, le, &a.IsDelegated)
	binary.Read(buf, le, &a.IsRevealed)
	binary.Read(buf, le, &a.IsBaker)
	binary.Read(buf, le, &a.IsContract)
	var n int32
	binary.Read(buf, le, &n)
	a.NOps = int(n)
	binary.Read(buf, le, &n)
	a.NOpsFailed = int(n)
	binary.Read(buf, le, &n)
	a.NTx = int(n)
	binary.Read(buf, le, &n)
	a.NDelegation = int(n)
	binary.Read(buf, le, &n)
	a.NOrigination = int(n)
	binary.Read(buf, le, &n)
	a.NConstants = int(n)
	binary.Read(buf, le, &a.TokenGenMin)
	binary.Read(buf, le, &a.TokenGenMax)
	return nil
}
