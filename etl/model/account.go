// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
)

const (
	DustLimit       int64 = 10_000 // 0.01 tez
	AccountTableKey       = "account"
)

var (
	accountPool = &sync.Pool{
		New: func() interface{} { return new(Account) },
	}

	ErrNoAccount      = errors.New("account not indexed")
	ErrInvalidAddress = errors.New("invalid address")
)

type AccountID uint64

func (id AccountID) U64() uint64 {
	return uint64(id)
}

// Handle with care, this is OK as long as number of accounts < 4,294,967,295
func (id AccountID) U32() uint32 {
	return uint32(id)
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
	RowId            AccountID         `pack:"I,pk" json:"row_id"`
	Address          tezos.Address     `pack:"H"    json:"address"`
	Type             tezos.AddressType `pack:"t"    json:"address_type"`
	Pubkey           tezos.Key         `pack:"k"    json:"pubkey"`
	Counter          int64             `pack:"j"    json:"counter"`
	BakerId          AccountID         `pack:"D"    json:"baker_id"`
	CreatorId        AccountID         `pack:"C"    json:"creator_id"`
	FirstIn          int64             `pack:"i"    json:"first_in"`
	FirstOut         int64             `pack:"o"    json:"first_out"`
	LastIn           int64             `pack:"J"    json:"last_in"`
	LastOut          int64             `pack:"O"    json:"last_out"`
	FirstSeen        int64             `pack:"0"    json:"first_seen"`
	LastSeen         int64             `pack:"l"    json:"last_seen"`
	DelegatedSince   int64             `pack:"+"    json:"delegated_since"`
	TotalReceived    int64             `pack:"R"    json:"total_received"`
	TotalSent        int64             `pack:"S"    json:"total_sent"`
	TotalBurned      int64             `pack:"B"    json:"total_burned"`
	TotalFeesPaid    int64             `pack:"F"    json:"total_fees_paid"`
	TotalFeesUsed    int64             `pack:"u"    json:"total_fees_used"`
	UnclaimedBalance int64             `pack:"U"    json:"unclaimed_balance"`
	SpendableBalance int64             `pack:"s"    json:"spendable_balance"`
	FrozenBond       int64             `pack:"L"    json:"frozen_bond"`
	LostBond         int64             `pack:"X"    json:"lost_bond"`
	IsFunded         bool              `pack:"f"    json:"is_funded"`
	IsActivated      bool              `pack:"A"    json:"is_activated"`
	IsDelegated      bool              `pack:"="    json:"is_delegated"`
	IsRevealed       bool              `pack:"r"    json:"is_revealed"`
	IsBaker          bool              `pack:"d"    json:"is_baker"`
	IsContract       bool              `pack:"c"    json:"is_contract"`
	NTxSuccess       int               `pack:"1"    json:"n_tx_successs"`
	NTxFailed        int               `pack:"2"    json:"n_tx_failed"`
	NTxIn            int               `pack:"3"    json:"n_tx_in"`
	NTxOut           int               `pack:"4"    json:"n_tx_out"`

	// used during block processing, not stored in DB
	IsNew       bool  `pack:"-" json:"-"` // first seen this block
	WasFunded   bool  `pack:"-" json:"-"` // true if account was funded before processing this block
	WasDust     bool  `pack:"-" json:"-"` // true if account had 0 < balance < 1tez at start of block
	IsDirty     bool  `pack:"-" json:"-"` // indicates an update happened
	MustDelete  bool  `pack:"-" json:"-"` // indicates the account should be deleted (during rollback)
	PrevBalance int64 `pack:"-" json:"-"` // previous balance before update
}

// Ensure Account implements the pack.Item interface.
var _ pack.Item = (*Account)(nil)

func NewAccount(addr tezos.Address) *Account {
	acc := AllocAccount()
	acc.Type = addr.Type()
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

func (m Account) TableKey() string {
	return AccountTableKey
}

func (m Account) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    14,
		JournalSizeLog2: 17,
		CacheSize:       512,
		FillLevel:       100,
	}
}

func (m Account) IndexOpts(key string) pack.Options {
	return pack.Options{
		PackSizeLog2:    14,
		JournalSizeLog2: 17,
		CacheSize:       64,
		FillLevel:       90,
	}
}

func (a Account) String() string {
	return a.Address.String()
}

func (a Account) IsDust() bool {
	b := a.Balance()
	return b > 0 && b < DustLimit
}

func (a Account) Balance() int64 {
	return a.SpendableBalance + a.FrozenBond
}

func (a *Account) Reset() {
	*a = Account{}
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
			FlowTypeReveal, FlowTypeRegisterConstant, FlowTypeDepositsLimit,
			FlowTypeUpdateConsensusKey, FlowTypeDrain, FlowTypeTransferTicket:
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

	case FlowCategoryBond:
		if a.FrozenBond < f.AmountOut-f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen bond %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenBond, f.AmountOut-f.AmountIn)
		}
		a.FrozenBond += f.AmountIn - f.AmountOut
		if f.IsBurned {
			a.TotalBurned += f.AmountOut
			a.LostBond += f.AmountOut
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
	a.IsFunded = a.Balance() > 0
	a.LastSeen = util.Max64N(a.LastSeen, f.Height)

	// reset token generation
	if !a.IsFunded && !a.IsBaker {
		a.IsRevealed = false
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
	wasEmpty := a.Balance() == 0

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
			FlowTypeReveal, FlowTypeRegisterConstant, FlowTypeDepositsLimit,
			FlowTypeUpdateConsensusKey, FlowTypeDrain, FlowTypeTransferTicket:
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
	case FlowCategoryBond:
		if a.FrozenBond < f.AmountIn-f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen bond %d is smaller than "+
				"reversed incoming  amount %d", a.RowId, a, a.FrozenBond, f.AmountIn-f.AmountOut)
		}
		a.FrozenBond -= f.AmountIn - f.AmountOut
		if f.IsBurned {
			a.TotalBurned -= f.AmountOut
			a.LostBond -= f.AmountOut
		}
	}

	// reset revealed flag
	if a.IsFunded && wasEmpty && !a.IsRevealed && a.Pubkey.IsValid() {
		a.IsRevealed = true
	}

	// skip activity updates (too complex to track previous in/out heights)
	// and rely on subsequent block updates, we still set LastSeen to the current block
	a.IsFunded = a.Balance() > 0
	a.LastSeen = util.Min64(f.Height, util.Max64N(a.LastIn, a.LastOut))
	return nil
}

func (a Account) MarshalBinary() ([]byte, error) {
	le := binary.LittleEndian
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	_ = binary.Write(buf, le, a.RowId)
	_, _ = buf.Write(a.Address[:])
	b := a.Pubkey.Bytes()
	_ = binary.Write(buf, le, int32(len(b)))
	_, _ = buf.Write(b)
	_ = binary.Write(buf, le, a.Counter)
	_ = binary.Write(buf, le, a.BakerId)
	_ = binary.Write(buf, le, a.CreatorId)
	_ = binary.Write(buf, le, a.FirstIn)
	_ = binary.Write(buf, le, a.FirstOut)
	_ = binary.Write(buf, le, a.LastIn)
	_ = binary.Write(buf, le, a.LastOut)
	_ = binary.Write(buf, le, a.FirstSeen)
	_ = binary.Write(buf, le, a.LastSeen)
	_ = binary.Write(buf, le, a.DelegatedSince)
	_ = binary.Write(buf, le, a.TotalReceived)
	_ = binary.Write(buf, le, a.TotalSent)
	_ = binary.Write(buf, le, a.TotalBurned)
	_ = binary.Write(buf, le, a.TotalFeesPaid)
	_ = binary.Write(buf, le, a.TotalFeesUsed)
	_ = binary.Write(buf, le, a.UnclaimedBalance)
	_ = binary.Write(buf, le, a.SpendableBalance)
	_ = binary.Write(buf, le, a.FrozenBond)
	_ = binary.Write(buf, le, a.LostBond)
	_ = binary.Write(buf, le, a.IsFunded)
	_ = binary.Write(buf, le, a.IsActivated)
	_ = binary.Write(buf, le, a.IsDelegated)
	_ = binary.Write(buf, le, a.IsRevealed)
	_ = binary.Write(buf, le, a.IsBaker)
	_ = binary.Write(buf, le, a.IsContract)
	_ = binary.Write(buf, le, int32(a.NTxSuccess))
	_ = binary.Write(buf, le, int32(a.NTxFailed))
	_ = binary.Write(buf, le, int32(a.NTxIn))
	_ = binary.Write(buf, le, int32(a.NTxOut))
	return buf.Bytes(), nil
}

func (a *Account) UnmarshalBinary(data []byte) error {
	le := binary.LittleEndian
	buf := bytes.NewBuffer(data)
	_ = binary.Read(buf, le, &a.RowId)
	_ = a.Address.UnmarshalBinary(buf.Next(21))
	a.Type = a.Address.Type()
	var l int32
	_ = binary.Read(buf, le, &l)
	_ = a.Pubkey.UnmarshalBinary(buf.Next(int(l)))
	_ = binary.Read(buf, le, &a.Counter)
	_ = binary.Read(buf, le, &a.BakerId)
	_ = binary.Read(buf, le, &a.CreatorId)
	_ = binary.Read(buf, le, &a.FirstIn)
	_ = binary.Read(buf, le, &a.FirstOut)
	_ = binary.Read(buf, le, &a.LastIn)
	_ = binary.Read(buf, le, &a.LastOut)
	_ = binary.Read(buf, le, &a.FirstSeen)
	_ = binary.Read(buf, le, &a.LastSeen)
	_ = binary.Read(buf, le, &a.DelegatedSince)
	_ = binary.Read(buf, le, &a.TotalReceived)
	_ = binary.Read(buf, le, &a.TotalSent)
	_ = binary.Read(buf, le, &a.TotalBurned)
	_ = binary.Read(buf, le, &a.TotalFeesPaid)
	_ = binary.Read(buf, le, &a.TotalFeesUsed)
	_ = binary.Read(buf, le, &a.UnclaimedBalance)
	_ = binary.Read(buf, le, &a.SpendableBalance)
	_ = binary.Read(buf, le, &a.FrozenBond)
	_ = binary.Read(buf, le, &a.LostBond)
	_ = binary.Read(buf, le, &a.IsFunded)
	_ = binary.Read(buf, le, &a.IsActivated)
	_ = binary.Read(buf, le, &a.IsDelegated)
	_ = binary.Read(buf, le, &a.IsRevealed)
	_ = binary.Read(buf, le, &a.IsBaker)
	_ = binary.Read(buf, le, &a.IsContract)
	var n int32
	_ = binary.Read(buf, le, &n)
	a.NTxSuccess = int(n)
	_ = binary.Read(buf, le, &n)
	a.NTxFailed = int(n)
	_ = binary.Read(buf, le, &n)
	a.NTxIn = int(n)
	_ = binary.Read(buf, le, &n)
	a.NTxOut = int(n)
	_ = binary.Read(buf, le, &n)
	return nil
}
