// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"blockwatch.cc/packdb/pack"
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
	RowId            AccountID         `pack:"I,pk"      json:"row_id"`
	Address          tezos.Address     `pack:"H,bloom=3" json:"address"`
	Type             tezos.AddressType `pack:"t,u8"      json:"address_type"`
	Pubkey           tezos.Key         `pack:"k"         json:"pubkey"`
	Counter          int64             `pack:"j,i32"     json:"counter"`
	BakerId          AccountID         `pack:"D"         json:"baker_id"`
	CreatorId        AccountID         `pack:"C"         json:"creator_id"`
	FirstIn          int64             `pack:"i,i32"     json:"first_in"`
	FirstOut         int64             `pack:"o,i32"     json:"first_out"`
	LastIn           int64             `pack:"J,i32"     json:"last_in"`
	LastOut          int64             `pack:"O,i32"     json:"last_out"`
	FirstSeen        int64             `pack:"0,i32"     json:"first_seen"`
	LastSeen         int64             `pack:"l,i32"     json:"last_seen"`
	DelegatedSince   int64             `pack:"+,i32"     json:"delegated_since"`
	TotalReceived    int64             `pack:"R"         json:"total_received"`
	TotalSent        int64             `pack:"S"         json:"total_sent"`
	TotalBurned      int64             `pack:"B"         json:"total_burned"`
	TotalFeesPaid    int64             `pack:"F"         json:"total_fees_paid"`
	TotalFeesUsed    int64             `pack:"u"         json:"total_fees_used"`
	UnclaimedBalance int64             `pack:"U"         json:"unclaimed_balance"`
	SpendableBalance int64             `pack:"s"         json:"spendable_balance"`
	FrozenRollupBond int64             `pack:"L"         json:"frozen_rollup_bond"` // rollup
	LostRollupBond   int64             `pack:"X"         json:"lost_rollup_bond"`   // rollup
	StakedBalance    int64             `pack:"Y"         json:"staked_balance"`     // stake
	UnstakedBalance  int64             `pack:"Z"         json:"unstaked_balance"`   // stake
	LostStake        int64             `pack:"V"         json:"lost_stake"`         // stake
	StakeShares      int64             `pack:"W"         json:"stake_shares"`       // stake
	IsFunded         bool              `pack:"f,snappy"  json:"is_funded"`
	IsActivated      bool              `pack:"A,snappy"  json:"is_activated"`
	IsDelegated      bool              `pack:"=,snappy"  json:"is_delegated"`
	IsStaked         bool              `pack:"?,snappy"  json:"is_staked"`
	IsRevealed       bool              `pack:"r,snappy"  json:"is_revealed"`
	IsBaker          bool              `pack:"d,snappy"  json:"is_baker"`
	IsContract       bool              `pack:"c,snappy"  json:"is_contract"`
	NTxSuccess       int               `pack:"1,i32"     json:"n_tx_success"`
	NTxFailed        int               `pack:"2,i32"     json:"n_tx_failed"`
	NTxIn            int               `pack:"3,i32"     json:"n_tx_in"`
	NTxOut           int               `pack:"4,i32"     json:"n_tx_out"`

	// used during block processing, not stored in DB
	IsNew       bool   `pack:"-" json:"-"` // first seen this block
	WasFunded   bool   `pack:"-" json:"-"` // true if account was funded before processing this block
	WasDust     bool   `pack:"-" json:"-"` // true if account had 0 < balance < 1tez at start of block
	IsDirty     bool   `pack:"-" json:"-"` // indicates an update happened
	MustDelete  bool   `pack:"-" json:"-"` // indicates the account should be deleted (during rollback)
	PrevBalance int64  `pack:"-" json:"-"` // previous balance before update
	Baker       *Baker `pack:"-" json:"-"` // used for stake updates
	AuxBaker    *Baker `pack:"-" json:"-"` // used for unstake during delegation
}

// Ensure Account implements the pack.Item interface.
var _ pack.Item = (*Account)(nil)

func NewAccount(addr tezos.Address) *Account {
	acc := AllocAccount()
	acc.Type = addr.Type()
	acc.Address = addr
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
	return pack.NoOptions
}

func (a Account) String() string {
	return a.Address.String()
}

func (a Account) IsDust() bool {
	b := a.Balance()
	return b > 0 && b < DustLimit
}

func (a Account) Balance() int64 {
	return a.SpendableBalance + a.FrozenRollupBond + a.UnstakedBalance
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

	switch f.Kind {
	case FlowKindBalance:
		if a.SpendableBalance < f.AmountOut-f.AmountIn {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.SpendableBalance, f.AmountOut-f.AmountIn)
		}
		switch f.Type {
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

	case FlowKindBond:
		if a.FrozenRollupBond < f.AmountOut-f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen bond %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenRollupBond, f.AmountOut-f.AmountIn)
		}
		a.FrozenRollupBond += f.AmountIn - f.AmountOut
		if f.IsBurned {
			a.TotalBurned += f.AmountOut
			a.LostRollupBond += f.AmountOut
		}

	case FlowKindStake:
		switch f.Type {
		case FlowTypeStake:
			// log.Infof("Stake: %s %d", a, f.AmountIn)
			// log.Infof(">> before total_stake=%d total_shares=%d my_stake=%d my_shares=%d",
			// 	a.Baker.TotalStake, a.Baker.TotalShares, a.StakedBalance, a.StakeShares)

			// Note: minted shares may be zero due to rounding
			sh := a.Baker.MintStake(f.AmountIn)
			if sh < 0 {
				// log.Warnf("ACC %#v", a)
				// log.Warnf("BKR %#v", a.Baker)
				return fmt.Errorf("acc.update id %d %s neg stake %d shares for amount %d",
					a.RowId, a, sh, f.AmountIn)
			}
			if a.StakeShares == 0 {
				a.Baker.ActiveStakers++
			}
			a.StakedBalance += f.AmountIn
			a.StakeShares += sh
			a.IsStaked = a.StakeShares > 0
			// log.Infof(">> after total_stake=%d total_shares=%d my_stake=%d my_shares=%d",
			// 	a.Baker.TotalStake, a.Baker.TotalShares, a.StakedBalance, a.StakeShares)
			// log.Infof(">> minted %d shares for %d stake, bkr: %s stake=%d shares=%d",
			// 	sh, f.AmountIn, a.Baker, a.Baker.TotalStake, a.Baker.TotalShares)

		case FlowTypeUnstake:
			// log.Infof("Unstake: %s %d", a, f.AmountOut)
			// handle only out flow
			if f.AmountOut > 0 {
				// Oxford: select the correct baker
				// - an implicit unstake happens during re-delegation, but the new baker
				// is already assigned to the account by the operation decoder; for this
				// case to work we must check which baker is referenced by the stake flow
				bkr := a.Baker
				if bkr == nil || bkr.AccountId != f.CounterPartyId {
					bkr = a.AuxBaker
				}
				if bkr == nil || bkr.AccountId != f.CounterPartyId {
					return fmt.Errorf("acc.update A_%d %s invalid baker %d referenced by %s flow",
						a.RowId, a, f.CounterPartyId, f.Type)
				}

				// log.Infof(">> before total_stake=%d total_shares=%d my_stake=%d my_shares=%d",
				// 	bkr.TotalStake, bkr.TotalShares, a.StakedBalance, a.StakeShares)

				// calculate how much of the current baker pool is owned by us?
				ownAmount := bkr.StakeAmount(a.StakeShares)

				if ownAmount < a.StakedBalance {
					// log.Warnf("ACC %#v", a)
					return fmt.Errorf("acc.update A_%d %s owned stake %d (%d shares) is smaller than "+
						"frozen principal %d", a.RowId, a, ownAmount, a.StakeShares, a.StakedBalance)
				}

				pendingReward := ownAmount - a.StakedBalance
				// log.Infof(">> own=%d reward=%d principal=%d", ownAmount, pendingReward, ownAmount-pendingReward)

				// reduce stake principal or just unstake rewards?
				a.StakedBalance -= f.AmountOut - pendingReward
				a.UnstakedBalance += f.AmountOut

				// Note: burned shares may be zero due to rounding
				sh := bkr.BurnStake(f.AmountOut)
				if sh < 0 {
					// log.Warnf("ACC %#v", a)
					// log.Warnf("BKR %#v", bkr)
					return fmt.Errorf("acc.update A_%d %s neg stake %d shares for amount %d",
						a.RowId, a, sh, f.AmountOut)
				}
				if sh > a.StakeShares {
					// log.Warnf("ACC %#v", a)
					// log.Warnf("BKR %#v", bkr)
					return fmt.Errorf("acc.update A_%d %s burned shares %d > own shares %d",
						a.RowId, a, sh, a.StakeShares)
				}
				a.StakeShares -= sh
				a.IsStaked = a.StakeShares > 0
				if a.StakeShares == 0 {
					bkr.ActiveStakers--
				}
				// log.Infof(">> after total_stake=%d total_shares=%d my_stake=%d my_shares=%d",
				// 	bkr.TotalStake, bkr.TotalShares, a.StakedBalance, a.StakeShares)

				// log.Infof(">> burned %d shares, bkr: %s stake=%d shares=%d",
				// 	sh, bkr, bkr.TotalStake, bkr.TotalShares)
			}

		case FlowTypeFinalizeUnstake:
			// log.Infof("Finalize: %s %d", a, f.AmountOut)
			if a.UnstakedBalance < f.AmountOut {
				return fmt.Errorf("acc.update A_%d %s unfrozen stake %d is smaller than "+
					"outgoing amount %d", a.RowId, a, a.UnstakedBalance, f.AmountOut)
			}
			a.UnstakedBalance -= f.AmountOut

			// log.Infof(">> unstaked_balance=%d, bkr: %s stake=%d shares=%d",
			// 	a.UnstakedBalance, a.Baker, a.Baker.TotalStake, a.Baker.TotalShares)

		case FlowTypePenalty:
			// slash at end of cycle
			// log.Infof("Penalty: %s %d", a, f.AmountOut)
			ownAmount := a.Baker.StakeAmount(a.StakeShares)
			pendingReward := ownAmount - a.StakedBalance
			a.StakedBalance -= f.AmountOut - pendingReward
			a.LostStake += f.AmountOut

			// log.Infof(">> staked_balance=%d, bkr: %s stake=%d shares=%d",
			// 	a.StakedBalance, a.Baker, a.Baker.TotalStake, a.Baker.TotalShares)

			// WIP
			// howto handle slashing losses for stakers?
		}
	}

	// update in/out events for balance updates
	if f.Kind == FlowKindBalance {
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
	a.LastSeen = max(a.LastSeen, f.Height)

	// reset revealed flag
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

	switch f.Kind {

	case FlowKindBalance:
		if a.SpendableBalance < f.AmountIn-f.AmountOut {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.SpendableBalance, f.AmountIn-f.AmountOut)
		}

		switch f.Type {
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
	case FlowKindBond:
		if a.FrozenRollupBond < f.AmountIn-f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen bond %d is smaller than "+
				"reversed incoming  amount %d", a.RowId, a, a.FrozenRollupBond, f.AmountIn-f.AmountOut)
		}
		a.FrozenRollupBond -= f.AmountIn - f.AmountOut
		if f.IsBurned {
			a.TotalBurned -= f.AmountOut
			a.LostRollupBond -= f.AmountOut
		}

	case FlowKindStake:
		switch f.Type {
		case FlowTypeStake:
			a.StakeShares -= a.Baker.BurnStake(f.AmountIn)
			if a.StakeShares < 0 {
				a.StakeShares = 0
			}
			if a.StakeShares == 0 {
				a.Baker.ActiveStakers--
			}
			a.StakedBalance -= f.AmountIn
			a.IsStaked = a.StakeShares > 0

		case FlowTypeUnstake:
			// handle only out flow
			if f.AmountOut > 0 {
				// Oxford: select the correct baker
				// - an implicit unstake happens during re-delegation, but the new baker
				// is already assigned to the account by the operation decoder; for this
				// case to work we must check which baker is referenced by the stake flow
				bkr := a.Baker
				if bkr == nil || bkr.AccountId != f.CounterPartyId {
					bkr = a.AuxBaker
				}
				if bkr == nil || bkr.AccountId != f.CounterPartyId {
					return fmt.Errorf("acc.update A_%d %s invalid baker %d referenced by %s flow",
						a.RowId, a, f.CounterPartyId, f.Type)
				}

				// Note: correct rollback impossible because previous StakedBalance
				// (stake principal) cannot be computed backwards
				if a.StakeShares == 0 {
					bkr.ActiveStakers++
				}

				// reverse share burn by minting
				a.StakeShares += bkr.MintStake(f.AmountOut)
				a.IsStaked = a.StakeShares > 0
				a.UnstakedBalance -= f.AmountOut

				// FIXME: reverse calculate rewards/principal split
				// add full withdrawn amount to frozen stake (this underestimates
				// rewards and overestimates stake, but remains factually correct)
				a.StakedBalance += f.AmountOut
			}
		case FlowTypeFinalizeUnstake:
			a.UnstakedBalance += f.AmountOut

		case FlowTypePenalty:
			// revert slash at end of cycle
			// FIXME: reverse calculate rewards/principal split
			a.StakedBalance += f.AmountOut
			a.LostStake -= f.AmountOut
		}
	}

	// reset revealed flag
	if a.IsFunded && wasEmpty && !a.IsRevealed && a.Pubkey.IsValid() {
		a.IsRevealed = true
	}

	// skip activity updates (too complex to track previous in/out heights)
	// and rely on subsequent block updates, we still set LastSeen to the current block
	a.IsFunded = a.Balance() > 0
	a.LastSeen = min(f.Height, max(a.LastIn, a.LastOut))

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
	_ = binary.Write(buf, le, a.FrozenRollupBond)
	_ = binary.Write(buf, le, a.LostRollupBond)
	_ = binary.Write(buf, le, a.StakedBalance)
	_ = binary.Write(buf, le, a.UnstakedBalance)
	_ = binary.Write(buf, le, a.LostStake)
	_ = binary.Write(buf, le, a.StakeShares)
	_ = binary.Write(buf, le, a.IsFunded)
	_ = binary.Write(buf, le, a.IsActivated)
	_ = binary.Write(buf, le, a.IsDelegated)
	_ = binary.Write(buf, le, a.IsStaked)
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
	_ = binary.Read(buf, le, &a.FrozenRollupBond)
	_ = binary.Read(buf, le, &a.LostRollupBond)
	_ = binary.Read(buf, le, &a.StakedBalance)
	_ = binary.Read(buf, le, &a.UnstakedBalance)
	_ = binary.Read(buf, le, &a.LostStake)
	_ = binary.Read(buf, le, &a.StakeShares)
	_ = binary.Read(buf, le, &a.IsFunded)
	_ = binary.Read(buf, le, &a.IsActivated)
	_ = binary.Read(buf, le, &a.IsDelegated)
	_ = binary.Read(buf, le, &a.IsStaked)
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
