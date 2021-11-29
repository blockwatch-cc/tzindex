// Copyright (c) 2020-2021 Blockwatch Data Inc.
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

// Account is an up-to-date snapshot of the current status. For history look at Flow (balance updates).
type Account struct {
	RowId              AccountID         `pack:"I,pk,snappy" json:"row_id"`
	Address            tezos.Address     `pack:"H"           json:"address"`
	DelegateId         AccountID         `pack:"D,snappy"    json:"delegate_id"`
	CreatorId          AccountID         `pack:"M,snappy"    json:"creator_id"`
	Pubkey             []byte            `pack:"k"           json:"pubkey"`
	Type               tezos.AddressType `pack:"t,snappy"    json:"address_type"`
	FirstIn            int64             `pack:"i,snappy"    json:"first_in"`
	FirstOut           int64             `pack:"o,snappy"    json:"first_out"`
	LastIn             int64             `pack:"J,snappy"    json:"last_in"`
	LastOut            int64             `pack:"O,snappy"    json:"last_out"`
	FirstSeen          int64             `pack:"0,snappy"    json:"first_seen"`
	LastSeen           int64             `pack:"l,snappy"    json:"last_seen"`
	DelegatedSince     int64             `pack:"+,snappy"    json:"delegated_since"`
	DelegateSince      int64             `pack:"*,snappy"    json:"delegate_since"`
	DelegateUntil      int64             `pack:"/,snappy"    json:"delegate_until"`
	TotalReceived      int64             `pack:"R,snappy"    json:"total_received"`
	TotalSent          int64             `pack:"S,snappy"    json:"total_sent"`
	TotalBurned        int64             `pack:"B,snappy"    json:"total_burned"`
	TotalFeesPaid      int64             `pack:"F,snappy"    json:"total_fees_paid"`
	TotalRewardsEarned int64             `pack:"W,snappy"    json:"total_rewards_earned"`
	TotalFeesEarned    int64             `pack:"E,snappy"    json:"total_fees_earned"`
	TotalLost          int64             `pack:"L,snappy"    json:"total_lost"` // lost due to denounciation
	FrozenDeposits     int64             `pack:"z,snappy"    json:"frozen_deposits"`
	FrozenRewards      int64             `pack:"Z,snappy"    json:"frozen_rewards"`
	FrozenFees         int64             `pack:"Y,snappy"    json:"frozen_fees"`
	UnclaimedBalance   int64             `pack:"U,snappy"    json:"unclaimed_balance"` // vesting or not activated
	SpendableBalance   int64             `pack:"s,snappy"    json:"spendable_balance"`
	DelegatedBalance   int64             `pack:"~,snappy"    json:"delegated_balance"`
	TotalDelegations   int64             `pack:">,snappy"    json:"total_delegations"`  // from delegate ops
	ActiveDelegations  int64             `pack:"a,snappy"    json:"active_delegations"` // with non-zero balance
	IsFunded           bool              `pack:"f,snappy"    json:"is_funded"`
	IsActivated        bool              `pack:"A,snappy"    json:"is_activated"` // bc: fundraiser account
	IsSpendable        bool              `pack:"p,snappy"    json:"is_spendable"` // manager can move funds without running any code
	IsDelegatable      bool              `pack:"?,snappy"    json:"is_delegatable"`
	IsDelegated        bool              `pack:"=,snappy"    json:"is_delegated"`
	IsRevealed         bool              `pack:"r,snappy"    json:"is_revealed"`
	IsDelegate         bool              `pack:"d,snappy"    json:"is_delegate"`
	IsActiveDelegate   bool              `pack:"v,snappy"    json:"is_active_delegate"`
	IsContract         bool              `pack:"c,snappy"    json:"is_contract"` // smart contract with code
	BlocksBaked        int               `pack:"b,snappy"    json:"blocks_baked"`
	BlocksMissed       int               `pack:"m,snappy"    json:"blocks_missed"`
	BlocksStolen       int               `pack:"n,snappy"    json:"blocks_stolen"`
	BlocksEndorsed     int               `pack:"e,snappy"    json:"blocks_endorsed"`
	SlotsEndorsed      int               `pack:"x,snappy"    json:"slots_endorsed"`
	SlotsMissed        int               `pack:"y,snappy"    json:"slots_missed"`
	NOps               int               `pack:"1,snappy"    json:"n_ops"`         // stats: successful operation count
	NOpsFailed         int               `pack:"2,snappy"    json:"n_ops_failed"`  // stats: failed operation count
	NTx                int               `pack:"3,snappy"    json:"n_tx"`          // stats: number of Tx operations
	NDelegation        int               `pack:"4,snappy"    json:"n_delegation"`  // stats: number of Delegations operations
	NOrigination       int               `pack:"5,snappy"    json:"n_origination"` // stats: number of Originations operations
	NProposal          int               `pack:"6,snappy"    json:"n_proposal"`    // stats: number of Proposals operations
	NBallot            int               `pack:"7,snappy"    json:"n_ballot"`      // stats: number of Ballots operations
	TokenGenMin        int64             `pack:"g,snappy"    json:"token_gen_min"` // hops
	TokenGenMax        int64             `pack:"G,snappy"    json:"token_gen_max"` // hops
	GracePeriod        int64             `pack:"P,snappy"    json:"grace_period"`  // deactivation cycle
	BakerVersion       uint32            `pack:"N,snappy"    json:"baker_version"` // baker version

	// used during block processing, not stored in DB
	IsNew      bool `pack:"-" json:"-"` // first seen this block
	WasFunded  bool `pack:"-" json:"-"` // true if account was funded before processing this block
	WasDust    bool `pack:"-" json:"-"` // true if account had 0 < balance < 1tez at start of block
	IsDirty    bool `pack:"-" json:"-"` // indicates an update happened
	MustDelete bool `pack:"-" json:"-"` // indicates the account should be deleted (during rollback)
}

// Ensure Account implements the pack.Item interface.
var _ pack.Item = (*Account)(nil)

func NewAccount(addr tezos.Address) *Account {
	acc := AllocAccount()
	acc.Type = addr.Type
	acc.Address = addr.Clone()
	acc.IsNew = true
	acc.IsDirty = true
	acc.IsSpendable = addr.Type != tezos.AddressTypeContract // tz1/2/3 spendable by default
	return acc
}

func AllocAccount() *Account {
	return accountPool.Get().(*Account)
}

func (a *Account) Free() {
	// skip for delegates because we keep them out of cache
	if a.IsDelegate {
		return
	}
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
	b := a.Balance()
	return b > 0 && b < DustLimit
}

func (a Account) Balance() int64 {
	return a.FrozenBalance() + a.SpendableBalance
}

func (a Account) FrozenBalance() int64 {
	return a.FrozenDeposits + a.FrozenFees + a.FrozenRewards
}

func (a Account) TotalBalance() int64 {
	return a.SpendableBalance + a.FrozenDeposits + a.FrozenFees
}

// own balance plus frozen deposits+fees (NOT REWARDS!) plus
// all delegated balances (this is self-delegation safe)
func (a Account) StakingBalance() int64 {
	return a.FrozenDeposits + a.FrozenFees + a.SpendableBalance + a.DelegatedBalance
}

func (a Account) Rolls(p *tezos.Params) int64 {
	if p.TokensPerRoll == 0 {
		return 0
	}
	return a.StakingBalance() / p.TokensPerRoll
}

func (a Account) StakingCapacity(p *tezos.Params, netRolls int64) int64 {
	blockDeposits := p.BlockSecurityDeposit + p.EndorsementSecurityDeposit*int64(p.EndorsersPerBlock)
	netBond := blockDeposits * p.BlocksPerCycle * (p.PreservedCycles + 1)
	netStake := netRolls * p.TokensPerRoll
	// numeric overflow is likely
	return int64(float64(a.TotalBalance()) * float64(netStake) / float64(netBond))
}

func (a *Account) SetVersion(nonce uint64) {
	a.BakerVersion = uint32(nonce >> 32)
}

func (a *Account) GetVersionBytes() []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], a.BakerVersion)
	return b[:]
}

func (a *Account) Reset() {
	a.RowId = 0
	a.Address = tezos.Address{}
	a.DelegateId = 0
	a.CreatorId = 0
	a.Pubkey = nil
	a.Type = 0
	a.FirstIn = 0
	a.FirstOut = 0
	a.LastIn = 0
	a.LastOut = 0
	a.FirstSeen = 0
	a.LastSeen = 0
	a.DelegatedSince = 0
	a.DelegateSince = 0
	a.DelegateUntil = 0
	a.TotalReceived = 0
	a.TotalSent = 0
	a.TotalBurned = 0
	a.TotalFeesPaid = 0
	a.TotalRewardsEarned = 0
	a.TotalFeesEarned = 0
	a.TotalLost = 0
	a.FrozenDeposits = 0
	a.FrozenRewards = 0
	a.FrozenFees = 0
	a.UnclaimedBalance = 0
	a.SpendableBalance = 0
	a.DelegatedBalance = 0
	a.TotalDelegations = 0
	a.ActiveDelegations = 0
	a.IsFunded = false
	a.IsActivated = false
	a.IsSpendable = false
	a.IsDelegatable = false
	a.IsDelegated = false
	a.IsRevealed = false
	a.IsDelegate = false
	a.IsActiveDelegate = false
	a.IsContract = false
	a.BlocksBaked = 0
	a.BlocksMissed = 0
	a.BlocksStolen = 0
	a.BlocksEndorsed = 0
	a.SlotsEndorsed = 0
	a.SlotsMissed = 0
	a.NOps = 0
	a.NOpsFailed = 0
	a.NTx = 0
	a.NDelegation = 0
	a.NOrigination = 0
	a.NProposal = 0
	a.NBallot = 0
	a.TokenGenMin = 0
	a.TokenGenMax = 0
	a.GracePeriod = 0
	a.BakerVersion = 0
	a.IsNew = false
	a.WasFunded = false
	a.WasDust = false
	a.IsDirty = false
	a.MustDelete = false
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
	case FlowCategoryRewards:
		if a.FrozenRewards < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen rewards %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenRewards, f.AmountOut)
		}
		a.TotalRewardsEarned += f.AmountIn
		a.FrozenRewards += f.AmountIn - f.AmountOut
		a.TokenGenMin = 1
		a.TokenGenMax = util.Max64(a.TokenGenMax, 1)
		if f.Operation == FlowTypeDenunciation {
			a.TotalLost += f.AmountOut
			a.TotalRewardsEarned -= f.AmountOut
		}
	case FlowCategoryDeposits:
		if a.FrozenDeposits < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen deposits %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenDeposits, f.AmountOut)
		}
		a.FrozenDeposits += f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenunciation {
			a.TotalLost += f.AmountOut
		}
	case FlowCategoryFees:
		if a.FrozenFees < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s frozen fees %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.FrozenFees, f.AmountOut)
		}
		if f.IsFrozen {
			a.TotalFeesEarned += f.AmountIn
		}
		if f.Operation == FlowTypeDenunciation {
			a.TotalLost += f.AmountOut
		}
		a.FrozenFees += f.AmountIn - f.AmountOut
	case FlowCategoryBalance:
		if a.SpendableBalance < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.SpendableBalance, f.AmountOut)
		}
		if f.IsFee {
			a.TotalFeesPaid += f.AmountOut
		}
		if f.IsBurned {
			a.TotalBurned += f.AmountOut
		}
		switch f.Operation {
		case FlowTypeAirdrop, FlowTypeInvoice:
			// update generation
			a.TokenGenMax = util.Max64(a.TokenGenMax, 1)
			a.TokenGenMin = util.NonZeroMin64(a.TokenGenMin, 1)
		case FlowTypeTransaction, FlowTypeOrigination:
			// both transactions and originations can send funds
			// count send/received only for non-fee and non-burn flows
			if !f.IsBurned && !f.IsFee {
				a.TotalReceived += f.AmountIn
				a.TotalSent += f.AmountOut
				// update generation for in-flows
				if f.AmountIn > 0 {
					a.TokenGenMax = util.Max64(a.TokenGenMax, f.TokenGenMax+1)
					a.TokenGenMin = util.NonZeroMin64(a.TokenGenMin, f.TokenGenMin+1)
				}
			}
		case FlowTypeActivation:
			if a.UnclaimedBalance < f.AmountIn {
				return fmt.Errorf("acc.update id %d %s unclaimed balance %d is smaller than "+
					"activated amount %d", a.RowId, a, a.UnclaimedBalance, f.AmountIn)
			}
			a.UnclaimedBalance -= f.AmountIn
			a.TokenGenMin = 1
			a.TokenGenMax = util.Max64(a.TokenGenMax, 1)
		}
		a.SpendableBalance += f.AmountIn - f.AmountOut

	case FlowCategoryDelegation:
		if a.DelegatedBalance < f.AmountOut {
			return fmt.Errorf("acc.update id %d %s delegated balance %d is smaller than "+
				"outgoing amount %d", a.RowId, a, a.DelegatedBalance, f.AmountOut)
		}
		a.DelegatedBalance += f.AmountIn - f.AmountOut
	}

	// any flow except delegation balance updates and internal unfreeze/payouts
	// count towards account activity
	if f.Category != FlowCategoryDelegation && f.Operation != FlowTypeInternal {
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

	a.IsFunded = (a.FrozenBalance() + a.SpendableBalance + a.UnclaimedBalance) > 0
	a.LastSeen = util.Max64N(a.LastSeen, a.LastIn, a.LastOut)

	// reset token generation
	if !a.IsFunded {
		if !a.IsDelegate {
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
	case FlowCategoryRewards:
		if a.FrozenRewards < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen rewards %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.FrozenRewards, f.AmountIn)
		}
		a.TotalRewardsEarned -= f.AmountIn
		a.FrozenRewards -= f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenunciation {
			a.TotalLost -= f.AmountOut
			a.TotalRewardsEarned += f.AmountOut
		}

	case FlowCategoryDeposits:
		if a.FrozenDeposits < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen deposits %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.FrozenDeposits, f.AmountIn)
		}
		a.FrozenDeposits -= f.AmountIn - f.AmountOut
		if f.Operation == FlowTypeDenunciation {
			a.TotalLost -= f.AmountOut
		}

	case FlowCategoryFees:
		if a.FrozenFees < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s frozen fees %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.FrozenFees, f.AmountIn)
		}
		if f.IsFrozen {
			a.TotalFeesEarned -= f.AmountIn
		}
		if f.Operation == FlowTypeDenunciation {
			a.TotalLost -= f.AmountOut
		}
		a.FrozenFees -= f.AmountIn - f.AmountOut

	case FlowCategoryBalance:
		if a.SpendableBalance < f.AmountIn-f.AmountOut {
			return fmt.Errorf("acc.update id %d %s balance %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.SpendableBalance, f.AmountIn)
		}
		if f.IsFee {
			a.TotalFeesPaid -= f.AmountOut
		}
		if f.IsBurned {
			a.TotalBurned -= f.AmountOut
		}

		switch f.Operation {
		case FlowTypeTransaction, FlowTypeOrigination, FlowTypeAirdrop:
			a.TotalReceived -= f.AmountIn
			a.TotalSent -= f.AmountOut
			// FIXME: reverse generation update for in-flows lacks previous info
			// if f.AmountIn > 0 {
			// 	a.TokenGenMax = util.Max64(a.TokenGenMax, f.TokenGenMax+1)
			// 	a.TokenGenMin = util.NonZeroMin64(a.TokenGenMin, f.TokenGenMin+1)
			// }
		case FlowTypeActivation:
			a.UnclaimedBalance += f.AmountIn
		}
		a.SpendableBalance -= f.AmountIn - f.AmountOut

	case FlowCategoryDelegation:
		if a.DelegatedBalance < f.AmountIn {
			return fmt.Errorf("acc.update id %d %s delegated balance %d is smaller than "+
				"reversed incoming amount %d", a.RowId, a, a.DelegatedBalance, f.AmountIn)
		}
		a.DelegatedBalance -= f.AmountIn - f.AmountOut
	}

	// skip activity updates (too complex to track previous in/out heights)
	// and rely on subsequent block updates, we still set LastSeen to the current block
	a.IsFunded = (a.FrozenBalance() + a.SpendableBalance + a.UnclaimedBalance) > 0
	a.LastSeen = util.Min64(f.Height, util.Max64N(a.LastIn, a.LastOut))
	return nil
}

// init 11 cycles ahead of current cycle
func (a *Account) InitGracePeriod(cycle int64, params *tezos.Params) {
	a.GracePeriod = cycle + 2*params.PreservedCycles + 1 // (11)
}

// keep initial (+11) max grace period, otherwise cycle + 6
func (a *Account) UpdateGracePeriod(cycle int64, params *tezos.Params) {
	a.GracePeriod = util.Max64(cycle+params.PreservedCycles+1, a.GracePeriod)
}

func (a Account) MarshalBinary() ([]byte, error) {
	le := binary.LittleEndian
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	binary.Write(buf, le, a.RowId)
	binary.Write(buf, le, int32(len(a.Address.Hash)))
	buf.Write(a.Address.Hash)
	binary.Write(buf, le, a.DelegateId)
	binary.Write(buf, le, a.CreatorId)
	binary.Write(buf, le, int32(len(a.Pubkey)))
	buf.Write(a.Pubkey)
	binary.Write(buf, le, a.Type)
	binary.Write(buf, le, a.FirstIn)
	binary.Write(buf, le, a.FirstOut)
	binary.Write(buf, le, a.LastIn)
	binary.Write(buf, le, a.LastOut)
	binary.Write(buf, le, a.FirstSeen)
	binary.Write(buf, le, a.LastSeen)
	binary.Write(buf, le, a.DelegatedSince)
	binary.Write(buf, le, a.DelegateSince)
	binary.Write(buf, le, a.DelegateUntil)
	binary.Write(buf, le, a.TotalReceived)
	binary.Write(buf, le, a.TotalSent)
	binary.Write(buf, le, a.TotalBurned)
	binary.Write(buf, le, a.TotalFeesPaid)
	binary.Write(buf, le, a.TotalRewardsEarned)
	binary.Write(buf, le, a.TotalFeesEarned)
	binary.Write(buf, le, a.TotalLost)
	binary.Write(buf, le, a.FrozenDeposits)
	binary.Write(buf, le, a.FrozenRewards)
	binary.Write(buf, le, a.FrozenFees)
	binary.Write(buf, le, a.UnclaimedBalance)
	binary.Write(buf, le, a.SpendableBalance)
	binary.Write(buf, le, a.DelegatedBalance)
	binary.Write(buf, le, a.TotalDelegations)
	binary.Write(buf, le, a.ActiveDelegations)
	binary.Write(buf, le, a.IsFunded)
	binary.Write(buf, le, a.IsActivated)
	binary.Write(buf, le, a.IsSpendable)
	binary.Write(buf, le, a.IsDelegatable)
	binary.Write(buf, le, a.IsDelegated)
	binary.Write(buf, le, a.IsRevealed)
	binary.Write(buf, le, a.IsDelegate)
	binary.Write(buf, le, a.IsActiveDelegate)
	binary.Write(buf, le, a.IsContract)
	binary.Write(buf, le, int32(a.BlocksBaked))
	binary.Write(buf, le, int32(a.BlocksMissed))
	binary.Write(buf, le, int32(a.BlocksStolen))
	binary.Write(buf, le, int32(a.BlocksEndorsed))
	binary.Write(buf, le, int32(a.SlotsEndorsed))
	binary.Write(buf, le, int32(a.SlotsMissed))
	binary.Write(buf, le, int32(a.NOps))
	binary.Write(buf, le, int32(a.NOpsFailed))
	binary.Write(buf, le, int32(a.NTx))
	binary.Write(buf, le, int32(a.NDelegation))
	binary.Write(buf, le, int32(a.NOrigination))
	binary.Write(buf, le, int32(a.NProposal))
	binary.Write(buf, le, int32(a.NBallot))
	binary.Write(buf, le, a.TokenGenMin)
	binary.Write(buf, le, a.TokenGenMax)
	binary.Write(buf, le, a.GracePeriod)
	binary.Write(buf, le, a.BakerVersion)
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
	binary.Read(buf, le, &a.DelegateId)
	binary.Read(buf, le, &a.CreatorId)
	binary.Read(buf, le, &l)
	if l > 0 {
		a.Pubkey = make([]byte, int(l))
		buf.Read(a.Pubkey)
	}
	binary.Read(buf, le, &a.Type)
	a.Address.Type = a.Type
	binary.Read(buf, le, &a.FirstIn)
	binary.Read(buf, le, &a.FirstOut)
	binary.Read(buf, le, &a.LastIn)
	binary.Read(buf, le, &a.LastOut)
	binary.Read(buf, le, &a.FirstSeen)
	binary.Read(buf, le, &a.LastSeen)
	binary.Read(buf, le, &a.DelegatedSince)
	binary.Read(buf, le, &a.DelegateSince)
	binary.Read(buf, le, &a.DelegateUntil)
	binary.Read(buf, le, &a.TotalReceived)
	binary.Read(buf, le, &a.TotalSent)
	binary.Read(buf, le, &a.TotalBurned)
	binary.Read(buf, le, &a.TotalFeesPaid)
	binary.Read(buf, le, &a.TotalRewardsEarned)
	binary.Read(buf, le, &a.TotalFeesEarned)
	binary.Read(buf, le, &a.TotalLost)
	binary.Read(buf, le, &a.FrozenDeposits)
	binary.Read(buf, le, &a.FrozenRewards)
	binary.Read(buf, le, &a.FrozenFees)
	binary.Read(buf, le, &a.UnclaimedBalance)
	binary.Read(buf, le, &a.SpendableBalance)
	binary.Read(buf, le, &a.DelegatedBalance)
	binary.Read(buf, le, &a.TotalDelegations)
	binary.Read(buf, le, &a.ActiveDelegations)
	binary.Read(buf, le, &a.IsFunded)
	binary.Read(buf, le, &a.IsActivated)
	binary.Read(buf, le, &a.IsSpendable)
	binary.Read(buf, le, &a.IsDelegatable)
	binary.Read(buf, le, &a.IsDelegated)
	binary.Read(buf, le, &a.IsRevealed)
	binary.Read(buf, le, &a.IsDelegate)
	binary.Read(buf, le, &a.IsActiveDelegate)
	binary.Read(buf, le, &a.IsContract)
	var n int32
	binary.Read(buf, le, &n)
	a.BlocksBaked = int(n)
	binary.Read(buf, le, &n)
	a.BlocksMissed = int(n)
	binary.Read(buf, le, &n)
	a.BlocksStolen = int(n)
	binary.Read(buf, le, &n)
	a.BlocksEndorsed = int(n)
	binary.Read(buf, le, &n)
	a.SlotsEndorsed = int(n)
	binary.Read(buf, le, &n)
	a.SlotsMissed = int(n)
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
	a.NProposal = int(n)
	binary.Read(buf, le, &n)
	a.NBallot = int(n)
	binary.Read(buf, le, &a.TokenGenMin)
	binary.Read(buf, le, &a.TokenGenMax)
	binary.Read(buf, le, &a.GracePeriod)
	binary.Read(buf, le, &a.BakerVersion)
	return nil
}
