// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

const BakerTableKey = "baker"

var ErrNoBaker = errors.New("baker not indexed")

type BakerID uint64

func (id BakerID) Value() uint64 {
	return uint64(id)
}

// Baker is an up-to-date snapshot of the current status of active or inactive bakers.
// For history look at Op and Flow (balance updates), for generic info look
// at Account.
type Baker struct {
	RowId               BakerID       `pack:"I,pk"     json:"row_id"`
	AccountId           AccountID     `pack:"A"        json:"account_id"`
	Address             tezos.Address `pack:"H,snappy" json:"address"`
	ConsensusKey        tezos.Key     `pack:"K"        json:"consensus_key"`
	IsActive            bool          `pack:"v,snappy" json:"is_active"`
	BakerSince          int64         `pack:"*"        json:"baker_since"`
	BakerUntil          int64         `pack:"/"        json:"baker_until"`
	TotalRewardsEarned  int64         `pack:"W"        json:"total_rewards_earned"`
	TotalFeesEarned     int64         `pack:"E"        json:"total_fees_earned"`
	TotalLost           int64         `pack:"L"        json:"total_lost"`
	FrozenDeposits      int64         `pack:"z"        json:"frozen_deposits"`
	FrozenRewards       int64         `pack:"Z"        json:"frozen_rewards"`
	FrozenFees          int64         `pack:"Y"        json:"frozen_fees"`
	DelegatedBalance    int64         `pack:"~"        json:"delegated_balance"`
	DepositsLimit       int64         `pack:"T"        json:"deposit_limit"`
	TotalDelegations    int64         `pack:">"        json:"total_delegations"`
	ActiveDelegations   int64         `pack:"a"        json:"active_delegations"`
	BlocksBaked         int64         `pack:"b"        json:"blocks_baked"`
	BlocksProposed      int64         `pack:"P"        json:"blocks_proposed"`
	BlocksNotBaked      int64         `pack:"N"        json:"blocks_not_baked"`
	BlocksEndorsed      int64         `pack:"x"        json:"blocks_endorsed"`
	BlocksNotEndorsed   int64         `pack:"y"        json:"blocks_not_endorsed"`
	SlotsEndorsed       int64         `pack:"e"        json:"slots_endorsed"`
	NBakerOps           int64         `pack:"1"        json:"n_baker_ops"`
	NProposal           int64         `pack:"2"        json:"n_proposals"`
	NBallot             int64         `pack:"3"        json:"n_ballots"`
	NEndorsement        int64         `pack:"4"        json:"n_endorsements"`
	NPreendorsement     int64         `pack:"5"        json:"n_preendorsements"`
	NSeedNonce          int64         `pack:"6"        json:"n_nonce_revelations"`
	N2Baking            int64         `pack:"7"        json:"n_double_bakings"`
	N2Endorsement       int64         `pack:"8"        json:"n_double_endorsements"`
	NSetDepositsLimit   int64         `pack:"9"        json:"n_set_limits"`
	NAccusations        int64         `pack:"0"        json:"n_accusations"`
	NUpdateConsensusKey int64         `pack:"!"        json:"n_update_consensus_key"`
	NDrainDelegate      int64         `pack:"="        json:"n_drain_delegate"`
	GracePeriod         int64         `pack:"G"        json:"grace_period"`
	Version             uint32        `pack:"V,snappy" json:"baker_version"`

	Account     *Account `pack:"-" json:"-"` // related account
	Reliability int64    `pack:"-" json:"-"` // current cycle reliability from rights
	IsNew       bool     `pack:"-" json:"-"` // first seen this block
	IsDirty     bool     `pack:"-" json:"-"` // indicates an update happened
}

// Ensure Baker implements the pack.Item interface.
var _ pack.Item = (*Baker)(nil)

func NewBaker(acc *Account) *Baker {
	return &Baker{
		AccountId:     acc.RowId,
		Address:       acc.Address.Clone(),
		Account:       acc,
		DepositsLimit: -1, // unset
		IsNew:         true,
		IsDirty:       true,
	}
}

func (b Baker) ID() uint64 {
	return uint64(b.RowId)
}

func (b *Baker) SetID(id uint64) {
	b.RowId = BakerID(id)
}

func (b Baker) TableKey() string {
	return BakerTableKey
}

func (b Baker) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    10,
		JournalSizeLog2: 11,
		CacheSize:       4,
		FillLevel:       100,
	}
}

func (b Baker) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (b Baker) String() string {
	return b.Address.String()
}

func (b Baker) Balance() int64 {
	return b.FrozenBalance() + b.Account.Balance()
}

func (b Baker) FrozenBalance() int64 {
	return b.FrozenDeposits + b.FrozenFees + b.FrozenRewards
}

func (b Baker) TotalBalance() int64 {
	return b.Account.Balance() + b.FrozenDeposits + b.FrozenFees
}

// own balance plus frozen deposits+fees (NOT REWARDS!) plus
// all delegated balances (this is self-delegation safe)
func (b Baker) StakingBalance() int64 {
	return b.TotalBalance() + b.DelegatedBalance
}

func (b Baker) ActiveStake(p *rpc.Params, adjust int64) int64 {
	if p.Version < 12 {
		return b.ActiveStakeEmmy(p, adjust)
	}
	return b.ActiveStakeTenderbake(p, adjust)
}

func (b Baker) ActiveStakeEmmy(p *rpc.Params, adjust int64) int64 {
	// adjust for snapshot 15 end of cycle (rewards are not yet paid when shapshot is done)
	return b.StakingBalance() - adjust
}

// v12+ staking balance capped by locked deposits
func (b Baker) ActiveStakeTenderbake(p *rpc.Params, adjust int64) int64 {
	capacity := b.StakingCapacityTenderbake(p, adjust)
	stake := b.Account.Balance() + b.FrozenDeposits + b.DelegatedBalance - adjust
	return util.Min64(stake, capacity)
}

func (b Baker) StakeAdjust(block *Block) int64 {
	if !block.TZ.IsCycleEnd() {
		return 0
	}
	if block.Params.Version < 12 {
		return b.StakeAdjustEmmy(block)
	}
	return b.StakeAdjustTenderbake(block)
}

func (b Baker) StakeAdjustEmmy(block *Block) (adjust int64) {
	for _, f := range block.Flows {
		if f.AccountId != b.AccountId {
			continue
		}
		if f.Category != FlowCategoryRewards {
			continue
		}
		if f.Operation != FlowTypeInternal {
			continue
		}
		// flows from frozen rewards sub-account
		adjust += f.AmountOut
		break
	}
	return
}

func (b Baker) StakeAdjustTenderbake(block *Block) (adjust int64) {
	for _, f := range block.Flows {
		if f.AccountId != b.AccountId {
			continue
		}
		if f.Category != FlowCategoryBalance {
			continue
		}
		if f.IsBurned {
			continue
		}
		// flows from/to spendable balance sub-account
		switch f.Operation {
		case FlowTypeBaking: // baker rewards + fees
			adjust += f.AmountIn
		case FlowTypeBonus: // proposer bonus
			adjust += f.AmountIn
		case FlowTypeReward: // endorsing rewards
			adjust += f.AmountIn
		}
	}
	return
}

func (b Baker) Rolls(p *rpc.Params, adjust int64) int64 {
	if p.MinimalStake == 0 {
		return 0
	}
	return (b.StakingBalance() - adjust) / p.MinimalStake
}

func (b Baker) StakingCapacity(p *rpc.Params, netRolls, adjust int64) int64 {
	if p.Version < 12 {
		return b.StakingCapacityEmmy(p, netRolls, adjust)
	}
	return b.StakingCapacityTenderbake(p, adjust)
}

func (b Baker) StakingCapacityEmmy(p *rpc.Params, netRolls, adjust int64) int64 {
	blockDeposits := p.BlockSecurityDeposit + p.EndorsementSecurityDeposit*int64(p.EndorsersPerBlock)
	netBond := tezos.NewZ(blockDeposits * p.BlocksPerCycle * (p.PreservedCycles + 1))
	netStake := tezos.NewZ(netRolls * p.MinimalStake)
	bakerStake := tezos.NewZ(b.Account.Balance() + b.FrozenDeposits + b.FrozenFees - adjust)
	return bakerStake.Mul(netStake).Div(netBond).Int64()
}

func (b Baker) StakingCapacityTenderbake(p *rpc.Params, adjust int64) int64 {
	// 10% of staking balance must be locked
	// deposit = stake * frozen_deposits_percentage / 100
	// max_stake = total_balance / frozen_deposits_percentage * 100
	// adjust for end of cycle snapshot difference
	stake := b.Account.Balance() + b.FrozenDeposits - adjust
	ceiling := util.Min64(b.DepositsLimit, stake)
	if ceiling < 0 {
		ceiling = stake
	}
	return ceiling * 100 / int64(p.FrozenDepositsPercentage)
}

func (b *Baker) SetVersion(nonce uint64) {
	b.Version = uint32(nonce >> 32)
}

func (b *Baker) GetVersionBytes() []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], b.Version)
	return buf[:]
}

func (b *Baker) Reset() {
	*b = Baker{}
}

func (b *Baker) UpdateBalanceN(flows []*Flow) error {
	for _, f := range flows {
		if err := b.UpdateBalance(f); err != nil {
			return err
		}
	}
	return nil
}

func (b *Baker) UpdateBalance(f *Flow) error {
	b.IsDirty = true
	if err := b.Account.UpdateBalance(f); err != nil {
		return err
	}

	switch f.Category {
	case FlowCategoryRewards:
		if b.FrozenRewards < f.AmountOut-f.AmountIn {
			log.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.FrozenRewards, f.AmountOut-f.AmountIn)
			// this happens on Ithaca upgrade due to a seed nonce slash bug earlier
			b.FrozenRewards = f.AmountOut - f.AmountIn
		}
		b.TotalRewardsEarned += f.AmountIn
		b.FrozenRewards += f.AmountIn - f.AmountOut
		if f.Operation == FlowTypePenalty || f.Operation == FlowTypeNonceRevelation {
			b.TotalLost += f.AmountOut
		}

	case FlowCategoryDeposits:
		if b.FrozenDeposits < f.AmountOut-f.AmountIn {
			log.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.FrozenDeposits, f.AmountOut-f.AmountIn)
		}
		b.FrozenDeposits += f.AmountIn - f.AmountOut
		if f.Operation == FlowTypePenalty {
			b.TotalLost += f.AmountOut
		}

	case FlowCategoryFees:
		if b.FrozenFees < f.AmountOut-f.AmountIn {
			log.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.FrozenFees, f.AmountOut-f.AmountIn)
			// this happens on Ithaca upgrade due to a seed nonce slash bug earlier
			b.FrozenFees = f.AmountOut - f.AmountIn
		}
		b.TotalFeesEarned += f.AmountIn
		if f.Operation == FlowTypePenalty || f.Operation == FlowTypeNonceRevelation {
			b.TotalLost += f.AmountOut
		}
		b.FrozenFees += f.AmountIn - f.AmountOut

	case FlowCategoryBalance:
		// post-Ithaca some rewards are paid immediately
		// Note: careful do not double-count frozen deposits/rewards from pre-Ithaca
		// on baking/endorsing/nonce types
		if !f.IsFrozen && !f.IsUnfrozen {
			switch f.Operation {
			case FlowTypeReward:
				// reward can be given or burned
				b.TotalRewardsEarned += f.AmountIn - f.AmountOut
				b.TotalLost += f.AmountOut

			case FlowTypeNonceRevelation:
				// reward can be given or burned
				b.TotalRewardsEarned += f.AmountIn - f.AmountOut
				b.TotalLost += f.AmountOut

			case FlowTypeBaking, FlowTypeBonus:
				// Ithaca+ rewards and fees are paid directly
				if f.IsFee {
					b.TotalFeesEarned += f.AmountIn
				} else {
					b.TotalRewardsEarned += f.AmountIn
				}
			}
		}

	case FlowCategoryDelegation:
		if b.DelegatedBalance < f.AmountOut-f.AmountIn {
			return fmt.Errorf("baker.update id %d %s delegated balance %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.DelegatedBalance, f.AmountOut-f.AmountIn)
		}
		b.DelegatedBalance += f.AmountIn - f.AmountOut
	}
	return nil
}

func (b *Baker) RollbackBalanceN(flows []*Flow) error {
	for _, f := range flows {
		if err := b.RollbackBalance(f); err != nil {
			return err
		}
	}
	return nil
}

func (b *Baker) RollbackBalance(f *Flow) error {
	b.IsDirty = true
	switch f.Category {
	case FlowCategoryRewards:
		// pre-Ithaca only
		if b.FrozenRewards < f.AmountIn-f.AmountOut {
			log.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.FrozenRewards, f.AmountIn-f.AmountOut)
		}
		b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
		b.FrozenRewards -= f.AmountIn - f.AmountOut
		if f.Operation == FlowTypePenalty {
			b.TotalLost -= f.AmountOut
			b.TotalRewardsEarned += f.AmountOut
		}

	case FlowCategoryDeposits:
		// pre/post-Ithaca
		if b.FrozenDeposits < f.AmountIn-f.AmountOut {
			log.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.FrozenDeposits, f.AmountIn-f.AmountOut)
		}
		b.FrozenDeposits -= f.AmountIn - f.AmountOut
		if f.Operation == FlowTypePenalty {
			b.TotalLost -= f.AmountOut
		}

	case FlowCategoryFees:
		// pre-Ithaca only
		if b.FrozenFees < f.AmountIn-f.AmountOut {
			log.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.FrozenFees, f.AmountIn-f.AmountOut)
		}
		b.TotalFeesEarned -= f.AmountIn
		if f.Operation == FlowTypePenalty {
			b.TotalLost -= f.AmountOut
		}
		b.FrozenFees -= f.AmountIn - f.AmountOut

	case FlowCategoryBalance:
		// post-Ithaca some rewards are paid immediately
		// Note: careful do not double-count frozen deposits/rewards from pre-Ithaca
		// on baking/endorsing/nonce types
		if !f.IsFrozen {
			switch f.Operation {
			case FlowTypeReward:
				// reward can be given or burned
				b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
				b.TotalLost -= f.AmountOut

			case FlowTypeNonceRevelation:
				// reward can be given or burned
				b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
				b.TotalLost -= f.AmountOut

			case FlowTypeBaking, FlowTypeBonus:
				// Ithaca+ rewards and fees are paid directly
				if f.IsFee {
					b.TotalFeesEarned -= f.AmountIn
				} else {
					b.TotalRewardsEarned -= f.AmountIn
				}
			}
		}

	case FlowCategoryDelegation:
		if b.DelegatedBalance < f.AmountIn-f.AmountOut {
			return fmt.Errorf("baker.update id %d %s delegated balance %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.DelegatedBalance, f.AmountIn-f.AmountOut)
		}
		b.DelegatedBalance -= f.AmountIn - f.AmountOut
	}
	if err := b.Account.RollbackBalance(f); err != nil {
		return err
	}
	return nil
}

// init 11 cycles ahead of current cycle
func (b *Baker) InitGracePeriod(cycle int64, params *rpc.Params) {
	b.GracePeriod = cycle + 2*params.PreservedCycles + 1 // (11)
	b.IsDirty = true
}

// keep initial (+11) max grace period, otherwise cycle + 6
func (b *Baker) UpdateGracePeriod(cycle int64, params *rpc.Params) {
	if b.IsActive {
		b.GracePeriod = util.Max64(cycle+params.PreservedCycles+1, b.GracePeriod)
	} else {
		// reset after inactivity
		b.IsActive = true
		b.BakerUntil = 0
		b.InitGracePeriod(cycle, params)
	}
	b.IsDirty = true
}
