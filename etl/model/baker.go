// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

const BakerTableKey = "baker"

var ErrNoBaker = errors.New("baker not indexed")

type BakerID uint64

func (id BakerID) U64() uint64 {
	return uint64(id)
}

// Baker is an up-to-date snapshot of the current status of active or inactive bakers.
// For history look at Op and Flow (balance updates), for generic info look
// at Account.
type Baker struct {
	RowId               BakerID       `pack:"I,pk"      json:"row_id"`
	AccountId           AccountID     `pack:"A,u32"     json:"account_id"`
	Address             tezos.Address `pack:"H,bloom=3" json:"address"`
	ConsensusKey        tezos.Key     `pack:"K"         json:"consensus_key"`
	IsActive            bool          `pack:"v,snappy"  json:"is_active"`
	BakerSince          int64         `pack:"*,i32"     json:"baker_since"`
	BakerUntil          int64         `pack:"/,i32"     json:"baker_until"`
	TotalRewardsEarned  int64         `pack:"W"         json:"total_rewards_earned"`
	TotalFeesEarned     int64         `pack:"E"         json:"total_fees_earned"`
	TotalLost           int64         `pack:"L"         json:"total_lost"`
	FrozenDeposits      int64         `pack:"z"         json:"frozen_deposits"`
	FrozenRewards       int64         `pack:"Z"         json:"frozen_rewards"`
	FrozenFees          int64         `pack:"Y"         json:"frozen_fees"`
	DelegatedBalance    int64         `pack:"~"         json:"delegated_balance"`
	TotalStake          int64         `pack:"g"         json:"total_stake"`
	TotalShares         int64         `pack:"h"         json:"total_shares"`
	ActiveStakers       int64         `pack:"j"         json:"active_stakers"`
	StakingEdge         int64         `pack:"k"         json:"staking_edge"`
	StakingLimit        int64         `pack:"l"         json:"staking_limit"`
	DepositsLimit       int64         `pack:"T"         json:"deposit_limit"`
	ActiveDelegations   int64         `pack:"a,i32"     json:"active_delegations"`
	BlocksBaked         int64         `pack:"b,i32"     json:"blocks_baked"`
	BlocksProposed      int64         `pack:"P,i32"     json:"blocks_proposed"`
	BlocksNotBaked      int64         `pack:"N,i32"     json:"blocks_not_baked"`
	BlocksEndorsed      int64         `pack:"x,i32"     json:"blocks_endorsed"`
	BlocksNotEndorsed   int64         `pack:"y,i32"     json:"blocks_not_endorsed"`
	SlotsEndorsed       int64         `pack:"e"         json:"slots_endorsed"`
	NBakerOps           int64         `pack:"1,i32"     json:"n_baker_ops"`
	NProposal           int64         `pack:"2,i32"     json:"n_proposals"`
	NBallot             int64         `pack:"3,i32"     json:"n_ballots"`
	NEndorsement        int64         `pack:"4,i32"     json:"n_endorsements"`
	NPreendorsement     int64         `pack:"5,i32"     json:"n_preendorsements"`
	NSeedNonce          int64         `pack:"6,i32"     json:"n_nonce_revelations"`
	N2Baking            int64         `pack:"7,i32"     json:"n_double_bakings"`
	N2Endorsement       int64         `pack:"8,i32"     json:"n_double_endorsements"`
	NSetDepositsLimit   int64         `pack:"9,i32"     json:"n_set_limits"`
	NAccusations        int64         `pack:"0,i32"     json:"n_accusations"`
	NUpdateConsensusKey int64         `pack:"!,i32"     json:"n_update_consensus_key"`
	NDrainDelegate      int64         `pack:"=,i32"     json:"n_drain_delegate"`
	GracePeriod         int64         `pack:"G,i32"     json:"grace_period"`
	Version             uint32        `pack:"V,snappy"  json:"baker_version"`

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

func (b *Baker) Reset() {
	*b = Baker{}
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
	return b.FrozenStake() + b.Account.Balance()
}

func (b Baker) FrozenStake() int64 {
	return b.FrozenDeposits + b.FrozenFees + b.FrozenRewards + b.StakeAmount(b.Account.StakeShares)
}

func (b Baker) TotalBalance() int64 {
	return b.Account.Balance() + b.FrozenDeposits + b.FrozenFees + b.StakeAmount(b.Account.StakeShares)
}

// own balance plus frozen deposits+fees (NOT REWARDS!) plus
// all delegated balances (this is self-delegation safe)
// post oxford this is equal to TotalStake + SpendableBalance + DelegatedBalance
func (b Baker) StakingBalance() int64 {
	return b.Account.Balance() + // spendable + frozen rollup bond + unstaked balance
		b.FrozenDeposits + b.FrozenFees + // pre-Oxford
		b.DelegatedBalance + // pre/post Oxford
		b.TotalStake // post-Oxford
}

// BakingPower calculates how much of the baker's stake (deposits and delegated)
// is used to derive consensus rights. There are subtle differences between
// versions. Post Oxford this is influenced by global and local limits.
func (b Baker) BakingPower(p *rpc.Params, adjust int64) int64 {
	switch {
	case p.Version < 12:
		return b.BakingPowerEmmy(p, adjust)
	case p.Version < 18:
		return b.BakingPowerTenderbake(p, adjust)
	default:
		return b.BakingPowerOxford(p, adjust)
	}
}

func (b Baker) BakingPowerEmmy(p *rpc.Params, adjust int64) int64 {
	// adjust for snapshot 15 end of cycle (rewards are not yet paid when shapshot is done)
	return b.StakingBalance() - adjust
}

// v12+ staking balance capped by locked deposits
func (b Baker) BakingPowerTenderbake(p *rpc.Params, adjust int64) int64 {
	capacity := b.Account.SpendableBalance + b.FrozenDeposits
	if b.DepositsLimit > -1 && capacity > b.DepositsLimit {
		capacity = b.DepositsLimit
	}
	capacity = capacity * 100 / int64(p.FrozenDepositsPercentage)
	stake := b.Account.Balance() + b.FrozenDeposits + b.DelegatedBalance - adjust
	return min(stake, capacity)
}

// Ofxord active stake depends on global 5x limit and baker's local limit
// anything above these limits is counted as delegation and
// delegations are further capped to 9x stake
func (b Baker) BakingPowerOxford(p *rpc.Params, adjust int64) int64 {
	ownStake := b.StakeAmount(b.Account.StakeShares)
	staked, delegated := b.TotalStake-ownStake, b.DelegatedBalance+b.Account.Balance()
	stakeCap, delegationCap := b.StakingCapacity(p), b.DelegationCapacityOxford(p, adjust)
	if staked > stakeCap {
		delegated += staked - stakeCap
		staked = stakeCap
	}
	if delegated > delegationCap {
		delegated = delegationCap
	}
	return ownStake + staked + delegated
}

func (b Baker) IsOverStaked(p *rpc.Params) bool {
	switch {
	case p.Version < 18:
		return false
	default:
		ownStake := b.StakeAmount(b.Account.StakeShares)
		return (b.TotalStake - ownStake) > b.StakingCapacity(p)
	}
}

func (b Baker) IsOverDelegated(p *rpc.Params) bool {
	switch {
	case p.Version < 18:
		return b.StakingBalance() >= b.DelegationCapacity(p, 0, 0)
	default:
		ownStake := b.StakeAmount(b.Account.StakeShares)
		staked, delegated := b.TotalStake-ownStake, b.DelegatedBalance+b.Account.Balance()
		stakeCap, delegationCap := b.StakingCapacity(p), b.DelegationCapacityOxford(p, 0)
		if staked > stakeCap {
			delegated += staked - stakeCap
		}
		return delegated > delegationCap
	}
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
		if f.Kind != FlowKindRewards {
			continue
		}
		if f.Type != FlowTypeInternal {
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
		if f.Kind != FlowKindBalance {
			continue
		}
		if f.IsBurned {
			continue
		}
		// flows from/to spendable balance sub-account
		switch f.Type {
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

// Oxford staking
// baker setting limit_of_staking_over_baking = 5(.000.000) means 5x own stake
// global_limit_of_staking_over_baking = 5 means baker total stake capcacity == 5x own stake
func (b Baker) StakingCapacity(p *rpc.Params) int64 {
	// basis is current stake owned by the baker which may change all the time
	// due to rewards flowing in
	ownStake := b.StakeAmount(b.Account.StakeShares)

	// limit is scaled by 1.000.000
	limit := b.StakingLimit

	// global cap is unscaled (!)
	if limit > p.GlobalLimitOfStakingOverBaking*1_000_000 {
		limit = p.GlobalLimitOfStakingOverBaking * 1_000_000
	}

	// calculate exact and scale back
	return tezos.NewZ(ownStake).Mul64(limit).Scale(-6).Int64()
}

func (b Baker) DelegationCapacity(p *rpc.Params, netRolls, adjust int64) int64 {
	switch {
	case p.Version < 12:
		return b.DelegationCapacityEmmy(p, netRolls, adjust)
	case p.Version < 18:
		return b.DelegationCapacityTenderbake(p, adjust)
	default:
		return b.DelegationCapacityOxford(p, adjust)
	}
}

// Emmy
// - capacity depends on ratio between on stake and network-wide stake
// - adjust for end of cycle snapshot difference
func (b Baker) DelegationCapacityEmmy(p *rpc.Params, netRolls, adjust int64) int64 {
	blockDeposits := p.BlockSecurityDeposit + p.EndorsementSecurityDeposit*int64(p.EndorsersPerBlock)
	netBond := tezos.NewZ(blockDeposits * p.BlocksPerCycle * (p.PreservedCycles + 1))
	netStake := tezos.NewZ(netRolls * p.MinimalStake)
	bakerStake := tezos.NewZ(b.Account.Balance() + b.FrozenDeposits + b.FrozenFees - adjust)
	return bakerStake.Mul(netStake).Div(netBond).Int64()
}

// Tenderbake
// - 10% of staking balance must be locked
// - deposit = stake * frozen_deposits_percentage / 100
// - max_stake = total_balance / frozen_deposits_percentage * 100
// - adjust for end of cycle snapshot difference
func (b Baker) DelegationCapacityTenderbake(p *rpc.Params, adjust int64) int64 {
	ceiling := (b.Account.SpendableBalance + b.FrozenDeposits - adjust) * 100 / int64(p.FrozenDepositsPercentage)
	if ceiling < 0 {
		ceiling = 0
	}
	return ceiling
}

// Oxford staking
// same as in Tenderbake, but config option semantic has changed to
// - FrozenDepositsPercentage = 10 // means deposit must be >= 10% of eligible delegation
// - LimitOfDelegationOverBaking = 9 // means max delegation = 9x stake
func (b Baker) DelegationCapacityOxford(p *rpc.Params, adjust int64) int64 {
	ownStake := b.StakeAmount(b.Account.StakeShares) - adjust
	return ownStake * p.LimitOfDelegationOverBaking
}

func (b *Baker) SetVersion(nonce uint64) {
	b.Version = uint32(nonce >> 32)
}

func (b *Baker) GetVersionBytes() []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], b.Version)
	return buf[:]
}

// Oxford stake pool ownership tracking
func (b *Baker) MintStake(stake int64) int64 {
	shares := stake
	if b.TotalStake > 0 {
		shares = tezos.NewZ(stake).Mul64(b.TotalShares).Div64(b.TotalStake).Int64()
	}
	b.TotalShares += shares
	b.TotalStake += stake
	return shares
}

func (b *Baker) BurnStake(stake int64) int64 {
	shares := stake
	if b.TotalStake > 0 {
		shares = tezos.NewZ(stake).Mul64(b.TotalShares).Div64(b.TotalStake).Int64()
	}
	if shares > b.TotalShares {
		shares = b.TotalShares
	}
	b.TotalShares -= shares
	b.TotalStake -= stake
	return shares
}

func (b Baker) StakeAmount(shares int64) int64 {
	if b.TotalShares <= 0 {
		return 0
	}
	return tezos.NewZ(b.TotalStake).Mul64(shares).Div64(b.TotalShares).Int64()
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
	b.Account.Baker = b
	if err := b.Account.UpdateBalance(f); err != nil {
		return err
	}
	b.Account.Baker = nil

	switch f.Kind {
	case FlowKindRewards:
		// pre-oxford only
		if b.FrozenRewards < f.AmountOut-f.AmountIn {
			return fmt.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.FrozenRewards, f.AmountOut-f.AmountIn)
			// this happens on Ithaca upgrade due to a seed nonce slash bug earlier
			// b.FrozenRewards = f.AmountOut - f.AmountIn
		}
		b.TotalRewardsEarned += f.AmountIn
		b.FrozenRewards += f.AmountIn - f.AmountOut
		if f.Type == FlowTypePenalty || f.Type == FlowTypeNonceRevelation {
			b.TotalLost += f.AmountOut
		}

	case FlowKindDeposits:
		if b.FrozenDeposits < f.AmountOut-f.AmountIn {
			return fmt.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.FrozenDeposits, f.AmountOut-f.AmountIn)
		}
		b.FrozenDeposits += f.AmountIn - f.AmountOut
		if f.Type == FlowTypePenalty {
			b.TotalLost += f.AmountOut
		}

	case FlowKindFees:
		if b.FrozenFees < f.AmountOut-f.AmountIn {
			return fmt.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.FrozenFees, f.AmountOut-f.AmountIn)
			// this happens on Ithaca upgrade due to a seed nonce slash bug earlier
			// b.FrozenFees = f.AmountOut - f.AmountIn
		}
		b.TotalFeesEarned += f.AmountIn
		if f.Type == FlowTypePenalty || f.Type == FlowTypeNonceRevelation {
			b.TotalLost += f.AmountOut
		}
		b.FrozenFees += f.AmountIn - f.AmountOut

	case FlowKindBalance:
		// post-Ithaca some rewards are paid immediately
		// Note: careful do not double-count frozen deposits/rewards from pre-Ithaca
		// on baking/endorsing/nonce types
		if !f.IsFrozen && !f.IsUnfrozen {
			switch f.Type {
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
		if f.Type == FlowTypePenalty {
			b.TotalRewardsEarned += f.AmountIn
		}

	case FlowKindDelegation:
		if b.DelegatedBalance < f.AmountOut-f.AmountIn {
			return fmt.Errorf("baker.update id %d %s delegated balance %d is smaller than "+
				"outgoing amount %d", b.RowId, b, b.DelegatedBalance, f.AmountOut-f.AmountIn)
		}
		b.DelegatedBalance += f.AmountIn - f.AmountOut

	case FlowKindStake:
		// update own stake counter only, other counters are updated in account
		switch f.Type {
		case FlowTypeBaking, FlowTypeBonus, FlowTypeReward, FlowTypeNonceRevelation:
			// update pool rewards
			b.TotalStake += f.AmountIn
			b.TotalRewardsEarned += f.AmountIn

			// adjust for rewards flow while no stake shares exist
			if b.TotalStake > 0 && b.TotalShares == 0 {
				b.TotalShares = 1
				b.Account.StakeShares = 1
			}

			// log.Infof(">> bkr: %s stake=%d shares=%d", b.Account, b.TotalStake, b.TotalShares)

		case FlowTypePenalty:
			// slash at end of cycle
			b.TotalLost += f.AmountOut
			b.TotalStake -= f.AmountOut

			// log.Infof(">> bkr: %s stake=%d shares=%d", b.Account, b.TotalStake, b.TotalShares)

			// FIXME: should there be an extra delegation outflow?
			// burn out of unstaked decreases delegation balance
			// if f.IsUnfrozen {
			// 	b.DelegatedBalance -= f.AmountOut
			// }

			// FIXME
			// redelegated finalize unstake reduces delegation balance
			// 	// b.DelegatedBalance -= f.AmountOut
		}
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
	switch f.Kind {
	case FlowKindRewards:
		// pre-Ithaca only
		if b.FrozenRewards < f.AmountIn-f.AmountOut {
			return fmt.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.FrozenRewards, f.AmountIn-f.AmountOut)
		}
		b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
		b.FrozenRewards -= f.AmountIn - f.AmountOut
		if f.Type == FlowTypePenalty {
			b.TotalLost -= f.AmountOut
			b.TotalRewardsEarned += f.AmountOut
		}

	case FlowKindDeposits:
		// pre/post-Ithaca
		if b.FrozenDeposits < f.AmountIn-f.AmountOut {
			return fmt.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.FrozenDeposits, f.AmountIn-f.AmountOut)
		}
		b.FrozenDeposits -= f.AmountIn - f.AmountOut
		if f.Type == FlowTypePenalty {
			b.TotalLost -= f.AmountOut
		}

	case FlowKindFees:
		// pre-Ithaca only
		if b.FrozenFees < f.AmountIn-f.AmountOut {
			return fmt.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.FrozenFees, f.AmountIn-f.AmountOut)
		}
		b.TotalFeesEarned -= f.AmountIn
		if f.Type == FlowTypePenalty {
			b.TotalLost -= f.AmountOut
		}
		b.FrozenFees -= f.AmountIn - f.AmountOut

	case FlowKindBalance:
		// post-Ithaca some rewards are paid immediately
		// Note: careful do not double-count frozen deposits/rewards from pre-Ithaca
		// on baking/endorsing/nonce types
		if !f.IsFrozen {
			switch f.Type {
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
		if f.Type == FlowTypePenalty {
			b.TotalRewardsEarned -= f.AmountIn
		}

	case FlowKindDelegation:
		if b.DelegatedBalance < f.AmountIn-f.AmountOut {
			return fmt.Errorf("baker.update id %d %s delegated balance %d is smaller than "+
				"reversed incoming amount %d", b.RowId, b, b.DelegatedBalance, f.AmountIn-f.AmountOut)
		}
		b.DelegatedBalance -= f.AmountIn - f.AmountOut

	case FlowKindStake:
		// update own stake counter only, other counters are updated in account
		switch f.Type {
		case FlowTypeBaking, FlowTypeBonus, FlowTypeReward, FlowTypeNonceRevelation:
			// reverse pool rewards
			b.TotalStake -= f.AmountIn
			b.TotalRewardsEarned -= f.AmountIn

			// adjust for rewards flow while no stake shares exist
			if b.TotalStake > 0 && b.TotalShares == 0 {
				b.TotalShares = 1
				b.Account.StakeShares = 1
			}

		case FlowTypePenalty:
			// reverse staked and unstaked loss
			b.TotalLost -= f.AmountOut

			// reverse burn out of staked balance
			b.TotalStake += f.AmountOut

			// FIXME: should there be an extra delegation outflow?
			// burn out of unstaked decreases delegation balance
			// if f.IsUnfrozen {
			// b.DelegatedBalance += f.AmountOut
			// }
		}
	}
	b.Account.Baker = b
	if err := b.Account.RollbackBalance(f); err != nil {
		return err
	}
	b.Account.Baker = nil
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
		b.GracePeriod = max(cycle+params.PreservedCycles+1, b.GracePeriod)
	} else {
		// reset after inactivity
		b.IsActive = true
		b.BakerUntil = 0
		b.InitGracePeriod(cycle, params)
	}
	b.IsDirty = true
}
