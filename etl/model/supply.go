// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"time"

	"blockwatch.cc/packdb/pack"
)

const SupplyTableKey = "supply"

var ErrNoSupply = errors.New("supply not indexed")

// Note: removed vesting supply in v9.1, TF vesting time is over
type Supply struct {
	RowId               uint64    `pack:"I,pk"         json:"row_id"`             // unique id
	Height              int64     `pack:"h,i32,snappy" json:"height"`             // bc: block height (also for orphans)
	Cycle               int64     `pack:"c,i16,snappy" json:"cycle"`              // bc: block cycle (tezos specific)
	Timestamp           time.Time `pack:"T,snappy"     json:"time"`               // bc: block creation time
	Total               int64     `pack:"t,snappy"     json:"total"`              // total available supply (including unclaimed)
	Activated           int64     `pack:"A,snappy"     json:"activated"`          // activated fundraiser supply
	Unclaimed           int64     `pack:"U,snappy"     json:"unclaimed"`          // all non-activated fundraiser supply
	Circulating         int64     `pack:"C,snappy"     json:"circulating"`        // (total - unclaimed)
	Liquid              int64     `pack:"L,snappy"     json:"liquid"`             // (total - frozen - unclaimed)
	Delegated           int64     `pack:"E,snappy"     json:"delegated"`          // all delegated balances (includes overdelegation)
	Staking             int64     `pack:"D,snappy"     json:"staking"`            // baker's own balances / v18+ total stake (includes overstaking)
	Unstaking           int64     `pack:"N,snappy"     json:"unstaking"`          // staking exit queue
	Shielded            int64     `pack:"S,snappy"     json:"shielded"`           // Sapling shielded supply
	ActiveStake         int64     `pack:"K,snappy"     json:"active_stake"`       // network wide stake used for consensus
	ActiveDelegated     int64     `pack:"G,snappy"     json:"active_delegated"`   // delegated  balances to active bakers
	ActiveStaking       int64     `pack:"J,snappy"     json:"active_staking"`     // active baker's slashable / v18+ stake
	InactiveDelegated   int64     `pack:"g,snappy"     json:"inactive_delegated"` // delegated balances to inactive bakers
	InactiveStaking     int64     `pack:"j,snappy"     json:"inactive_staking"`   // inactive baker's slashable / v18+ stake
	Minted              int64     `pack:"M,snappy"     json:"minted"`
	MintedBaking        int64     `pack:"b,snappy"     json:"minted_baking"`
	MintedEndorsing     int64     `pack:"e,snappy"     json:"minted_endorsing"`
	MintedSeeding       int64     `pack:"s,snappy"     json:"minted_seeding"`
	MintedAirdrop       int64     `pack:"a,snappy"     json:"minted_airdrop"`
	MintedSubsidy       int64     `pack:"y,snappy"     json:"minted_subsidy"`
	Burned              int64     `pack:"B,snappy"     json:"burned"`
	BurnedDoubleBaking  int64     `pack:"1,snappy"     json:"burned_double_baking"`
	BurnedDoubleEndorse int64     `pack:"2,snappy"     json:"burned_double_endorse"`
	BurnedOrigination   int64     `pack:"3,snappy"     json:"burned_origination"`
	BurnedAllocation    int64     `pack:"4,snappy"     json:"burned_allocation"`
	BurnedSeedMiss      int64     `pack:"5,snappy"     json:"burned_seed_miss"`
	BurnedStorage       int64     `pack:"6,snappy"     json:"burned_storage"`
	BurnedExplicit      int64     `pack:"7,snappy"     json:"burned_explicit"`
	BurnedOffline       int64     `pack:"8,snappy"     json:"burned_offline"`
	BurnedRollup        int64     `pack:"9,snappy"     json:"burned_rollup"`
	Frozen              int64     `pack:"F,snappy"     json:"frozen"`
	FrozenDeposits      int64     `pack:"d,snappy"     json:"frozen_deposits"`     // <v18
	FrozenRewards       int64     `pack:"r,snappy"     json:"frozen_rewards"`      // <v18
	FrozenFees          int64     `pack:"f,snappy"     json:"frozen_fees"`         // <v18
	FrozenBonds         int64     `pack:"o,snappy"     json:"frozen_bonds"`        // rollup
	FrozenStake         int64     `pack:"x,snappy"     json:"frozen_stake"`        // v18+ all
	FrozenBakerStake    int64     `pack:"i,snappy"     json:"frozen_baker_stake"`  // v18+ baker only
	FrozenStakerStake   int64     `pack:"k,snappy"     json:"frozen_staker_stake"` // v18+ staker only
}

// Ensure Supply implements the pack.Item interface.
var _ pack.Item = (*Supply)(nil)

func (s *Supply) ID() uint64 {
	return s.RowId
}

func (s *Supply) SetID(id uint64) {
	s.RowId = id
}

func (m Supply) TableKey() string {
	return SupplyTableKey
}

func (m Supply) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 15,
		CacheSize:       16,
		FillLevel:       100,
	}
}

func (m Supply) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (s *Supply) Reset() {
	*s = Supply{}
}

// be compatible with time series interface
func (s Supply) Time() time.Time {
	return s.Timestamp
}

func (s *Supply) Update(b *Block, bakers map[AccountID]*Baker) {
	s.RowId = 0 // force allocating new id
	s.Height = b.Height
	s.Cycle = b.Cycle
	s.Timestamp = b.Timestamp

	// global mint/burn stats
	s.Total += b.MintedSupply - b.BurnedSupply
	s.Minted += b.MintedSupply
	s.Burned += b.BurnedSupply

	// replay events from flows for info that is not available in operations
	for _, f := range b.Flows {
		if f.IsUnfrozen {
			switch f.Kind {
			case FlowKindDeposits:
				s.FrozenDeposits -= f.AmountOut
			case FlowKindRewards:
				s.FrozenRewards -= f.AmountOut
			case FlowKindFees:
				s.FrozenFees -= f.AmountOut
			case FlowKindBond:
				s.FrozenBonds -= f.AmountOut
			}
		}
		if f.IsFrozen {
			switch f.Kind {
			case FlowKindDeposits:
				s.FrozenDeposits += f.AmountIn
			case FlowKindRewards:
				s.FrozenRewards += f.AmountIn
			case FlowKindFees:
				s.FrozenFees += f.AmountIn
			case FlowKindBond:
				s.FrozenBonds += f.AmountIn
			}
		}
		// count total Sapling shielded supply across all pools
		if f.IsShielded {
			s.Shielded += f.AmountIn
		}
		if f.IsUnshielded {
			s.Shielded -= f.AmountOut
		}
		// deduct unstaked burn
		if f.Type == FlowTypePenalty && f.IsUnfrozen && f.Kind == FlowKindStake {
			s.Unstaking -= f.AmountOut
		}
	}

	// replay events from ops to count details
	for _, op := range b.Ops {
		switch op.Type {
		case OpTypeBake, OpTypeBonus:
			s.MintedBaking += op.Reward

		case OpTypeSubsidy:
			s.MintedSubsidy += op.Reward

		case OpTypeActivation:
			s.Activated += op.Volume
			s.Unclaimed -= op.Volume

		case OpTypeNonceRevelation:
			s.MintedSeeding += op.Reward

		case OpTypeSeedSlash:
			s.BurnedSeedMiss += op.Burned

		case OpTypeEndorsement:
			// pre-Ithaca
			s.MintedEndorsing += op.Reward

		case OpTypeReward:
			// post-Ithaca rewards
			s.MintedEndorsing += op.Reward
			s.BurnedOffline += op.Burned

		case OpTypeDoubleBaking:
			s.BurnedDoubleBaking += op.Burned

		case OpTypeDoubleEndorsement, OpTypeDoublePreendorsement:
			s.BurnedDoubleEndorse += op.Burned

		case OpTypeInvoice, OpTypeAirdrop:
			s.MintedAirdrop += op.Reward

		case OpTypeOrigination, OpTypeRollupOrigination:
			if op.IsSuccess && !op.IsEvent {
				storageBurn := b.Params.CostPerByte * op.StoragePaid
				s.BurnedOrigination += op.Burned - storageBurn
				s.BurnedStorage += storageBurn
			}

		case OpTypeTransaction:
			// skip implicit ops like LB subsidy mint, but process internal tx
			if op.IsSuccess && !op.IsEvent {
				// general burn is already accounted for in block.BurnedSupply
				// here we only assign burn to different reasons
				storageBurn := b.Params.CostPerByte * op.StoragePaid
				s.BurnedAllocation += op.Burned - storageBurn
				s.BurnedStorage += storageBurn
				if op.IsBurnAddress {
					s.BurnedExplicit += op.Volume
				}
			}

		case OpTypeRegisterConstant, OpTypeTransferTicket:
			s.BurnedStorage += op.Burned

		case OpTypeRollupTransaction:
			s.BurnedRollup += op.Burned
			// output message sends internal tx which can allocate
			if op.IsSuccess && !op.IsEvent {
				// general burn is already accounted for in block.BurnedSupply
				// here we only assign burn to different reasons
				storageBurn := b.Params.CostPerByte * op.StoragePaid
				s.BurnedAllocation += op.Burned - storageBurn
				s.BurnedStorage += storageBurn
				if op.IsBurnAddress {
					s.BurnedExplicit += op.Volume
				}
			}

		case OpTypeUnstake:
			// oxford staking: we only count unstaked coins here,
			// total stake is taken from bakers below
			if op.IsSuccess {
				s.Unstaking += op.Deposit // op.Volume
			}

		case OpTypeFinalizeUnstake:
			if op.IsSuccess {
				s.Unstaking -= op.Volume
			}
		}
	}

	// update delegation info
	s.Staking = 0
	s.Delegated = 0
	s.ActiveStake = 0
	s.ActiveStaking = 0
	s.ActiveDelegated = 0
	s.InactiveStaking = 0
	s.InactiveDelegated = 0
	s.FrozenStake = 0
	s.FrozenBakerStake = 0
	s.FrozenStakerStake = 0
	for _, bkr := range bakers {
		own := bkr.StakeAmount(bkr.Account.StakeShares)
		s.Staking += bkr.TotalStake + bkr.FrozenDeposits
		s.FrozenStake += bkr.TotalStake
		s.Delegated += bkr.DelegatedBalance
		s.FrozenBakerStake += own
		s.FrozenStakerStake += bkr.TotalStake - own

		// does not account for overflows due to overstaking and overdelegation!
		if bkr.IsActive {
			s.ActiveStaking += bkr.TotalStake + bkr.FrozenDeposits
			s.ActiveDelegated += bkr.DelegatedBalance
		} else {
			s.InactiveStaking += bkr.TotalStake + bkr.FrozenDeposits
			s.InactiveDelegated += bkr.DelegatedBalance
		}

		// active stake used for rights requires adjustment for EOC
		s.ActiveStake += bkr.BakingPower(b.Params, bkr.StakeAdjust(b))
	}

	// add frozen coins
	s.Frozen = s.FrozenDeposits + s.FrozenFees + s.FrozenRewards + s.FrozenBonds + s.FrozenStake

	// general consensus: frozen is part of circulating even though baking
	// rewards are subject to slashing
	s.Circulating = s.Total - s.Unclaimed

	// metric to show how much is economically available for sale with the
	// reasonable assumption that activation (unclaimed) does not move as easily
	s.Liquid = s.Total - s.Frozen - s.Unclaimed
}

func (s *Supply) Rollback(b *Block) {
	// update identity only
	s.Height = b.Height
	s.Cycle = b.Cycle
}
