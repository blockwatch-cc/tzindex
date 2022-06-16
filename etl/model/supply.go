// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

// Note: removed vesting supply in v9.1, TF vesting time is over
type Supply struct {
	RowId               uint64    `pack:"I,pk"  json:"row_id"`             // unique id
	Height              int64     `pack:"h,i32" json:"height"`             // bc: block height (also for orphans)
	Cycle               int64     `pack:"c,i16" json:"cycle"`              // bc: block cycle (tezos specific)
	Timestamp           time.Time `pack:"T"     json:"time"`               // bc: block creation time
	Total               int64     `pack:"t"     json:"total"`              // total available supply (including unclaimed)
	Activated           int64     `pack:"A"     json:"activated"`          // activated fundraiser supply
	Unclaimed           int64     `pack:"U"     json:"unclaimed"`          // all non-activated fundraiser supply
	Circulating         int64     `pack:"C"     json:"circulating"`        // (total - unclaimed)
	Liquid              int64     `pack:"L"     json:"liquid"`             // (total - frozen - unclaimed)
	Delegated           int64     `pack:"E"     json:"delegated"`          // all delegated balances
	Staking             int64     `pack:"D"     json:"staking"`            // all delegated + delegate's own balances
	Shielded            int64     `pack:"S"     json:"shielded"`           // Sapling shielded supply
	ActiveStake         int64     `pack:"K"     json:"active_stake"`       // active network wide stake
	ActiveDelegated     int64     `pack:"G"     json:"active_delegated"`   // delegated  balances to active delegates
	ActiveStaking       int64     `pack:"J"     json:"active_staking"`     // delegated + delegate's own balances for active delegates
	InactiveDelegated   int64     `pack:"g"     json:"inactive_delegated"` // delegated  balances to inactive delegates
	InactiveStaking     int64     `pack:"j"     json:"inactive_staking"`   // delegated + delegate's own balances for inactive delegates
	Minted              int64     `pack:"M"     json:"minted"`
	MintedBaking        int64     `pack:"b"     json:"minted_baking"`
	MintedEndorsing     int64     `pack:"e"     json:"minted_endorsing"`
	MintedSeeding       int64     `pack:"s"     json:"minted_seeding"`
	MintedAirdrop       int64     `pack:"a"     json:"minted_airdrop"`
	MintedSubsidy       int64     `pack:"y"     json:"minted_subsidy"`
	Burned              int64     `pack:"B"     json:"burned"`
	BurnedDoubleBaking  int64     `pack:"1"     json:"burned_double_baking"`
	BurnedDoubleEndorse int64     `pack:"2"     json:"burned_double_endorse"`
	BurnedOrigination   int64     `pack:"3"     json:"burned_origination"`
	BurnedAllocation    int64     `pack:"4"     json:"burned_allocation"`
	BurnedSeedMiss      int64     `pack:"5"     json:"burned_seed_miss"`
	BurnedStorage       int64     `pack:"6"     json:"burned_storage"`
	BurnedExplicit      int64     `pack:"7"     json:"burned_explicit"`
	BurnedAbsence       int64     `pack:"8"     json:"burned_absence"`
	BurnedRollup        int64     `pack:"9"     json:"burned_rollup"`
	Frozen              int64     `pack:"F"     json:"frozen"`
	FrozenDeposits      int64     `pack:"d"     json:"frozen_deposits"`
	FrozenRewards       int64     `pack:"r"     json:"frozen_rewards"`
	FrozenFees          int64     `pack:"f"     json:"frozen_fees"`
	FrozenBonds         int64     `pack:"o"     json:"frozen_bonds"`
}

// Ensure Supply implements the pack.Item interface.
var _ pack.Item = (*Supply)(nil)

func (s *Supply) ID() uint64 {
	return s.RowId
}

func (s *Supply) SetID(id uint64) {
	s.RowId = id
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
			switch f.Category {
			case FlowCategoryDeposits:
				s.FrozenDeposits -= f.AmountOut
			case FlowCategoryRewards:
				s.FrozenRewards -= f.AmountOut
			case FlowCategoryFees:
				s.FrozenFees -= f.AmountOut
			case FlowCategoryBond:
				s.FrozenBonds -= f.AmountOut
			}
		}
		if f.IsFrozen {
			switch f.Category {
			case FlowCategoryDeposits:
				s.FrozenDeposits += f.AmountIn
			case FlowCategoryRewards:
				s.FrozenRewards += f.AmountIn
			case FlowCategoryFees:
				s.FrozenFees += f.AmountIn
			case FlowCategoryBond:
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
			s.BurnedAbsence += op.Burned

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
			// skip implicit ops like LB subsidy mint
			if op.IsSuccess && !op.IsEvent {
				// general burn is already accounted for in block.BurnedSupply
				// here we only assign burn to different reasons
				storageBurn := b.Params.CostPerByte * op.StoragePaid
				s.BurnedAllocation += op.Burned - storageBurn
				s.BurnedStorage += storageBurn
				tx, _ := op.Raw.(*rpc.Transaction)
				if tx.Destination.Equal(tezos.ZeroAddress) {
					s.BurnedExplicit += op.Volume
				}
				for _, iop := range tx.Metadata.InternalResults {
					if iop.Kind != tezos.OpTypeTransaction || !iop.Destination.IsValid() {
						continue
					}
					if iop.Destination.Equal(tezos.ZeroAddress) {
						s.BurnedExplicit += iop.Amount
					}
				}
			}

		case OpTypeRegisterConstant:
			s.BurnedStorage += op.Burned

		case OpTypeRollupTransaction:
			s.BurnedRollup += op.Burned
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
	for _, bkr := range bakers {
		sb, db := bkr.StakingBalance(), bkr.DelegatedBalance
		s.Staking += sb
		s.Delegated += db
		s.ActiveStake += bkr.ActiveStake(b.Params, b.Chain.Rolls)
		if bkr.IsActive {
			s.ActiveStaking += sb
			s.ActiveDelegated += db
		} else {
			s.InactiveStaking += sb
			s.InactiveDelegated += db
		}
	}

	// add frozen coins
	s.Frozen = s.FrozenDeposits + s.FrozenFees + s.FrozenRewards + s.FrozenBonds

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
