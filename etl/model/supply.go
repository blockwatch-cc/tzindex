// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

type Supply struct {
	RowId               uint64    `pack:"I,pk,snappy"  json:"row_id"`
	Height              int64     `pack:"h,snappy"     json:"height"`
	Cycle               int64     `pack:"c,snappy"     json:"cycle"`
	Timestamp           time.Time `pack:"T,snappy"     json:"time"`
	Total               int64     `pack:"t,snappy"     json:"total"`
	Activated           int64     `pack:"A,snappy"     json:"activated"`
	Unclaimed           int64     `pack:"U,snappy"     json:"unclaimed"`
	Circulating         int64     `pack:"C,snappy"     json:"circulating"`
	Liquid              int64     `pack:"L,snappy"     json:"liquid"`
	Delegated           int64     `pack:"E,snappy"     json:"delegated"`
	Staking             int64     `pack:"D,snappy"     json:"staking"`
	Shielded            int64     `pack:"S,snappy"     json:"shielded"`
	ActiveDelegated     int64     `pack:"G,snappy"     json:"active_delegated"`
	ActiveStaking       int64     `pack:"J,snappy"     json:"active_staking"`
	InactiveDelegated   int64     `pack:"g,snappy"     json:"inactive_delegated"`
	InactiveStaking     int64     `pack:"j,snappy"     json:"inactive_staking"`
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
	BurnedAbsence       int64     `pack:"8,snappy"     json:"burned_absence"`
	Frozen              int64     `pack:"F,snappy"     json:"frozen"`
	FrozenDeposits      int64     `pack:"d,snappy"     json:"frozen_deposits"`
	FrozenRewards       int64     `pack:"r,snappy"     json:"frozen_rewards"`
	FrozenFees          int64     `pack:"f,snappy"     json:"frozen_fees"`
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

		case OpTypeOrigination:
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
		}
	}

	// update delegation info
	s.Staking = 0
	s.Delegated = 0
	s.ActiveStaking = 0
	s.ActiveDelegated = 0
	s.InactiveStaking = 0
	s.InactiveDelegated = 0
	for _, bkr := range bakers {
		sb, db := bkr.StakingBalance(), bkr.DelegatedBalance
		s.Staking += sb
		s.Delegated += db
		if bkr.IsActive {
			s.ActiveStaking += sb
			s.ActiveDelegated += db
		} else {
			s.InactiveStaking += sb
			s.InactiveDelegated += db
		}
	}

	// add frozen coins
	s.Frozen = s.FrozenDeposits + s.FrozenFees + s.FrozenRewards

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
