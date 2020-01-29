// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
)

type Supply struct {
	RowId               uint64    `pack:"I,pk,snappy" json:"row_id"`             // unique id
	Height              int64     `pack:"h,snappy"    json:"height"`             // bc: block height (also for orphans)
	Cycle               int64     `pack:"c,snappy"    json:"cycle"`              // bc: block cycle (tezos specific)
	Timestamp           time.Time `pack:"T,snappy"    json:"time"`               // bc: block creation time
	Total               int64     `pack:"t,snappy"    json:"total"`              // total available supply (including unclaimed)
	Activated           int64     `pack:"A,snappy"    json:"activated"`          // activated fundraiser supply
	Unclaimed           int64     `pack:"U,snappy"    json:"unclaimed"`          // all non-activated fundraiser supply
	Vested              int64     `pack:"V,snappy"    json:"vested"`             // foundation vested supply
	Unvested            int64     `pack:"N,snappy"    json:"unvested"`           // remaining unvested supply
	Circulating         int64     `pack:"C,snappy"    json:"circulating"`        // able to move next block floating (total - unvested)
	Delegated           int64     `pack:"E,snappy"    json:"delegated"`          // all delegated balances
	Staking             int64     `pack:"D,snappy"    json:"staking"`            // all delegated + delegate's own balances
	ActiveDelegated     int64     `pack:"G,snappy"    json:"active_delegated"`   // delegated  balances to active delegates
	ActiveStaking       int64     `pack:"J,snappy"    json:"active_staking"`     // delegated + delegate's own balances for active delegates
	InactiveDelegated   int64     `pack:"g,snappy"    json:"inactive_delegated"` // delegated  balances to inactive delegates
	InactiveStaking     int64     `pack:"j,snappy"    json:"inactive_staking"`   // delegated + delegate's own balances for inactive delegates
	Minted              int64     `pack:"M,snappy"    json:"minted"`
	MintedBaking        int64     `pack:"b,snappy"    json:"minted_baking"`
	MintedEndorsing     int64     `pack:"e,snappy"    json:"minted_endorsing"`
	MintedSeeding       int64     `pack:"s,snappy"    json:"minted_seeding"`
	MintedAirdrop       int64     `pack:"a,snappy"    json:"minted_airdrop"`
	Burned              int64     `pack:"B,snappy"    json:"burned"`
	BurnedDoubleBaking  int64     `pack:"1,snappy"    json:"burned_double_baking"`
	BurnedDoubleEndorse int64     `pack:"2,snappy"    json:"burned_double_endorse"`
	BurnedOrigination   int64     `pack:"3,snappy"    json:"burned_origination"`
	BurnedImplicit      int64     `pack:"4,snappy"    json:"burned_implicit"`
	BurnedSeedMiss      int64     `pack:"5,snappy"    json:"burned_seed_miss"`
	Frozen              int64     `pack:"F,snappy"    json:"frozen"`
	FrozenDeposits      int64     `pack:"d,snappy"    json:"frozen_deposits"`
	FrozenRewards       int64     `pack:"r,snappy"    json:"frozen_rewards"`
	FrozenFees          int64     `pack:"f,snappy"    json:"frozen_fees"`
}

// Ensure Supply implements the pack.Item interface.
var _ pack.Item = (*Supply)(nil)

func (s *Supply) ID() uint64 {
	return s.RowId
}

func (s *Supply) SetID(id uint64) {
	s.RowId = id
}

func (s *Supply) Update(b *Block, delegates map[AccountID]*Account) {
	s.RowId = 0 // force allocating new id
	s.Height = b.Height
	s.Cycle = b.Cycle
	s.Timestamp = b.Timestamp
	s.Total += b.Rewards - b.BurnedSupply
	s.Minted += b.Rewards
	s.Burned += b.BurnedSupply
	s.FrozenDeposits += b.Deposits - b.UnfrozenDeposits
	s.FrozenRewards += b.Rewards - b.UnfrozenRewards
	s.FrozenFees += b.Fees - b.UnfrozenFees
	s.Frozen = s.FrozenDeposits + s.FrozenFees + s.FrozenRewards

	// activated/unclaimed, vested/unvested, invoice/airdrop from flows
	for _, f := range b.Flows {
		switch f.Operation {
		case FlowTypeActivation:
			s.Activated += f.AmountIn
			s.Unclaimed -= f.AmountIn
		case FlowTypeVest:
			s.Vested += f.AmountIn
			s.Unvested -= f.AmountIn
		case FlowTypeNonceRevelation:
			// adjust different supply types because block.BurnedSupply contains
			// seed burn already, but it comes from frozen supply and not from
			// circulating supply
			if f.IsBurned {
				s.BurnedSeedMiss += f.AmountOut
				s.Frozen -= f.AmountOut
				switch f.Category {
				case FlowCategoryRewards:
					s.FrozenRewards -= f.AmountOut
				case FlowCategoryFees:
					s.FrozenFees -= f.AmountOut
				}
			}
		case FlowTypeInvoice, FlowTypeAirdrop:
			s.Total += f.AmountIn
			s.MintedAirdrop += f.AmountIn
			s.Minted += f.AmountIn
		}
	}

	// use ops to update bake and burn details
	for _, op := range b.Ops {
		switch op.Type {
		case chain.OpTypeSeedNonceRevelation:
			s.MintedSeeding += op.Reward
		case chain.OpTypeEndorsement:
			s.MintedEndorsing += op.Reward
		case chain.OpTypeDoubleBakingEvidence:
			s.BurnedDoubleBaking += op.Burned
		case chain.OpTypeDoubleEndorsementEvidence:
			s.BurnedDoubleEndorse += op.Burned
		case chain.OpTypeOrigination:
			s.BurnedOrigination += op.Burned
		case chain.OpTypeTransaction:
			s.BurnedImplicit += op.Burned
		}
	}
	// update supply totals across all delegates
	s.Staking = 0
	s.Delegated = 0
	s.ActiveStaking = 0
	s.ActiveDelegated = 0
	s.InactiveStaking = 0
	s.InactiveDelegated = 0
	for _, acc := range delegates {
		sb, db := acc.StakingBalance(), acc.DelegatedBalance
		s.Staking += sb
		s.Delegated += db
		if acc.IsActiveDelegate {
			s.ActiveStaking += sb
			s.ActiveDelegated += db
		} else {
			s.InactiveStaking += sb
			s.InactiveDelegated += db
		}
	}

	// we don't explicitly count baking and there's also no explicit op, but
	// we can calculate total baking rewards as difference to total rewards
	s.MintedBaking = s.Minted - s.MintedSeeding - s.MintedEndorsing - s.MintedAirdrop

	// unanimous consent that unclaimed can move at next block and frozen is
	// generally considered as part of circulating
	s.Circulating = s.Total - s.Unvested
}

func (s *Supply) Rollback(b *Block) {
	// update identity only
	s.Height = b.Height
	s.Cycle = b.Cycle
}
