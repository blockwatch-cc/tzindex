// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"math"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/rpc"
)

const IncomeTableKey = "income"

var (
	incomePool = &sync.Pool{
		New: func() interface{} { return new(Income) },
	}

	ErrNoIncome = errors.New("income not indexed")
)

// Income is a per-cycle income sheet for baker accounts.
type Income struct {
	RowId                  uint64    `pack:"I,pk,snappy"     json:"row_id"`
	Cycle                  int64     `pack:"c,i16,snappy"    json:"cycle"`
	StartHeight            int64     `pack:"h,i32"           json:"start_height"`
	EndHeight              int64     `pack:"e,i32"           json:"end_height"`
	AccountId              AccountID `pack:"A,u32,snappy"    json:"account_id"`
	Balance                int64     `pack:"B,snappy"        json:"balance"`         // @snapshot
	Delegated              int64     `pack:"d,snappy"        json:"delegated"`       // @snapshot
	OwnStake               int64     `pack:"b,snappy"        json:"own_stake"`       // @snapshot
	StakingBalance         int64     `pack:"S,snappy"        json:"staking_balance"` // @snapshot
	NDelegations           int64     `pack:"n,i32,snappy"    json:"n_delegations"`   // @snapshot
	NStakers               int64     `pack:"#,i32,snappy"    json:"n_stakers"`       // @snapshot
	NBakingRights          int64     `pack:"R,i16,snappy"    json:"n_baking_rights"`
	NEndorsingRights       int64     `pack:"r,i32,snappy"    json:"n_endorsing_rights"`
	Luck                   int64     `pack:"L,snappy"        json:"luck"`                       // coins by fair share of stake
	LuckPct                int64     `pack:"l,d32,scale=2,snappy"  json:"luck_percent"`         // 0.0 .. +N.00 by fair share of stake
	ContributionPct        int64     `pack:"t,d32,scale=2,snappy"  json:"contribution_percent"` // 0.0 .. +N.00 by rights utilized
	PerformancePct         int64     `pack:"p,d32,scale=2,snappy"  json:"performance_percent"`  // -N.00 .. +N.00 by expected income
	NBlocksBaked           int64     `pack:"k,i16,snappy"    json:"n_blocks_baked"`
	NBlocksProposed        int64     `pack:"K,i16,snappy"    json:"n_blocks_proposed"`
	NBlocksNotBaked        int64     `pack:"N,i16,snappy"    json:"n_blocks_not_baked"`
	NBlocksEndorsed        int64     `pack:"E,i16,snappy"    json:"n_blocks_endorsed"`
	NBlocksNotEndorsed     int64     `pack:"X,i16,snappy"    json:"n_blocks_not_endorsed"`
	NSlotsEndorsed         int64     `pack:"Z,i32,snappy"    json:"n_slots_endorsed"`
	NSeedsRevealed         int64     `pack:"s,i16,snappy"    json:"n_seeds_revealed"`
	ExpectedIncome         int64     `pack:"f,snappy"        json:"expected_income"`
	TotalIncome            int64     `pack:"i,snappy"        json:"total_income"`
	BakingIncome           int64     `pack:"1,snappy"        json:"baking_income"`
	EndorsingIncome        int64     `pack:"2,snappy"        json:"endorsing_income"`
	AccusationIncome       int64     `pack:"3,snappy"        json:"accusation_income"`
	SeedIncome             int64     `pack:"4,snappy"        json:"seed_income"`
	FeesIncome             int64     `pack:"5,snappy"        json:"fees_income"`
	TotalLoss              int64     `pack:"6,snappy"        json:"total_loss"`
	AccusationLoss         int64     `pack:"7,snappy"        json:"accusation_loss"`
	SeedLoss               int64     `pack:"8,snappy"        json:"seed_loss"`
	EndorsingLoss          int64     `pack:"9,snappy"        json:"endorsing_loss"`
	LostAccusationFees     int64     `pack:"F,snappy"        json:"lost_accusation_fees"`     // from denunciations
	LostAccusationRewards  int64     `pack:"W,snappy"        json:"lost_accusation_rewards"`  // from denunciations
	LostAccusationDeposits int64     `pack:"D,snappy"        json:"lost_accusation_deposits"` // from denunciations
	LostSeedFees           int64     `pack:"V,snappy"        json:"lost_seed_fees"`           // from missed seed nonce revelations
	LostSeedRewards        int64     `pack:"Y,snappy"        json:"lost_seed_rewards"`        // from missed seed nonce revelations

	// LostAccusationStake
}

// Ensure Income implements the pack.Item interface.
var _ pack.Item = (*Income)(nil)

func NewIncome() *Income {
	return allocIncome()
}

func allocIncome() *Income {
	return incomePool.Get().(*Income)
}

func (s *Income) Free() {
	s.Reset()
	incomePool.Put(s)
}

func (s Income) ID() uint64 {
	return s.RowId
}

func (s *Income) SetID(id uint64) {
	s.RowId = id
}

func (m Income) TableKey() string {
	return IncomeTableKey
}

func (m Income) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    12,
		JournalSizeLog2: 12,
		CacheSize:       32,
		FillLevel:       100,
	}
}

func (m Income) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (s *Income) Reset() {
	*s = Income{}
}

func (s *Income) UpdateLuck(totalStake int64, p *rpc.Params) {
	// fraction of all stake
	share := float64(s.StakingBalance) / float64(totalStake)

	// full blocks, truncated
	fairBakingShare := int64(math.Round(share * float64(p.BlocksPerCycle)))

	// full endorsements, truncated
	fairEndorsingShare := int64(math.Round(share * float64(p.BlocksPerCycle) * float64(p.EndorsersPerBlock+p.ConsensusCommitteeSize)))

	// fair income as a multiple of blocks and endorsements
	fairIncome := fairBakingShare * p.BlockReward
	fairIncome += fairEndorsingShare * p.EndorsementReward

	// diff between expected and fair (positive when higher, negative when lower)
	s.Luck = s.ExpectedIncome - fairIncome

	// absolute luck as expected vs fair income where 100% is the ideal case
	// =100%: fair == expected (luck == 0)
	// <100%: fair > expected (luck < 0)
	// >100%: fair < expected (luck > 0)
	if fairIncome > 0 {
		s.LuckPct = 10000 + s.Luck*10000/fairIncome
	}
}

func (v *Income) UpdatePerformance(reliability int64) {
	// absolute performance as expected vs actual income where 100% is the ideal case
	// use running totals as benchmark to keep updating while a cycle is filled
	// =100%: total == expected
	// <100%: total < expected (may be <0 if slashed)
	// >100%: total > expected
	//
	// total loss = accusation + seed + endorsing losses
	// post Ithaca seed & endorsing are not part of income
	diff := v.TotalIncome - v.AccusationLoss - v.ExpectedIncome
	if v.ExpectedIncome > 0 {
		v.PerformancePct = max(0, 10000+diff*10000/v.ExpectedIncome)
	} else {
		v.PerformancePct = 10000
	}
	// reliability resp. contribution to consensus is based on rights used
	// this value is caluclated in rights index and passed via baker objects
	totalRights := v.NBakingRights + v.NEndorsingRights
	if totalRights > 0 {
		v.ContributionPct = reliability
	} else {
		v.ContributionPct = 10000
	}
}
