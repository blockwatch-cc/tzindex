// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"math"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
)

var incomePool = &sync.Pool{
	New: func() interface{} { return new(Income) },
}

// Income is a per-cycle income sheet for baker accounts.
type Income struct {
	RowId                  uint64    `pack:"I,pk,snappy" json:"row_id"`
	Cycle                  int64     `pack:"c,snappy"    json:"cycle"` // this income cycle (=snapshot+7)
	AccountId              AccountID `pack:"A,snappy"    json:"account_id"`
	Rolls                  int64     `pack:"o,snappy"    json:"rolls"`         // at snapshot block
	Balance                int64     `pack:"B,snappy"    json:"balance"`       // at snapshot block
	Delegated              int64     `pack:"d,snappy"    json:"delegated"`     // at snapshot block
	NDelegations           int64     `pack:"n,snappy"    json:"n_delegations"` // at snapshot block
	NBakingRights          int64     `pack:"R,snappy"    json:"n_baking_rights"`
	NEndorsingRights       int64     `pack:"r,snappy"    json:"n_endorsing_rights"`
	Luck                   int64     `pack:"L,snappy"    json:"luck"`                 // in coins based on fair share by rolls
	LuckPct                int64     `pack:"l,snappy"    json:"luck_percent"`         // 0.0 .. +N.00 based on fair share by rolls
	ContributionPct        int64     `pack:"t,snappy"    json:"contribution_percent"` // 0.0 .. +N.00 based on rights utilized
	PerformancePct         int64     `pack:"p,snappy"    json:"performance_percent"`  // -N.00 .. +N.00 based on expected income
	NBlocksBaked           int64     `pack:"k,snappy"    json:"n_blocks_baked"`
	NBlocksLost            int64     `pack:"X,snappy"    json:"n_blocks_lost"`
	NBlocksStolen          int64     `pack:"Y,snappy"    json:"n_blocks_stolen"`
	NSlotsEndorsed         int64     `pack:"Z,snappy"    json:"n_slots_endorsed"`
	NSlotsMissed           int64     `pack:"M,snappy"    json:"n_slots_missed"`
	NSeedsRevealed         int64     `pack:"s,snappy"    json:"n_seeds_revealed"`
	ExpectedIncome         int64     `pack:"f,snappy"    json:"expected_income"`
	ExpectedBonds          int64     `pack:"g,snappy"    json:"expected_bonds"`
	TotalIncome            int64     `pack:"i,snappy"    json:"total_income"`
	TotalBonds             int64     `pack:"b,snappy"    json:"total_bonds"`
	BakingIncome           int64     `pack:"1,snappy"    json:"baking_income"`
	EndorsingIncome        int64     `pack:"2,snappy"    json:"endorsing_income"`
	DoubleBakingIncome     int64     `pack:"3,snappy"    json:"double_baking_income"`
	DoubleEndorsingIncome  int64     `pack:"4,snappy"    json:"double_endorsing_income"`
	SeedIncome             int64     `pack:"5,snappy"    json:"seed_income"`
	FeesIncome             int64     `pack:"6,snappy"    json:"fees_income"`
	MissedBakingIncome     int64     `pack:"x,snappy"    json:"missed_baking_income"`     // from lost blocks
	MissedEndorsingIncome  int64     `pack:"m,snappy"    json:"missed_endorsing_income"`  // from missed endorsements
	StolenBakingIncome     int64     `pack:"y,snappy"    json:"stolen_baking_income"`     // from others
	TotalLost              int64     `pack:"z,snappy"    json:"total_lost"`               // from all denounciations and missed seed nonce revelations
	LostAccusationFees     int64     `pack:"F,snappy"    json:"lost_accusation_fees"`     // from denounciations
	LostAccusationRewards  int64     `pack:"E,snappy"    json:"lost_accusation_rewards"`  // from denounciations
	LostAccusationDeposits int64     `pack:"D,snappy"    json:"lost_accusation_deposits"` // from denounciations
	LostRevelationFees     int64     `pack:"V,snappy"    json:"lost_revelation_fees"`     // from missed seed nonce revelations
	LostRevelationRewards  int64     `pack:"W,snappy"    json:"lost_revelation_rewards"`  // from missed seed nonce revelations
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
	return uint64(s.RowId)
}

func (s *Income) SetID(id uint64) {
	s.RowId = id
}

func (s *Income) Reset() {
	s.RowId = 0
	s.Cycle = 0
	s.AccountId = 0
	s.Rolls = 0
	s.Balance = 0
	s.Delegated = 0
	s.NDelegations = 0
	s.NBakingRights = 0
	s.NEndorsingRights = 0
	s.Luck = 0
	s.LuckPct = 0
	s.ContributionPct = 0
	s.PerformancePct = 0
	s.NBlocksBaked = 0
	s.NBlocksLost = 0
	s.NBlocksStolen = 0
	s.NSlotsEndorsed = 0
	s.NSlotsMissed = 0
	s.NSeedsRevealed = 0
	s.ExpectedIncome = 0
	s.ExpectedBonds = 0
	s.TotalIncome = 0
	s.TotalBonds = 0
	s.BakingIncome = 0
	s.EndorsingIncome = 0
	s.DoubleBakingIncome = 0
	s.DoubleEndorsingIncome = 0
	s.SeedIncome = 0
	s.FeesIncome = 0
	s.MissedBakingIncome = 0
	s.MissedEndorsingIncome = 0
	s.StolenBakingIncome = 0
	s.TotalLost = 0
	s.LostAccusationFees = 0
	s.LostAccusationRewards = 0
	s.LostAccusationDeposits = 0
	s.LostRevelationFees = 0
	s.LostRevelationRewards = 0
}

func (s *Income) UpdateLuck(totalRolls int64, p *chain.Params) {
	// fraction of all rolls
	rollsShare := float64(s.Rolls) / float64(totalRolls)

	// full blocks, truncated
	fairBakingShare := int64(math.Round(rollsShare * float64(p.BlocksPerCycle)))

	// full endorsements, truncated
	fairEndorsingShare := int64(math.Round(rollsShare * float64(p.BlocksPerCycle) * float64(p.EndorsersPerBlock)))

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
