// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"

	"blockwatch.cc/packdb/pack"
)

const CycleTableKey = "cycle"

var ErrNoCycle = errors.New("cycle not indexed")

// Cycle is a per-cycle stats collector.
type Cycle struct {
	RowId                    uint64 `pack:"I,pk" json:"row_id"`
	Cycle                    int64  `pack:"a"    json:"cycle"`
	StartHeight              int64  `pack:"b"    json:"start_height"`
	EndHeight                int64  `pack:"c"    json:"end_height"`
	SnapshotHeight           int64  `pack:"d"    json:"snapshot_height"`
	SnapshotIndex            int    `pack:"e"    json:"snapshot_index"`
	MissedRounds             int    `pack:"f"    json:"missed_rounds"`
	MissedEndorsements       int    `pack:"g"    json:"missed_endorsements"`
	Num2Baking               int    `pack:"h"    json:"n_double_baking"`
	Num2Endorsement          int    `pack:"i"    json:"n_double_endorsement"`
	NumSeeds                 int    `pack:"j"    json:"n_seed_nonces"`
	SolveTimeMin             int    `pack:"k"    json:"solvetime_min"`
	SolveTimeMax             int    `pack:"l"    json:"solvetime_max"`
	SolveTimeSum             int    `pack:"m"    json:"solvetime_sum"`
	RoundMin                 int    `pack:"n"    json:"round_min"`
	RoundMax                 int    `pack:"o"    json:"round_max"`
	EndorsementsMin          int    `pack:"q"    json:"endorsements_min"`
	EndorsementsMax          int    `pack:"r"    json:"endorsements_max"`
	WorstBakedBlock          int64  `pack:"t"    json:"worst_baked_block"`
	WorstEndorsedBlock       int64  `pack:"u"    json:"worst_endorsed_block"`
	UniqueBakers             int    `pack:"v"    json:"unique_bakers"`
	BlockReward              int64  `pack:"w"    json:"block_reward"`
	BlockBonusPerSlot        int64  `pack:"x"    json:"block_bonus_per_slot"`
	MaxBlockReward           int64  `pack:"y"    json:"max_block_reward"`
	EndorsementRewardPerSlot int64  `pack:"z"    json:"endorsement_reward_per_slot"`
	NonceRevelationReward    int64  `pack:"1"    json:"nonce_revelation_reward"`
	VdfRevelationReward      int64  `pack:"2"    json:"vdf_revelation_reward"`
	LBSubsidy                int64  `pack:"3"    json:"lb_subsidy"`
}

// Ensure Cycle implements the pack.Item interface.
var _ pack.Item = (*Cycle)(nil)

func (c Cycle) ID() uint64 {
	return c.RowId
}

func (c *Cycle) SetID(id uint64) {
	c.RowId = id
}

func (m Cycle) TableKey() string {
	return CycleTableKey
}

func (m Cycle) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    8,
		JournalSizeLog2: 8,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Cycle) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}
