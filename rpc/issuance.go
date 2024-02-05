// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
)

type Issuance struct {
	Cycle           int64 `json:"cycle"`
	BakingReward    int64 `json:"baking_reward_fixed_portion,string"`
	BakingBonus     int64 `json:"baking_reward_bonus_per_slot,string"`
	AttestingReward int64 `json:"attesting_reward_per_slot,string"`
	LBSubsidy       int64 `json:"liquidity_baking_subsidy,string"`
	SeedNonceTip    int64 `json:"seed_nonce_revelation_tip,string"`
	VdfTip          int64 `json:"vdf_revelation_tip,string"`
}

// GetIssuance returns expected xtz issuance for known future cycles
func (c *Client) GetIssuance(ctx context.Context, id BlockID) ([]Issuance, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/context/issuance/expected_issuance", id)
	p := make([]Issuance, 0, 5)
	if err := c.Get(ctx, u, &p); err != nil {
		return nil, err
	}
	return p, nil
}

func (c *Client) FetchIssuanceByCycle(ctx context.Context, height, cycle int64, bundle *Bundle) error {
	// this feature is only supported when adaptive issuance is activated
	// before AI, we fill issuance from params
	switch {
	case bundle.Params.Version == 0:
		// no issuance on genesis
	case bundle.Params.Version < 12:
		bundle.Issuance = []Issuance{
			{
				Cycle:           cycle - bundle.Params.PreservedCycles,
				BakingReward:    bundle.Params.BlockReward,
				AttestingReward: bundle.Params.EndorsementReward,
				SeedNonceTip:    bundle.Params.SeedNonceRevelationTip,
			},
			{
				Cycle:           cycle,
				BakingReward:    bundle.Params.BlockReward,
				AttestingReward: bundle.Params.EndorsementReward,
				SeedNonceTip:    bundle.Params.SeedNonceRevelationTip,
			},
		}
	case bundle.Params.Version < 18:
		bundle.Issuance = []Issuance{
			{
				Cycle:           cycle - bundle.Params.PreservedCycles,
				BakingReward:    bundle.Params.BakingRewardFixedPortion,
				BakingBonus:     bundle.Params.BakingRewardBonusPerSlot,
				AttestingReward: bundle.Params.EndorsingRewardPerSlot,
				LBSubsidy:       bundle.Params.LiquidityBakingSubsidy,
				SeedNonceTip:    bundle.Params.SeedNonceRevelationTip,
				VdfTip:          bundle.Params.SeedNonceRevelationTip,
			},
			{
				Cycle:           cycle,
				BakingReward:    bundle.Params.BakingRewardFixedPortion,
				BakingBonus:     bundle.Params.BakingRewardBonusPerSlot,
				AttestingReward: bundle.Params.EndorsingRewardPerSlot,
				LBSubsidy:       bundle.Params.LiquidityBakingSubsidy,
				SeedNonceTip:    bundle.Params.SeedNonceRevelationTip,
				VdfTip:          bundle.Params.SeedNonceRevelationTip,
			},
		}
	default:
		// this should fetch all future cycles' issuance data
		is, err := c.GetIssuance(ctx, BlockLevel(height))
		if err != nil {
			return fmt.Errorf("issuance: %v", err)
		}
		if len(is) == 0 {
			return fmt.Errorf("empty issuance array, make sure your Tezos node runs in archive mode")
		}
		var found bool
		for _, v := range is {
			if v.Cycle != cycle {
				continue
			}
			found = true
		}
		if !found {
			return fmt.Errorf("missing issuance for C_%d from block %d, have %#v", cycle, height, is)
		}
		bundle.Issuance = is
	}
	return nil
}
