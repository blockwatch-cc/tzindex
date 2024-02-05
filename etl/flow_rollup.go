// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewRollupOriginationFlows(
	src, dst *model.Account,
	srcbkr *model.Baker,
	fees, bal rpc.BalanceUpdates,
	id model.OpRef) []*model.Flow {

	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// origination may burn funds
	var burned int64
	for _, u := range bal {
		// we only consider contract out flows and calculate
		// the burn even though Ithaca makes this explict
		if u.Kind == "contract" {
			if u.Change < 0 {
				burned += -u.Change
			}
		}
	}

	if burned > 0 {
		// debit from source as burn
		f := model.NewFlow(b.block, src, nil, id)
		f.Kind = model.FlowKindBalance
		f.Type = model.FlowTypeRollupOrigination
		f.AmountOut = burned
		f.IsBurned = true
		flows = append(flows, f)
	}

	// handle delegation updates

	// debit from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && feespaid+burned > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeRollupOrigination
		f.AmountOut = feespaid + burned // fees and value burned
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

var (
	rollupRewardTypes = map[string]struct{}{
		"tx_rollup_rejection_rewards":     {},
		"smart_rollup_refutation_rewards": {},
	}
	rollupPunishTypes = map[string]struct{}{
		"tx_rollup_rejection_punishments":     {},
		"smart_rollup_refutation_punishments": {},
	}
)

func (b *Builder) NewRollupTransactionFlows(
	src, dst, loser, winner, recv *model.Account,
	sbkr, lbkr, wbkr *model.Baker,
	fees, bal rpc.BalanceUpdates,
	block *model.Block,
	id model.OpRef) []*model.Flow {

	// apply fees
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// transaction may burn
	var srcBurn, loserBurn int64
	for i, u := range bal {
		switch u.Kind {
		case "contract":
			if u.Change < 0 {
				// ignore deposit here (handled by freezer updates below)
				if len(bal) > i+1 && bal[i+1].Kind == "storage fees" {
					// origination burn
					f := model.NewFlow(b.block, src, dst, id)
					f.Kind = model.FlowKindBalance
					f.Type = model.FlowTypeRollupOrigination
					f.AmountOut = -u.Change
					f.IsBurned = true
					srcBurn += -u.Change
					flows = append(flows, f)
				}
			} else {
				_, isReward := rollupRewardTypes[bal[i-1].Category]
				if i > 0 && isReward {
					// accuser rewards
					f := model.NewFlow(b.block, winner, dst, id)
					f.Kind = model.FlowKindBalance
					f.Type = model.FlowTypeRollupReward
					f.AmountIn = u.Change
					flows = append(flows, f)

					// credit to winner delegation unless winner is a baker
					if wbkr != nil && !winner.IsBaker {
						f := model.NewFlow(b.block, wbkr.Account, winner, id)
						f.Kind = model.FlowKindDelegation
						f.Type = model.FlowTypeRollupPenalty
						f.AmountIn = u.Change
						flows = append(flows, f)
					}
				}
			}
		case "freezer":
			if u.Change > 0 {
				// deposit from balance to bond
				f := model.NewFlow(b.block, src, dst, id)
				f.Kind = model.FlowKindBalance
				f.Type = model.FlowTypeRollupTransaction
				f.AmountOut = u.Change
				flows = append(flows, f)
				f = model.NewFlow(b.block, src, dst, id)
				f.Kind = model.FlowKindBond
				f.Type = model.FlowTypeRollupTransaction
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			} else {
				// slash or unlock
				_, isSlash := rollupPunishTypes[bal[i+1].Category]
				if isSlash {
					// slash loser
					f := model.NewFlow(b.block, loser, dst, id)
					f.Kind = model.FlowKindBond
					f.Type = model.FlowTypeRollupPenalty
					f.AmountOut = -u.Change
					f.IsBurned = true
					loserBurn += -u.Change
					flows = append(flows, f)
				} else {
					// unlock (move bond back to balance)
					f := model.NewFlow(b.block, recv, dst, id)
					f.Kind = model.FlowKindBond
					f.Type = model.FlowTypeRollupTransaction
					f.AmountOut = -u.Change
					f.IsUnfrozen = true
					flows = append(flows, f)
					f = model.NewFlow(b.block, recv, dst, id)
					f.Kind = model.FlowKindBalance
					f.Type = model.FlowTypeRollupTransaction
					f.AmountIn = -u.Change
					flows = append(flows, f)
				}
			}
		}
	}

	// debit from source delegation unless source is a baker
	if feespaid+srcBurn > 0 && sbkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, sbkr.Account, src, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeRollupTransaction
		f.AmountOut = feespaid + srcBurn // fees and amount burned
		flows = append(flows, f)
	}

	// debit from loser delegation unless loser is a baker
	if loserBurn > 0 && lbkr != nil && !loser.IsBaker {
		f := model.NewFlow(b.block, lbkr.Account, loser, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeRollupPenalty
		f.AmountOut = loserBurn // amount burned
		flows = append(flows, f)
	}

	// receiver delegation does not change on unlock

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
