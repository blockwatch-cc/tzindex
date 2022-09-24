// Copyright (c) 2020-2022 Blockwatch Data Inc.
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
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeRollupOrigination
        f.AmountOut = burned
        f.IsBurned = true
        flows = append(flows, f)
    }

    // handle delegation updates

    // debit from source delegation if not baker
    if srcbkr != nil && !src.IsBaker && feespaid+burned > 0 {
        f := model.NewFlow(b.block, srcbkr.Account, src, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeRollupOrigination
        f.AmountOut = feespaid + burned // fees and value burned
        flows = append(flows, f)
    }

    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

func (b *Builder) NewRollupTransactionFlows(
    src, dst, offender *model.Account,
    sbkr, obkr *model.Baker,
    fees, bal rpc.BalanceUpdates,
    block *model.Block,
    id model.OpRef) []*model.Flow {

    // apply fees
    flows, feespaid := b.NewFeeFlows(src, fees, id)

    // transaction may burn
    var srcBurn, offenderBurn int64
    for i, u := range bal {
        switch u.Kind {
        case "contract":
            if u.Change < 0 {
                // ignore deposit here (handled by freezer updates below)
                if len(bal) > i+1 && bal[i+1].Category == "storage fees" {
                    // origination burn
                    f := model.NewFlow(b.block, src, dst, id)
                    f.Category = model.FlowCategoryBalance
                    f.Operation = model.FlowTypeRollupOrigination
                    f.AmountOut = -u.Change
                    f.IsBurned = true
                    srcBurn += -u.Change
                    flows = append(flows, f)
                }
            } else {
                if i > 0 && bal[i-1].Category == "tx_rollup_rejection_rewards" {
                    // accuser rewards
                    f := model.NewFlow(b.block, src, dst, id)
                    f.Category = model.FlowCategoryBalance
                    f.Operation = model.FlowTypeRollupReward
                    f.AmountIn = u.Change
                    flows = append(flows, f)
                }
            }
        case "freezer":
            if u.Change > 0 {
                // deposit from balance to bond
                f := model.NewFlow(b.block, src, dst, id)
                f.Category = model.FlowCategoryBalance
                f.Operation = model.FlowTypeRollupTransaction
                f.AmountOut = u.Change
                flows = append(flows, f)
                f = model.NewFlow(b.block, src, dst, id)
                f.Category = model.FlowCategoryBond
                f.Operation = model.FlowTypeRollupTransaction
                f.AmountIn = u.Change
                f.IsFrozen = true
                flows = append(flows, f)
            } else {
                // slash or unlock
                if len(bal) > i+1 && bal[i+1].Category == "tx_rollup_rejection_punishments" {
                    // slash offender
                    f := model.NewFlow(b.block, offender, dst, id)
                    f.Category = model.FlowCategoryBond
                    f.Operation = model.FlowTypeRollupPenalty
                    f.AmountOut = -u.Change
                    f.IsBurned = true
                    offenderBurn += -u.Change
                    flows = append(flows, f)
                } else {
                    // unlock (move bond back to balance)
                    f := model.NewFlow(b.block, src, dst, id)
                    f.Category = model.FlowCategoryBond
                    f.Operation = model.FlowTypeRollupTransaction
                    f.AmountOut = -u.Change
                    f.IsUnfrozen = true
                    flows = append(flows, f)
                    f = model.NewFlow(b.block, src, dst, id)
                    f.Category = model.FlowCategoryBalance
                    f.Operation = model.FlowTypeRollupTransaction
                    f.AmountIn = -u.Change
                    flows = append(flows, f)
                }
            }
        }
    }

    // debit from source delegation unless source is a baker
    if feespaid+srcBurn > 0 && sbkr != nil && !src.IsBaker {
        f := model.NewFlow(b.block, sbkr.Account, src, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeRollupTransaction
        f.AmountOut = feespaid + srcBurn // fees and amount burned
        flows = append(flows, f)
    }

    // debit from offender delegation unless offender is a baker
    if offenderBurn > 0 && obkr != nil && !offender.IsBaker {
        f := model.NewFlow(b.block, obkr.Account, offender, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeRollupPenalty
        f.AmountOut = offenderBurn // amount burned
        flows = append(flows, f)
    }

    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}
