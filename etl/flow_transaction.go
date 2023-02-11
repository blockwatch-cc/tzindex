// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "blockwatch.cc/tzgo/micheline"
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewTransactionFlows(
    src, dst *model.Account,
    sbkr, dbkr *model.Baker,
    srccon, dstcon *model.Contract,
    fees, bal rpc.BalanceUpdates,
    block *model.Block,
    id model.OpRef) []*model.Flow {

    // apply fees
    flows, feespaid := b.NewFeeFlows(src, fees, id)

    // transaction may burn and transfer
    var moved, burnedAndMoved int64
    for _, u := range bal {
        // we only consider contract in/out flows and calculate
        // the burn even though Ithaca makes this explict
        if u.Kind == "contract" {
            if u.Change < 0 {
                burnedAndMoved += -u.Change
            } else {
                moved += u.Change
            }
        }
    }

    // create move and burn flows when necessary
    if moved > 0 && dst != nil {
        // debit from source
        f := model.NewFlow(b.block, src, dst, id)
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeTransaction
        f.AmountOut = moved
        f.TokenAge = block.Age(src.LastIn)
        f.IsUnshielded = srccon != nil && srccon.Features.Contains(micheline.FeatureSapling)
        flows = append(flows, f)
        // credit to dest
        f = model.NewFlow(b.block, dst, src, id)
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeTransaction
        f.AmountIn = moved
        f.TokenAge = block.Age(src.LastIn)
        f.IsShielded = dstcon != nil && dstcon.Features.Contains(micheline.FeatureSapling)
        flows = append(flows, f)
    }

    if burnedAndMoved-moved > 0 {
        // debit burn from source
        f := model.NewFlow(b.block, src, nil, id)
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeTransaction
        f.AmountOut = burnedAndMoved - moved
        f.IsBurned = true
        flows = append(flows, f)
    }

    // debit from source delegation unless source is a baker
    if sbkr != nil && !src.IsBaker && feespaid+burnedAndMoved > 0 {
        f := model.NewFlow(b.block, sbkr.Account, src, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeTransaction
        f.AmountOut = feespaid + burnedAndMoved // fees and amount moved
        flows = append(flows, f)
    }

    // credit to destination baker unless dest is a baker
    if dbkr != nil && !dst.IsBaker && moved > 0 {
        f := model.NewFlow(b.block, dbkr.Account, src, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeTransaction
        f.AmountIn = moved
        flows = append(flows, f)
    }

    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

// fees are already paid by outer tx
// burn is attributed to outer source
func (b *Builder) NewInternalTransactionFlows(
    origsrc, src, dst *model.Account,
    origbkr, srcbkr, dstbkr *model.Baker,
    srccon, dstcon *model.Contract,
    bal rpc.BalanceUpdates,
    block *model.Block,
    id model.OpRef,
) []*model.Flow {
    flows := make([]*model.Flow, 0)

    // transaction may burn and transfer
    var moved, burnedAndMoved int64
    for _, u := range bal {
        // we only consider contract in/out flows and calculate
        // the burn even though Ithaca makes this explict
        if u.Kind == "contract" {
            if u.Change < 0 {
                burnedAndMoved += -u.Change
            } else {
                moved += u.Change
            }
        }
    }

    // create move and burn flows when necessary
    if moved > 0 && dst != nil {
        // deducted from source
        f := model.NewFlow(b.block, src, dst, id)
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeTransaction
        f.AmountOut = moved
        f.TokenAge = block.Age(src.LastIn)
        f.IsUnshielded = srccon != nil && srccon.Features.Contains(micheline.FeatureSapling)
        flows = append(flows, f)
        // credit to dest
        f = model.NewFlow(b.block, dst, src, id)
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeTransaction
        f.AmountIn = moved
        f.TokenAge = block.Age(src.LastIn)
        f.IsShielded = dstcon != nil && dstcon.Features.Contains(micheline.FeatureSapling)
        flows = append(flows, f)
    }

    if burnedAndMoved-moved > 0 {
        // Note: use outer source to burn
        f := model.NewFlow(b.block, origsrc, nil, id)
        f.Category = model.FlowCategoryBalance
        f.Operation = model.FlowTypeTransaction
        f.AmountOut = burnedAndMoved - moved
        f.IsBurned = true
        flows = append(flows, f)
        // adjust delegated balance if not a baker
        if origbkr != nil && !origsrc.IsBaker {
            f = model.NewFlow(b.block, origbkr.Account, origsrc, id)
            f.Category = model.FlowCategoryDelegation
            f.Operation = model.FlowTypeTransaction
            f.AmountOut = burnedAndMoved - moved
            flows = append(flows, f)
        }
    }

    // handle delegation updates

    // debut moved amount from source delegation if not baker
    if srcbkr != nil && !src.IsBaker && moved > 0 {
        f := model.NewFlow(b.block, srcbkr.Account, src, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeTransaction
        f.AmountOut = moved
        flows = append(flows, f)
    }

    // credit to dest delegate unless its a baker
    if dstbkr != nil && !dst.IsBaker && moved > 0 {
        f := model.NewFlow(b.block, dstbkr.Account, src, id)
        f.Category = model.FlowCategoryDelegation
        f.Operation = model.FlowTypeTransaction
        f.AmountIn = moved
        flows = append(flows, f)
    }

    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}
