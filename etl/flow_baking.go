// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

// Note: during chain bootstrap there used to be blocks without rewards
//
//  and no balance updates were issued to endorsers
//
// Note: on Ithaca balance updates are empty since deposit/reward is paid
//
//  before cycle start (technically at cycle end)
func (b *Builder) NewEndorserFlows(acc *model.Account, bal rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
    flows := make([]*model.Flow, 0)
    for _, u := range bal {
        switch u.Kind {
        case "contract":
            // deposits paid from balance
            f := model.NewFlow(b.block, acc, nil, id)
            f.Category = model.FlowCategoryBalance
            f.Operation = model.FlowTypeEndorsement
            f.AmountOut = -u.Change // note the negation!
            flows = append(flows, f)
        case "freezer":
            switch u.Category {
            case "deposits":
                f := model.NewFlow(b.block, acc, nil, id)
                f.Category = model.FlowCategoryDeposits
                f.Operation = model.FlowTypeEndorsement
                f.AmountIn = u.Change
                f.IsFrozen = true
                flows = append(flows, f)
            case "rewards":
                f := model.NewFlow(b.block, acc, nil, id)
                f.Category = model.FlowCategoryRewards
                f.Operation = model.FlowTypeEndorsement
                f.AmountIn = u.Change
                f.IsFrozen = true
                flows = append(flows, f)
            }
        }
    }
    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

// injected by the baker only
func (b *Builder) NewSeedNonceFlows(bal rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
    flows := make([]*model.Flow, 0)
    for _, u := range bal {
        switch u.Kind {
        case "freezer":
            // before Ithaca
            f := model.NewFlow(b.block, b.block.Baker.Account, nil, id)
            f.Category = model.FlowCategoryRewards
            f.Operation = model.FlowTypeNonceRevelation
            f.AmountIn = u.Change
            f.IsFrozen = true
            flows = append(flows, f)
        case "contract":
            // after Ithaca (not frozen, goes to block proposer)
            f := model.NewFlow(b.block, b.block.Proposer.Account, nil, id)
            f.Category = model.FlowCategoryBalance
            f.Operation = model.FlowTypeNonceRevelation
            f.AmountIn = u.Change
            flows = append(flows, f)
        }
    }
    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

// works for double-bake, double-endorse, double-preendorse
func (b *Builder) NewDenunciationFlows(accuser, offender *model.Baker, bal rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
    flows := make([]*model.Flow, 0)
    for _, u := range bal {
        // penalties
        // pre-Ithaca: kind=freezer & amount < 0 (up to 3 categories)
        // post-Ithaca: kind=freezer & amount < 0
        // rewards
        // pre-Ithaca: kind=freezer & amount > 0
        // post-Ithaca: kind=contract
        switch u.Kind {
        case "freezer":
            switch u.Category {
            case "rewards":
                if u.Change > 0 {
                    // pre-Ithaca accuser reward
                    f := model.NewFlow(b.block, accuser.Account, offender.Account, id)
                    f.Operation = model.FlowTypePenalty
                    f.Category = model.FlowCategoryRewards
                    f.AmountIn = u.Change
                    f.IsFrozen = true
                    flows = append(flows, f)
                } else {
                    // offender penalty
                    f := model.NewFlow(b.block, offender.Account, accuser.Account, id)
                    f.Operation = model.FlowTypePenalty
                    f.Category = model.FlowCategoryRewards
                    f.AmountOut = -u.Change
                    f.IsUnfrozen = true
                    f.IsBurned = true
                    flows = append(flows, f)
                }
            case "deposits":
                // pre&post-Ithaca offender penalty
                f := model.NewFlow(b.block, offender.Account, accuser.Account, id)
                f.Operation = model.FlowTypePenalty
                f.Category = model.FlowCategoryDeposits
                f.AmountOut = -u.Change
                f.IsUnfrozen = true
                f.IsBurned = true
                flows = append(flows, f)
            case "fees":
                // pre-Ithaca offender penalty
                f := model.NewFlow(b.block, offender.Account, accuser.Account, id)
                f.Operation = model.FlowTypePenalty
                f.Category = model.FlowCategoryFees
                f.AmountOut = -u.Change
                f.IsUnfrozen = true
                f.IsBurned = true
                flows = append(flows, f)
            }

        case "contract":
            // post-Ithaca reward
            f := model.NewFlow(b.block, accuser.Account, offender.Account, id)
            f.Operation = model.FlowTypePenalty
            f.Category = model.FlowCategoryBalance // not frozen (!)
            f.AmountIn = u.Change
            flows = append(flows, f)
        }
    }
    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

// sent by baker, so no delegation update required
// post-Ithaca only op, so no pre-Ithaca fee handling
func (b *Builder) NewSetDepositsLimitFlows(src *model.Account, fees rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
    flows, _ := b.NewFeeFlows(src, fees, id)
    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

// only fees are paid
func (b *Builder) NewUpdateConsensusKeyFlows(src *model.Account, fees rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
    flows, _ := b.NewFeeFlows(src, fees, id)
    b.block.Flows = append(b.block.Flows, flows...)
    return flows
}

// - sends amount from drained baker to destination
// - sends tip from drained baker to block producer
// - pays no fee
func (b *Builder) NewDrainDelegateFlows(src, dst *model.Account, bal rpc.BalanceUpdates, id model.OpRef) (int64, int64, []*model.Flow) {
    var (
        vol, reward int64
        flows       []*model.Flow
    )

    // drain to dest
    f := model.NewFlow(b.block, src, dst, id)
    f.Category = model.FlowCategoryBalance
    f.Operation = model.FlowTypeDrain
    f.AmountOut = -bal[0].Amount()
    flows = append(flows, f)

    f = model.NewFlow(b.block, src, dst, id)
    f.Category = model.FlowCategoryBalance
    f.Operation = model.FlowTypeDrain
    f.AmountIn = bal[1].Amount()
    flows = append(flows, f)
    vol = f.AmountIn

    // tip to block proposer
    f = model.NewFlow(b.block, src, b.block.Proposer.Account, id)
    f.Category = model.FlowCategoryBalance
    f.Operation = model.FlowTypeDrain
    f.AmountOut = -bal[2].Amount()
    flows = append(flows, f)

    f = model.NewFlow(b.block, src, b.block.Proposer.Account, id)
    f.Category = model.FlowCategoryBalance
    f.Operation = model.FlowTypeDrain
    f.AmountIn = bal[3].Amount()
    flows = append(flows, f)
    reward = f.AmountIn

    b.block.Flows = append(b.block.Flows, flows...)
    return vol, reward, flows
}
