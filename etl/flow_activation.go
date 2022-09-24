// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewActivationFlow(acc *model.Account, aop *rpc.Activation, id model.OpRef) []*model.Flow {
    bal := aop.Fees()
    if len(bal) < 1 {
        log.Warnf("Empty balance update for activation op at height %d", b.block.Height)
    }
    f := model.NewFlow(b.block, acc, nil, id)
    f.Category = model.FlowCategoryBalance
    f.Operation = model.FlowTypeActivation
    for _, u := range bal {
        if u.Kind == "contract" {
            f.AmountIn = u.Amount()
        }
    }
    f.TokenGenMin = 1
    f.TokenGenMax = 1
    b.block.Flows = append(b.block.Flows, f)
    return []*model.Flow{f}
}
