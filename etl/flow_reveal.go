// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewRevealFlows(src *model.Account, bkr *model.Baker, fees rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// if src is delegated (and not baker, subtract paid fees from delegated balance
	if feespaid > 0 && bkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, bkr.Account, src, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeReveal
		f.AmountOut = feespaid
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
