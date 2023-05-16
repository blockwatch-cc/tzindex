// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewTransferTicketFlows(
	src *model.Account,
	sbkr *model.Baker,
	fees, bal rpc.BalanceUpdates,
	block *model.Block,
	id model.OpRef) []*model.Flow {

	// apply fees
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// transaction may burn
	var srcBurn int64
	for i, u := range bal {
		if u.Kind == "contract" && u.Change < 0 {
			if len(bal) > i+1 && bal[i+1].Category == "storage fees" {
				// storage burn
				f := model.NewFlow(b.block, src, nil, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeTransferTicket
				f.AmountOut = -u.Change
				f.IsBurned = true
				srcBurn += -u.Change
				flows = append(flows, f)
			}
		}
	}

	// debit from source delegation unless source is a baker
	// if feespaid+srcBurn > 0 && sbkr != nil && !src.IsBaker {
	if feespaid > 0 && sbkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, sbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeTransferTicket
		f.AmountOut = feespaid + srcBurn // fees and amount burned
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
