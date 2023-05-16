// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewOriginationFlows(
	src, dst *model.Account,
	srcbkr, newbkr *model.Baker,
	fees, bal rpc.BalanceUpdates,
	id model.OpRef) []*model.Flow {

	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// origination may burn and may transfer funds
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
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = moved
		flows = append(flows, f)
		// credit to dest
		f = model.NewFlow(b.block, dst, src, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}
	if burnedAndMoved-moved > 0 {
		// debit from source as burn
		f := model.NewFlow(b.block, src, nil, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
	}

	// handle delegation updates

	// debit from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && feespaid+burnedAndMoved > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = feespaid + burnedAndMoved // fees and value moved
		flows = append(flows, f)
	}

	// credit to new baker when set
	if newbkr != nil && moved > 0 {
		f := model.NewFlow(b.block, newbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewInternalOriginationFlows(
	origsrc, src, dst *model.Account,
	origbkr, srcbkr, newbkr *model.Baker,
	bal rpc.BalanceUpdates,
	id model.OpRef) []*model.Flow {

	// fees are paid by outer transaction, here we only handle burned coins
	flows := make([]*model.Flow, 0)
	var burned, moved int64
	for _, u := range bal {
		if u.Kind == "contract" {
			addr := u.Address()
			switch addr {
			case origsrc.Address:
				// burned from original source balance
				f := model.NewFlow(b.block, origsrc, nil, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeOrigination
				f.AmountOut = -u.Change // note the negation!
				f.IsBurned = true
				flows = append(flows, f)
				burned += -u.Change
			case src.Address:
				// transfers from src contract to dst contract
				f := model.NewFlow(b.block, src, dst, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeOrigination
				f.AmountOut = -u.Change // note the negation!
				flows = append(flows, f)
				moved += -u.Change
			case dst.Address:
				// transfers from src contract to dst contract
				f := model.NewFlow(b.block, dst, src, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeOrigination
				f.AmountIn = u.Change
				flows = append(flows, f)
			}
		}
	}

	// handle delegation updates

	// debit burn from original source delegation iff not baker
	if origbkr != nil && !origsrc.IsBaker && burned > 0 {
		f := model.NewFlow(b.block, origbkr.Account, origsrc, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = burned // deduct burned amount
		flows = append(flows, f)
	}

	// debit moved funds from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && moved > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = moved // deduct moved amount
		flows = append(flows, f)
	}

	// credit moved funds to target baker when set
	if newbkr != nil && moved > 0 {
		f := model.NewFlow(b.block, newbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
