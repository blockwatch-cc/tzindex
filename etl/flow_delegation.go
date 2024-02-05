// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// used for internal an non-internal delegations
func (b *Builder) NewDelegationFlows(src *model.Account, newbkr, oldbkr *model.Baker, fees, upd rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	// apply fees first
	flows, feespaid := b.NewFeeFlows(src, fees, id)
	var pending int64
	balance := src.Balance()

	// if delegation is renewed/duplicate, handle fee out-flow only
	if newbkr != nil && oldbkr != nil && newbkr.AccountId == oldbkr.AccountId {
		// create flow only if fees are paid
		if feespaid > 0 && !src.IsBaker {
			f := model.NewFlow(b.block, oldbkr.Account, src, id)
			f.Kind = model.FlowKindDelegation
			f.Type = model.FlowTypeDelegation
			f.AmountOut = feespaid // deduct this operation's fees only
			flows = append(flows, f)
		}
	} else {
		// handle any change in baker

		// source account may have run multiple ops in the same block, so the
		// (re-)delegated balance must be adjusted by all pending updates
		// because they have already created delegation out-flows
		for _, f := range b.block.Flows {
			if f.AccountId == src.RowId && f.Kind == model.FlowKindBalance {
				pending += f.AmountOut - f.AmountIn
			}
		}

		// if src is already delegated, create an (un)delegation flow from old baker
		// also cover the case where src registers as baker
		if oldbkr != nil {
			f := model.NewFlow(b.block, oldbkr.Account, src, id)
			f.Kind = model.FlowKindDelegation
			f.Type = model.FlowTypeDelegation
			f.AmountOut = balance - pending // deduct difference including fees
			flows = append(flows, f)
		}

		// create delegation to new baker using balance minus delegation fees from above
		// and minus pending balance updates, unless its a self-delegation
		// (i.e. baker registration)
		if newbkr != nil && !src.IsBaker && balance-pending-feespaid > 0 {
			f := model.NewFlow(b.block, newbkr.Account, src, id)
			f.Kind = model.FlowKindDelegation
			f.Type = model.FlowTypeDelegation
			f.AmountIn = balance - feespaid - pending // add difference without fees
			flows = append(flows, f)
		}

		// if src is staked, create unstake flows
		// unstake flows, assuming unstaked amount does not count into delegation
		for _, v := range upd {
			if v.Category == "deposits" {
				// baker staking pool out
				f := model.NewFlow(b.block, src, oldbkr.Account, id)
				f.Kind = model.FlowKindStake
				f.Type = model.FlowTypeUnstake
				f.AmountOut = -v.Amount()
				flows = append(flows, f)

				// unstaked amount remains in delegation with the old baker
				f = model.NewFlow(b.block, src, oldbkr.Account, id)
				f.Kind = model.FlowKindDelegation
				f.Type = model.FlowTypeUnstake
				f.AmountIn = -v.Amount()
				flows = append(flows, f)

				// TODO: on finalize, this delegation moves to the new baker
				// note: there can only be one stake/unstake-baker, user must wait
				// for finalize until he can stake again

				// unused
				// case "unstaked_deposits":
				// 	// wallet unstake pool in
				// 	f := model.NewFlow(b.block, src, oldbkr.Account, id) // flow to self
				// 	f.Kind = model.FlowKindStake
				// 	f.Type = model.FlowTypeUnstake
				// 	f.AmountIn = v.Amount()
				// 	flows = append(flows, f)
			}
		}
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
