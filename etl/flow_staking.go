// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewStakeFlows(src *model.Account, bkr *model.Baker, fees, upd rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	// apply fees first
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// fees are deducted from baker delegation
	if feespaid > 0 && bkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, bkr.Account, src, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeStake
		f.AmountOut = feespaid
		flows = append(flows, f)
	}

	// stake flows
	for _, v := range upd {
		switch v.Kind {
		case "contract":
			// balance out
			f := model.NewFlow(b.block, src, bkr.Account, id)
			f.Kind = model.FlowKindBalance
			f.Type = model.FlowTypeStake
			f.AmountOut = -v.Amount()
			flows = append(flows, f)

			// delegation out (only if src != bkr)
			if src.RowId != bkr.AccountId {
				f = model.NewFlow(b.block, bkr.Account, src, id)
				f.Kind = model.FlowKindDelegation
				f.Type = model.FlowTypeStake
				f.AmountOut = -v.Amount()
				flows = append(flows, f)
			}

		case "freezer":
			// baker staking pool in
			f := model.NewFlow(b.block, src, bkr.Account, id) // flow to self
			f.Kind = model.FlowKindStake
			f.Type = model.FlowTypeStake
			f.IsFrozen = true
			f.AmountIn = v.Amount()
			flows = append(flows, f)
		}
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewUnstakeFlows(src *model.Account, bkr *model.Baker, fees, upd rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	// apply fees first
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// fees are deducted from baker delegation
	if feespaid > 0 && bkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, bkr.Account, src, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeUnstake
		f.AmountOut = feespaid
		flows = append(flows, f)
	}

	// unstake flows
	for _, v := range upd {
		switch v.Category {
		case "deposits":
			// baker staking pool out
			f := model.NewFlow(b.block, src, bkr.Account, id) // flow to self
			f.Kind = model.FlowKindStake
			f.Type = model.FlowTypeUnstake
			f.AmountOut = -v.Amount()
			flows = append(flows, f)

		case "unstaked_deposits":
			// wallet unstake pool in
			f := model.NewFlow(b.block, src, bkr.Account, id) // flow to self
			f.Kind = model.FlowKindStake
			f.Type = model.FlowTypeUnstake
			f.AmountIn = v.Amount()
			flows = append(flows, f)

			// unstake auto-delegates
			f = model.NewFlow(b.block, src, bkr.Account, id) // flow to baker
			f.Kind = model.FlowKindDelegation
			f.Type = model.FlowTypeUnstake
			f.AmountIn = v.Amount()
			flows = append(flows, f)
		}
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewFinalizeUnstakeFlows(src *model.Account, bkr, auxbkr *model.Baker, fees, upd rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	// apply fees first
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// fees are deducted from baker delegation (note: any account can send finalize
	// even undelegated)
	if feespaid > 0 && bkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, bkr.Account, src, id)
		f.Kind = model.FlowKindDelegation
		f.Type = model.FlowTypeFinalizeUnstake
		f.AmountOut = feespaid
		flows = append(flows, f)
	}

	// finalize flows
	// Note: there may be multiple balance updates from different cycles
	// wich we combine here. Unstaked funds are delegated to the original
	// staking baker. When this baker is still the same, we do not need to
	// emit a delegation flow because that has happened in unstake already.
	// If, however, the unstake happened because the user redelegated we must
	// switch delegation from the old to the new baker.
	for _, v := range upd {
		switch v.Kind {
		case "contract":
			// balance in
			f := model.NewFlow(b.block, src, src, id) // flow to self
			f.Kind = model.FlowKindBalance
			f.Type = model.FlowTypeFinalizeUnstake
			f.AmountIn = v.Amount()
			flows = append(flows, f)

			// when unstake happens as part of a re-delegation the unstaked amount
			// stays with the old baker until now, so we must redirect it
			if auxbkr != nil {
				// remove unstaked delegation from previous baker
				f := model.NewFlow(b.block, auxbkr.Account, src, id) // flow to baker
				f.Kind = model.FlowKindDelegation
				f.Type = model.FlowTypeFinalizeUnstake
				f.AmountOut = v.Amount()
				flows = append(flows, f)

				// and add it to the current baker (if set)
				if bkr != nil {
					f = model.NewFlow(b.block, bkr.Account, src, id)
					f.Kind = model.FlowKindDelegation
					f.Type = model.FlowTypeFinalizeUnstake
					f.AmountIn = v.Amount()
					flows = append(flows, f)
				}
			}

		case "freezer":
			// unstake pool out
			f := model.NewFlow(b.block, src, src, id) // flow to self
			f.Kind = model.FlowKindStake
			f.Type = model.FlowTypeFinalizeUnstake
			f.IsUnfrozen = true
			f.AmountOut = -v.Amount()
			flows = append(flows, f)
		}
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
