// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// generate synthetic ops from flows for
// OpTypeInvoice
// OpTypeBake
// OpTypeUnfreeze
// OpTypeSeedSlash
// OpTypeBonus - reward to Ithaca proposer when <> baker
// OpTypeDeposit - Ithaca deposit event
// OpTypeReward - Ithaca endorsing reward
func (b *Builder) AppendImplicitEvents(ctx context.Context) error {
	flows := b.NewImplicitFlows()
	if len(flows) == 0 {
		return nil
	}

	// validate flows: no sequence gaps, no invalid types, no zero or negative balances
	for _, f := range flows {
		if f.AmountIn < 0 {
			return fmt.Errorf("negative amount in block flow %#v", f)
		}
		if f.AmountOut < 0 {
			return fmt.Errorf("negative amount in block flow %#v", f)
		}
		if f.AmountIn+f.AmountOut == 0 {
			return fmt.Errorf("zero amount in block flow %#v", f)
		}
		if !f.Kind.IsValid() {
			return fmt.Errorf("invalid kind in block flow %#v", f)
		}
		if !f.Type.IsValid() {
			return fmt.Errorf("invalid type in block flow %#v", f)
		}
	}

	b.block.Flows = append(b.block.Flows, flows...)

	// prepare ops
	ops := make([]*model.Op, flows[len(flows)-1].OpN+1)

	// parse all flows and reverse-assign to ops
	for _, f := range flows {
		if f.OpN < 0 || f.OpN >= len(ops) {
			log.Errorf("Implicit ops: out of range %d/%d", f.OpN, len(ops))
			continue
		}
		id := model.OpRef{
			N: f.OpN,                  // pos in block
			L: model.OPL_BLOCK_EVENTS, // list id
			P: f.OpN,                  // pos in list
		}
		switch f.Type {
		case model.FlowTypeInvoice:
			// only append additional invoice op post-Florence
			if b.block.Params.Version >= 9 {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeInvoice
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				ops[f.OpN].Reward += f.AmountIn
			}
		case model.FlowTypeBaking:
			if ops[f.OpN] == nil {
				id.Kind = model.OpTypeBake
				ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
				ops[f.OpN].SenderId = f.AccountId
			}
			// assuming only one flow per category per baker
			switch f.Kind {
			case model.FlowKindDeposits:
				ops[f.OpN].Deposit += f.AmountIn
				// ops[f.OpN].Volume += f.AmountIn
			case model.FlowKindRewards:
				ops[f.OpN].Reward += f.AmountIn
				// ops[f.OpN].Volume += f.AmountIn
			case model.FlowKindBalance:
				// post-Ithaca only: fee is explicit (we hava a flow), so we can
				// add fee here; on pre-Ithaca protocols we sum op fees when updating
				// a block and then later add the block fee in the op indexer
				if f.IsFee {
					ops[f.OpN].Fee += f.AmountIn
					// ops[f.OpN].Volume += f.AmountIn
				} else {
					ops[f.OpN].Reward += f.AmountIn
					// ops[f.OpN].Volume += f.AmountIn
				}
			case model.FlowKindStake:
				// oxford: some staker pool reward from bake is auto-staked again
				ops[f.OpN].Reward += f.AmountIn
				ops[f.OpN].Deposit += f.AmountIn
			}
		case model.FlowTypeInternal:
			// only create ops for unfreeze-related internal events here
			if f.IsUnfrozen {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeUnfreeze
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				// sum multiple flows per category per baker
				switch f.Kind {
				case model.FlowKindDeposits:
					ops[f.OpN].Deposit += f.AmountOut
					// ops[f.OpN].Volume += f.AmountOut
				case model.FlowKindRewards:
					ops[f.OpN].Reward += f.AmountOut
					// ops[f.OpN].Volume += f.AmountOut
				case model.FlowKindFees:
					ops[f.OpN].Fee += f.AmountOut
					// ops[f.OpN].Volume += f.AmountOut
				}
			}
		case model.FlowTypeNonceRevelation:
			// only seed slash events
			if f.IsBurned {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeSeedSlash
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
				}
				// sum multiple consecutive seed slashes into one op
				switch f.Kind {
				case model.FlowKindRewards:
					ops[f.OpN].Reward += f.AmountOut
					ops[f.OpN].Burned += f.AmountOut
				case model.FlowKindFees:
					ops[f.OpN].Fee += f.AmountOut
					ops[f.OpN].Burned += f.AmountOut
				case model.FlowKindBalance:
					ops[f.OpN].Reward += f.AmountIn
					ops[f.OpN].Burned += f.AmountOut
				}
			}
		case model.FlowTypeBonus:
			// Ithaca+
			if ops[f.OpN] == nil {
				id.Kind = model.OpTypeBonus
				ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
				ops[f.OpN].SenderId = f.AccountId
			}
			if f.Kind == model.FlowKindStake {
				// oxford: some reward from bake bonus is auto-staked again
				ops[f.OpN].Reward += f.AmountIn
				ops[f.OpN].Deposit += f.AmountIn
			} else {
				// non-frozen bonus to existing block proposer
				ops[f.OpN].Reward += f.AmountIn
				// ops[f.OpN].Volume += f.AmountIn
			}
		case model.FlowTypeReward:
			// Ithaca+
			if f.IsBurned {
				// participation burn
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeReward
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				ops[f.OpN].Reward += f.AmountIn
				ops[f.OpN].Burned += f.AmountIn
			} else {
				// endorsement reward
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeReward
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				if f.Kind == model.FlowKindStake {
					// oxford: some reward from endorsing is auto-staked again
					ops[f.OpN].Reward += f.AmountIn
					ops[f.OpN].Deposit += f.AmountIn
				} else {
					// pre/post-oxford non-frozen reward goes to spendable balances
					ops[f.OpN].Reward += f.AmountIn
					// ops[f.OpN].Volume += f.AmountIn
				}
			}
		case model.FlowTypeDeposit:
			// Ithaca+ until Oxford, then replaced by stake
			// explicit deposit payment (positive)
			// refund is translated into an unfreeze event
			if f.Kind == model.FlowKindDeposits {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeDeposit
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				ops[f.OpN].Deposit += f.AmountIn
				// ops[f.OpN].Volume += f.AmountIn
			}
		case model.FlowTypeStake:
			// Oxford+ deposit is staked
			// only handle stake kind (skip balance flows)
			if f.Kind == model.FlowKindStake {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeStake
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				ops[f.OpN].Deposit += f.AmountIn
				// ops[f.OpN].Volume += f.AmountIn
			}
		case model.FlowTypeUnstake:
			// Oxford+ frozen deposit is unstaked
			// only handle stake kind (no other flow should exist, but make sure)
			if f.Kind == model.FlowKindStake {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeUnstake
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				ops[f.OpN].Deposit += f.AmountOut
			}
		case model.FlowTypeFinalizeUnstake:
			// Oxford+ frozen unstaked deposit is returned to spendable balance
			// only handle stake kind (no other flow should exist, but make sure)
			if f.Kind == model.FlowKindStake {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeFinalizeUnstake
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				ops[f.OpN].Volume += f.AmountOut
			}
		case model.FlowTypePenalty:
			// Oxford+ staking slash happens EOC
			// TODO: differentiate staked & unstaked amount?
			switch f.Kind {
			case model.FlowKindStake:
				// penalty burn
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeStakeSlash
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.CounterPartyId // accuser
					ops[f.OpN].ReceiverId = f.AccountId    // offender
				}
				if f.IsBurned {
					ops[f.OpN].Burned += f.AmountOut
				}
				ops[f.OpN].Deposit += f.AmountOut
			case model.FlowKindBalance:
				// reward
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeStakeSlash
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId        // accuser
					ops[f.OpN].ReceiverId = f.CounterPartyId // offender
				}
				ops[f.OpN].Reward += f.AmountIn
				// ops[f.OpN].Volume += f.AmountIn
			}
		}
	}

	// make sure we don't accidentally add a nil op
	for _, v := range ops {
		if v == nil {
			continue
		}
		b.block.Ops = append(b.block.Ops, v)
	}

	return nil
}

// generate synthetic ops from block implicit ops (Granada+)
// Originations (on migration)
// Transactions / Subsidy
func (b *Builder) AppendImplicitBlockOps(ctx context.Context) error {
	for _, op := range b.block.TZ.Block.Metadata.ImplicitOperationsResults {
		Errorf := func(format string, args ...interface{}) error {
			return fmt.Errorf(
				"implicit block %s op [%d]: "+format,
				append([]interface{}{op.Kind, b.block.NextN()}, args...)...,
			)
		}
		n := b.block.NextN()
		id := model.OpRef{
			N: n,                      // pos in block
			L: model.OPL_BLOCK_HEADER, // list id
			P: n,                      // pos in list
		}
		switch op.Kind {
		case tezos.OpTypeOrigination:
			// for now we expect a single address only
			dst, ok := b.AccountByAddress(op.OriginatedContracts[0])
			if !ok {
				return Errorf("missing originated contract %s", op.OriginatedContracts[0])
			}
			// load script from RPC
			if op.Script == nil {
				var err error
				op.Script, err = b.rpc.GetContractScript(ctx, dst.Address, rpc.BlockLevel(b.block.Height))
				if err != nil {
					return Errorf("loading contract script %s: %v", dst.Address, err)
				}
			}
			id.Kind = model.MapOpType(op.Kind)
			o := model.NewEventOp(b.block, dst.RowId, id)
			o.IsContract = true
			dst.IsContract = true
			o.GasUsed = op.Gas()
			o.StoragePaid = op.PaidStorageSizeDiff
			o.CodeHash = op.Script.CodeHash()
			if op.Storage.IsValid() {
				o.Storage, _ = op.Storage.MarshalBinary()
				o.StorageHash = op.Storage.Hash64()
				o.IsStorageUpdate = true
			}

			// patch missing bigmap allocs
			typs := op.Script.BigmapTypes()
			ids := op.Script.Bigmaps()
			if len(ids) > 0 {
				bmd := make(micheline.BigmapEvents, 0)
				for n, id := range ids {
					typ := typs[n]
					diff := micheline.BigmapEvent{
						Action:    micheline.DiffActionAlloc,
						Id:        id,
						KeyType:   typ.Prim.Args[0],
						ValueType: typ.Prim.Args[1],
					}
					bmd = append(bmd, diff)
				}
				// o.Diff, _ = bmd.MarshalBinary()
				o.BigmapEvents = bmd
			}

			// add volume if balance update exists
			for _, v := range op.BalanceUpdates {
				// skip mint/burn flows
				if v.Kind == "minted" || v.Kind == "burned" {
					continue
				}
				o.Volume += v.Amount()
				f := b.NewSubsidyFlow(dst, v.Amount(), id)
				b.block.Flows = append(b.block.Flows, f)
			}

			b.block.Ops = append(b.block.Ops, o)

			// register new implicit contract
			o.Contract = model.NewImplicitContract(dst, *op, o, b.block.Params)
			b.conMap[dst.RowId] = o.Contract

		case tezos.OpTypeTransaction:
			for _, v := range op.BalanceUpdates {
				addr := v.Address()
				if !addr.IsValid() {
					continue
				}
				dst, ok := b.AccountByAddress(addr)
				if !ok {
					return Errorf("missing account %s", v.Address())
				}
				dCon, err := b.LoadContractByAccountId(ctx, dst.RowId)
				if err != nil {
					return Errorf("loading contract %d %s: %v", dst.RowId, v.Address(), err)
				}
				id.Kind = model.OpTypeSubsidy
				o := model.NewEventOp(b.block, dst.RowId, id)
				o.IsContract = true
				o.Volume = v.Amount()
				o.Reward = v.Amount()
				o.GasUsed = op.Gas()
				o.StoragePaid = op.PaidStorageSizeDiff
				o.Entrypoint = 1
				o.Storage, _ = op.Storage.MarshalBinary()
				o.StorageHash = op.Storage.Hash64()
				o.CodeHash = dCon.CodeHash
				o.IsStorageUpdate = dCon.Update(o, b.block.Params)
				o.Contract = dCon
				b.block.Ops = append(b.block.Ops, o)
				if o.Volume > 0 {
					b.block.Flows = append(b.block.Flows, b.NewSubsidyFlow(dst, o.Volume, id))
				}
			}
		}
	}
	return nil
}
