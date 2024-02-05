// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) AppendStakeOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s stake op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	tx, ok := o.(*rpc.Transaction)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(tx.Source)
	if !ok {
		return Errorf("missing source account %s", tx.Source)
	}

	// src is either baker or staker
	var bkr *model.Baker
	if src.BakerId != 0 {
		if bkr, ok = b.BakerById(src.BakerId); !ok && tx.Result().IsSuccess() {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
		src.Baker = bkr
	}

	// build op
	op := model.NewOp(b.block, id)
	op.Type = model.OpTypeStake
	op.SenderId = src.RowId
	op.ReceiverId = src.RowId
	op.Counter = tx.Counter
	op.Fee = tx.Fee
	op.GasLimit = tx.GasLimit
	op.StorageLimit = tx.StorageLimit

	res := tx.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	op.Volume = tx.Amount
	b.block.Ops = append(b.block.Ops, op)

	// on stake we ignore params (= Unit), the value is in amount
	if op.IsSuccess {
		b.NewStakeFlows(
			src,            // sender
			bkr,            // baker
			tx.Fees(),      // fees
			res.Balances(), // move
			id,
		)
		log.Debugf("%d Stake OK %d baker=%s staker=%s - %s", op.Height, op.Volume, bkr.Account, src, id.Url(op.Height))
	} else {
		// fees only
		b.NewStakeFlows(src, bkr, tx.Fees(), nil, id)

		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
		log.Debugf("%d Stake failed staker=%s - %s", op.Height, src, id.Url(op.Height))
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
		} else {
			src.NTxFailed--
		}
	}
	return nil
}

func (b *Builder) AppendUnstakeOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s unstake op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	tx, ok := o.(*rpc.Transaction)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(tx.Source)
	if !ok {
		return Errorf("missing source account %s", tx.Source)
	}

	// src is either baker or staker
	var bkr *model.Baker
	if src.BakerId != 0 {
		if bkr, ok = b.BakerById(src.BakerId); !ok && tx.Result().IsSuccess() {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
		src.Baker = bkr
	}

	// build op
	op := model.NewOp(b.block, id)
	op.Type = model.OpTypeUnstake
	op.SenderId = src.RowId
	op.ReceiverId = src.RowId
	op.BakerId = src.BakerId
	op.Counter = tx.Counter
	op.Fee = tx.Fee
	op.Volume = tx.Amount
	op.GasLimit = tx.GasLimit
	op.StorageLimit = tx.StorageLimit

	res := tx.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	b.block.Ops = append(b.block.Ops, op)

	// TODO: split unstake amount into reward and principal, keep volume as both
	// .Reward
	// .Deposit

	if op.IsSuccess {
		flows := b.NewUnstakeFlows(
			src,            // sender
			bkr,            // baker
			tx.Fees(),      // fees
			res.Balances(), // move
			id,
		)

		// fill volume from flow because params cannot be trusted
		for _, f := range flows {
			if f.Kind != model.FlowKindStake || f.AccountId != src.RowId {
				continue
			}
			op.Volume += f.AmountOut
		}
		log.Debugf("%d Unstake OK %d baker=%s staker=%s - %s", op.Height, op.Volume, bkr.Account, src, id.Url(op.Height))
	} else {
		// fees only
		b.NewUnstakeFlows(src, bkr, tx.Fees(), nil, id)

		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
		log.Debugf("%d Unstake failed staker=%s - %s", op.Height, src, id.Url(op.Height))
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
		} else {
			src.NTxFailed--
		}
	}

	return nil
}

// Implicit unstake can happen as part of (internal) re-delegations.
// We can only detect this by looking at flows, so we're re-using
// the already populated delegation op and decoded flows here.
func (b *Builder) AppendImplicitUnstakeOp(ctx context.Context, src *model.Account, bkr *model.Baker, flows []*model.Flow, id model.OpRef, rollback bool) error {
	// scan flows for unstake
	if len(flows) == 0 || flows[len(flows)-1].Kind != model.FlowKindStake {
		return nil
	}

	// build event
	op := model.NewEventOp(b.block, src.RowId, id)
	op.Type = model.OpTypeUnstake
	op.SenderId = src.RowId
	op.ReceiverId = src.RowId
	op.BakerId = src.BakerId
	op.Volume = flows[len(flows)-1].AmountOut
	b.block.Ops = append(b.block.Ops, op)

	// implicit unstaked funds are auto-delegated to the old baker
	f := model.NewFlow(b.block, src, bkr.Account, id) // flow to baker
	f.Kind = model.FlowKindDelegation
	f.Type = model.FlowTypeUnstake
	f.AmountIn = op.Volume
	b.block.Flows = append(b.block.Flows, f)

	log.Debugf("%d Unstake EVENT %d baker=%s staker=%s - %s", op.Height, op.Volume, bkr.Account, src, id.Url(op.Height))

	return nil
}

func (b *Builder) AppendFinalizeUnstakeOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s finalize unstake op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	tx, ok := o.(*rpc.Transaction)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(tx.Source)
	if !ok {
		return Errorf("missing source account %s", tx.Source)
	}

	// src is either baker or staker
	var bkr *model.Baker
	if src.BakerId != 0 {
		if bkr, ok = b.BakerById(src.BakerId); !ok && tx.Result().IsSuccess() {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
		src.Baker = bkr
	}

	// build op
	op := model.NewOp(b.block, id)
	op.Type = model.OpTypeFinalizeUnstake
	op.SenderId = src.RowId
	op.ReceiverId = src.RowId
	op.BakerId = src.BakerId
	op.Counter = tx.Counter
	op.Fee = tx.Fee
	op.GasLimit = tx.GasLimit
	op.StorageLimit = tx.StorageLimit

	res := tx.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	b.block.Ops = append(b.block.Ops, op)

	// on finalize unstake we ignore params (= Unit), the value is implicit in balance upd
	if op.IsSuccess {
		// Search for the most recent unstake event in case this unstake was triggered
		// by a re-delegation. In this case we must undelegate from the old baker and
		// delegate to the new baker.
		var auxbkr *model.Baker
		if !src.IsBaker {
			if ev, err := b.idx.FindLatestUnstake(ctx, src.RowId, op.Height); err == nil && ev.BakerId > 0 {
				var ok bool
				if auxbkr, ok = b.BakerById(ev.BakerId); !ok {
					return Errorf("missing previous baker %d for unstake request %d on account %s %d", ev.BakerId, ev.RowId, src, src.RowId)
				}
			}
		}

		flows := b.NewFinalizeUnstakeFlows(
			src,         // sender
			bkr, auxbkr, // baker
			tx.Fees(),      // fees
			res.Balances(), // move
			id,
		)

		// fill volume from flow
		for _, f := range flows {
			if f.Kind != model.FlowKindBalance {
				continue
			}
			op.Volume += f.AmountIn
		}

		if bkr != nil {
			log.Debugf("%d Finalize %d burn=%d baker=%s staker=%s - %s",
				op.Height, op.Volume, op.Burned, bkr.Account, src, id.Url(op.Height))
		} else {
			log.Debugf("%d Finalize without delegation %d burn=%d staker=%s - %s",
				op.Height, op.Volume, op.Burned, src, id.Url(op.Height))
		}

	} else {
		// fees only
		b.NewFinalizeUnstakeFlows(src, bkr, nil, tx.Fees(), nil, id)

		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
		log.Debugf("%d finalize failed staker=%s - %s", op.Height, src, id.Url(op.Height))
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
		} else {
			src.NTxFailed--
		}
	}

	return nil
}

func (b *Builder) AppendSetDelegateParametersOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s set_delegate_parameterd op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	tx, ok := o.(*rpc.Transaction)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(tx.Source)
	if !ok {
		return Errorf("missing source account %s", tx.Source)
	}

	// src must be baker on success
	var bkr *model.Baker
	if src.BakerId != 0 && tx.Result().IsSuccess() {
		if bkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.Type = model.OpTypeSetDelegateParameters
	op.SenderId = src.RowId
	op.ReceiverId = src.RowId
	op.Counter = tx.Counter
	op.Fee = tx.Fee
	op.GasLimit = tx.GasLimit
	op.StorageLimit = tx.StorageLimit

	res := tx.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	op.Parameters, _ = tx.Parameters.MarshalBinary()
	b.block.Ops = append(b.block.Ops, op)

	// fees only
	flows, _ := b.NewFeeFlows(src, tx.Fees(), id)
	b.block.Flows = append(b.block.Flows, flows...)

	if !op.IsSuccess {
		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
		log.Debugf("%d set params baker=%s - %s", op.Height, src, id.Url(op.Height))
	} else {
		log.Debugf("%d Set Params baker=%s - %s", op.Height, src, id.Url(op.Height))
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
			// params activate in 5 cycles, but we store them here anyways
			bkr.StakingEdge = tx.Parameters.Value.Args[0].Int.Int64()
			// may use comb pair or nested pair
			if tx.Parameters.Value.Args[1].IsPair() {
				bkr.StakingLimit = tx.Parameters.Value.Args[1].Args[0].Int.Int64()
			} else {
				bkr.StakingLimit = tx.Parameters.Value.Args[1].Int.Int64()
			}
			bkr.IsDirty = true
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			// use defaults
			bkr.StakingEdge = 1000000000 // = 100%
			bkr.StakingLimit = 0         // = 0
			bkr.IsDirty = true
		} else {
			src.NTxFailed--
		}
	}

	return nil
}
