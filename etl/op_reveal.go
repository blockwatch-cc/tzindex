// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// manager operation, extends grace period
func (b *Builder) AppendRevealOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	rop, ok := o.(*rpc.Reveal)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(rop.Source)
	if !ok {
		return Errorf("missing account %s", rop.Source)
	}
	var sbkr *model.Baker
	if src.BakerId != 0 {
		if sbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker account %d for source account %d %s", src.BakerId, src.RowId, src)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.Counter = rop.Counter
	op.Fee = rop.Fee
	op.GasLimit = rop.GasLimit
	op.StorageLimit = rop.StorageLimit
	op.Data = rop.PublicKey.String()

	res := rop.Result()
	op.GasUsed = res.Gas()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	b.block.Ops = append(b.block.Ops, op)

	// pays fees on success and fail
	_ = b.NewRevealFlows(src, sbkr, rop.Fees(), id)

	// extend grace period for bakers who send reveal ops
	if src.IsBaker {
		if bkr, ok := b.BakerById(src.BakerId); ok {
			bkr.UpdateGracePeriod(b.block.Cycle, b.block.Params)
		}
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++
			src.IsRevealed = true
			src.Pubkey = rop.PublicKey
		} else {
			src.NTxFailed++
			// keep errors
			op.Errors, _ = json.Marshal(res.Errors)
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--
			src.IsRevealed = false
			src.Pubkey = tezos.InvalidKey
		} else {
			src.NTxFailed--
		}
	}
	return nil
}
