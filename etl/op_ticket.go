// Copyright (c) 2020-2024 Blockwatch Data Inc.
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

func (b *Builder) AppendTransferTicketOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	tx, ok := o.(*rpc.TransferTicket)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	src, ok := b.AccountByAddress(tx.Source)
	if !ok {
		return Errorf("missing sender account %s", tx.Source)
	}

	res := tx.Result()

	dst, ok := b.AccountByAddress(tx.Destination)
	if !ok && res.Status.IsSuccess() {
		return Errorf("missing target contract %s", tx.Destination)
	}

	var (
		sbkr *model.Baker
		dCon *model.Contract
		err  error
	)
	if src.BakerId != 0 {
		if sbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %s", src.BakerId, src)
		}
	}
	if dst.IsContract {
		dCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %v", tx.Destination, dst.RowId, err)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.ReceiverId = dst.RowId
	op.Counter = tx.Counter
	op.Fee = tx.Fee
	op.GasLimit = tx.GasLimit
	op.StorageLimit = tx.StorageLimit
	op.IsContract = tx.Destination.IsContract()
	op.IsRollup = tx.Destination.IsRollup()

	op.Status = res.Status
	op.IsSuccess = res.IsSuccess()
	op.GasUsed = res.Gas()
	op.StoragePaid = res.PaidStorageSizeDiff
	b.block.Ops = append(b.block.Ops, op)

	if dCon != nil && op.IsContract {
		// receiver is KT1 contract (used for transfer_ticket)
		script, err := dCon.LoadScript()
		if err != nil || script == nil {
			log.Error(Errorf("loading script: %v", err))
		} else {
			eps, _ := script.Entrypoints(false)
			ep, ok := eps[tx.Entrypoint]
			if !ok && op.IsSuccess {
				log.Error(Errorf("missing entrypoint %q", tx.Entrypoint))
			} else {
				op.Entrypoint = ep.Id
				op.Data = tx.Entrypoint
			}
		}
		op.CodeHash = dCon.CodeHash
	}
	// ticket deposit
	if dCon != nil && op.IsRollup {
		op.Entrypoint = 0
		op.Data = tx.Entrypoint
	}

	// add ticket args as parameters
	op.Parameters, err = tx.EncodeParameters().MarshalBinary()
	if err != nil {
		log.Error(Errorf("marshal parameters errors: %s", err))
	}

	// keep ticket updates
	op.RawTicketUpdates = res.TicketUpdates()

	if op.IsSuccess {
		flows := b.NewTransferTicketFlows(
			src,            // involved accounts
			sbkr,           // related bakers (optional)
			tx.Fees(),      // fees
			res.Balances(), // storage burn
			b.block,
			id,
		)
		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

	} else {
		// fees only
		_ = b.NewTransferTicketFlows(
			src,       // just source
			sbkr,      // just source baker
			tx.Fees(), // fees
			nil,       // no burn
			b.block,
			id,
		)

		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
	}

	// update sender account
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		dst.LastSeen = b.block.Height
		dst.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++
			dst.NTxIn++
			dst.TotalFeesUsed += op.Fee
			if dCon != nil {
				_ = dCon.Update(op, b.block.Params)
			}
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		dst.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--
			dst.NTxIn--
			dst.TotalFeesUsed -= op.Fee
			if dCon != nil {
				dCon.Rollback(op, nil, b.block.Params)
			}
		} else {
			src.NTxFailed--
		}
	}

	// apply internal operation result (may generate new op and flows)
	for i, v := range tx.Metadata.InternalResults {
		// skip events, they are processed in event index
		if v.Kind == tezos.OpTypeEvent {
			continue
		}
		id.I = i
		id.Kind = model.MapOpType(v.Kind)
		id.N++
		switch id.Kind {
		case model.OpTypeTransaction:
			if err := b.AppendInternalTransactionOp(ctx, src, sbkr, oh, *v, id, rollback); err != nil {
				return err
			}
		case model.OpTypeDelegation:
			if err := b.AppendInternalDelegationOp(ctx, src, sbkr, oh, *v, id, rollback); err != nil {
				return err
			}
		case model.OpTypeOrigination:
			if err := b.AppendInternalOriginationOp(ctx, src, sbkr, oh, *v, id, rollback); err != nil {
				return err
			}
		default:
			return Errorf("unsupported internal operation type %s", v.Kind)
		}
	}

	return nil
}
