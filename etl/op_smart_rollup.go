// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) AppendSmartRollupOriginationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	tx, ok := o.(*rpc.SmartRollupOriginate)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	src, ok := b.AccountByAddress(tx.Source)
	if !ok {
		return Errorf("missing source account %s", tx.Source)
	}
	var (
		dst    *model.Account
		srcbkr *model.Baker
	)
	if src.BakerId != 0 {
		if srcbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}
	res := o.Result()
	if res.Address.IsValid() && res.IsSuccess() {
		dst, ok = b.AccountByAddress(*res.Address)
		if !ok {
			return Errorf("missing smart rollup %s", res.Address)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.Counter = tx.Counter
	op.Fee = tx.Fee
	op.GasLimit = tx.GasLimit
	op.StorageLimit = tx.StorageLimit
	op.IsRollup = true
	op.Status = res.Status
	op.IsSuccess = res.IsSuccess()
	op.GasUsed = res.Gas()
	op.StoragePaid = res.PaidStorageSizeDiff
	op.Parameters = micheline.NewBytes(tx.Encode()).ToBytes()
	b.block.Ops = append(b.block.Ops, op)

	if op.IsSuccess {
		flows := b.NewRollupOriginationFlows(
			src, dst,
			srcbkr,
			tx.Fees(),
			res.Balances(), // bond payment
			id,
		)
		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}
	} else {
		// fees flows
		_ = b.NewRollupOriginationFlows(src, nil, srcbkr, tx.Fees(), nil, id)
		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true

		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++

			// initialize originated rollup
			op.ReceiverId = dst.RowId
			dst.IsContract = true
			dst.CreatorId = src.RowId
			dst.LastSeen = b.block.Height
			dst.TotalFeesUsed += op.Fee
			dst.IsDirty = true

			// create and register a new contract here; the contract index
			// will pick this up later & inserted a database row
			con := model.NewRollupContract(dst, op, res, b.block.Params)
			b.conMap[dst.RowId] = con
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--
			dst.TotalFeesUsed -= op.Fee

			// reverse origination, dst will be deleted
			dst.MustDelete = true
		} else {
			dst.IsDirty = true
			src.NTxFailed--
		}
	}

	return nil
}

func (b *Builder) AppendSmartRollupTransactionOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	var (
		src, dst, loser, recv *model.Account
		mgr                   rpc.Manager
		ok                    bool
		params                []byte
		ires                  []rpc.InternalResult
	)
	res := o.Result()

	// resolve involved parties
	switch tx := o.(type) {
	case *rpc.SmartRollupAddMessages:
		mgr = tx.Manager
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		params = tx.Encode()

	case *rpc.SmartRollupCement:
		mgr = tx.Manager
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		dst, ok = b.AccountByAddress(tx.Rollup)
		if !ok {
			return Errorf("missing target rollup %s", tx.Rollup)
		}
		params = tx.Encode()

	case *rpc.SmartRollupPublish:
		mgr = tx.Manager
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		dst, ok = b.AccountByAddress(tx.Rollup)
		if !ok {
			return Errorf("missing target rollup %s", tx.Rollup)
		}
		params = tx.Encode()

	case *rpc.SmartRollupRefute:
		mgr = tx.Manager
		// can end a game and slash
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		dst, ok = b.AccountByAddress(tx.Rollup)
		if !ok {
			return Errorf("missing target rollup %s", tx.Rollup)
		}
		if res.IsSuccess() && res.GameStatus.Kind == "loser" {
			loser, ok = b.AccountByAddress(*res.GameStatus.Player)
			if !ok {
				return Errorf("missing loser account %s", res.GameStatus.Player)
			}
		}
		params = tx.Encode()

	case *rpc.SmartRollupTimeout:
		mgr = tx.Manager
		// always ends a game and slashes
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		dst, ok = b.AccountByAddress(tx.Rollup)
		if !ok {
			return Errorf("missing target rollup %s", tx.Rollup)
		}
		// log.Warnf("Timeout: Game status %#v", res.GameStatus)
		if res.IsSuccess() && res.GameStatus.Kind == "loser" {
			loser, ok = b.AccountByAddress(*res.GameStatus.Player)
			if !ok {
				return Errorf("missing loser account %s", res.GameStatus.Player)
			}
		}
		params = tx.Encode()

	case *rpc.SmartRollupExecuteOutboxMessage:
		mgr = tx.Manager
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		dst, ok = b.AccountByAddress(tx.Rollup)
		if !ok {
			return Errorf("missing target rollup %s", tx.Rollup)
		}
		params = tx.Encode()
		ires = tx.Metadata.InternalResults

	case *rpc.SmartRollupRecoverBond:
		mgr = tx.Manager
		src, ok = b.AccountByAddress(tx.Source)
		if !ok {
			return Errorf("missing sender account %s", tx.Source)
		}
		dst, ok = b.AccountByAddress(tx.Rollup)
		if !ok {
			return Errorf("missing target rollup %s", tx.Rollup)
		}
		recv, ok = b.AccountByAddress(tx.Staker)
		if !ok {
			return Errorf("missing staker account %s", tx.Staker)
		}
		params = tx.Encode()
	default:
		return Errorf("unsupported %s op", o.Kind())
	}

	// resolve bakers
	var sbkr, lbkr *model.Baker
	if src.BakerId != 0 {
		if sbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %s", src.BakerId, src)
		}
	}
	if loser != nil && loser.BakerId != 0 {
		if lbkr, ok = b.BakerById(loser.BakerId); !ok {
			return Errorf("missing baker %d for loser account %s", loser.BakerId, src)
		}
	}

	// resolve rollup contract
	var (
		rCon *model.Contract
		err  error
	)
	if dst != nil {
		rCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %v", dst, dst.RowId, err)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	if dst != nil {
		op.ReceiverId = dst.RowId
	}
	op.Counter = mgr.Counter
	op.Fee = mgr.Fee
	op.GasLimit = mgr.GasLimit
	op.StorageLimit = mgr.StorageLimit
	op.IsRollup = true

	op.Status = res.Status
	op.IsSuccess = res.IsSuccess()
	op.GasUsed = res.Gas()
	op.StoragePaid = res.PaidStorageSizeDiff
	b.block.Ops = append(b.block.Ops, op)

	op.Data = o.Kind().String()
	op.Entrypoint = int(o.Kind()) - int(tezos.OpTypeSmartRollupOriginate)
	if loser != nil {
		op.CreatorId = loser.RowId
	}
	if recv != nil {
		op.CreatorId = recv.RowId
	}

	// store rollup args and result as parameters
	op.Parameters = micheline.NewPair(
		micheline.NewBytes(params),
		micheline.NewBytes(res.SmartRollupResult.Encode()),
	).ToBytes()

	// keep ticket updates
	op.RawTicketUpdates = res.TicketUpdates()

	if op.IsSuccess {
		flows := b.NewRollupTransactionFlows(
			src, dst, loser, recv, // involved accounts
			sbkr, lbkr, // related bakers (optional)
			o.Fees(),       // fees
			res.Balances(), // move, slash
			b.block,
			id,
		)

		// update burn from burn flow (on bond slash)
		for _, f := range flows {
			switch {
			case f.IsBurned:
				// storage or penalty
				op.Burned += f.AmountOut
			case f.IsUnfrozen:
				// bond unfreeze
				op.Volume = f.AmountOut
			default:
				if f.Operation == model.FlowTypeRollupReward {
					// winner reward
					op.Reward += f.AmountIn
				} else if f.Category == model.FlowCategoryBond {
					// bond freeze
					op.Deposit = f.AmountIn
				}
			}
		}
	} else {
		// fees only
		_ = b.NewRollupTransactionFlows(
			src, nil, nil, nil, // just source
			sbkr, nil, // just source baker
			mgr.Fees(),
			nil, // no result balance updates
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
		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++

			// add messages is global (n specific rollup)
			if dst != nil {
				dst.LastSeen = b.block.Height
				dst.IsDirty = true
				dst.NTxIn++
				dst.TotalFeesUsed += op.Fee
				_ = rCon.Update(op, b.block.Params)
			}
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--

			// add messages is global (n specific rollup)
			if dst != nil {
				dst.IsDirty = true
				dst.NTxIn--
				dst.TotalFeesUsed -= op.Fee
				rCon.Rollback(op, nil, b.block.Params)
			}
		} else {
			src.NTxFailed--
		}
	}

	// apply internal operation result (may generate new op and flows)
	for i, v := range ires {
		// skip events, they are processed in event index
		if v.Kind == tezos.OpTypeEvent {
			continue
		}
		id.I = i
		id.Kind = model.MapOpType(v.Kind)
		id.N++
		switch id.Kind {
		case model.OpTypeTransaction:
			if err := b.AppendInternalTransactionOp(ctx, src, sbkr, oh, v, id, rollback); err != nil {
				return err
			}
		case model.OpTypeDelegation:
			if err := b.AppendInternalDelegationOp(ctx, src, sbkr, oh, v, id, rollback); err != nil {
				return err
			}
		case model.OpTypeOrigination:
			if err := b.AppendInternalOriginationOp(ctx, src, sbkr, oh, v, id, rollback); err != nil {
				return err
			}
		default:
			return Errorf("unsupported internal operation type %s", v.Kind)
		}
	}

	return nil
}
