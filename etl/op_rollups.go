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

func (b *Builder) AppendRollupOriginationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
    o := id.Get(oh)

    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "%s op [%d:%d:%d]: "+format,
            append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
        )
    }

    rollup, ok := o.(*rpc.Rollup)
    if !ok {
        return Errorf("unexpected type %T", o)
    }

    src, ok := b.AccountByAddress(rollup.Source)
    if !ok {
        return Errorf("missing source account %s", rollup.Source)
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
    res := rollup.Result()
    if res.OriginatedRollup.IsValid() && res.Status.IsSuccess() {
        dst, ok = b.AccountByAddress(res.OriginatedRollup)
        if !ok {
            return Errorf("missing originated rollup %s", res.OriginatedRollup)
        }
    }

    // build op
    op := model.NewOp(b.block, id)
    op.SenderId = src.RowId
    op.Counter = rollup.Counter
    op.Fee = rollup.Fee
    op.GasLimit = rollup.GasLimit
    op.StorageLimit = rollup.StorageLimit
    op.IsRollup = true
    op.Status = res.Status
    op.IsSuccess = res.Status.IsSuccess()
    op.GasUsed = res.Gas()
    op.StoragePaid = res.PaidStorageSizeDiff
    b.block.Ops = append(b.block.Ops, op)

    if op.IsSuccess {
        flows := b.NewRollupOriginationFlows(
            src, dst,
            srcbkr,
            rollup.Fees(),
            res.Balances(),
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
        _ = b.NewRollupOriginationFlows(src, nil, srcbkr, rollup.Fees(), nil, id)
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
            con := model.NewRollupContract(dst, op, b.block.Params)
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

func (b *Builder) AppendRollupTransactionOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
    o := id.Get(oh)

    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "%s op [%d:%d:%d]: "+format,
            append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
        )
    }

    rollup, ok := o.(*rpc.Rollup)
    if !ok {
        return Errorf("unexpected type %T", o)
    }

    src, ok := b.AccountByAddress(rollup.Source)
    if !ok {
        return Errorf("missing sender account %s", rollup.Source)
    }

    res := rollup.Result()

    dst, ok := b.AccountByAddress(rollup.Target())
    if !ok && res.Status.IsSuccess() {
        return Errorf("missing target contract %s", rollup.Target())
    }

    var (
        offender *model.Account
        sbkr     *model.Baker
        obkr     *model.Baker
        dCon     *model.Contract
        err      error
    )
    if src.BakerId != 0 {
        if sbkr, ok = b.BakerById(src.BakerId); !ok {
            return Errorf("missing baker %d for source account %s", src.BakerId, src)
        }
    }
    dCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
    if err != nil {
        return Errorf("loading contract %s %d: %v", rollup.Rollup, dst.RowId, err)
    }

    // rejection can slash offender (who we only find inside the result's balance update)
    if rollup.Kind() == tezos.OpTypeToruRejection && res.Status.IsSuccess() {
        addr := res.Balances()[0].Address()
        offender, ok = b.AccountByAddress(addr)
        if !ok {
            return Errorf("missing rollup offender %s", addr)
        }
        if offender.BakerId != 0 {
            if obkr, ok = b.BakerById(offender.BakerId); !ok {
                return Errorf("missing baker %d for offender account %s", offender.BakerId, offender)
            }
        }
    }

    // build op
    op := model.NewOp(b.block, id)
    op.SenderId = src.RowId
    op.ReceiverId = dst.RowId
    op.Counter = rollup.Counter
    op.Fee = rollup.Fee
    op.GasLimit = rollup.GasLimit
    op.StorageLimit = rollup.StorageLimit
    op.IsRollup = true

    op.Status = res.Status
    op.IsSuccess = res.Status.IsSuccess()
    op.GasUsed = res.Gas()
    op.StoragePaid = res.PaidStorageSizeDiff
    b.block.Ops = append(b.block.Ops, op)

    if dCon.Address.IsRollup() {
        // receiver is rollup
        op.Data = rollup.Kind().String()
        op.Entrypoint = int(rollup.Kind()) - int(tezos.OpTypeToruOrigination)
        if offender != nil {
            op.CreatorId = offender.RowId
        }
    } else {
        // receiver is KT1 contract (used for transfer_ticket)
        script, err := dCon.LoadScript()
        if err != nil || script == nil {
            log.Error(Errorf("loading script: %v", err))
        } else {
            eps, _ := script.Entrypoints(false)
            ep, ok := eps[rollup.Transfer.Entrypoint]
            if !ok && op.IsSuccess {
                log.Error(Errorf("missing entrypoint %q", rollup.Transfer.Entrypoint))
            } else {
                op.Entrypoint = ep.Id
                op.Data = rollup.Transfer.Entrypoint
            }
        }
    }

    // add rollup args as parameters
    op.Parameters, err = rollup.EncodeParameters().MarshalBinary()
    if err != nil {
        log.Error(Errorf("marshal parameters errors: %s", err))
    }

    // keep ticket updates
    op.RawTicketUpdates = res.TicketUpdates()

    if op.IsSuccess {
        flows := b.NewRollupTransactionFlows(
            src, dst, offender, // involved accounts
            sbkr, obkr, // related bakers (optional)
            rollup.Fees(),  // fees
            res.Balances(), // move
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
                op.Reward -= f.AmountOut
            default:
                // deduct accuser reward from burn as this is not burned
                if f.Operation == model.FlowTypeRollupReward {
                    op.Burned -= f.AmountIn
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
            src, nil, nil, // just source
            sbkr, nil, // just source baker
            rollup.Fees(),
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
        dst.LastSeen = b.block.Height
        dst.IsDirty = true
        if op.IsSuccess {
            src.NTxSuccess++
            src.NTxOut++
            dst.NTxIn++
            dst.TotalFeesUsed += op.Fee
            _ = dCon.Update(op, b.block.Params)
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
            dCon.Rollback(op, nil, b.block.Params)
        } else {
            src.NTxFailed--
        }
    }

    return nil
}
