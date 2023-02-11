// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "encoding/json"
    "fmt"

    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

func (b *Builder) AppendRegisterConstantOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
    o := id.Get(oh)

    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "%s op [%d:%d:%d]: "+format,
            append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
        )
    }

    gop, ok := o.(*rpc.ConstantRegistration)
    if !ok {
        return Errorf("unexpected type %T", o)
    }

    src, ok := b.AccountByAddress(gop.Source)
    if !ok {
        return Errorf("missing source account %s", gop.Source)
    }
    var srcbkr *model.Baker
    if src.BakerId != 0 {
        if srcbkr, ok = b.BakerById(src.BakerId); !ok {
            return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
        }
    }

    // build op
    op := model.NewOp(b.block, id)
    op.SenderId = src.RowId
    op.Counter = gop.Counter
    op.Fee = gop.Fee
    op.GasLimit = gop.GasLimit
    op.StorageLimit = gop.StorageLimit
    res := gop.Result()
    op.Status = res.Status
    op.IsSuccess = op.Status.IsSuccess()
    op.GasUsed = res.Gas()
    op.StoragePaid = res.StorageSize // sic!
    b.block.Ops = append(b.block.Ops, op)

    var flows []*model.Flow
    if op.IsSuccess {
        // fee flows
        flows = b.NewConstantRegistrationFlows(
            src, srcbkr,
            gop.Fees(),
            res.Balances(),
            id,
        )

        // update burn from burn flow
        for _, f := range flows {
            if f.IsBurned {
                op.Burned += f.AmountOut
            }
        }

        op.Data = res.GlobalAddress.String()

    } else {
        // fees flows
        b.NewConstantRegistrationFlows(src, srcbkr, gop.Fees(), nil, id)

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
        } else {
            src.NTxFailed++
        }
    } else {
        src.Counter = op.Counter - 1
        src.IsDirty = true
        if op.IsSuccess {
            src.NTxSuccess--
            src.NTxOut--
        } else {
            src.NTxFailed--
        }
    }

    return nil
}

func (b *Builder) AppendStorageLimitOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
    o := id.Get(oh)

    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "%s op [%d:%d:%d]: "+format,
            append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
        )
    }

    sop, ok := o.(*rpc.IncreasePaidStorage)
    if !ok {
        return Errorf("unexpected type %T", o)
    }

    src, ok := b.AccountByAddress(sop.Source)
    if !ok {
        return Errorf("missing source account %s", sop.Source)
    }
    recv, ok := b.AccountByAddress(sop.Destination)
    if !ok {
        return Errorf("missing destination account %s", sop.Destination)
    }
    var srcbkr *model.Baker
    if src.BakerId != 0 {
        if srcbkr, ok = b.BakerById(src.BakerId); !ok {
            return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
        }
    }
    con, err := b.LoadContractByAccountId(ctx, recv.RowId)
    if err != nil {
        return Errorf("loading contract %s %d: %v", sop.Destination, recv.RowId, err)
    }

    // build op
    op := model.NewOp(b.block, id)
    op.SenderId = src.RowId
    op.ReceiverId = recv.RowId
    op.Counter = sop.Counter
    op.Fee = sop.Fee
    op.GasLimit = sop.GasLimit
    op.StorageLimit = sop.StorageLimit
    res := sop.Result()
    op.Status = res.Status
    op.IsSuccess = res.IsSuccess()
    op.GasUsed = res.Gas()
    op.StoragePaid = sop.Amount
    b.block.Ops = append(b.block.Ops, op)

    if op.IsSuccess {
        flows := b.NewIncreasePaidStorageFlows(
            src, srcbkr,
            sop.Fees(),
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
        b.NewIncreasePaidStorageFlows(src, srcbkr, sop.Fees(), nil, id)

        // handle errors
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
            recv.NTxIn++
            recv.IsDirty = true

            // increase contract storage limit and paid sum
            con.StoragePaid += op.StoragePaid
            con.StorageBurn += op.StoragePaid * b.block.Params.CostPerByte
            con.IsDirty = true
        } else {
            src.NTxFailed++
        }
    } else {
        src.Counter = op.Counter - 1
        src.IsDirty = true
        if op.IsSuccess {
            src.NTxSuccess--
            src.NTxOut--
            recv.NTxIn--
            recv.IsDirty = true

            // decrease contract storage limit and paid sum
            con.StoragePaid -= op.StoragePaid
            con.StorageBurn -= op.StoragePaid * b.block.Params.CostPerByte
            con.IsDirty = true
        } else {
            src.NTxFailed--
        }
    }

    return nil
}
