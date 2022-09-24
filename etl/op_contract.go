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

    var (
        flows []*model.Flow
        // err   error
    )
    if op.IsSuccess {
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
        // op.Storage, err = gop.Value.MarshalBinary()
        // if err != nil {
        //  return Errorf("marshal value: %v", err)
        // }

    } else {
        // handle errors
        if len(res.Errors) > 0 {
            if buf, err := json.Marshal(res.Errors); err == nil {
                op.Errors = buf
            } else {
                // non-fatal, but error data will be missing from index
                log.Error(Errorf("marshal op errors: %s", err))
            }
        }

        // fees flows
        b.NewConstantRegistrationFlows(src, srcbkr, gop.Fees(), nil, id)
    }

    b.block.Ops = append(b.block.Ops, op)

    // update sender account
    if !rollback {
        src.Counter = op.Counter
        src.NOps++
        src.NConstants++
        src.LastSeen = b.block.Height
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed++
        }
    } else {
        src.Counter = op.Counter - 1
        src.NOps--
        src.NConstants--
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed--
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
        if len(res.Errors) > 0 {
            if buf, err := json.Marshal(res.Errors); err == nil {
                op.Errors = buf
            } else {
                // non-fatal, but error data will be missing from index
                log.Error(Errorf("marshal op errors: %s", err))
            }
        }
    }

    b.block.Ops = append(b.block.Ops, op)

    // update sender account
    if !rollback {
        src.Counter = op.Counter
        src.NOps++
        src.LastSeen = b.block.Height
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed++
        } else {
            // increase contract storage limit and paid sum
            con.StoragePaid += op.StoragePaid
            con.StorageBurn += op.StoragePaid * b.block.Params.CostPerByte
            con.IsDirty = true
        }
    } else {
        src.Counter = op.Counter - 1
        src.NOps--
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed--
        } else {
            // increase contract storage limit and paid sum
            con.StoragePaid -= op.StoragePaid
            con.StorageBurn -= op.StoragePaid * b.block.Params.CostPerByte
            con.IsDirty = true
        }
    }

    return nil
}
