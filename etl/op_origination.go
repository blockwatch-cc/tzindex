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

// NOTE: does not extend grace period although it's a manager operation
// - burns a fee (optional, not used early on)
// - can delegate funds
// - only originated accounts (KT1) can delegate
// - only implicit accounts (tz1) can be delegates
// - by default originated accounts are not delegatable (but initial delegate can be set)
func (b *Builder) AppendOriginationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
    o := id.Get(oh)

    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "%s op [%d:%d:%d]: "+format,
            append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
        )
    }

    oop, ok := o.(*rpc.Origination)
    if !ok {
        return Errorf("unexpected type %T", o)
    }

    src, ok := b.AccountByAddress(oop.Source)
    if !ok {
        return Errorf("missing source account %s", oop.Source)
    }
    var (
        mgr, dst       *model.Account
        srcbkr, newbkr *model.Baker
    )
    if a := oop.ManagerAddress(); a.IsValid() {
        mgr, ok = b.AccountByAddress(a)
        if !ok {
            return Errorf("missing manager account %s", a)
        }
    }
    if src.BakerId != 0 {
        if srcbkr, ok = b.BakerById(src.BakerId); !ok {
            return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
        }
    }
    res := oop.Result()
    if oop.Delegate != nil && res.Status.IsSuccess() {
        newbkr, ok = b.BakerByAddress(*oop.Delegate)
        // register self bakers if not registered yet (v002 fixed this)
        if !ok && b.block.Params.Version <= 1 && src.Address.Equal(*oop.Delegate) && !src.IsBaker {
            newbkr = b.RegisterBaker(src, false)
            // log.Infof("Origination bug baker register %s", newbkr)
        }
        if newbkr == nil {
            return Errorf("missing baker account %s", oop.Delegate)
        }
    }

    // build op
    op := model.NewOp(b.block, id)
    op.SenderId = src.RowId
    op.Counter = oop.Counter
    op.Fee = oop.Fee
    op.GasLimit = oop.GasLimit
    op.StorageLimit = oop.StorageLimit
    op.IsContract = oop.Script != nil
    op.Status = res.Status
    op.IsSuccess = op.Status.IsSuccess()
    op.GasUsed = res.Gas()
    op.Volume = oop.Balance
    op.StoragePaid = res.PaidStorageSizeDiff

    // store manager and baker
    if mgr != nil {
        op.CreatorId = mgr.RowId
    }
    if newbkr != nil {
        op.BakerId = newbkr.AccountId
    }

    var (
        flows []*model.Flow
        err   error
    )
    if op.IsSuccess {
        if l := len(res.OriginatedContracts); l != 1 {
            return Errorf("%d originated accounts, expected exactly 1", l)
        }

        addr := res.OriginatedContracts[0]
        dst, ok = b.AccountByAddress(addr)
        if !ok {
            return Errorf("missing originated account %s", addr)
        }
        op.ReceiverId = dst.RowId
        flows = b.NewOriginationFlows(
            src, dst,
            srcbkr, newbkr,
            oop.Fees(),
            res.Balances(),
            id,
        )

        // update burn from burn flow
        for _, f := range flows {
            if f.IsBurned {
                op.Burned += f.AmountOut
            }
        }

        // create or extend bigmap diff to inject alloc for proto < v005
        // overwrite original diff
        if ev := res.BigmapEvents(); len(ev) > 0 {
            op.BigmapEvents, err = b.PatchBigmapEvents(ctx, ev, dst.Address, oop.Script)
            if err != nil {
                return Errorf("patch bigmap: %v", err)
            }
        }

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
        b.NewOriginationFlows(src, nil, srcbkr, nil, oop.Fees(), nil, id)
    }

    b.block.Ops = append(b.block.Ops, op)

    // update accounts
    if !rollback {
        src.Counter = op.Counter
        src.NOps++
        src.NOrigination++
        src.LastSeen = b.block.Height
        src.IsDirty = true

        if !op.IsSuccess {
            src.NOpsFailed++
        } else {
            // initialize originated account
            // pre-Babylon delegator KT1s had no code, post-babylon keep sender
            // as creator, until Athens it was called manager
            dst.IsContract = oop.Script != nil
            if mgr != nil {
                dst.CreatorId = mgr.RowId
                mgr.LastSeen = b.block.Height
                mgr.IsDirty = true
            } else {
                dst.CreatorId = src.RowId
            }
            dst.UnclaimedBalance = int64(oop.BabylonFlags(b.block.Params.Version))
            dst.LastSeen = b.block.Height
            dst.IsDirty = true

            // handle delegation
            if newbkr != nil {
                dst.IsDelegated = true
                dst.BakerId = newbkr.AccountId
                dst.DelegatedSince = b.block.Height

                newbkr.TotalDelegations++
                newbkr.ActiveDelegations++
                newbkr.IsDirty = true
                newbkr.Account.LastSeen = b.block.Height
                newbkr.Account.IsDirty = true
            }

            // create and register a new contract here; the contract index
            // will pick this up later & inserted a database row
            con := model.NewContract(dst, oop, op, b.Constants(), b.block.Params)
            b.conMap[dst.RowId] = con
            op.Storage = con.Storage
            op.StorageHash = con.StorageHash
            op.IsStorageUpdate = true
            op.Contract = con
        }
    } else {
        src.Counter = op.Counter - 1
        src.NOps--
        src.NOrigination--
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed--
        } else {
            // reverse origination, dst will be deleted
            dst.MustDelete = true
            dst.IsDirty = true
            if newbkr != nil {
                dst.IsDelegated = false
                dst.BakerId = 0
                newbkr.TotalDelegations--
                newbkr.ActiveDelegations--
                newbkr.IsDirty = true
            }
            // ignore, not possible anymore
            // // handle self-delegate deregistration (note: there is no previous delegate)
            // if b.block.Params.HasOriginationBug && newdlg != nil && newdlg.TotalDelegations == 0 && src.RowId == newdlg.RowId {
            //  b.UnregisterDelegate(newdlg)
            // }
        }
    }

    return nil
}

func (b *Builder) AppendInternalOriginationOp(
    ctx context.Context,
    origsrc *model.Account,
    origbkr *model.Baker,
    oh *rpc.Operation,
    iop rpc.InternalResult,
    id model.OpRef,
    rollback bool) error {

    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "internal %s op [%d:%d:%d:%d]: "+format,
            append([]interface{}{iop.Kind, id.L, id.P, id.C, id.I}, args...)...,
        )
    }

    src, ok := b.AccountByAddress(iop.Source)
    if !ok {
        return Errorf("missing source account %s", iop.Source)
    }

    var (
        srcbkr, newbkr *model.Baker
        dst            *model.Account
    )
    if src.BakerId != 0 {
        if srcbkr, ok = b.BakerById(src.BakerId); !ok {
            return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
        }
    }
    if iop.Delegate.IsValid() {
        if newbkr, ok = b.BakerByAddress(iop.Delegate); !ok && iop.Result.Status.IsSuccess() {
            return fmt.Errorf("internal origination op [%d:%d:%d]: missing baker account %s",
                id.L, id.P, id.I, iop.Delegate)
        }
    }

    // build op (internal and outer op share the same hash and block location)
    op := model.NewOp(b.block, id)
    op.IsInternal = true
    op.SenderId = origsrc.RowId
    op.CreatorId = src.RowId
    op.Counter = iop.Nonce
    op.Fee = 0           // n.a. for internal ops
    op.GasLimit = 0      // n.a. for internal ops
    op.StorageLimit = 0  // n.a. for internal ops
    op.IsContract = true // orign is always a contract op!
    res := iop.Result    // internal result
    op.Status = res.Status
    op.GasUsed = res.Gas()
    op.IsSuccess = op.Status.IsSuccess()
    op.Volume = iop.Balance
    op.StoragePaid = res.PaidStorageSizeDiff

    // store baker
    if newbkr != nil {
        op.BakerId = newbkr.AccountId
    }

    var (
        flows []*model.Flow
        err   error
    )
    if op.IsSuccess {
        if l := len(res.OriginatedContracts); l != 1 {
            return Errorf("%d originated accounts", l)
        }

        addr := res.OriginatedContracts[0]
        dst, ok = b.AccountByAddress(addr)
        if !ok {
            return Errorf("missing originated account %s", addr)
        }
        op.ReceiverId = dst.RowId
        flows = b.NewInternalOriginationFlows(
            origsrc,
            src,
            dst,
            origbkr,
            srcbkr,
            newbkr,
            res.Balances(),
            id,
        )

        // update burn from burn flow
        for _, f := range flows {
            if f.IsBurned {
                op.Burned += f.AmountOut
            }
        }

        // create or extend bigmap diff to inject alloc for proto < v005
        // patch original result
        if ev := res.BigmapEvents(); len(ev) > 0 {
            op.BigmapEvents, err = b.PatchBigmapEvents(ctx, ev, dst.Address, iop.Script)
            if err != nil {
                return Errorf("patch bigmap: %v", err)
            }
        }

    } else if len(res.Errors) > 0 {
        // handle errors
        if buf, err := json.Marshal(res.Errors); err == nil {
            op.Errors = buf
        } else {
            // non-fatal, but error data will be missing from index
            log.Error(Errorf("marshal op errors: %s", err))
        }

        // no internal fees, no flows on failure
    }

    b.block.Ops = append(b.block.Ops, op)

    // update accounts
    if !rollback {
        src.Counter = op.Counter
        src.NOps++
        src.NOrigination++
        src.LastSeen = b.block.Height
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed++
        } else {
            dst.LastSeen = b.block.Height
            dst.IsContract = iop.Script != nil
            dst.IsDirty = true

            // internal originations have no manager, baker and flags (in protocol v5)
            // but we still keep the original caller as manager to track contract ownership
            dst.CreatorId = origsrc.RowId

            // handle delegation
            if newbkr != nil {
                dst.IsDelegated = true
                dst.BakerId = newbkr.AccountId
                dst.DelegatedSince = b.block.Height

                newbkr.TotalDelegations++
                newbkr.ActiveDelegations++
                newbkr.IsDirty = true
                newbkr.Account.LastSeen = b.block.Height
                newbkr.Account.IsDirty = true
            }

            // create and register a new contract here; the contract index
            // will pick this up later & inserted a database row
            con := model.NewInternalContract(dst, iop, op, b.Constants(), b.block.Params)
            b.conMap[dst.RowId] = con
            op.Storage = con.Storage
            op.StorageHash = con.StorageHash
            op.IsStorageUpdate = true
            op.Contract = con
        }
    } else {
        src.Counter = op.Counter - 1
        src.NOps--
        src.NOrigination--
        src.IsDirty = true
        if !op.IsSuccess {
            src.NOpsFailed--
        } else {
            // reverse origination
            dst.MustDelete = true
            dst.IsDirty = true
            if newbkr != nil {
                dst.IsDelegated = false
                dst.BakerId = 0
                newbkr.TotalDelegations--
                newbkr.ActiveDelegations--
                newbkr.IsDirty = true
            }
        }
    }

    return nil
}
