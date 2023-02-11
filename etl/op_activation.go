// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "encoding/hex"
    "fmt"

    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

func (b *Builder) AppendActivationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
    o := id.Get(oh)
    Errorf := func(format string, args ...interface{}) error {
        return fmt.Errorf(
            "%s op [%d:%d]: "+format,
            append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
        )
    }

    aop, ok := o.(*rpc.Activation)
    if !ok {
        return Errorf("unexpected type %T ", o)
    }

    // need to lookup using blinded key
    bkey, err := tezos.BlindAddress(aop.Pkh, aop.Secret)
    if err != nil {
        return Errorf("blinded address creation failed: %v", err)
    }

    acc, ok := b.AccountByAddress(bkey)
    if !ok {
        return Errorf("missing account %s", aop.Pkh)
    }

    // cross-check if account exists under it's implicit address
    origacc, ok := b.AccountByAddress(aop.Pkh)
    if !ok {
        origacc, _ = b.idx.LookupAccount(ctx, aop.Pkh)
    }

    // read activated amount, required for potential re-routing
    fees := aop.Fees()
    activated := fees[len(fees)-1].Amount()

    // build op
    op := model.NewOp(b.block, id)
    op.IsSuccess = true
    op.Status = tezos.OpStatusApplied
    op.Volume = activated
    op.SenderId = acc.RowId
    op.ReceiverId = acc.RowId
    op.Data = hex.EncodeToString(aop.Secret) + "," + bkey.String()
    b.block.Ops = append(b.block.Ops, op)

    // update account
    if !rollback {
        // remove blinded hash from builder
        key := b.accCache.AccountHashKey(acc)
        delete(b.accHashMap, key)

        // merge with original account, empty blinded account
        if origacc != nil {
            // move funds and deactivate blinded account
            origacc.UnclaimedBalance = acc.UnclaimedBalance
            origacc.NTxSuccess++
            origacc.NTxOut++
            origacc.IsActivated = true
            origacc.IsDirty = true
            op.ReceiverId = origacc.RowId // keep reference to activated account
            acc.UnclaimedBalance = 0
            acc.LastSeen = b.block.Height
            acc.IsActivated = true
            acc.NTxSuccess++
            acc.NTxOut++
            acc.IsDirty = true

            // register original account with builder
            b.accHashMap[b.accCache.AccountHashKey(origacc)] = origacc
            b.accMap[origacc.RowId] = origacc

            // use original account from now
            acc = origacc
        } else {
            // update blinded account with new hash
            acc.Address = aop.Pkh
            acc.Type = aop.Pkh.Type
            acc.FirstSeen = b.block.Height
            acc.LastSeen = b.block.Height
            acc.FirstIn = b.block.Height
            acc.IsActivated = true
            acc.IsFunded = true
            acc.IsDirty = true
            b.accHashMap[b.accCache.AccountHashKey(acc)] = acc
        }
    } else {
        // check if deactivated blinded account exists
        blindedacc, _ := b.idx.LookupAccount(ctx, bkey)
        if blindedacc != nil {
            // reactivate blinded account
            blindedacc.SpendableBalance = activated

            // register blinded account with builder
            b.accHashMap[b.accCache.AccountHashKey(blindedacc)] = blindedacc
            b.accMap[blindedacc.RowId] = blindedacc

            // rollback current account (adjust spendable balance here!)
            acc.SpendableBalance -= activated
            acc.NTxSuccess--
            acc.NTxOut--
            acc.IsActivated = false
            acc.IsDirty = true

            // use blinded account for flow updates
            acc = blindedacc
        } else {
            // replace implicit hash with blinded hash
            delete(b.accHashMap, b.accCache.AccountHashKey(acc))
            acc.NTxSuccess--
            acc.NTxOut--
            acc.Address = bkey
            acc.Type = bkey.Type
            acc.IsActivated = false
            acc.IsDirty = true
            acc.FirstSeen = 1 // reset to genesis
            acc.LastSeen = 1  // reset to genesis
            acc.FirstIn = 0   // reset to ghost account (pre-genesis)
            b.accHashMap[b.accCache.AccountHashKey(acc)] = acc
        }
    }

    // build flows after rerouting happend
    _ = b.NewActivationFlow(acc, aop, id)
    return nil
}
