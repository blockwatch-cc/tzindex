// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "fmt"

    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
)

// insert invoice
func (b *Builder) MigrateAthens(ctx context.Context, oldparams, params *tezos.Params) error {
    account, err := b.idx.Table(index.AccountIndexKey)
    if err != nil {
        return err
    }

    log.Infof("Migrate v%03d: inserting invoices", params.Version)

    var count int
    for n, amount := range map[string]int64{
        "tz1iSQEcaGpUn6EW5uAy3XhPiNg7BHMnRSXi": 100 * 1000000,
    } {
        addr, err := tezos.ParseAddress(n)
        if err != nil {
            return fmt.Errorf("decoding invoice address %s: %w", n, err)
        }
        acc := model.NewAccount(addr)
        acc.FirstSeen = b.block.Height
        acc.LastIn = b.block.Height
        acc.LastSeen = b.block.Height
        acc.IsDirty = true

        // insert into db
        if err := account.Insert(ctx, acc); err != nil {
            return err
        }

        // insert into cache
        b.accMap[acc.RowId] = acc
        b.accHashMap[b.accCache.AccountHashKey(acc)] = acc

        // add invoice op
        b.AppendInvoiceOp(ctx, acc, amount, count)
        count++
    }

    log.Infof("Migrate v%03d: complete", params.Version)
    return nil
}
