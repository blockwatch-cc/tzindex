// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "sort"
    "strings"

    "blockwatch.cc/tzgo/tezos"
)

func (b *Builder) MigrateIthaca(ctx context.Context, oldparams, params *tezos.Params) error {
    // nothing to do in light mode or when chain starts with this proto
    if b.block.Height <= 2 {
        return nil
    }

    // resort balance updates to collate legacy unfreeze events by baker
    balances := b.block.TZ.Block.Metadata.BalanceUpdates
    sort.SliceStable(balances, func(i, j int) bool {
        iLegacy := strings.HasPrefix(balances[i].Category, "legacy")
        jLegacy := strings.HasPrefix(balances[j].Category, "legacy")
        if iLegacy && jLegacy {
            return balances[i].Delegate < balances[j].Delegate
        }
        return iLegacy && !jLegacy
    })
    // ensure the baker of the last legacy update and the following baker are
    // not identical; this is to prevent the flow -> op matching algorithm
    // to mistakenly collate non-legacy unfreezes with other events
    // Note: search finds the first non-legacy event
    idx := sort.Search(len(balances), func(i int) bool {
        return !strings.HasPrefix(balances[i].Category, "legacy")
    })
    if idx > 0 && idx+1 < len(balances) {
        if balances[idx-1].Address().Equal(balances[idx].Address()) {
            // swap the two updates following the last legacy event
            balances[idx], balances[idx+1] = balances[idx+1], balances[idx]
        }
    } else {
        log.Warnf("Migrate v%03d: check balance update order, we may miss events", params.Version)
    }

    log.Infof("Migrate v%03d: complete", params.Version)
    return nil
}
