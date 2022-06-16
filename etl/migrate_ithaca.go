// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "fmt"
    "sort"
    "strings"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/rpc"
)

func (b *Builder) MigrateIthaca(ctx context.Context, oldparams, params *tezos.Params) error {
    // nothing to do in light mode or when chain starts with this proto
    if b.idx.lightMode || b.block.Height <= 2 {
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

    // fetch and build rights + income for future 5 cycles
    if err := b.RebuildIthacaSnapshotsRightsAndIncome(ctx, params); err != nil {
        return err
    }

    // since snapshot distance changes from N-7 to N-6, update the snapshot index
    // for 1 more cycle (b.block.TZ.Snapshot contains the latest snapshot, we need
    // one more earlier snapshot)
    if b.block.TZ.Snapshot != nil {
        extraSnap, err := b.rpc.GetSnapshotIndexCycle(
            ctx,
            rpc.BlockLevel(b.block.Height),
            b.block.TZ.Snapshot.Cycle-1,
            params,
        )
        if err != nil {
            return fmt.Errorf("cannot fetch extra snapshot for cycle %d: %v", b.block.TZ.Snapshot.Cycle-1, err)
        }
        log.Infof("Migrate v%03d: updating extra snapshot index c%d/%d", params.Version, extraSnap.Base, extraSnap.Index)
        snap, err := b.idx.Table(index.SnapshotTableKey)
        if err != nil {
            return fmt.Errorf("cannot open snapshot table: %v", err)
        }
        upd := make([]pack.Item, 0)
        err = pack.NewQuery("snapshot.update", snap).
            WithoutCache().
            AndEqual("cycle", extraSnap.Base).
            AndEqual("index", extraSnap.Index).
            Stream(ctx, func(r pack.Row) error {
                s := &model.Snapshot{}
                if err := r.Decode(s); err != nil {
                    return err
                }
                s.IsSelected = true
                upd = append(upd, s)
                return nil
            })
        if err != nil {
            return fmt.Errorf("cannot scan snapshot table: %v", err)
        }
        // store update
        if err := snap.Update(ctx, upd); err != nil {
            return fmt.Errorf("cannot update snapshot table: %v", err)
        }
    }

    log.Infof("Migrate v%03d: complete", params.Version)
    return nil
}

func (b *Builder) RebuildIthacaSnapshotsRightsAndIncome(ctx context.Context, params *tezos.Params) error {
    // we need to update all snapshots (c462..467), rights and income indexes (c468..473)
    // Note: this code runs after the migration block 2244608 has been fully processed
    // and in the context of the next block (block.Height == 2244609), but before
    // any effects of the block are visible in accounts, so we can be sure that account
    // and baker state is at block 2244608 end !!
    income, err := b.idx.Index(index.IncomeIndexKey)
    if err != nil {
        return err
    }
    rights, err := b.idx.Index(index.RightsIndexKey)
    if err != nil {
        return err
    }
    snaps, err := b.idx.Table(index.SnapshotTableKey)
    if err != nil {
        return err
    }
    accounts, err := b.idx.Table(index.AccountTableKey)
    if err != nil {
        return err
    }

    // empty fetched rights since we update everything here, this prevents indexers
    // to attempt another cycle update after block 2244609 has been processed
    b.block.TZ.Baking = nil
    b.block.TZ.Endorsing = nil
    b.block.TZ.PrevEndorsing = nil
    b.block.TZ.Snapshot = nil
    b.block.TZ.SnapInfo = nil

    // we fetch and rebuild cycles 468 (n) .. 473 (n+5); similar on testnet with +3
    startCycle, endCycle := params.StartCycle, params.StartCycle+params.PreservedCycles
    if startCycle == 0 {
        startCycle, endCycle = b.block.Cycle, b.block.Cycle+params.PreservedCycles
    }
    log.Infof("Migrate v%03d: updating snapshots, rights and baker income for cycles %d..%d", params.Version, startCycle, endCycle)

    // 1 delete all future cycle rights and income, delete past snapshots
    log.Infof("Migrate v%03d: removing deprecated future rights", params.Version)
    for cycle := startCycle; cycle <= endCycle; cycle++ {
        _ = rights.DeleteCycle(ctx, cycle)
        _ = income.DeleteCycle(ctx, cycle)
    }
    if err := rights.Flush(ctx); err != nil {
        return fmt.Errorf("migrate: flushing rights after clear: %w", err)
    }
    if err := income.Flush(ctx); err != nil {
        return fmt.Errorf("migrate: flushing income after clear: %w", err)
    }

    log.Infof("Migrate v%03d: removing deprecated past snapshots", params.Version)
    for cycle := startCycle; cycle <= endCycle; cycle++ {
        _, _ = pack.NewQuery("migrate.snapshot.delete", snaps).
            AndEqual("cycle", cycle-params.PreservedCycles-1).
            Delete(ctx)
    }
    if err := snaps.Flush(ctx); err != nil {
        return fmt.Errorf("migrate: flushing snapshots after clear: %w", err)
    }

    // 2
    //
    // fetch and insert new rights for cycle .. cycle + preserved_cycles - 1
    // Note: some new bakers may appear, so check and update builder caches
    //
    for cycle := startCycle; cycle <= endCycle; cycle++ {
        log.Infof("Migrate v%03d: processing cycle %d", params.Version, cycle)

        // 2.1 fetch new rights
        bundle := &rpc.Bundle{}
        err := b.rpc.FetchRightsByCycle(ctx, b.block.Height, cycle, bundle)
        if err != nil {
            return fmt.Errorf("migrate: %w", err)
        }
        log.Infof("Migrate v%03d: fetched %d + %d new rights for cycle %d",
            params.Version, len(bundle.Baking[0]), len(bundle.Endorsing[0]), cycle)

        // strip pre-cycle rights if current block is not start of cycle
        // this only happens in Ithaca testnet due to a setup mistake
        if !params.IsCycleStart(b.block.Height) {
            bundle.PrevEndorsing = nil
        }

        // 2.2 analyze rights for new bakers (optional)
        for _, v := range bundle.Baking[0] {
            _, ok := b.AccountByAddress(v.Address())
            if !ok {
                log.Errorf("migrate: missing baker account %s with rights", v.Address())
            }
        }
        for _, v := range bundle.Endorsing[0] {
            _, ok := b.AccountByAddress(v.Address())
            if !ok {
                log.Errorf("migrate: missing endorser account %s with rights", v.Address())
            }
        }

        // 2.3 insert a fake snapshot with index 16 (!sic) for this cycle's base cycle
        // The snapshot balances come from the current state of bakers and
        // delegator accounts, they are the same for all 6 first Ithaca cycles
        // see https://gitlab.com/tezos/tezos/-/issues/2764#note_902498093
        // Skip on testnet because there are no snapshots earlier than cycle 0!
        if cycle > params.PreservedCycles+1 {
            rollOwners := make([]uint64, 0)
            ins := make([]pack.Item, 0)

            // bakers first
            for _, bkr := range b.Bakers() {
                if bkr.Rolls(params) == 0 {
                    continue
                }
                ins = append(ins, &model.Snapshot{
                    Height:       b.parent.Height,                    // snapshot happened at parent block
                    Timestamp:    b.parent.Timestamp,                 // snapshot happened at parent block
                    Cycle:        cycle - params.PreservedCycles - 1, // base cycle
                    Index:        16,                                 // sic!, its the 17th snapshot
                    IsSelected:   true,
                    Rolls:        bkr.Rolls(params),
                    AccountId:    bkr.AccountId,
                    BakerId:      bkr.AccountId,
                    IsBaker:      true,
                    IsActive:     bkr.IsActive,
                    Balance:      bkr.TotalBalance() + bkr.FrozenRewards, // receipt in next block (!)
                    Delegated:    bkr.DelegatedBalance,
                    NDelegations: bkr.ActiveDelegations,
                    Since:        bkr.BakerSince,
                })
                rollOwners = append(rollOwners, bkr.AccountId.Value())
            }

            // sort and insert
            sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Snapshot).AccountId < ins[j].(*model.Snapshot).AccountId })
            err = snaps.Insert(ctx, ins)
            ins = ins[:0]
            if err != nil {
                return fmt.Errorf("migrate: insert baker snapshot for cycle %d: %w", cycle, err)
            }

            // delegators second (non-zero balance, delegating to one of the roll owners)
            type XAccount struct {
                Id               model.AccountID `pack:"row_id"`
                BakerId          model.AccountID `pack:"baker_id"`
                SpendableBalance int64           `pack:"spendable_balance"`
                DelegatedSince   int64           `pack:"delegated_since"`
            }
            a := &XAccount{}
            err = pack.NewQuery("snapshot_delegators", accounts).
                WithoutCache().
                WithFields("row_id", "baker_id", "spendable_balance", "delegated_since").
                AndIn("baker_id", rollOwners).
                Stream(ctx, func(r pack.Row) error {
                    if err := r.Decode(a); err != nil {
                        return err
                    }
                    // skip all self-delegations because the're already handled above
                    if a.Id == a.BakerId {
                        return nil
                    }
                    ins = append(ins, &model.Snapshot{
                        Height:     b.parent.Height,
                        Timestamp:  b.parent.Timestamp,
                        Cycle:      cycle - params.PreservedCycles - 1,
                        Index:      16,
                        IsSelected: true,
                        AccountId:  a.Id,
                        BakerId:    a.BakerId,
                        Balance:    a.SpendableBalance,
                        Since:      a.DelegatedSince,
                    })
                    return nil
                })
            if err != nil {
                return fmt.Errorf("migrate: loading snapshot delegators for cycle %d: %w", cycle, err)
            }

            // sort and insert
            sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Snapshot).AccountId < ins[j].(*model.Snapshot).AccountId })
            err = snaps.Insert(ctx, ins)
            ins = ins[:0]
            if err != nil {
                return fmt.Errorf("migrate: insert delegator snapshot for cycle %d: %w", cycle, err)
            }

            // patch snapshot info to point to the fake index
            bundle.Snapshot.Index = 16
        }

        // 2.4 construct an empty block to insert rights into indexers
        block := &model.Block{
            Height: b.block.Height,
            Params: params,
            TZ:     bundle,
        }

        if err := rights.ConnectBlock(ctx, block, b); err != nil {
            return fmt.Errorf("migrate: insert rights for cycle %d: %w", cycle, err)
        }

        if err := income.ConnectBlock(ctx, block, b); err != nil {
            return fmt.Errorf("migrate: insert income for cycle %d: %w", cycle, err)
        }
    }

    if err := snaps.Flush(ctx); err != nil {
        return fmt.Errorf("migrate: flushing snapshots after upgrade: %w", err)
    }
    if err := rights.Flush(ctx); err != nil {
        return fmt.Errorf("migrate: flushing rights after upgrade: %w", err)
    }
    if err := income.Flush(ctx); err != nil {
        return fmt.Errorf("migrate: flushing income after upgrade: %w", err)
    }

    return nil
}
