// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/etl/model"
)

const (
	SnapshotPackSizeLog2    = 15 // =32k packs ~ 3M unpacked
	SnapshotJournalSizeLog2 = 16 // =64k entries for busy blockchains
	SnapshotCacheSize       = 2  // minimum
	SnapshotFillLevel       = 100
	SnapshotIndexKey        = "snapshot"
	SnapshotTableKey        = "snapshot"
)

var (
	ErrNoSnapshotEntry = errors.New("snapshot not indexed")
)

type SnapshotIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
}

var _ model.BlockIndexer = (*SnapshotIndex)(nil)

func NewSnapshotIndex(opts pack.Options) *SnapshotIndex {
	return &SnapshotIndex{opts: opts}
}

func (idx *SnapshotIndex) DB() *pack.DB {
	return idx.db
}

func (idx *SnapshotIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *SnapshotIndex) Key() string {
	return SnapshotIndexKey
}

func (idx *SnapshotIndex) Name() string {
	return SnapshotIndexKey + " index"
}

func (idx *SnapshotIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(model.Snapshot{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		SnapshotTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, SnapshotPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, SnapshotJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, SnapshotCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, SnapshotFillLevel),
		})
	if err != nil {
		return err
	}
	return nil
}

func (idx *SnapshotIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		SnapshotTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, SnapshotJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, SnapshotCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *SnapshotIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *SnapshotIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %s", idx.Name(), err)
		}
		idx.table = nil
	}
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *SnapshotIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// handle snapshot index
	if block.TZ.Snapshot != nil {
		if err := idx.updateCycleSnapshot(ctx, block); err != nil {
			log.Error(err)
			// return err
			return nil
		}
	}

	// skip non-snapshot blocks
	if block.Height == 0 || !block.Params.IsSnapshotBlock(block.Height) {
		return nil
	}

	// first snapshot (0 based) is block 255 (0 based index) in a cycle
	// snapshot 15 is without unfrozen rewards from end-of-cycle block
	sn := block.Params.SnapshotIndex(block.Height)
	isCycleEnd := block.Params.IsCycleEnd(block.Height)

	// snapshot all currently funded accounts and delegates
	accounts, err := builder.Table(AccountTableKey)
	if err != nil {
		return err
	}

	// snapshot all active bakers with at least 1 roll (deactivation happens at
	// start of the next cycle, so here bakers are still active!)
	rollOwners := make([]uint64, 0, block.Chain.RollOwners)
	ins := make([]pack.Item, 0, int(block.Chain.FundedAccounts)) // hint
	for _, b := range builder.Bakers() {
		stake := b.ActiveStake(block.Params, block.Chain.Rolls)
		isActive := b.IsActive
		if block.Params.Version < 12 {
			stake = b.StakingBalance()
			// adjust end-of-cycle snapshot: reduce balance by unfrozen rewards
			if isCycleEnd {
				for _, flow := range block.Flows {
					if flow.AccountId != b.AccountId {
						continue
					}
					if flow.Category != model.FlowCategoryRewards {
						continue
					}
					if flow.Operation != model.FlowTypeInternal {
						continue
					}
					stake -= flow.AmountOut
					break
				}
				// predict deactivation at end of cycle
				isActive = isActive && b.GracePeriod > block.Cycle
			}
		}

		// skip non-roll owners
		if b.StakingBalance() < block.Params.MinimalStake {
			continue
		}

		snap := model.NewSnapshot()
		snap.Height = block.Height
		snap.Cycle = block.Cycle
		snap.Timestamp = block.Timestamp
		snap.Index = sn
		snap.Rolls = b.StakingBalance() / block.Params.MinimalStake
		snap.ActiveStake = stake
		snap.AccountId = b.AccountId
		snap.BakerId = b.AccountId
		snap.IsBaker = true
		snap.IsActive = isActive
		snap.Balance = b.TotalBalance()
		snap.Delegated = b.DelegatedBalance
		snap.NDelegations = b.ActiveDelegations
		snap.Since = b.BakerSince
		ins = append(ins, snap)
		rollOwners = append(rollOwners, b.AccountId.Value())
	}

	// log.Infof("snapshot: creating snapshot c%d/%d with %d bakers at block %d",
	// 	block.Cycle, sn, len(ins), block.Height)

	// sort by account id for improved table compression
	sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Snapshot).AccountId < ins[j].(*model.Snapshot).AccountId })

	err = idx.table.Insert(ctx, ins)
	for _, v := range ins {
		v.(*model.Snapshot).Free()
	}
	ins = ins[:0]
	if err != nil {
		return err
	}

	// snapshot all delegating accounts that reference one of the roll owners
	type XAccount struct {
		Id               model.AccountID `pack:"row_id"`
		BakerId          model.AccountID `pack:"baker_id"`
		SpendableBalance int64           `pack:"spendable_balance"`
		FrozenBond       int64           `pack:"frozen_bond"`
		DelegatedSince   int64           `pack:"delegated_since"`
	}
	a := &XAccount{}
	err = pack.NewQuery("snapshot.delegators").
		WithTable(accounts).
		WithoutCache().
		WithFields("row_id", "baker_id", "spendable_balance", "frozen_bond", "delegated_since").
		AndIn("baker_id", rollOwners).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(a); err != nil {
				return err
			}
			// skip all self-delegations because the're already handled above
			if a.Id == a.BakerId {
				return nil
			}

			snap := model.NewSnapshot()
			snap.Height = block.Height
			snap.Cycle = block.Cycle
			snap.Timestamp = block.Timestamp
			snap.Index = sn
			snap.Rolls = 0
			snap.AccountId = a.Id
			snap.BakerId = a.BakerId
			snap.IsBaker = false
			snap.IsActive = false
			snap.Balance = a.SpendableBalance + a.FrozenBond
			snap.Delegated = 0
			snap.NDelegations = 0
			snap.Since = a.DelegatedSince
			ins = append(ins, snap)
			return nil
		})
	if err != nil {
		return err
	}

	// log.Infof("snapshot: creating snapshot c%d/%d with %d delegators at block %d",
	// 	block.Cycle, sn, len(ins), block.Height)

	// sort by account id for improved table compression
	sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Snapshot).AccountId < ins[j].(*model.Snapshot).AccountId })

	err = idx.table.Insert(ctx, ins)
	for _, v := range ins {
		v.(*model.Snapshot).Free()
	}
	return err
}

func (idx *SnapshotIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	// skip non-snapshot blocks
	if block.Height == 0 || !block.Params.IsSnapshotBlock(block.Height) {
		return nil
	}
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *SnapshotIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting snapshots at height %d", height)
	_, err := pack.NewQuery("etl.snapshot.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *SnapshotIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting snapshots for cycle %d", cycle)
	_, err := pack.NewQuery("etl.snapshot.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

// fetches all rows from table, updates contents and removes all non-selected
// snapshot rows to save table space
func (idx *SnapshotIndex) updateCycleSnapshot(ctx context.Context, block *model.Block) error {
	// update all snapshot rows at snapshot cycle & index
	snap := block.TZ.Snapshot

	// adjust to source snapshot cycle
	if snap.Base < 0 {
		return nil
	}

	// log.Infof("snapshot: selecting index %d in c%d at block %d [c%d]",
	// 	snap.Index, snap.Base, block.Height, block.Cycle)

	upd := make([]pack.Item, 0)
	err := pack.NewQuery("snapshot.update").
		WithTable(idx.table).
		WithoutCache().
		AndEqual("cycle", snap.Base).
		AndEqual("index", snap.Index).
		Stream(ctx, func(r pack.Row) error {
			s := model.NewSnapshot()
			if err := r.Decode(s); err != nil {
				return err
			}
			s.IsSelected = true
			upd = append(upd, s)
			return nil
		})
	if err != nil {
		return err
	}

	// store update
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}
	for _, v := range upd {
		v.(*model.Snapshot).Free()
	}

	// delete non-selected snapshots
	_, err = pack.NewQuery("snapshot.delete").
		WithTable(idx.table).
		WithoutCache().
		AndEqual("cycle", snap.Base).
		AndEqual("is_selected", false).
		Delete(ctx)
	if err != nil {
		return err
	}

	// compact
	return idx.table.Compact(ctx)
}

func (idx *SnapshotIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
