// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

const SnapshotIndexKey = "snapshot"

type SnapshotIndex struct {
	db    *pack.DB
	table *pack.Table
	stage *pack.Table
}

var _ model.BlockIndexer = (*SnapshotIndex)(nil)

func NewSnapshotIndex() *SnapshotIndex {
	return &SnapshotIndex{}
}

func (idx *SnapshotIndex) DB() *pack.DB {
	return idx.db
}

func (idx *SnapshotIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table, idx.stage}
}

func (idx *SnapshotIndex) Key() string {
	return SnapshotIndexKey
}

func (idx *SnapshotIndex) Name() string {
	return SnapshotIndexKey + " index"
}

func (idx *SnapshotIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Snapshot{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	// create storage for finalized snapshots
	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		return err
	}

	// create staging storage (same config as main storage)
	_, err = db.CreateTableIfNotExists(model.SnapshotStagingTableKey, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))

	return err
}

func (idx *SnapshotIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Snapshot{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}

	idx.stage, err = idx.db.Table(model.SnapshotStagingTableKey, m.TableOpts().Merge(model.ReadConfigOpts(key)))
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
		if err := idx.stage.Close(); err != nil {
			log.Errorf("Closing %s (staging): %s", idx.Name(), err)
		}
		idx.stage = nil
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
	if block.Height == 0 || !block.TZ.IsSnapshotBlock() {
		return nil
	}

	// first snapshot (0 based) is block 255 (0 based index) in a cycle
	// snapshot 15 is without unfrozen rewards from end-of-cycle block
	sn := block.TZ.GetSnapshotIndex()
	isCycleEnd := block.TZ.IsCycleEnd()

	// snapshot all currently funded accounts and delegates
	accounts, err := builder.Table(model.AccountTableKey)
	if err != nil {
		return err
	}

	// snapshot all active bakers with minimum stake (deactivation happens at
	// start of the next cycle, so here bakers are still active!)
	activeBakers := make([]uint64, 0, block.Chain.EligibleBakers)
	ins := make([]pack.Item, 0, int(block.Chain.FundedAccounts)) // hint
	for _, b := range builder.Bakers() {
		// adjust end-of-cycle snapshot; a positive value of adjust means the
		// balance will be reduced when calculating stake
		adjust := b.StakeAdjust(block)

		// predict deactivation at end of cycle
		isActive := b.IsActive
		if isCycleEnd {
			isActive = isActive && b.GracePeriod > block.Cycle
		}

		// calculate stake and balance with potential adjustment
		stakingBalance := b.StakingBalance() - adjust
		balance := b.TotalBalance() - adjust
		ownStake := b.FrozenStake() - adjust

		// skip bakers below minimum staking balance (not active stake!)
		if stakingBalance < block.Params.MinimalStake {
			continue
		}

		snap := model.NewSnapshot()
		snap.Height = block.Height
		snap.Cycle = block.Cycle
		snap.Timestamp = block.Timestamp
		snap.Index = sn
		snap.OwnStake = ownStake
		snap.StakingBalance = stakingBalance
		snap.AccountId = b.AccountId
		snap.BakerId = b.AccountId
		snap.IsBaker = true
		snap.IsActive = isActive
		snap.Balance = balance
		snap.Delegated = b.DelegatedBalance
		snap.NDelegations = b.ActiveDelegations
		snap.NStakers = b.ActiveStakers
		snap.Since = b.BakerSince
		ins = append(ins, snap)
		activeBakers = append(activeBakers, b.AccountId.U64())
	}

	// log.Infof("snapshot: creating snapshot c%d/%d with %d bakers at block %d",
	// 	block.Cycle, sn, len(ins), block.Height)

	// sort by account id for improved table compression
	sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Snapshot).AccountId < ins[j].(*model.Snapshot).AccountId })

	err = idx.stage.Insert(ctx, ins)
	for _, v := range ins {
		v.(*model.Snapshot).Free()
	}
	ins = ins[:0]
	if err != nil {
		return err
	}

	// snapshot all delegating accounts that reference eligible bakers
	type XAccount struct {
		Id               model.AccountID `pack:"row_id"`
		BakerId          model.AccountID `pack:"baker_id"`
		SpendableBalance int64           `pack:"spendable_balance"`
		UnstakedBalance  int64           `pack:"unstaked_balance"`
		FrozenBond       int64           `pack:"frozen_rollup_bond"`
		DelegatedSince   int64           `pack:"delegated_since"`
		StakeShares      int64           `pack:"stake_shares"`
	}
	a := &XAccount{}
	err = pack.NewQuery("etl.delegators").
		WithTable(accounts).
		WithoutCache().
		WithFields(
			"row_id",
			"baker_id",
			"spendable_balance",
			"unstaked_balance",
			"frozen_rollup_bond",
			"delegated_since",
			"stake_shares",
		).
		AndIn("baker_id", activeBakers).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(a); err != nil {
				return err
			}
			// skip all self-delegations because the're already handled above
			if a.Id == a.BakerId {
				return nil
			}
			bkr, ok := builder.BakerById(a.BakerId)
			if !ok {
				return fmt.Errorf("missing baker B_%d", a.BakerId)
			}

			snap := model.NewSnapshot()
			snap.Height = block.Height
			snap.Cycle = block.Cycle
			snap.Timestamp = block.Timestamp
			snap.Index = sn
			snap.AccountId = a.Id
			snap.BakerId = a.BakerId
			snap.Balance = a.SpendableBalance + a.FrozenBond + a.UnstakedBalance
			snap.OwnStake = bkr.StakeAmount(a.StakeShares)
			snap.Since = a.DelegatedSince
			ins = append(ins, snap)
			return nil
		})
	if err != nil {
		return err
	}

	// log.Infof("snapshot: creating snapshot C_%d/%d with %d delegators at block B_%d",
	// 	block.Cycle, sn, len(ins), block.Height)

	// sort by account id for improved table compression
	sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Snapshot).AccountId < ins[j].(*model.Snapshot).AccountId })

	err = idx.stage.Insert(ctx, ins)
	for _, v := range ins {
		v.(*model.Snapshot).Free()
	}
	return err
}

func (idx *SnapshotIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	// skip non-snapshot blocks
	// if block.Height == 0 || !block.Params.IsSnapshotBlock(block.Height) {
	if block.Height == 0 || !block.TZ.IsSnapshotBlock() {
		return nil
	}
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *SnapshotIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting snapshots at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.stage).
		WithoutCache().
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *SnapshotIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting snapshots for cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		WithoutCache().
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

// moves rows from selected snapshot from staging to snapshot table, removes old
// snapshot rows from staging and compacts
func (idx *SnapshotIndex) updateCycleSnapshot(ctx context.Context, block *model.Block) error {
	// update all snapshot rows at snapshot cycle & index
	snap := block.TZ.Snapshot

	// adjust to source snapshot cycle
	if snap.Base < 0 {
		return nil
	}

	// log.Infof("snapshot: selecting index I_%d in C_%d at block B_%d [C_%d]",
	// 	snap.Index, snap.Base, block.Height, block.Cycle)

	// copy selected snapshot data from staging table to snapshot table
	chunkSize := int(1 << idx.table.Options().JournalSizeLog2)
	snaps := make([]*model.Snapshot, 0, chunkSize)
	move := make([]pack.Item, 0, chunkSize)
	var cursor uint64
	var cnt int
	for {
		err := pack.NewQuery("etl.move").
			WithTable(idx.stage).
			WithLimit(chunkSize).
			WithoutCache().
			AndEqual("cycle", snap.Base).
			AndEqual("index", snap.Index).
			AndGt("row_id", cursor).
			Execute(ctx, &snaps)
		if err != nil {
			return err
		}
		if len(snaps) == 0 {
			break
		}
		cnt += len(snaps)
		move = move[:len(snaps)]
		cursor = snaps[len(snaps)-1].RowId
		for i := range snaps {
			snaps[i].RowId = 0
			move[i] = snaps[i]
		}
		if err := idx.table.Insert(ctx, move); err != nil {
			return err
		}
		snaps = snaps[:0]
	}

	// log.Infof("snapshot: copied %d rows for C_%d", cnt, snap.Base)
	// log.Infof("snapshot: deleting all snapshots before C_%d", snap.Base)

	// delete unused snapshots from stage
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.stage).
		WithoutCache().
		AndLt("cycle", snap.Base).
		Delete(ctx)
	if err != nil {
		return err
	}

	// flush and compact every 16 cycles
	if block.Cycle > 0 && block.Cycle%16 == 0 {
		// if err := idx.stage.Flush(ctx); err != nil {
		// 	return err
		// }
		if err := idx.stage.Compact(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (idx *SnapshotIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *SnapshotIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
