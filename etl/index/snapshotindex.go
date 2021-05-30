// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	. "blockwatch.cc/tzindex/etl/model"
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

var _ BlockIndexer = (*SnapshotIndex)(nil)

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
	fields, err := pack.Fields(Snapshot{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
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

func (idx *SnapshotIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %v", idx.Name(), err)
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

func (idx *SnapshotIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// handle snapshot index
	if block.TZ.Snapshot != nil {
		if err := idx.UpdateCycleSnapshot(ctx, block); err != nil {
			return err
		}
	}

	// skip non-snapshot blocks
	if block.Height == 0 || block.Height%block.Params.BlocksPerRollSnapshot != 0 {
		return nil
	}

	// first snapshot (0 based) is block 255 (0 based index) in a cycle
	// snapshot 15 is without unfrozen rewards from end-of-cycle block
	// for governance we use snapshot 15 (at end of cycle == start of voting period)
	// but adjust for unfrozen rewards because technically the voting snapshot
	// happens after unfreeze
	sn := ((block.Height - block.Params.BlocksPerRollSnapshot) % block.Params.BlocksPerCycle) / block.Params.BlocksPerRollSnapshot
	isCycleEnd := block.Params.IsCycleEnd(block.Height)

	// snapshot all currently funded accounts and delegates
	table, err := builder.Table(AccountTableKey)
	if err != nil {
		return err
	}

	// snapshot all active delegates with at least 1 roll (deactivation happens at
	// start of the next cycle, so here bakers are still active)
	q := pack.NewQuery("snapshot_bakers", table).
		WithoutCache().
		WithFields("I", "D", "d", "v", "s", "z", "Y", "~", "a", "*", "P").
		AndEqual("is_active_delegate", true)
	type XAccount struct {
		Id                AccountID `pack:"I"`
		DelegateId        AccountID `pack:"D"`
		IsDelegate        bool      `pack:"d"`
		IsActiveDelegate  bool      `pack:"v"`
		SpendableBalance  int64     `pack:"s"`
		FrozenDeposits    int64     `pack:"z"`
		FrozenFees        int64     `pack:"Y"`
		DelegatedBalance  int64     `pack:"~"`
		ActiveDelegations int64     `pack:"a"`
		DelegateSince     int64     `pack:"*"`
		GracePeriod       int64     `pack:"P"`
		// used for second query only
		DelegatedSince   int64 `pack:"+"`
		UnclaimedBalance int64 `pack:"U"`
	}
	a := &XAccount{}
	rollOwners := make([]uint64, 0, block.Chain.RollOwners)
	ins := make([]pack.Item, 0, int(block.Chain.FundedAccounts)) // hint
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(a); err != nil {
			return err
		}
		// check account owns at least one roll
		ownbalance := a.SpendableBalance + a.FrozenDeposits + a.FrozenFees
		isActive := a.IsActiveDelegate

		// adjust end-of-cycle snapshot: reduce balance by unfrozen rewards
		if isCycleEnd {
			for _, flow := range block.Flows {
				if flow.AccountId != a.Id {
					continue
				}
				if flow.Category != FlowCategoryRewards {
					continue
				}
				if flow.Operation != FlowTypeInternal {
					continue
				}
				ownbalance -= flow.AmountOut
				break
			}
			// predict deactivation at end of cycle
			isActive = isActive && a.GracePeriod > block.Cycle
		}

		// add delegated balance
		stakingBalance := ownbalance + a.DelegatedBalance
		if stakingBalance < block.Params.TokensPerRoll {
			return nil
		}

		snap := NewSnapshot()
		snap.Height = block.Height
		snap.Cycle = block.Cycle
		snap.Timestamp = block.Timestamp
		snap.Index = sn
		snap.Rolls = int64(stakingBalance / block.Params.TokensPerRoll)
		snap.AccountId = a.Id
		snap.DelegateId = a.DelegateId
		snap.IsDelegate = a.IsDelegate
		snap.IsActive = isActive
		snap.Balance = ownbalance
		snap.Delegated = a.DelegatedBalance
		snap.NDelegations = a.ActiveDelegations
		snap.Since = a.DelegateSince
		ins = append(ins, snap)
		rollOwners = append(rollOwners, a.Id.Value())
		return nil
	})
	if err != nil {
		return err
	}

	err = idx.table.Insert(ctx, ins)
	for _, v := range ins {
		v.(*Snapshot).Free()
	}
	ins = ins[:0]
	if err != nil {
		return err
	}

	// snapshot all delegating accounts with non-zero balance that reference one of the
	// roll owners
	err = pack.NewQuery("snapshot_delegators", table).
		WithoutCache().
		WithFields("I", "D", "s", "+", "U").
		AndEqual("is_funded", true).
		AndIn("delegate_id", rollOwners).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(a); err != nil {
				return err
			}
			// skip all self-delegations because the're already handled above
			if a.Id == a.DelegateId {
				return nil
			}
			snap := NewSnapshot()
			snap.Height = block.Height
			snap.Cycle = block.Cycle
			snap.Timestamp = block.Timestamp
			snap.Index = sn
			snap.Rolls = 0
			snap.AccountId = a.Id
			snap.DelegateId = a.DelegateId
			snap.IsDelegate = false
			snap.IsActive = false
			snap.Balance = a.SpendableBalance
			snap.Delegated = 0
			snap.NDelegations = 0
			snap.Since = a.DelegatedSince
			ins = append(ins, snap)
			return nil
		})
	if err != nil {
		return err
	}
	err = idx.table.Insert(ctx, ins)
	for _, v := range ins {
		v.(*Snapshot).Free()
	}
	return err
}

func (idx *SnapshotIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	// skip non-snapshot blocks
	if block.Height == 0 || block.Height%block.Params.BlocksPerRollSnapshot != 0 {
		return nil
	}
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *SnapshotIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting snapshots at height %d", height)
	_, err := pack.NewQuery("etl.snapshot.delete", idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *SnapshotIndex) UpdateCycleSnapshot(ctx context.Context, block *Block) error {
	// update all snapshot rows at snapshot cycle & index
	snap := block.TZ.Snapshot
	// fetch all rows from table, updating contents and removing all non-required
	// snapshot rows to save table space; keep bakers in the last snapshot in a cycle
	// for governance purposes
	keepIndex := block.Params.MaxSnapshotIndex()
	upd := make([]pack.Item, 0)
	del := make([]uint64, 0)
	err := pack.NewQuery("snapshot.update", idx.table).
		WithoutCache().
		AndEqual("cycle", snap.Cycle-(block.Params.PreservedCycles+2)). // adjust to source snapshot cycle
		Stream(ctx, func(r pack.Row) error {
			s := NewSnapshot()
			if err := r.Decode(s); err != nil {
				return err
			}
			if s.Index == snap.RollSnapshot {
				s.IsSelected = true
				upd = append(upd, s)
			} else if s.Index != keepIndex || !s.IsDelegate {
				del = append(del, s.RowId)
				s.Free()
			}
			return nil
		})
	if err != nil {
		return err
	}
	// store update
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}
	log.Debugf("Snapshot: deleting %d unused snapshots from cycle %d at block %d [%d]",
		len(del), block.Cycle-1, block.Height, block.Cycle)
	if err := idx.table.DeleteIds(ctx, del); err != nil {
		return err
	}
	// free allocations
	for _, v := range upd {
		v.(*Snapshot).Free()
	}
	// compact
	if err := idx.table.Compact(ctx); err != nil {
		return err
	}
	return nil
}
