// Copyright (c) 2020 Blockwatch Data Inc.
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
	SnapshotPackSizeLog2    = 15 // =32k packs
	SnapshotJournalSizeLog2 = 17 // =128k entries
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
	// for governance, snapshot 15 (at end of cycle == start of voting period is used)
	sn := ((block.Height - block.Params.BlocksPerRollSnapshot) % block.Params.BlocksPerCycle) / block.Params.BlocksPerRollSnapshot

	// snapshot all currently funded accounts and delegates
	table, err := builder.Table(AccountTableKey)
	if err != nil {
		return err
	}

	// snapshot all active delegates with at least 1 roll
	q := pack.Query{
		Name:    "snapshot.accounts",
		NoCache: true,
		Fields:  table.Fields().Select("I", "D", "d", "v", "s", "z", "Y", "~", "a", "*"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("v"), // is active delegate
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
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
		DelegatedSince    int64     `pack:"+"`
		DelegateSince     int64     `pack:"*"`
		UnclaimedBalance  int64     `pack:"U"`
		IsVesting         bool      `pack:"V"`
	}
	a := &XAccount{}
	rollOwners := make([]uint64, 0, block.Chain.RollOwners)
	ins := make([]pack.Item, 0, int(block.Chain.FundedAccounts)) // hint
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(a); err != nil {
			return err
		}
		// check account owns at least one roll
		ownbalance := a.SpendableBalance + a.FrozenDeposits + a.FrozenFees
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
		snap.IsActive = a.IsActiveDelegate
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
	q = pack.Query{
		Name:    "snapshot.accounts",
		NoCache: true,
		Fields:  table.Fields().Select("I", "D", "s", "+", "U", "V"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("f"), // is funded
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
			pack.Condition{
				Field: table.Fields().Find("D"), // delegates to a roll owner
				Mode:  pack.FilterModeIn,
				Value: rollOwners,
			},
		},
	}
	err = table.Stream(ctx, q, func(r pack.Row) error {
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
		if a.IsVesting {
			snap.Balance += a.UnclaimedBalance
		}
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
	log.Debugf("Rollback deleting snapshots at height %d", height)
	q := pack.Query{
		Name: "etl.snapshot.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("h"),
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}

func (idx *SnapshotIndex) UpdateCycleSnapshot(ctx context.Context, block *Block) error {
	// update all snapshot rows at snapshot cycle & index
	snap := block.TZ.Snapshot
	q := pack.Query{
		Name:    "snapshot.update",
		NoCache: true,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.table.Fields().Find("c"), // cycle
				Mode:  pack.FilterModeEqual,
				Value: snap.Cycle - (block.Params.PreservedCycles + 2), // adjust to source snapshot cycle
			},
			pack.Condition{
				Field: idx.table.Fields().Find("i"), // index
				Mode:  pack.FilterModeEqual,
				Value: snap.RollSnapshot, // the selected index
			},
		},
	}
	// fetch all rows from table, updating contents
	rows := make([]pack.Item, 0, 1024)
	err := idx.table.Stream(ctx, q, func(r pack.Row) error {
		s := NewSnapshot()
		if err := r.Decode(s); err != nil {
			return err
		}
		s.IsSelected = true
		rows = append(rows, s)
		return nil
	})
	if err != nil {
		return err
	}
	// store update
	if err := idx.table.Update(ctx, rows); err != nil {
		return err
	}

	// FIXME: consider flushing the table for sorted results after update
	// if this becomes a problem

	// free allocations
	for _, v := range rows {
		v.(*Snapshot).Free()
	}
	return nil
}
