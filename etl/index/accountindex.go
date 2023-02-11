// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"

	"blockwatch.cc/tzindex/etl/model"
)

const (
	AccountPackSizeLog2         = 14 // =16k packs
	AccountJournalSizeLog2      = 17 // =128k
	AccountCacheSize            = 256
	AccountFillLevel            = 100
	AccountIndexPackSizeLog2    = 14 // =8k packs (16k split size)
	AccountIndexJournalSizeLog2 = 17 // 128k
	AccountIndexCacheSize       = 512
	AccountIndexFillLevel       = 90
	AccountIndexKey             = "account"
	AccountTableKey             = "account"
	BakerTableKey               = "baker"
)

var (
	ErrNoAccountEntry = errors.New("account not indexed")
	ErrNoBakerEntry   = errors.New("baker not indexed")
)

type AccountIndex struct {
	db       *pack.DB
	opts     pack.Options
	iopts    pack.Options
	accounts *pack.Table
	bakers   *pack.Table
}

var _ model.BlockIndexer = (*AccountIndex)(nil)

func NewAccountIndex(opts, iopts pack.Options) *AccountIndex {
	return &AccountIndex{opts: opts, iopts: iopts}
}

func (idx *AccountIndex) DB() *pack.DB {
	return idx.db
}

func (idx *AccountIndex) Tables() []*pack.Table {
	return []*pack.Table{
		idx.accounts,
		idx.bakers,
	}
}

func (idx *AccountIndex) Key() string {
	return AccountIndexKey
}

func (idx *AccountIndex) Name() string {
	return AccountIndexKey + " index"
}

func (idx *AccountIndex) Create(path, label string, opts interface{}) error {
	accountFields, err := pack.Fields(model.Account{})
	if err != nil {
		return err
	}
	bakerFields, err := pack.Fields(model.Baker{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	accounts, err := db.CreateTableIfNotExists(
		AccountTableKey,
		accountFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, AccountPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, AccountJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, AccountCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, AccountFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = accounts.CreateIndexIfNotExists(
		"address",
		accountFields.Find("H"), // account hash field (21 byte hashes)
		pack.IndexTypeHash,      // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, AccountIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, AccountIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, AccountIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, AccountIndexFillLevel),
		})
	if err != nil {
		return err
	}

	bakers, err := db.CreateTableIfNotExists(
		BakerTableKey,
		bakerFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, AccountPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, AccountJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, AccountCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, AccountFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = bakers.CreateIndexIfNotExists(
		"address",
		bakerFields.Find("H"), // account hash field (21 byte hashes)
		pack.IndexTypeHash,    // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, AccountIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, AccountIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, AccountIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, AccountIndexFillLevel),
		})
	if err != nil {
		return err
	}

	return nil
}

func (idx *AccountIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.accounts, err = idx.db.Table(
		AccountTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, AccountJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, AccountCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, AccountIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, AccountIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	idx.bakers, err = idx.db.Table(
		BakerTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, AccountJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, AccountCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, AccountIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, AccountIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *AccountIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *AccountIndex) Close() error {
	for _, v := range idx.Tables() {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", v.Name(), err)
			}
		}
	}
	idx.accounts = nil
	idx.bakers = nil
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *AccountIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	accupd := make([]pack.Item, 0)
	bkrupd := make([]pack.Item, 0)
	bkrins := make([]pack.Item, 0)
	// regular accounts
	for _, acc := range builder.Accounts() {
		if !acc.IsDirty {
			continue
		}
		accupd = append(accupd, acc)
	}
	// baker accounts
	for _, bkr := range builder.Bakers() {
		if bkr.Account.IsDirty {
			accupd = append(accupd, bkr.Account)
		}
		if bkr.IsNew {
			bkrins = append(bkrins, bkr)
		} else if bkr.IsDirty {
			bkrupd = append(bkrupd, bkr)
		}
	}
	// send to tables
	if len(accupd) > 0 {
		if err := idx.accounts.Update(ctx, accupd); err != nil {
			return err
		}
	}
	if len(bkrins) > 0 {
		if err := idx.bakers.Insert(ctx, bkrins); err != nil {
			return err
		}
	}
	if len(bkrupd) > 0 {
		if err := idx.bakers.Update(ctx, bkrupd); err != nil {
			return err
		}
	}
	return nil
}

func (idx *AccountIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// accounts to delete
	accdel := make([]uint64, 0)
	bkrdel := make([]uint64, 0)
	// accounts to update
	accupd := make([]pack.Item, 0)
	bkrupd := make([]pack.Item, 0)

	// regular accounts
	for _, acc := range builder.Accounts() {
		if acc.MustDelete {
			accdel = append(accdel, acc.RowId.Value())
		} else if acc.IsDirty {
			accupd = append(accupd, acc)
		}
	}

	// delegate accounts
	for _, bkr := range builder.Bakers() {
		if bkr.Account.MustDelete {
			accdel = append(accdel, bkr.Account.RowId.Value())
			bkrdel = append(bkrdel, bkr.RowId.Value())
		} else if bkr.Account.IsDirty {
			accupd = append(accupd, bkr.Account)
		}
		if bkr.IsDirty {
			bkrupd = append(bkrupd, bkr)
		}
	}

	if len(accdel) > 0 {
		// remove duplicates and sort; returns new slice
		accdel = vec.UniqueUint64Slice(accdel)
		// log.Debugf("Rollback removing accounts %#v", del)
		if err := idx.accounts.DeleteIds(ctx, accdel); err != nil {
			return err
		}
	}
	if len(bkrdel) > 0 {
		// remove duplicates and sort; returns new slice
		bkrdel = vec.UniqueUint64Slice(bkrdel)
		// log.Debugf("Rollback removing accounts %#v", del)
		if err := idx.bakers.DeleteIds(ctx, bkrdel); err != nil {
			return err
		}
	}

	// Note on rebuild:
	// we don't rebuild last in/out counters since we assume
	// after reorg completes these counters are set properly again

	// update accounts
	if len(accupd) > 0 {
		if err := idx.accounts.Update(ctx, accupd); err != nil {
			return err
		}
	}
	// update bakers
	if len(bkrupd) > 0 {
		if err := idx.bakers.Update(ctx, bkrupd); err != nil {
			return err
		}
	}
	return nil
}

func (idx *AccountIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting accounts at height %d", height)
	_, err := pack.NewQuery("etl.account.delete").
		WithTable(idx.accounts).
		AndEqual("first_seen", height).
		Delete(ctx)
	if err != nil {
		return err
	}
	_, err = pack.NewQuery("etl.baker.delete").
		WithTable(idx.bakers).
		AndEqual("baker_since", height).
		Delete(ctx)
	return err
}

func (idx *AccountIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *AccountIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
