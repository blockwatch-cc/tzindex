// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"

	. "blockwatch.cc/tzindex/etl/model"
)

const (
	AccountPackSizeLog2         = 15 // =32k packs
	AccountJournalSizeLog2      = 16 // =64k entries
	AccountCacheSize            = 4
	AccountFillLevel            = 100
	AccountIndexPackSizeLog2    = 15 // =16k packs (32k split size) ~256k unpacked
	AccountIndexJournalSizeLog2 = 16 // 64k
	AccountIndexCacheSize       = 8
	AccountIndexFillLevel       = 90
	AccountIndexKey             = "account"
	AccountTableKey             = "account"
)

var (
	ErrNoAccountEntry = errors.New("account not indexed")
)

type AccountIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*AccountIndex)(nil)

func NewAccountIndex(opts, iopts pack.Options) *AccountIndex {
	return &AccountIndex{opts: opts, iopts: iopts}
}

func (idx *AccountIndex) DB() *pack.DB {
	return idx.db
}

func (idx *AccountIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *AccountIndex) Key() string {
	return AccountIndexKey
}

func (idx *AccountIndex) Name() string {
	return AccountIndexKey + " index"
}

func (idx *AccountIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(Account{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
	}
	defer db.Close()

	table, err := db.CreateTableIfNotExists(
		AccountTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, AccountPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, AccountJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, AccountCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, AccountFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // account hash field (20/32 byte hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
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
	idx.table, err = idx.db.Table(
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
	return nil
}

func (idx *AccountIndex) Close() error {
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

func (idx *AccountIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	upd := make([]pack.Item, 0, len(builder.Accounts()))
	// regular accounts
	for _, acc := range builder.Accounts() {
		if acc.IsDirty {
			upd = append(upd, acc)
		}
	}
	// delegate accounts
	for _, acc := range builder.Delegates() {
		if acc.IsDirty {
			upd = append(upd, acc)
		}
	}
	return idx.table.Update(ctx, upd)
}

func (idx *AccountIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// accounts to delete
	del := make([]uint64, 0)
	// accounts to update
	upd := make([]pack.Item, 0, len(builder.Accounts()))

	// regular accounts
	for _, acc := range builder.Accounts() {
		if acc.MustDelete {
			del = append(del, acc.RowId.Value())
		} else if acc.IsDirty {
			upd = append(upd, acc)
		}
	}

	// delegate accounts
	for _, acc := range builder.Delegates() {
		if acc.MustDelete {
			del = append(del, acc.RowId.Value())
		} else if acc.IsDirty {
			upd = append(upd, acc)
		}
	}

	if len(del) > 0 {
		// remove duplicates and sort; returns new slice
		del = vec.UniqueUint64Slice(del)
		log.Debugf("Rollback removing accounts %#v", del)
		if err := idx.table.DeleteIds(ctx, del); err != nil {
			return err
		}
	}

	// Note on rebuild:
	// we don't rebuild last in/out counters since we assume
	// after reorg completes these counters are set properly again

	// update accounts
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}
	return nil
}

func (idx *AccountIndex) DeleteBlock(ctx context.Context, height int64) error {
	log.Debugf("Rollback deleting accounts at height %d", height)
	q := pack.Query{
		Name: "etl.account.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("0"), // first seen height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}
