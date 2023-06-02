// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"

	"blockwatch.cc/tzindex/etl/model"
)

const AccountIndexKey = "account"

type AccountIndex struct {
	db     *pack.DB
	tables map[string]*pack.Table
}

var _ model.BlockIndexer = (*AccountIndex)(nil)

func NewAccountIndex() *AccountIndex {
	return &AccountIndex{
		tables: make(map[string]*pack.Table),
	}
}

func (idx *AccountIndex) DB() *pack.DB {
	return idx.db
}

func (idx *AccountIndex) Tables() []*pack.Table {
	t := []*pack.Table{}
	for _, v := range idx.tables {
		t = append(t, v)
	}
	return t
}

func (idx *AccountIndex) Key() string {
	return AccountIndexKey
}

func (idx *AccountIndex) Name() string {
	return AccountIndexKey + " index"
}

func (idx *AccountIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	for _, m := range []model.Model{
		model.Account{},
		model.Baker{},
	} {
		key := m.TableKey()
		fields, err := pack.Fields(m)
		if err != nil {
			return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
		}
		opts := m.TableOpts().Merge(readConfigOpts(key))
		table, err := db.CreateTableIfNotExists(key, fields, opts)
		if err != nil {
			return err
		}

		for _, f := range fields.Indexed() {
			ikey := f.Alias
			opts := m.IndexOpts(ikey).Merge(readConfigOpts(key, ikey+"_index"))
			if _, err := table.CreateIndexIfNotExists(ikey, f, pack.IndexTypeHash, opts); err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *AccountIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	for _, m := range []model.Model{
		model.Account{},
		model.Baker{},
	} {
		key := m.TableKey()
		fields, err := pack.Fields(m)
		if err != nil {
			return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
		}
		topts := m.TableOpts().Merge(readConfigOpts(key))
		var ikey string
		for _, v := range fields.Indexed() {
			ikey = v.Alias
			break
		}
		iopts := m.IndexOpts(ikey).Merge(readConfigOpts(key, ikey+"_index"))
		table, err := idx.db.Table(key, topts, iopts)
		if err != nil {
			idx.Close()
			return err
		}
		idx.tables[key] = table
	}

	return nil
}

func (idx *AccountIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *AccountIndex) Close() error {
	for n, v := range idx.tables {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", v.Name(), err)
			}
		}
		delete(idx.tables, n)
	}
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
	accounts := idx.tables[model.AccountTableKey]
	bakers := idx.tables[model.BakerTableKey]

	if len(accupd) > 0 {
		if err := accounts.Update(ctx, accupd); err != nil {
			return err
		}
	}
	if len(bkrins) > 0 {
		if err := bakers.Insert(ctx, bkrins); err != nil {
			return err
		}
	}
	if len(bkrupd) > 0 {
		if err := bakers.Update(ctx, bkrupd); err != nil {
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
			accdel = append(accdel, acc.RowId.U64())
		} else if acc.IsDirty {
			accupd = append(accupd, acc)
		}
	}

	// delegate accounts
	for _, bkr := range builder.Bakers() {
		if bkr.Account.MustDelete {
			accdel = append(accdel, bkr.Account.RowId.U64())
			bkrdel = append(bkrdel, bkr.RowId.U64())
		} else if bkr.Account.IsDirty {
			accupd = append(accupd, bkr.Account)
		}
		if bkr.IsDirty {
			bkrupd = append(bkrupd, bkr)
		}
	}

	accounts := idx.tables[model.AccountTableKey]
	bakers := idx.tables[model.BakerTableKey]

	if len(accdel) > 0 {
		// remove duplicates and sort; returns new slice
		accdel = vec.UniqueUint64Slice(accdel)
		if err := accounts.DeleteIds(ctx, accdel); err != nil {
			return err
		}
	}
	if len(bkrdel) > 0 {
		// remove duplicates and sort; returns new slice
		bkrdel = vec.UniqueUint64Slice(bkrdel)
		if err := bakers.DeleteIds(ctx, bkrdel); err != nil {
			return err
		}
	}

	// Note on rebuild:
	// we don't rebuild last in/out counters since we assume
	// after reorg completes these counters are set properly again

	// update accounts
	if len(accupd) > 0 {
		if err := accounts.Update(ctx, accupd); err != nil {
			return err
		}
	}
	// update bakers
	if len(bkrupd) > 0 {
		if err := bakers.Update(ctx, bkrupd); err != nil {
			return err
		}
	}
	return nil
}

func (idx *AccountIndex) DeleteBlock(ctx context.Context, height int64) error {
	accounts := idx.tables[model.AccountTableKey]
	bakers := idx.tables[model.BakerTableKey]

	_, err := pack.NewQuery("etl.delete").
		WithTable(accounts).
		AndEqual("first_seen", height).
		Delete(ctx)
	if err != nil {
		return err
	}
	_, err = pack.NewQuery("etl.delete").
		WithTable(bakers).
		AndEqual("baker_since", height).
		Delete(ctx)
	return err
}

func (idx *AccountIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *AccountIndex) Flush(ctx context.Context) error {
	for n, v := range idx.tables {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", n, err)
		}
	}
	return nil
}
