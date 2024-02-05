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

const BalanceIndexKey = "balance"

type BalanceIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*BalanceIndex)(nil)

func NewBalanceIndex() *BalanceIndex {
	return &BalanceIndex{}
}

func (idx *BalanceIndex) DB() *pack.DB {
	return idx.db
}

func (idx *BalanceIndex) Tables() []*pack.Table {
	return []*pack.Table{
		idx.table,
	}
}

func (idx *BalanceIndex) Key() string {
	return BalanceIndexKey
}

func (idx *BalanceIndex) Name() string {
	return BalanceIndexKey + " index"
}

func (idx *BalanceIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Balance{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return err
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *BalanceIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Balance{}
	key := m.TableKey()
	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *BalanceIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *BalanceIndex) Close() error {
	for _, v := range idx.Tables() {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", v.Name(), err)
			}
		}
	}
	idx.table = nil
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *BalanceIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	ins := make([]pack.Item, 0)

	// handle bakers
	for _, bkr := range builder.Bakers() {
		acc := bkr.Account
		if !acc.IsDirty || acc.PrevBalance == acc.Balance() {
			continue
		}
		bal := &model.Balance{
			AccountId: acc.RowId,
			Balance:   acc.Balance(),
			ValidFrom: block.Height,
		}
		ins = append(ins, bal)
	}

	// handle regular accounts
	for _, acc := range builder.Accounts() {
		if !acc.IsDirty || acc.PrevBalance == acc.Balance() {
			continue
		}
		bal := &model.Balance{
			AccountId: acc.RowId,
			Balance:   acc.Balance(),
			ValidFrom: block.Height,
		}
		ins = append(ins, bal)
	}

	// insert sorted values
	sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Balance).AccountId < ins[j].(*model.Balance).AccountId })
	return idx.table.Insert(ctx, ins)
}

func (idx *BalanceIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BalanceIndex) DeleteBlock(ctx context.Context, height int64) error {
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("valid_from", height).
		Delete(ctx)
	return err
}

func (idx *BalanceIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *BalanceIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *BalanceIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
