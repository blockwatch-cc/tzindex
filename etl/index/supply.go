// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

const SupplyIndexKey = "supply"

type SupplyIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*SupplyIndex)(nil)

func NewSupplyIndex() *SupplyIndex {
	return &SupplyIndex{}
}

func (idx *SupplyIndex) DB() *pack.DB {
	return idx.db
}

func (idx *SupplyIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *SupplyIndex) Key() string {
	return SupplyIndexKey
}

func (idx *SupplyIndex) Name() string {
	return SupplyIndexKey + " index"
}

func (idx *SupplyIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Supply{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *SupplyIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Supply{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *SupplyIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *SupplyIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s table: %s", idx.Name(), err)
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

func (idx *SupplyIndex) ConnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.table.Insert(ctx, []pack.Item{block.Supply})
}

func (idx *SupplyIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *SupplyIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting supply state at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *SupplyIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting supply for cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *SupplyIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *SupplyIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
