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

const ChainIndexKey = "chain"

type ChainIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*ChainIndex)(nil)

func NewChainIndex() *ChainIndex {
	return &ChainIndex{}
}

func (idx *ChainIndex) DB() *pack.DB {
	return idx.db
}

func (idx *ChainIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *ChainIndex) Key() string {
	return ChainIndexKey
}

func (idx *ChainIndex) Name() string {
	return ChainIndexKey + " index"
}

func (idx *ChainIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Chain{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *ChainIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Chain{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *ChainIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *ChainIndex) Close() error {
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

func (idx *ChainIndex) ConnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.table.Insert(ctx, []pack.Item{block.Chain})
}

func (idx *ChainIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ChainIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting chain state at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *ChainIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting chain cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *ChainIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *ChainIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
