// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
)

const FlowIndexKey = "flow"

type FlowIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*FlowIndex)(nil)

func NewFlowIndex() *FlowIndex {
	return &FlowIndex{}
}

func (idx *FlowIndex) DB() *pack.DB {
	return idx.db
}

func (idx *FlowIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *FlowIndex) Key() string {
	return FlowIndexKey
}

func (idx *FlowIndex) Name() string {
	return FlowIndexKey + " index"
}

func (idx *FlowIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Flow{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(readConfigOpts(key)))
	return err
}

func (idx *FlowIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Flow{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(readConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *FlowIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *FlowIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s table: %s", idx.Key(), err)
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

func (idx *FlowIndex) ConnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	flows := make([]pack.Item, 0, len(block.Flows))
	for _, f := range block.Flows {
		flows = append(flows, f)
	}
	return idx.table.Insert(ctx, flows)
}

func (idx *FlowIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *FlowIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting flows at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *FlowIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting flow cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *FlowIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}
