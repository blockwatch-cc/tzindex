// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

const ConstantIndexKey = "constant"

type ConstantIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*ConstantIndex)(nil)

func NewConstantIndex() *ConstantIndex {
	return &ConstantIndex{}
}

func (idx *ConstantIndex) DB() *pack.DB {
	return idx.db
}

func (idx *ConstantIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *ConstantIndex) Key() string {
	return ConstantIndexKey
}

func (idx *ConstantIndex) Name() string {
	return ConstantIndexKey + " index"
}

func (idx *ConstantIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	defer db.Close()

	m := model.Constant{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	table, err := db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(readConfigOpts(key)))
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
	return nil
}

func (idx *ConstantIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Constant{}
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
	idx.table = table

	return nil
}

func (idx *ConstantIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *ConstantIndex) Close() error {
	if idx.table != nil {
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

func (idx *ConstantIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	ins := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsSuccess || op.Type != model.OpTypeRegisterConstant {
			continue
		}
		gop, ok := op.Raw.(*rpc.ConstantRegistration)
		if !ok {
			return fmt.Errorf("constant: %s op [%d:%d]: unexpected type %T",
				op.Raw.Kind(), op.Type.ListId(), op.OpP, op.Raw)
		}
		ins = append(ins, model.NewConstant(gop, op))
	}

	if len(ins) > 0 {
		// insert, will generate unique row ids
		if err := idx.table.Insert(ctx, ins); err != nil {
			return fmt.Errorf("constant: insert: %w", err)
		}
	}

	return nil
}

func (idx *ConstantIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ConstantIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting contracts at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *ConstantIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *ConstantIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}
