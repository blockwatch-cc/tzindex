// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
)

const MetadataIndexKey = "metadata"

type MetadataIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*MetadataIndex)(nil)

func NewMetadataIndex() *MetadataIndex {
	return &MetadataIndex{}
}

func (idx *MetadataIndex) DB() *pack.DB {
	return idx.db
}

func (idx *MetadataIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *MetadataIndex) Key() string {
	return MetadataIndexKey
}

func (idx *MetadataIndex) Name() string {
	return MetadataIndexKey + " index"
}

func (idx *MetadataIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Metadata{}
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

func (idx *MetadataIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Metadata{}
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

func (idx *MetadataIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *MetadataIndex) Close() error {
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

func (idx *MetadataIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DeleteBlock(ctx context.Context, height int64) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// noop
	return nil
}

func (idx *MetadataIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}
