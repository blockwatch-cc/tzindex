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

const StorageIndexKey = "storage"

var MaxStorageEntrySize = 0 // skip data larger than this, 0 = disable

type StorageIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*StorageIndex)(nil)

func NewStorageIndex() *StorageIndex {
	return &StorageIndex{}
}

func (idx *StorageIndex) DB() *pack.DB {
	return idx.db
}

func (idx *StorageIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *StorageIndex) Key() string {
	return StorageIndexKey
}

func (idx *StorageIndex) Name() string {
	return StorageIndexKey + " index"
}

func (idx *StorageIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Storage{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *StorageIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Storage{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *StorageIndex) FinalizeSync(ctx context.Context) error {
	return nil
}

func (idx *StorageIndex) Close() error {
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

func (idx *StorageIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
	ins := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsStorageUpdate || !op.IsSuccess {
			continue
		}

		// Note: empirical study suggests this is not necessary (<5% saving on small data)
		// at +50% indexing costs due to uniqueness lookups)
		//
		// check if storage already exists, skip originations
		// if op.Type == model.OpTypeTransaction {
		//     if ok, err := idx.IsExist(ctx, cc.AccountId, op.StorageHash, cc.FirstSeen, cc.LastSeen); err != nil {
		//         return err
		//     } else if ok {
		//         continue
		//     }
		// }
		if l := len(op.Storage); MaxStorageEntrySize > 0 && l > MaxStorageEntrySize {
			// try snappy encoded size
			// buf := snappy.Encode(nil, op.Storage)
			// log.Warnf("%d %s storage update too large (%d bytes, %d compressed), dropping", op.Height, op.Contract, l, len(buf))
			log.Warnf("%d %s storage update too large (%d bytes), dropping", op.Height, op.Contract, l)
			continue
		}

		store := &model.Storage{
			AccountId: op.Contract.AccountId,
			Hash:      op.StorageHash,
			Height:    block.Height,
			Storage:   op.Storage,
		}
		ins = append(ins, store)
	}

	// insert, will generate unique row ids
	return idx.table.Insert(ctx, ins)
}

func (idx *StorageIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *StorageIndex) DeleteBlock(ctx context.Context, height int64) error {
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *StorageIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *StorageIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *StorageIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
