// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"

	"blockwatch.cc/tzindex/etl/model"
)

const BlockIndexKey = "block"

type BlockIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*BlockIndex)(nil)

func NewBlockIndex() *BlockIndex {
	return &BlockIndex{}
}

func (idx *BlockIndex) DB() *pack.DB {
	return idx.db
}

func (idx *BlockIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *BlockIndex) Key() string {
	return BlockIndexKey
}

func (idx *BlockIndex) Name() string {
	return BlockIndexKey + " index"
}

func (idx *BlockIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Block{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}
	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(readConfigOpts(key)))
	return err
}

func (idx *BlockIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Block{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(readConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *BlockIndex) FinalizeSync(ctx context.Context) error {
	return nil
}

func (idx *BlockIndex) Close() error {
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

func (idx *BlockIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
	// update parent block to write blocks endorsed bitmap
	if block.Parent != nil && block.Parent.RowId > 0 {
		if err := idx.table.Update(ctx, []pack.Item{block.Parent}); err != nil {
			return fmt.Errorf("parent update: %w", err)
		}
	}

	// fetch and update snapshot block
	if snap := block.TZ.Snapshot; snap != nil && block.Height > 1 {
		// protocol upgrades happen 1 block before cycle end. when changes to
		// cycle lenth happen, we miss snapshot 15
		p := block.Params
		if block.Parent != nil {
			p = block.Parent.Params
		}
		snapHeight := p.SnapshotBlock(snap.Cycle, snap.Index)
		log.Debugf("block: marking %d cycle %d index %d as roll snapshot for cycle %d",
			snapHeight, block.Params.HeightToCycle(snapHeight), snap.Index, snap.Cycle)
		snapBlock := &model.Block{}
		err := pack.NewQuery("etl.update").
			WithTable(idx.table).
			WithoutCache().
			WithLimit(1).
			AndEqual("height", snapHeight).
			Execute(ctx, snapBlock)
		if err != nil {
			return fmt.Errorf("snapshot index block %d for cycle %d index %d: %w", snapHeight, snap.Cycle, snap.Index, err)
		}
		if snapBlock.RowId == 0 {
			return fmt.Errorf("missing snapshot index block %d for cycle %d index %d", snapHeight, snap.Cycle, snap.Index)
		}
		snapBlock.IsCycleSnapshot = true
		if err := idx.table.Update(ctx, []pack.Item{snapBlock}); err != nil {
			return fmt.Errorf("snapshot index block %d: %w", snapHeight, err)
		}
	}

	// Note: during reorg some blocks may already exist (have a valid row id)
	// we assume insert will update such rows instead of creating new rows
	return idx.table.Insert(ctx, []pack.Item{block})
}

func (idx *BlockIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	// parent update will be done on next connect
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BlockIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting block at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *BlockIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting block cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *BlockIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}
