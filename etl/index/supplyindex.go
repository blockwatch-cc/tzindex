// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/etl/model"
)

var (
	SupplyPackSizeLog2    = 15 // 32k
	SupplyJournalSizeLog2 = 16 // 64k - can be big, no search required
	SupplyCacheSize       = 2
	SupplyFillLevel       = 100
	SupplyIndexKey        = "supply"
	SupplyTableKey        = "supply"
)

var (
	// ErrNoSupplyEntry is an error that indicates a requested entry does
	// not exist in the supply table.
	ErrNoSupplyEntry = errors.New("supply state not found")
)

type SupplyIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
}

var _ model.BlockIndexer = (*SupplyIndex)(nil)

func NewSupplyIndex(opts pack.Options) *SupplyIndex {
	return &SupplyIndex{opts: opts}
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
	fields, err := pack.Fields(model.Supply{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		SupplyTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, SupplyPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, SupplyJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, SupplyCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, SupplyFillLevel),
		})
	return err
}

func (idx *SupplyIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(SupplyTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, SupplyJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, SupplyCacheSize),
	})
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
	_, err := pack.NewQuery("etl.supply.delete", idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *SupplyIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting supply for cycle %d", cycle)
	_, err := pack.NewQuery("etl.supply.delete", idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *SupplyIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
