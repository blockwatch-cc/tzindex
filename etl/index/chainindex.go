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
	ChainPackSizeLog2    = 15 // 32k packs ~3.6M
	ChainJournalSizeLog2 = 16 // 64k - can be big, no search required
	ChainCacheSize       = 2  // ~1M
	ChainFillLevel       = 100
	ChainIndexKey        = "chain"
	ChainTableKey        = "chain"
)

var (
	// ErrNoChainEntry is an error that indicates a requested entry does
	// not exist in the chain table.
	ErrNoChainEntry = errors.New("chain state not found")
)

type ChainIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
}

var _ model.BlockIndexer = (*ChainIndex)(nil)

func NewChainIndex(opts pack.Options) *ChainIndex {
	return &ChainIndex{opts: opts}
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
	fields, err := pack.Fields(model.Chain{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		ChainTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, ChainPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, ChainJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, ChainCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, ChainFillLevel),
		})
	return err
}

func (idx *ChainIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(ChainTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, ChainJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, ChainCacheSize),
	})
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
	_, err := pack.NewQuery("etl.chain.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *ChainIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting chain cycle %d", cycle)
	_, err := pack.NewQuery("etl.chain.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *ChainIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
