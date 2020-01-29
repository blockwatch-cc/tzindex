// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	. "blockwatch.cc/tzindex/etl/model"
)

var (
	ChainPackSizeLog2    = 15 // 32k packs
	ChainJournalSizeLog2 = 16 // 65k - can be big, no search required
	ChainCacheSize       = 2
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

var _ BlockIndexer = (*ChainIndex)(nil)

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
	fields, err := pack.Fields(Chain{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
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

func (idx *ChainIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s table: %v", idx.Name(), err)
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

func (idx *ChainIndex) ConnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	return idx.table.Insert(ctx, []pack.Item{block.Chain})
}

func (idx *ChainIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ChainIndex) DeleteBlock(ctx context.Context, height int64) error {
	log.Debugf("Rollback deleting chain state at height %d", height)
	q := pack.Query{
		Name: "etl.chain.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("h"), // block height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}
