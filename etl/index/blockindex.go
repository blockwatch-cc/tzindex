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

const (
	BlockPackSizeLog2         = 15 // 32k packs
	BlockJournalSizeLog2      = 16 // 64k - search only on rollback and lookup
	BlockCacheSize            = 2
	BlockFillLevel            = 100
	BlockIndexPackSizeLog2    = 15 // 16k packs (32k split size)
	BlockIndexJournalSizeLog2 = 16 // 64k
	BlockIndexCacheSize       = 2  // not essential
	BlockIndexFillLevel       = 90
	BlockIndexKey             = "block"
	BlockTableKey             = "block"
)

var (
	// ErrNoBlockEntry is an error that indicates a requested entry does
	// not exist in the block bucket.
	ErrNoBlockEntry = errors.New("block not indexed")

	// ErrInvalidBlockHeight
	ErrInvalidBlockHeight = errors.New("invalid block height")

	// ErrInvalidBlockHash
	ErrInvalidBlockHash = errors.New("invalid block hash")
)

type BlockIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ model.BlockIndexer = (*BlockIndex)(nil)

func NewBlockIndex(opts, iopts pack.Options) *BlockIndex {
	return &BlockIndex{opts: opts, iopts: iopts}
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
	fields, err := pack.Fields(model.Block{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		BlockTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BlockPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BlockJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BlockCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, BlockFillLevel),
		})
	if err != nil {
		return err
	}
	return nil
}

func (idx *BlockIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		BlockTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BlockJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BlockCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, BlockIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, BlockIndexCacheSize),
		},
	)
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *BlockIndex) FinalizeSync(ctx context.Context) error {
	if idxs := idx.table.Indexes(); len(idxs) > 0 {
		return nil
	}
	index, err := idx.table.CreateIndex(
		"hash",
		idx.table.Fields().Find("H"), // any type
		pack.IndexTypeHash,           // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, BlockIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, BlockIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, BlockIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, BlockIndexFillLevel),
		})
	if err != nil {
		if err != pack.ErrIndexExists {
			return err
		}
		return nil
	}
	log.Infof("Building %s ... (this may take some time)", idx.Name())

	progress := make(chan float64, 100)
	defer close(progress)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case f := <-progress:
				log.Infof("%s build progress %.2f%%", idx.Name(), f)
				if f == 100 {
					return
				}
			}
		}
	}()

	return index.Reindex(ctx, 128, progress)
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
	_, err := pack.NewQuery("etl.block.delete", idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *BlockIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting block cycle %d", cycle)
	_, err := pack.NewQuery("etl.block.delete", idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *BlockIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
