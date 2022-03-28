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
	OpPackSizeLog2         = 15 // 32k packs
	OpJournalSizeLog2      = 16 // 64k
	OpCacheSize            = 128
	OpFillLevel            = 100
	OpIndexPackSizeLog2    = 15 // 16k packs (32k split size)
	OpIndexJournalSizeLog2 = 16 // 64k
	OpIndexCacheSize       = 2  // minimum, not essential
	OpIndexFillLevel       = 90
	OpIndexKey             = "op"
	OpTableKey             = "op"
)

var (
	ErrNoOpEntry   = errors.New("op not indexed")
	ErrInvalidOpID = errors.New("invalid op id")
)

type OpIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ model.BlockIndexer = (*OpIndex)(nil)

func NewOpIndex(opts, iopts pack.Options) *OpIndex {
	return &OpIndex{opts: opts, iopts: iopts}
}

func (idx *OpIndex) DB() *pack.DB {
	return idx.db
}

func (idx *OpIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *OpIndex) Key() string {
	return OpIndexKey
}

func (idx *OpIndex) Name() string {
	return OpIndexKey + " index"
}

func (idx *OpIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(model.Op{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		OpTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, OpPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, OpJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, OpCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, OpFillLevel),
		})
	return err
}

func (idx *OpIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		OpTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, OpJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, OpCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, OpIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, OpIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *OpIndex) FinalizeSync(ctx context.Context) error {
	if idxs := idx.table.Indexes(); len(idxs) > 0 {
		return nil
	}
	index, err := idx.table.CreateIndex(
		"hash",
		idx.table.Fields().Find("H"), // op hash field (32 byte op hashes)
		pack.IndexTypeHash,           // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, OpIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, OpIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, OpIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, OpIndexFillLevel),
		})
	if err != nil {
		if err != pack.ErrIndexExists {
			return err
		}
		return nil
	}
	log.Infof("Building %s index ... (this may take some time)", idx.table.Name())

	progress := make(chan float64, 100)
	defer close(progress)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case f := <-progress:
				log.Infof("%s index build progress %.2f%%", idx.table.Name(), f)
				if f == 100 {
					return
				}
			}
		}
	}()

	return index.Reindex(ctx, 128, progress)
}

func (idx *OpIndex) Close() error {
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

func (idx *OpIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
	ops := make([]pack.Item, 0)
	for _, op := range block.Ops {
		switch op.Type {
		case model.OpTypeEndorsement, model.OpTypePreendorsement:
			// skip endorsement ops
			continue
		case model.OpTypeBake:
			// assign block fees to the bake operation (pre-Ithaca)
			if op.Fee == 0 {
				op.Fee = block.Fee
			}
			ops = append(ops, op)
		default:
			ops = append(ops, op)
		}
	}
	return idx.table.Insert(ctx, ops)
}

func (idx *OpIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *OpIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting ops at height %d", height)
	_, err := pack.NewQuery("etl.op.delete", idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *OpIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting op for cycle %d", cycle)
	var first, last int64
	type XBlock struct {
		Height int64 `pack:"height"`
	}
	var xb XBlock
	err := pack.NewQuery("etl.op.scan", idx.table).
		AndEqual("cycle", cycle).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(&xb); err != nil {
				return err
			}
			if first == 0 {
				first = xb.Height
			}
			last = util.Max64(last, xb.Height)
			return nil
		})
	if err != nil {
		return err
	}
	_, err = pack.NewQuery("etl.op.delete", idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *OpIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
