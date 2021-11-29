// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	. "blockwatch.cc/tzindex/etl/model"
)

const (
	ConstantPackSizeLog2         = 12 // 4k packs
	ConstantJournalSizeLog2      = 13 // 8k
	ConstantCacheSize            = 2  // minimum
	ConstantFillLevel            = 100
	ConstantIndexPackSizeLog2    = 15 // 16k packs (32k split size) ~256k
	ConstantIndexJournalSizeLog2 = 16 // 64k
	ConstantIndexCacheSize       = 2  // minimum
	ConstantIndexFillLevel       = 90
	ConstantIndexKey             = "constant"
	ConstantTableKey             = "constant"
)

var (
	ErrNoConstantEntry = errors.New("constant not indexed")
)

type ConstantIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*ConstantIndex)(nil)

func NewConstantIndex(opts, iopts pack.Options) *ConstantIndex {
	return &ConstantIndex{opts: opts, iopts: iopts}
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
	fields, err := pack.Fields(Constant{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	defer db.Close()

	table, err := db.CreateTableIfNotExists(
		ConstantTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, ConstantPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, ConstantJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, ConstantCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, ConstantFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // constant hash field (32 byte expr hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, ConstantIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, ConstantIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, ConstantIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, ConstantIndexFillLevel),
		})
	if err != nil {
		return err
	}

	return nil
}

func (idx *ConstantIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		ConstantTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, ConstantJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, ConstantCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, ConstantIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, ConstantIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
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

func (idx *ConstantIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	ins := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsSuccess || op.Type != tezos.OpTypeRegisterConstant {
			continue
		}

		o, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
		if !ok {
			return fmt.Errorf("constant: missing %s op [%d:%d]", op.Type, op.OpL, op.OpP)
		}
		gop, ok := o.(*rpc.ConstantRegistrationOp)
		if !ok {
			return fmt.Errorf("constant: %s op [%d:%d]: unexpected type %T",
				o.OpKind(), op.OpL, op.OpP, o)
		}
		ins = append(ins, NewConstant(gop, op))
	}

	if len(ins) > 0 {
		// insert, will generate unique row ids
		if err := idx.table.Insert(ctx, ins); err != nil {
			return fmt.Errorf("constant: insert: %w", err)
		}
	}

	return nil
}

func (idx *ConstantIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ConstantIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting contracts at height %d", height)
	_, err := pack.NewQuery("etl.constant.delete", idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *ConstantIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}
