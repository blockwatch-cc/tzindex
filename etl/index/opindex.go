// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzgo/tezos"
	. "blockwatch.cc/tzindex/etl/model"
)

const (
	OpPackSizeLog2         = 15  // 32k packs ~4M
	OpJournalSizeLog2      = 16  // 64k
	OpCacheSize            = 128 // 128=512MB
	OpFillLevel            = 100
	OpIndexPackSizeLog2    = 15 // 16k packs (32k split size) ~256k
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

var _ BlockIndexer = (*OpIndex)(nil)

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
	fields, err := pack.Fields(Op{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
	}
	defer db.Close()

	table, err := db.CreateTableIfNotExists(
		OpTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, OpPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, OpJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, OpCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, OpFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // op hash field (32 byte op hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, OpIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, OpIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, OpIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, OpIndexFillLevel),
		})
	if err != nil {
		return err
	}

	return nil
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

func (idx *OpIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s table: %v", idx.Key(), err)
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

func (idx *OpIndex) ConnectBlock(ctx context.Context, block *Block, b BlockBuilder) error {
	ops := make([]pack.Item, 0, len(block.Ops))
	for i, op := range block.Ops {
		// skip all consensus-related ops in light mode
		if b.IsLightMode() {
			switch op.Type {
			case tezos.OpTypeBake,
				tezos.OpTypeDoubleBakingEvidence,
				tezos.OpTypeDoubleEndorsementEvidence,
				tezos.OpTypeSeedNonceRevelation,
				tezos.OpTypeEndorsement,
				tezos.OpTypeProposals,
				tezos.OpTypeBallot,
				tezos.OpTypeUnfreeze,
				tezos.OpTypeInvoice,
				tezos.OpTypeSeedSlash:
				continue
			}
		}
		// assign block fees to implicit baker operation
		if op.Type == tezos.OpTypeBake {
			op.Fee = block.Fee
		}
		// set batch flag if op_c > 0 or next op list+pos are the same
		op.IsBatch = op.OpC > 0
		if i < len(block.Ops)-1 {
			next := block.Ops[i+1]
			op.IsBatch = op.IsBatch || (next.OpL == op.OpL && next.OpP == op.OpP)
		}
		ops = append(ops, op)
	}
	return idx.table.Insert(ctx, ops)
}

func (idx *OpIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
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
	_, err := pack.NewQuery("etl.op.delete", idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}
