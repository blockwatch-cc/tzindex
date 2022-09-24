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
	OpPackSizeLog2    = 15 // 32k packs
	OpJournalSizeLog2 = 16 // 64k
	OpCacheSize       = 128
	OpFillLevel       = 100
	OpIndexKey        = "op"
	OpTableKey        = "op"

	EndorseOpPackSizeLog2    = 15 // 32k packs
	EndorseOpJournalSizeLog2 = 16 // 64k
	EndorseOpCacheSize       = 2  // minimum, not essential
	EndorseOpFillLevel       = 100
	EndorseOpTableKey        = "endorsement"
)

var (
	ErrNoOpEntry          = errors.New("op not indexed")
	ErrNoEndorsemebtEntry = errors.New("endorsement not indexed")
	ErrInvalidOpID        = errors.New("invalid op id")
)

type OpIndex struct {
	db      *pack.DB
	opts    pack.Options
	table   *pack.Table
	endorse *pack.Table // separate table, must query explicitly
}

var _ model.BlockIndexer = (*OpIndex)(nil)

func NewOpIndex(opts pack.Options) *OpIndex {
	return &OpIndex{opts: opts}
}

func (idx *OpIndex) DB() *pack.DB {
	return idx.db
}

func (idx *OpIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table, idx.endorse}
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
	if err != nil {
		return err
	}

	fields, err = pack.Fields(model.Endorsement{})
	if err != nil {
		return err
	}
	_, err = db.CreateTableIfNotExists(
		EndorseOpTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    EndorseOpPackSizeLog2,
			JournalSizeLog2: EndorseOpJournalSizeLog2,
			CacheSize:       EndorseOpCacheSize,
			FillLevel:       EndorseOpFillLevel,
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
		})
	if err != nil {
		idx.Close()
		return err
	}
	idx.endorse, err = idx.db.Table(
		EndorseOpTableKey,
		pack.Options{
			JournalSizeLog2: EndorseOpJournalSizeLog2,
			CacheSize:       EndorseOpCacheSize,
		})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *OpIndex) FinalizeSync(ctx context.Context) error {
	return nil
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
	idx.endorse = nil
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
	endorse := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// skip all consensus-related ops in light mode
		if b.IsLightMode() {
			switch op.Type {
			case model.OpTypeBake,
				model.OpTypeDoubleBaking,
				model.OpTypeDoubleEndorsement,
				model.OpTypeDoublePreendorsement,
				model.OpTypeNonceRevelation,
				model.OpTypeEndorsement,
				model.OpTypePreendorsement,
				model.OpTypeProposal,
				model.OpTypeBallot,
				model.OpTypeUnfreeze,
				model.OpTypeSeedSlash,
				model.OpTypeDeposit,
				model.OpTypeBonus,
				model.OpTypeReward,
				model.OpTypeDepositsLimit,
				model.OpTypeVdfRevelation:
				continue
			}
		}
		switch op.Type {
		case model.OpTypeEndorsement, model.OpTypePreendorsement:
			endorse = append(endorse, op.ToEndorsement())
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
	if len(endorse) > 0 {
		if err := idx.endorse.Insert(ctx, endorse); err != nil {
			return err
		}
		// assign op ids back to the original ops
		for _, v := range endorse {
			ed := v.(*model.Endorsement)
			block.Ops[ed.OpN].RowId = ed.RowId
		}
	}
	return idx.table.Insert(ctx, ops)
}

func (idx *OpIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *OpIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting ops at height %d", height)
	_, err := pack.NewQuery("etl.op.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}
	_, err = pack.NewQuery("etl.endorse.delete").
		WithTable(idx.endorse).
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
	err := pack.NewQuery("etl.op.scan").
		WithTable(idx.table).
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
	_, err = pack.NewQuery("etl.op.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	if err != nil {
		return err
	}
	if first > 0 && last >= first {
		_, err = pack.NewQuery("etl.endorse.delete").
			WithTable(idx.endorse).
			AndRange("height", first, last).
			Delete(ctx)
	}
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
