// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
)

const OpIndexKey = "op"

type OpIndex struct {
	db     *pack.DB
	tables map[string]*pack.Table
}

var _ model.BlockIndexer = (*OpIndex)(nil)

func NewOpIndex() *OpIndex {
	return &OpIndex{
		tables: make(map[string]*pack.Table),
	}
}

func (idx *OpIndex) DB() *pack.DB {
	return idx.db
}

func (idx *OpIndex) Tables() []*pack.Table {
	t := []*pack.Table{}
	for _, v := range idx.tables {
		t = append(t, v)
	}
	return t
}

func (idx *OpIndex) Key() string {
	return OpIndexKey
}

func (idx *OpIndex) Name() string {
	return OpIndexKey + " index"
}

func (idx *OpIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	for _, m := range []model.Model{
		model.Op{},
		model.Endorsement{},
	} {
		key := m.TableKey()
		fields, err := pack.Fields(m)
		if err != nil {
			return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
		}
		opts := m.TableOpts().Merge(readConfigOpts(key))
		_, err = db.CreateTableIfNotExists(key, fields, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *OpIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	for _, m := range []model.Model{
		model.Op{},
		model.Endorsement{},
	} {
		key := m.TableKey()
		t, err := idx.db.Table(key, m.TableOpts().Merge(readConfigOpts(key)))
		if err != nil {
			idx.Close()
			return err
		}
		idx.tables[key] = t
	}
	return nil
}

func (idx *OpIndex) FinalizeSync(ctx context.Context) error {
	return nil
}

func (idx *OpIndex) Close() error {
	for n, v := range idx.tables {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", n, err)
			}
		}
		delete(idx.tables, n)
	}
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
		if err := idx.tables[model.EndorseOpTableKey].Insert(ctx, endorse); err != nil {
			return err
		}
		// assign op ids back to the original ops
		for _, v := range endorse {
			ed := v.(*model.Endorsement)
			block.Ops[ed.OpN].RowId = ed.RowId
		}
	}
	return idx.tables[model.OpTableKey].Insert(ctx, ops)
}

func (idx *OpIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *OpIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting ops at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.OpTableKey]).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}
	_, err = pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.EndorseOpTableKey]).
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
	err := pack.NewQuery("etl.scan").
		WithTable(idx.tables[model.OpTableKey]).
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
	_, err = pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.OpTableKey]).
		AndEqual("cycle", cycle).
		Delete(ctx)
	if err != nil {
		return err
	}
	if first > 0 && last >= first {
		_, err = pack.NewQuery("etl.delete").
			WithTable(idx.tables[model.EndorseOpTableKey]).
			AndRange("height", first, last).
			Delete(ctx)
	}
	return err
}

func (idx *OpIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}
