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

const (
	FlowPackSizeLog2    = 15 // 32k packs
	FlowJournalSizeLog2 = 16 // 64k - searched for stats, so keep small
	FlowCacheSize       = 4
	FlowFillLevel       = 100
	FlowIndexKey        = "flow"
	FlowTableKey        = "flow"
)

var (
	ErrNoFlowEntry = errors.New("flow not indexed")
)

type FlowIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*FlowIndex)(nil)

func NewFlowIndex(opts pack.Options) *FlowIndex {
	return &FlowIndex{opts: opts}
}

func (idx *FlowIndex) DB() *pack.DB {
	return idx.db
}

func (idx *FlowIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *FlowIndex) Key() string {
	return FlowIndexKey
}

func (idx *FlowIndex) Name() string {
	return FlowIndexKey + " index"
}

func (idx *FlowIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(Flow{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		FlowTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, FlowPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, FlowJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, FlowCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, FlowFillLevel),
		})
	return err
}

func (idx *FlowIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(FlowTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, FlowJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, FlowCacheSize),
	})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *FlowIndex) Close() error {
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

func (idx *FlowIndex) ConnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	flows := make([]pack.Item, 0, len(block.Flows))
	for _, f := range block.Flows {
		flows = append(flows, f)
	}
	return idx.table.Insert(ctx, flows)
}

func (idx *FlowIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *FlowIndex) DeleteBlock(ctx context.Context, height int64) error {
	log.Debugf("Rollback deleting flows at height %d", height)
	q := pack.Query{
		Name: "etl.flow.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("h"), // block height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}
