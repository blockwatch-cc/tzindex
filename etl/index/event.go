// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

const EventIndexKey = "event"

type EventIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*EventIndex)(nil)

func NewEventIndex() *EventIndex {
	return &EventIndex{}
}

func (idx *EventIndex) DB() *pack.DB {
	return idx.db
}

func (idx *EventIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *EventIndex) Key() string {
	return EventIndexKey
}

func (idx *EventIndex) Name() string {
	return EventIndexKey + " index"
}

func (idx *EventIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	defer db.Close()

	m := model.Event{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *EventIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Event{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *EventIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *EventIndex) Close() error {
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

func (idx *EventIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	ins := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsSuccess || op.Raw == nil {
			continue
		}

		// walk all events and add them
		for _, v := range op.Raw.Meta().InternalResults {
			if v.Kind != tezos.OpTypeEvent {
				continue
			}
			src, ok := builder.AccountByAddress(v.Source)
			if !ok {
				return fmt.Errorf("event: missing source contract %s", v.Source)
			}
			ins = append(ins, model.NewEventWithData(*v, src.RowId, op))
		}
	}

	if len(ins) > 0 {
		// insert, will generate unique row ids
		if err := idx.table.Insert(ctx, ins); err != nil {
			return fmt.Errorf("event: insert: %w", err)
		}
	}

	return nil
}

func (idx *EventIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *EventIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting contracts at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("height", height).
		Delete(ctx)
	return err
}

func (idx *EventIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *EventIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *EventIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
