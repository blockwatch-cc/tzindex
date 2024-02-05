// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

const MetadataIndexKey = "metadata"

type MetadataIndex struct {
	db    *pack.DB
	table *pack.Table
	dec   map[uint64]metadata.Decoder
}

var _ model.BlockIndexer = (*MetadataIndex)(nil)

// TODO
// - signal change to API layer to flush caches
// - update API layer: merge data into alias from profile & domain on the fly

func NewMetadataIndex() *MetadataIndex {
	return &MetadataIndex{
		dec: make(map[uint64]metadata.Decoder),
	}
}

func (idx *MetadataIndex) DB() *pack.DB {
	return idx.db
}

func (idx *MetadataIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *MetadataIndex) Key() string {
	return MetadataIndexKey
}

func (idx *MetadataIndex) Name() string {
	return MetadataIndexKey + " index"
}

func (idx *MetadataIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	var m model.Metadata
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}
	if _, err := db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key))); err != nil {
		return err
	}
	return nil
}

func (idx *MetadataIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	var m model.Metadata
	key := m.TableKey()
	table, err := idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	idx.table = table

	return nil
}

func (idx *MetadataIndex) FinalizeSync(ctx context.Context) error {
	return nil
}

func (idx *MetadataIndex) Close() error {
	// close tables
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

func (idx *MetadataIndex) getDecoder(u uint64) (metadata.Decoder, bool) {
	dec, ok := idx.dec[u]
	if !ok {
		dec, ok = metadata.LookupDecoder(u)
		if ok {
			idx.dec[u] = dec
		}
	}
	return dec, ok
}

func (idx *MetadataIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	for _, op := range block.Ops {
		if !op.IsContract || !op.IsSuccess {
			continue
		}

		// TODO: resolve generic contract metadata (tz16) for any contract
		//
		// TODO: resolve metadata for ledgers and tokens
		// ledger: contract with LedgerType.IsValid, LedgerBigmap > 0, LedgerMeta == 0
		//
		// TODO: update ledger metadata on specific calls
		//
		// switch {
		// case ldgr.Metadata == 0:
		// 	if err := idx.resolveLedgerMetadata(ctx, ldgr, call.Block); err != nil {
		// 		return fmt.Errorf("L_%s ledger meta load: %w", ldgr.Address, err)
		// 	}
		// case ReloadLedgerMetadataOn.Contains(params.Entrypoint):
		// 	if err := idx.updateLedgerMetadata(ctx, ldgr, call.Block); err != nil {
		// 		return fmt.Errorf("L_%s ledger meta update: %w", ldgr.Address, err)
		// 	}
		// case ReloadTokenMetadataOn.Contains(params.Entrypoint):
		// 	if err := idx.updateTokenMetadata(ctx, call); err != nil {
		// 		return fmt.Errorf("L_%s token meta update: %w", ldgr.Address, err)
		// 	}
		// }

		// try load decoder
		dec, ok := idx.getDecoder(op.CodeHash)
		if !ok {
			continue
		}

		events, err := dec.OnOperation(ctx, op)
		if err != nil {
			log.Errorf("meta decode %s %d %s: %v",
				dec.Namespace(),
				op.Height,
				op.Hash,
				err,
			)
			continue
		}

		for _, ev := range events {
			log.Debugf("%d meta event %s %s %s", block.Height, dec.Namespace(), ev.Type, ev.Owner)
			acc, err := builder.LoadAccountByAddress(ctx, ev.Owner)
			if err != nil {
				log.Errorf("meta %s owner lookup: %v", ev.Owner, err)
				continue
			}
			m, err := idx.FindOrCreateMetaModel(ctx, ev.Owner, acc.RowId)
			if err != nil {
				log.Errorf("meta %s load: %v", ev.Owner, err)
				continue
			}
			content := make(map[string]json.RawMessage)
			_ = json.Unmarshal(m.Content, &content)

			switch ev.Type {
			case metadata.EventTypeUpdate:
				// override existing model
				content[dec.Namespace()], _ = json.Marshal(ev.Data)
			case metadata.EventTypeRemove:
				// remove metadata model
				delete(content, dec.Namespace())
			case metadata.EventTypeResolve:
				// schedule task
				req := task.TaskRequest{
					Index:   idx.Key(),
					Decoder: op.CodeHash,
					Owner:   ev.Owner,
					Ordered: true,
					Flags:   uint64(ev.Flags),
					Url:     ev.Data.(string),
				}
				_ = builder.Sched().Run(req)
			}
			// write back
			m.Content, _ = json.Marshal(content)
			isEmpty := len(content) == 0

			// save
			switch {
			case m.RowId == 0 && !isEmpty:
				err = idx.table.Insert(ctx, m)
			case isEmpty && m.RowId > 0:
				err = idx.table.DeleteIds(ctx, []uint64{m.RowId.U64()})
			case m.RowId > 0:
				err = idx.table.Update(ctx, m)
			}
			if err != nil {
				log.Errorf("%d meta %s [%d] save: %v", block.Height, ev.Owner, m.RowId, err)
			}
		}
	}

	return nil
}

func (idx *MetadataIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DeleteBlock(ctx context.Context, height int64) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// noop
	return nil
}

func (idx *MetadataIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *MetadataIndex) FindOrCreateMetaModel(ctx context.Context, addr tezos.Address, id model.AccountID) (*model.Metadata, error) {
	var m model.Metadata
	err := pack.NewQuery("find_meta").
		WithTable(idx.table).
		AndEqual("account_id", id).
		Execute(ctx, &m)
	if err != nil {
		return nil, err
	}
	if m.RowId == 0 {
		m.AccountId = id
		m.Address = addr
	}
	return &m, nil
}

// TaskCompletionCallback
func (idx *MetadataIndex) OnTaskComplete(ctx context.Context, res *task.TaskResult) error {
	dec, ok := idx.getDecoder(res.Decoder)
	if !ok {
		log.Errorf("meta task complete %s %s %s: cannot find decoder %016x", dec.Namespace(), res.Owner, res.Status, res.Decoder)
		return nil
	}
	m, err := idx.FindOrCreateMetaModel(ctx, res.Owner, model.AccountID(res.Account))
	if err != nil {
		log.Errorf("meta task complete %s %s %s: cannot find model %v", dec.Namespace(), res.Owner, res.Status, err)
		return err
	}
	log.Debugf("meta task complete %s %s %s", dec.Namespace(), res.Owner, res.Status)
	events, err := dec.OnTaskComplete(ctx, m, res)
	if err != nil {
		log.Errorf("meta %s event %s status=%s: %v",
			dec.Namespace(),
			res.Owner,
			res.Status,
			err,
		)
		return nil
	}
	for _, ev := range events {
		log.Debugf("meta event %s %s %s", dec.Namespace(), ev.Type, ev.Owner)
		content := make(map[string]json.RawMessage)
		_ = json.Unmarshal(m.Content, &content)

		switch ev.Type {
		case metadata.EventTypeUpdate:
			// override existing model
			content[dec.Namespace()], _ = json.Marshal(ev.Data)
		case metadata.EventTypeRemove:
			// remove metadata model
			delete(content, dec.Namespace())
		}
		// write back
		m.Content, _ = json.Marshal(content)
		isEmpty := len(content) == 0

		// save
		switch {
		case m.RowId == 0 && !isEmpty:
			err = idx.table.Insert(ctx, m)
		case isEmpty && m.RowId > 0:
			err = idx.table.DeleteIds(ctx, []uint64{m.RowId.U64()})
		case m.RowId > 0:
			err = idx.table.Update(ctx, m)
		}
		if err != nil {
			log.Errorf("meta %s [%d] save: %v", ev.Owner, m.RowId, err)
		}
	}
	return nil
}
