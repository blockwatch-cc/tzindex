// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

const ContractIndexKey = "contract"

type ContractIndex struct {
	db    *pack.DB
	table *pack.Table
}

var _ model.BlockIndexer = (*ContractIndex)(nil)

func NewContractIndex() *ContractIndex {
	return &ContractIndex{}
}

func (idx *ContractIndex) DB() *pack.DB {
	return idx.db
}

func (idx *ContractIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *ContractIndex) Key() string {
	return ContractIndexKey
}

func (idx *ContractIndex) Name() string {
	return ContractIndexKey + " index"
}

func (idx *ContractIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	defer db.Close()

	m := model.Contract{}
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

func (idx *ContractIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Contract{}
	key := m.TableKey()
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}
	topts := m.TableOpts().Merge(model.ReadConfigOpts(key))
	table, err := idx.db.Table(key, topts)
	if err != nil {
		idx.Close()
		return err
	}
	idx.table = table

	return nil
}

func (idx *ContractIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *ContractIndex) Close() error {
	for _, v := range idx.Tables() {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", v.Name(), err)
			}
		}
	}
	idx.table = nil
	return nil
}

func (idx *ContractIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	ins := make([]pack.Item, 0, block.NewContracts)
	upd := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsSuccess || !(op.IsContract || op.IsRollup) {
			continue
		}

		switch op.Type {
		case model.OpTypeRollupTransaction:
			// add message has no receiver!
			if op.ReceiverId == 0 {
				continue
			}
			fallthrough

		case model.OpTypeTransaction,
			model.OpTypeSubsidy:
			// load from builder cache
			contract, ok := builder.ContractById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]", op.ReceiverId, op.Type, 3, op.OpP)
			}

			// skip contracts that have been originated in this block
			// they are already added below
			if contract.RowId == 0 {
				continue
			}

			// try re-detect ledger type
			if contract.LedgerType.IsValid() && !contract.LedgerSchema.IsValid() {
				if s, err := contract.LoadScript(); err == nil {
					// use most recent storage
					var store micheline.Prim
					if err := store.UnmarshalBinary(contract.Storage); err == nil {
						s.Storage = store
						schema, typ, lid, mid := model.DetectLedger(*s)
						if lid > 0 {
							contract.LedgerSchema = schema
							contract.LedgerType = typ
							contract.LedgerBigmap = lid
							contract.MetadataBigmap = mid
							contract.IsDirty = true
						}
					}
				}
			}

			// add contracts only once, use IsDirty flag
			if contract.IsDirty {
				upd = append(upd, contract)
				contract.IsDirty = false
			}

		case model.OpTypeOrigination,
			model.OpTypeMigration,
			model.OpTypeRollupOrigination:
			// load from builder cache
			contract, ok := builder.ContractById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]", op.ReceiverId, op.Type, 3, op.OpP)
			}
			if contract.IsNew {
				// insert new contracts
				ins = append(ins, contract)
			} else {
				// update patched smart contracts on migration (only once is guaranteed)
				upd = append(upd, contract)
			}
			contract.IsDirty = false
		}
	}

	// insert, will generate unique row ids
	if err := idx.table.Insert(ctx, ins); err != nil {
		return fmt.Errorf("contract: insert: %w", err)
	}

	if err := idx.table.Update(ctx, upd); err != nil {
		return fmt.Errorf("contract: update: %w", err)
	}
	return nil
}

func (idx *ContractIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	upd := make([]pack.Item, 0)
	// update all dirty contracts, skip originated contracts (will be removed)
	for _, v := range builder.Contracts() {
		if !v.IsDirty || v.RowId == 0 {
			continue
		}
		v.IsDirty = false
		upd = append(upd, v)
	}
	if err := idx.table.Update(ctx, upd); err != nil {
		return fmt.Errorf("contract: update: %w", err)
	}

	// last, delete originated contracts
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ContractIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting contracts at height %d", height)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("first_seen", height).
		Delete(ctx)
	return err
}

func (idx *ContractIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *ContractIndex) Flush(ctx context.Context) error {
	if err := idx.table.Flush(ctx); err != nil {
		log.Errorf("Flushing %s table: %v", idx.table.Name(), err)
	}
	return nil
}

func (idx *ContractIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
