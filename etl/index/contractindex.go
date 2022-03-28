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
	"blockwatch.cc/tzindex/rpc"
)

const (
	ContractPackSizeLog2         = 15 // 32k packs
	ContractJournalSizeLog2      = 16 // 64k
	ContractCacheSize            = 8
	ContractFillLevel            = 100
	ContractIndexPackSizeLog2    = 15 // 16k packs (32k split size)
	ContractIndexJournalSizeLog2 = 16 // 64k
	ContractIndexCacheSize       = 8
	ContractIndexFillLevel       = 90
	ContractIndexKey             = "contract"
	ContractTableKey             = "contract"
)

var (
	ErrNoContractEntry = errors.New("contract not indexed")
)

type ContractIndex struct {
	db        *pack.DB
	opts      pack.Options
	iopts     pack.Options
	contracts *pack.Table
}

var _ model.BlockIndexer = (*ContractIndex)(nil)

func NewContractIndex(opts, iopts pack.Options) *ContractIndex {
	return &ContractIndex{opts: opts, iopts: iopts}
}

func (idx *ContractIndex) DB() *pack.DB {
	return idx.db
}

func (idx *ContractIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.contracts}
}

func (idx *ContractIndex) Key() string {
	return ContractIndexKey
}

func (idx *ContractIndex) Name() string {
	return ContractIndexKey + " index"
}

func (idx *ContractIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(model.Contract{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	defer db.Close()

	contracts, err := db.CreateTableIfNotExists(
		ContractTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, ContractPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, ContractJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, ContractCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, ContractFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = contracts.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // contract address field (20 byte address hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, ContractIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, ContractIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, ContractIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, ContractIndexFillLevel),
		})
	if err != nil {
		return err
	}
	return nil
}

func (idx *ContractIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.contracts, err = idx.db.Table(
		ContractTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, ContractJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, ContractCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, ContractIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, ContractIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
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
	idx.contracts = nil
	return nil
}

func (idx *ContractIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	ins := make([]pack.Item, 0, block.NewContracts)
	upd := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsSuccess || !op.IsContract {
			continue
		}

		// filter relevant op types
		switch op.Type {
		case model.OpTypeOrigination, model.OpTypeMigration, model.OpTypeTransaction:
		default:
			continue
		}

		switch op.Type {
		case model.OpTypeTransaction:
			// when contract is updated, load from builder cache
			contract, ok := builder.ContractById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]",
					op.ReceiverId, op.Type, 3, op.OpP)
			}
			// skip contracts that have been originated in this block, they will
			// be inserted below
			if contract.RowId == 0 {
				continue
			}

			// add contracts only once, use IsDirty flag
			if contract.IsDirty {
				upd = append(upd, contract)
				contract.IsDirty = false
			}

		case model.OpTypeOrigination:
			// load corresponding account
			acc, ok := builder.AccountById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing account %d in %s op", op.ReceiverId, op.Type)
			}
			if op.Raw != nil {
				// when contract is new, create model from rpc origination op
				if op.IsInternal {
					// on internal originations, find corresponding internal op
					top, ok := op.Raw.(*rpc.Transaction)
					if !ok {
						return fmt.Errorf("contract: internal %s op [%d:%d]: unexpected type %T",
							op.Raw.Kind(), 3, op.OpP, op.Raw)
					}
					iop := top.Metadata.InternalResults[op.OpI]
					ins = append(ins, model.NewInternalContract(acc, iop, op, builder.Constants()))
				} else {
					oop, ok := op.Raw.(*rpc.Origination)
					if !ok {
						return fmt.Errorf("contract: %s op [%d:%d]: unexpected type %T",
							op.Raw.Kind(), 3, op.OpP, op.Raw)
					}
					ins = append(ins, model.NewContract(acc, oop, op, builder.Constants(), block.Params))
				}
			} else {
				// implicit contracts from migration originations are in builder cache
				contract, ok := builder.ContractById(op.ReceiverId)
				if !ok {
					return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]",
						op.ReceiverId, op.Type, 3, op.OpP)
				}
				if contract.IsNew {
					// insert new delegator contracts
					ins = append(ins, contract)
				} else {
					// update patched smart contracts (only once is guaranteed)
					upd = append(upd, contract)
				}
			}

		case model.OpTypeMigration:
			// when contract is migrated, load from builder cache
			contract, ok := builder.ContractById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]",
					op.ReceiverId, op.Type, 3, op.OpP)
			}
			if contract.IsNew {
				// insert new delegator contracts
				ins = append(ins, contract)
			} else {
				// update patched smart contracts (only once is guaranteed)
				upd = append(upd, contract)
			}
		}
	}

	// insert, will generate unique row ids
	if err := idx.contracts.Insert(ctx, ins); err != nil {
		return fmt.Errorf("contract: insert: %w", err)
	}

	if err := idx.contracts.Update(ctx, upd); err != nil {
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
		upd = append(upd, v)
	}
	if err := idx.contracts.Update(ctx, upd); err != nil {
		return fmt.Errorf("contract: update: %w", err)
	}

	// last, delete originated contracts
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ContractIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting contracts at height %d", height)
	_, err := pack.NewQuery("etl.contract.delete", idx.contracts).
		AndEqual("first_seen", height).
		Delete(ctx)
	return err
}

func (idx *ContractIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *ContractIndex) Flush(ctx context.Context) error {
	return idx.contracts.Flush(ctx)
}
