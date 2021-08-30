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
	ContractPackSizeLog2         = 15 // 32k packs
	ContractJournalSizeLog2      = 16 // 64k
	ContractCacheSize            = 2  // minimum
	ContractFillLevel            = 100
	ContractIndexPackSizeLog2    = 15 // 16k packs (32k split size) ~256k
	ContractIndexJournalSizeLog2 = 16 // 64k
	ContractIndexCacheSize       = 2  // minimum
	ContractIndexFillLevel       = 90
	ContractIndexKey             = "contract"
	ContractTableKey             = "contract"
)

var (
	ErrNoContractEntry = errors.New("contract not indexed")
)

type ContractIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*ContractIndex)(nil)

func NewContractIndex(opts, iopts pack.Options) *ContractIndex {
	return &ContractIndex{opts: opts, iopts: iopts}
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
	fields, err := pack.Fields(Contract{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %v", err)
	}
	defer db.Close()

	table, err := db.CreateTableIfNotExists(
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

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // op hash field (20 byte address hashes)
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
	idx.table, err = idx.db.Table(
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

func (idx *ContractIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %v", idx.Name(), err)
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

func (idx *ContractIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	ins := make([]pack.Item, 0, block.NewContracts)
	upd := make([]pack.Item, 0)
	for _, op := range block.Ops {
		// don't process failed or unrelated ops
		if !op.IsSuccess || !op.IsContract {
			continue
		}

		// filter relevant op types
		switch op.Type {
		case tezos.OpTypeOrigination, tezos.OpTypeMigration, tezos.OpTypeTransaction:
		default:
			continue
		}

		switch op.Type {
		case tezos.OpTypeTransaction:
			// when contract is updated, load from builder cache
			contract, ok := builder.ContractById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]",
					op.ReceiverId, op.Type, op.OpL, op.OpP)
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

		case tezos.OpTypeOrigination:
			// load corresponding account
			acc, ok := builder.AccountById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing account %d in %s op", op.ReceiverId, op.Type)
			}
			if op.OpL >= 0 {
				// when contract is new, create model from rpc origination op
				o, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
				if !ok {
					return fmt.Errorf("contract: missing %s op [%d:%d]", o.OpKind(), op.OpL, op.OpP)
				}
				if op.IsInternal {
					// on internal originations, find corresponding internal op
					top, ok := o.(*rpc.TransactionOp)
					if !ok {
						return fmt.Errorf("contract: internal %s op [%d:%d]: unexpected type %T",
							o.OpKind(), op.OpL, op.OpP, o)
					}
					iop := top.Metadata.InternalResults[op.OpI]
					ins = append(ins, NewInternalContract(acc, iop, op))
				} else {
					oop, ok := o.(*rpc.OriginationOp)
					if !ok {
						return fmt.Errorf("contract: %s op [%d:%d]: unexpected type %T",
							o.OpKind(), op.OpL, op.OpP, o)
					}
					ins = append(ins, NewContract(acc, oop, op))
				}
			} else {
				// implicit contracts from migration originations are in builder cache
				contract, ok := builder.ContractById(op.ReceiverId)
				if !ok {
					return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]",
						op.ReceiverId, op.Type, op.OpL, op.OpP)
				}
				if contract.IsNew {
					// insert new delegator contracts
					ins = append(ins, contract)
				} else {
					// update patched smart contracts (only once is guaranteed)
					upd = append(upd, contract)
				}
			}

		case tezos.OpTypeMigration:
			// when contract is migrated, load from builder cache
			contract, ok := builder.ContractById(op.ReceiverId)
			if !ok {
				return fmt.Errorf("contract: missing contract %d in %s op [%d:%d]",
					op.ReceiverId, op.Type, op.OpL, op.OpP)
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
	if err := idx.table.Insert(ctx, ins); err != nil {
		return fmt.Errorf("contract: insert: %v", err)
	}

	if err := idx.table.Update(ctx, upd); err != nil {
		return fmt.Errorf("contract: update: %v", err)
	}
	return nil
}

func (idx *ContractIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	upd := make([]pack.Item, 0)
	// update all dirty contracts, skip originated contracts (will be removed)
	for _, v := range builder.Contracts() {
		if !v.IsDirty || v.RowId == 0 {
			continue
		}
		upd = append(upd, v)
	}
	if err := idx.table.Update(ctx, upd); err != nil {
		return fmt.Errorf("contract: update: %v", err)
	}

	// last, delete originated contracts
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ContractIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting contracts at height %d", height)
	_, err := pack.NewQuery("etl.contract.delete", idx.table).
		AndEqual("first_seen", height).
		Delete(ctx)
	return err
}

func (idx *ContractIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}
