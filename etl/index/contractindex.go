// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

const (
	ContractPackSizeLog2         = 10 // 8k packs
	ContractJournalSizeLog2      = 10 // 8k
	ContractCacheSize            = 2  // minimum
	ContractFillLevel            = 100
	ContractIndexPackSizeLog2    = 15 // 16k packs (32k split size) ~256k
	ContractIndexJournalSizeLog2 = 15 // 32k
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
	ct := make([]pack.Item, 0, block.NewContracts)
	for _, op := range block.Ops {
		if op.Type != chain.OpTypeOrigination {
			continue
		}
		if !op.IsSuccess {
			continue
		}
		if !op.IsContract {
			continue
		}
		// load rpc origination or transaction op
		o, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing contract origination op [%d:%d]", op.OpN, op.OpC)
		}
		// load corresponding account
		acc, ok := builder.AccountById(op.ReceiverId)
		if !ok {
			return fmt.Errorf("missing contract account %d", op.ReceiverId)
		}
		// skip when contract is not new (unlikely because every origination creates a
		// new account, but need to check invariant here)
		if !acc.IsNew {
			continue
		}
		if op.IsInternal {
			// on internal originations, find corresponding internal op
			top, ok := o.(*rpc.TransactionOp)
			if !ok {
				return fmt.Errorf("internal contract origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
			}
			iop := top.Metadata.InternalResults[op.OpI]
			ct = append(ct, NewInternalContract(acc, iop))
		} else {
			oop, ok := o.(*rpc.OriginationOp)
			if !ok {
				return fmt.Errorf("contract origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
			}
			ct = append(ct, NewContract(acc, oop))
		}
	}
	// insert, will generate unique row ids
	return idx.table.Insert(ctx, ct)
}

func (idx *ContractIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *ContractIndex) DeleteBlock(ctx context.Context, height int64) error {
	log.Debugf("Rollback deleting contracts at height %d", height)
	q := pack.Query{
		Name: "etl.contract.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("h"), // block height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}
