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
	"blockwatch.cc/tzindex/micheline"
	"blockwatch.cc/tzindex/rpc"

	. "blockwatch.cc/tzindex/etl/model"
)

const (
	BigMapPackSizeLog2         = 15 // 32k packs
	BigMapJournalSizeLog2      = 16 // 64k
	BigMapCacheSize            = 128
	BigMapFillLevel            = 100
	BigMapIndexPackSizeLog2    = 15 // 16k packs (32k split size) ~256k
	BigMapIndexJournalSizeLog2 = 16 // 64k
	BigMapIndexCacheSize       = 1024
	BigMapIndexFillLevel       = 90
	BigMapIndexKey             = "bigmap"
	BigMapTableKey             = "bigmap"
)

var (
	ErrNoBigMapEntry = errors.New("bigmap not indexed")
)

type BigMapIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*BigMapIndex)(nil)

func NewBigMapIndex(opts, iopts pack.Options) *BigMapIndex {
	return &BigMapIndex{opts: opts, iopts: iopts}
}

func (idx *BigMapIndex) DB() *pack.DB {
	return idx.db
}

func (idx *BigMapIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *BigMapIndex) Key() string {
	return BigMapIndexKey
}

func (idx *BigMapIndex) Name() string {
	return BigMapIndexKey + " index"
}

func (idx *BigMapIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(BigMapItem{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %v", err)
	}
	defer db.Close()

	table, err := db.CreateTableIfNotExists(
		BigMapTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BigMapPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigMapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigMapCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, BigMapFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // op hash field (32 byte hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, BigMapIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, BigMapIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, BigMapIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, BigMapIndexFillLevel),
		})
	if err != nil {
		return err
	}

	return nil
}

func (idx *BigMapIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		BigMapTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigMapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigMapCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, BigMapIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, BigMapIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *BigMapIndex) Close() error {
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

// asumes op ids are already set (must run after OpIndex)
// Note: zero is a valid bigmap id in all protocols
func (idx *BigMapIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	contracts, err := builder.Table(ContractIndexKey)
	if err != nil {
		return err
	}

	contract := &Contract{}
	for _, op := range block.Ops {
		if len(op.BigMapDiff) == 0 || !op.IsSuccess {
			continue
		}
		// load rpc op
		o, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing bigmap transaction op [%d:%d]", op.OpN, op.OpC)
		}
		// extract deserialized bigmap diff
		var bmd micheline.BigMapDiff
		switch op.Type {
		case chain.OpTypeTransaction:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap transaction op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigMapDiff
			} else {
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("contract bigmap transaction op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = top.Metadata.Result.BigMapDiff
			}
		case chain.OpTypeOrigination:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigMapDiff
			} else {
				oop, ok := o.(*rpc.OriginationOp)
				if !ok {
					return fmt.Errorf("contract bigmap origination op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, o)
				}
				bmd = oop.Metadata.Result.BigMapDiff
			}
		}

		// load corresponding contract
		if contract.AccountId != op.ReceiverId {
			err := contracts.Stream(ctx, pack.Query{
				Name: "etl.bigmap.lookup",
				Conditions: pack.ConditionList{
					pack.Condition{
						Field: contracts.Fields().Find("A"), // account id
						Mode:  pack.FilterModeEqual,
						Value: op.ReceiverId.Value(),
					},
				},
			}, func(r pack.Row) error {
				return r.Decode(contract)
			})
			if err != nil {
				return fmt.Errorf("missing contract account %d: %v", op.ReceiverId, err)
			}
		}

		// process bigmapdiffs
		alloc := &BigMapItem{}
		last := &BigMapItem{}
		for _, v := range bmd {
			// find and update previous key if any
			prev := &BigMapItem{}
			switch v.Action {
			case micheline.BigMapDiffActionUpdate, micheline.BigMapDiffActionRemove:
				// find bigmap allocation (required for real key type)
				if alloc.RowId == 0 || alloc.BigMapId != v.Id {
					err := idx.table.Stream(ctx, pack.Query{
						Name:  "etl.bigmap.find_alloc",
						Limit: 1, // there should only be one match anyways
						Conditions: pack.ConditionList{
							pack.Condition{
								Field: idx.table.Fields().Find("B"), // bigmap id
								Mode:  pack.FilterModeEqual,
								Value: v.Id,
							},
							pack.Condition{
								Field: idx.table.Fields().Find("a"), // alloc
								Mode:  pack.FilterModeEqual,
								Value: uint64(micheline.BigMapDiffActionAlloc),
							},
						},
					}, func(r pack.Row) error {
						return r.Decode(alloc)
					})
					if err != nil {
						return fmt.Errorf("etl.bigmap.alloc decode: %v", err)
					}
				}
				if last.RowId == 0 || last.BigMapId != alloc.BigMapId {
					err := idx.table.Stream(ctx, pack.Query{
						Name:  "etl.bigmap.find_last",
						Limit: 1, // there should only be one match anyways
						Order: pack.OrderDesc,
						Conditions: pack.ConditionList{
							pack.Condition{
								Field: idx.table.Fields().Find("B"), // bigmap id
								Mode:  pack.FilterModeEqual,
								Value: v.Id,
							},
						},
					}, func(r pack.Row) error {
						return r.Decode(last)
					})
					if err != nil {
						return fmt.Errorf("etl.bigmap.last decode: %v", err)
					}
				}

				// find the previuos entry at this key if exists
				err := idx.table.Stream(ctx, pack.Query{
					Name:  "etl.bigmap.update",
					Limit: 1, // there should only be one match anyways
					Conditions: pack.ConditionList{
						pack.Condition{
							Field: idx.table.Fields().Find("B"), // bigmap id
							Mode:  pack.FilterModeEqual,
							Value: v.Id,
						},
						pack.Condition{
							Field: idx.table.Fields().Find("H"), // key hash
							Mode:  pack.FilterModeEqual,
							Value: v.KeyHash.Hash.Hash, // as []byte slice
						},
						pack.Condition{
							Field: idx.table.Fields().Find("r"), // is_replaced
							Mode:  pack.FilterModeEqual,
							Value: false,
						},
						// don't filter by deleted flag so we find any deletion entry
					},
				}, func(r pack.Row) error {
					return r.Decode(prev)
				})
				if err != nil {
					return fmt.Errorf("etl.bigmap.update decode: %v", err)
				}

				// flush update immediately to allow sequence of updates
				if prev.RowId > 0 {
					prev.IsReplaced = true
					prev.Updated = block.Height
					if err := idx.table.Update(ctx, prev); err != nil {
						return fmt.Errorf("etl.bigmap.update: %v", err)
					}
				}

				// update counters for next entry
				nkeys := last.NKeys
				if v.Action == micheline.BigMapDiffActionRemove {
					// ignore double remove (if that's even possible)
					if prev.RowId > 0 && !prev.IsDeleted {
						nkeys--
					}
				} else {
					// new keys are detected by non-existing prev
					if prev.RowId == 0 {
						nkeys++
					}
				}

				// log.Infof("BigMap %s %d key %s in block %d", v.Action, v.Id, v.KeyHash, block.Height)

				// insert immediately to allow sequence of updates
				item := NewBigMapItem(op, contract, v, prev.RowId, alloc.KeyType, last.Counter+1, nkeys)
				if err := idx.table.Insert(ctx, item); err != nil {
					return fmt.Errorf("etl.bigmap.insert: %v", err)
				}
				last = item

			case micheline.BigMapDiffActionAlloc:
				// insert immediately to allow sequence of updates
				// log.Infof("BigMap %s %d in block %d", v.Action, v.Id, block.Height)
				item := NewBigMapItem(op, contract, v, prev.RowId, 0, 0, 0)
				if err := idx.table.Insert(ctx, item); err != nil {
					return fmt.Errorf("etl.bigmap.insert: %v", err)
				}
				last = item

			case micheline.BigMapDiffActionCopy:
				// copy the alloc and all current keys to new entries, set is_copied
				ins := make([]pack.Item, 0)
				item := &BigMapItem{}

				// find the source alloc and all current bigmap entries
				var counter int64
				err := idx.table.Stream(ctx, pack.Query{
					Name: "etl.bigmap.copy",
					Conditions: pack.ConditionList{
						pack.Condition{
							Field: idx.table.Fields().Find("B"), // bigmap id
							Mode:  pack.FilterModeEqual,
							Value: v.SourceId, // copy from source map
						},
						pack.Condition{
							Field: idx.table.Fields().Find("r"), // is_replaced
							Mode:  pack.FilterModeEqual,
							Value: false,
						},
						pack.Condition{
							Field: idx.table.Fields().Find("d"), // is_deleted
							Mode:  pack.FilterModeEqual,
							Value: false,
						},
					},
				}, func(r pack.Row) error {
					if err := r.Decode(item); err != nil {
						return err
					}
					// copy the item
					if item.Action == micheline.BigMapDiffActionAlloc {
						log.Infof("BigMap %d %s alloc in block %d", v.DestId, v.Action, block.Height)
						item := CopyBigMapAlloc(item, op, contract, v.DestId, counter+1, 0)
						ins = append(ins, item)
						last = item
					} else {
						log.Infof("BigMap %d %s item key %s in block %d", v.DestId, v.Action, v.KeyHash, block.Height)
						item := CopyBigMapValue(item, op, contract, v.DestId, counter+1, counter)
						ins = append(ins, item)
						last = item
					}
					counter++
					return nil
				})
				if err != nil {
					return fmt.Errorf("etl.bigmap.copy: %v", err)
				}
				if err := idx.table.Insert(ctx, ins); err != nil {
					return fmt.Errorf("etl.bigmap.insert: %v", err)
				}
			}
		}
	}

	return nil
}

func (idx *BigMapIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BigMapIndex) DeleteBlock(ctx context.Context, height int64) error {
	log.Debugf("Rollback deleting bigmap updates at height %d", height)

	// need to unset IsReplaced flags in prior rows for every entry we find here
	upd := make([]pack.Item, 0)
	del := make([]uint64, 0)
	ids := make([]uint64, 0)
	type XItem struct {
		RowId    uint64 `pack:"I"`
		PrevId   uint64 `pack:"P"`
		IsCopied bool   `pack:"c"`
	}
	item := &XItem{}

	// find all items to delete and all items to update
	err := idx.table.Stream(ctx, pack.Query{
		Name:   "etl.bigmap.delete_scan",
		Fields: idx.table.Fields().Select("I"),
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("h"), // block height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}, func(r pack.Row) error {
		if err := r.Decode(item); err != nil {
			return err
		}
		if !item.IsCopied {
			ids = append(ids, item.PrevId)
		}
		del = append(ids, item.RowId)
		return nil
	})
	if err != nil {
		return err
	}

	// load update items
	err = idx.table.StreamLookup(ctx, ids, func(r pack.Row) error {
		item := AllocBigMapItem()
		if err := r.Decode(item); err != nil {
			return err
		}
		item.IsReplaced = false
		upd = append(upd, item)
		return nil
	})
	if err != nil {
		return err
	}
	// and run update
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}

	// now delete the original items
	return idx.table.DeleteIds(ctx, del)
}
