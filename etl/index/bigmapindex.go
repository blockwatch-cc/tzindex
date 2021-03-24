// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
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

	// contracts can pass temporary bigmaps between internal calls
	var temporaryBigmaps map[int64][]*BigMapItem

	contract := &Contract{}
	for _, op := range block.Ops {
		if len(op.BigMapDiff) == 0 || !op.IsSuccess {
			continue
		}
		// load rpc op
		o, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
		if !ok {
			return fmt.Errorf("missing bigmap transaction op [%d:%d]", op.OpL, op.OpP)
		}

		// reset temp bigmap after a batch of internal ops has been processed
		if !op.IsInternal {
			temporaryBigmaps = nil
		}

		// extract deserialized bigmap diff
		var bmd micheline.BigMapDiff
		switch op.Type {
		case chain.OpTypeTransaction:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap transaction op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigMapDiff
			} else {
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("contract bigmap transaction op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = top.Metadata.Result.BigMapDiff
			}
		case chain.OpTypeOrigination:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap origination op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigMapDiff
			} else {
				oop, ok := o.(*rpc.OriginationOp)
				if !ok {
					return fmt.Errorf("contract bigmap origination op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = oop.Metadata.Result.BigMapDiff
			}
		}

		// load corresponding contract
		if contract.AccountId != op.ReceiverId {
			contract, ok = builder.ContractById(op.ReceiverId)
			if !ok {
				contract = &Contract{}
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
					return fmt.Errorf("bigmap update op [%d:%d] type=%s internal=%t: missing contract for account %d: %v",
						op.OpL, op.OpP, op.Type, op.IsInternal, op.ReceiverId, err)
				}
			}
		}

		// process bigmapdiffs
		alloc := &BigMapItem{}
		last := &BigMapItem{}
		for _, v := range bmd {
			// find and update previous key if any
			switch v.Action {
			case micheline.DiffActionUpdate, micheline.DiffActionRemove:
				if v.Id < 0 {
					// log.Infof("%s %d/%d/%d/%d Bigmap %s on temp id %d", op.Hash, op.OpL, op.OpP, op.OpC, op.OpI, v.Action, v.Id)
					// on temporary bigmaps find the alloc from map
					items, ok := temporaryBigmaps[v.Id]
					if !ok || len(items) == 0 {
						return fmt.Errorf("etl.bigmap.find_alloc: missing temporary bigmap %d", v.Id)
					}
					for _, vv := range items {
						if vv.Action == micheline.DiffActionAlloc {
							alloc = vv
							break
						}
					}
					if alloc.BigMapId != v.Id {
						// fail if alloc is missing
						return fmt.Errorf("etl.bigmap.find_alloc: missing alloc in temporary bigmap %d", v.Id)
					}
					// keep last value
					last = items[len(items)-1]

					if v.Key.OpCode == micheline.I_EMPTY_BIG_MAP {
						// clear from temp map
						// log.Infof("%s %d/%d/%d/%d Bigmap clear temp id %d", op.Hash, op.OpL, op.OpP, op.OpC, op.OpI, v.Id)
						delete(temporaryBigmaps, v.Id)
					} else {
						// update/remove a single key
						var item *BigMapItem
						for _, vv := range items {
							if vv.Action == micheline.DiffActionAlloc {
								continue
							}
							if vv.IsReplaced || vv.IsDeleted {
								continue
							}
							if bytes.Compare(vv.KeyHash, v.KeyHash.Hash.Hash) != 0 {
								continue
							}
							// update the temporary item
							vv.IsReplaced = true
							vv.Updated = block.Height
							item = vv
							break
						}

						nkeys := last.NKeys
						if v.Action == micheline.DiffActionRemove {
							// ignore double remove (if that's even possible)
							nkeys--
						} else {
							// count new item if not exist
							if item == nil {
								nkeys++
							}
						}
						// always add a new item
						prevId := uint64(0)
						if item != nil {
							prevId = item.RowId
						}
						last = NewBigMapItem(op, contract, v, alloc.KeyType, prevId, last.Counter+1, nkeys)
						// extend list (may create new list)
						items = append(items, last)
						// re-add potentially new list to map
						temporaryBigmaps[v.Id] = items
						// for _, vv := range items {
						// 	log.Infof("  %s %d %x", vv.Action, vv.BigMapId, vv.Key)
						// }
					}
				} else {
					// on regular (non-temporary) bigmaps
					// find bigmap allocation (required for key type)
					if alloc.RowId == 0 || alloc.BigMapId != v.Id {
						err := idx.table.Stream(ctx, pack.Query{
							Name:  "etl.bigmap.find_alloc",
							Limit: 1, // there should only be one match anyways
							Order: pack.OrderDesc,
							Conditions: pack.ConditionList{
								pack.Condition{
									Field: idx.table.Fields().Find("B"), // bigmap id
									Mode:  pack.FilterModeEqual,
									Value: v.Id,
								},
								pack.Condition{
									Field: idx.table.Fields().Find("a"), // alloc
									Mode:  pack.FilterModeEqual,
									Value: uint64(micheline.DiffActionAlloc),
								},
							},
						}, func(r pack.Row) error {
							alloc = &BigMapItem{}
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
							last = &BigMapItem{}
							return r.Decode(last)
						})
						if err != nil {
							return fmt.Errorf("etl.bigmap.last decode: %v", err)
						}
					}

					prev := &BigMapItem{}
					if v.Key.OpCode == micheline.I_EMPTY_BIG_MAP {
						// log.Debugf("BigMap emptying %d (%d entries) in block %d",
						// 	v.Id, last.NKeys, block.Height)
						// remove all keys from the bigmap including the bigmap itself
						for last.NKeys > 0 {
							// find the next entry to remove
							err := idx.table.Stream(ctx, pack.Query{
								Name:  "etl.bigmap.empty",
								Limit: 1, // limit to one match
								Order: pack.OrderDesc,
								Conditions: pack.ConditionList{
									pack.Condition{
										Field: idx.table.Fields().Find("B"), // bigmap id
										Mode:  pack.FilterModeEqual,
										Value: v.Id,
									},
									pack.Condition{
										Field: idx.table.Fields().Find("a"), // action
										Mode:  pack.FilterModeEqual,
										Value: uint64(micheline.DiffActionUpdate),
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
								return r.Decode(prev)
							})
							if err != nil {
								return fmt.Errorf("etl.bigmap.update decode: %v", err)
							}

							// flush update immediately to allow sequence of updates
							if prev.RowId > 0 {
								// log.Debugf("BigMap %s %d in block %d", v.Action, v.Id, block.Height)
								prev.IsReplaced = true
								prev.Updated = block.Height
								if err := idx.table.Update(ctx, prev); err != nil {
									return fmt.Errorf("etl.bigmap.update: %v", err)
								}

								// insert deletion entry immediately to allow sequence of updates
								prevdiff := prev.BigMapDiff()
								prevdiff.Action = micheline.DiffActionRemove
								item := NewBigMapItem(op, contract, prevdiff, alloc.KeyType, prev.RowId, last.Counter+1, last.NKeys-1)
								if err := idx.table.Insert(ctx, item); err != nil {
									return fmt.Errorf("etl.bigmap.insert: %v", err)
								}
								last = item
							}
						}

						// now mark the allocation as deleted
						alloc.IsDeleted = true
						alloc.Updated = block.Height
						if err := idx.table.Update(ctx, alloc); err != nil {
							return fmt.Errorf("etl.bigmap.update: %v", err)
						}

					} else {
						// update/remove a single key

						// find the previous entry at this key if exists
						err := idx.table.Stream(ctx, pack.Query{
							Name:    "etl.bigmap.update",
							Limit:   1,    // there should only be one match anyways
							NoIndex: true, // index may contain many duplicates, skip
							Order:   pack.OrderDesc,
							Conditions: pack.ConditionList{
								pack.Condition{
									Field: idx.table.Fields().Find("B"), // bigmap id
									Mode:  pack.FilterModeEqual,
									Value: v.Id,
								},
								pack.Condition{
									Field: idx.table.Fields().Find("r"), // is_replaced
									Mode:  pack.FilterModeEqual,
									Value: false,
								},
								pack.Condition{
									Field: idx.table.Fields().Find("H"), // key hash
									Mode:  pack.FilterModeEqual,
									Value: v.KeyHash.Hash.Hash, // as []byte slice
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
						if v.Action == micheline.DiffActionRemove {
							// ignore double remove (if that's even possible)
							if prev.RowId > 0 && !prev.IsDeleted {
								nkeys--
							}
						} else {
							// new keys are detected by non-existing prev
							if prev.RowId == 0 || prev.IsDeleted {
								nkeys++
							}
						}

						// log.Debugf("BigMap %s %d key %s in block %d", v.Action, v.Id, v.KeyHash, block.Height)

						// insert immediately to allow sequence of updates
						item := NewBigMapItem(op, contract, v, alloc.KeyType, prev.RowId, last.Counter+1, nkeys)
						if err := idx.table.Insert(ctx, item); err != nil {
							return fmt.Errorf("etl.bigmap.insert: %v", err)
						}
						last = item
					}
				}

			case micheline.DiffActionAlloc:
				// insert immediately to allow sequence of updates
				// log.Debugf("BigMap %s %d in block %d", v.Action, v.Id, block.Height)
				if v.Id < 0 {
					// alloc temp bigmap
					item := NewBigMapItem(op, contract, v, alloc.KeyType, 0, 0, 0)
					if temporaryBigmaps == nil {
						temporaryBigmaps = make(map[int64][]*BigMapItem)
					}
					temporaryBigmaps[v.Id] = []*BigMapItem{item}
				} else {
					item := NewBigMapItem(op, contract, v, alloc.KeyType, 0, 0, 0)
					if err := idx.table.Insert(ctx, item); err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
					last = item
				}

			case micheline.DiffActionCopy:
				// copy the alloc and all current keys to new entries, set is_copied
				items := make([]*BigMapItem, 0)

				if v.SourceId < 0 {
					// use temporary bigmap as source
					var ok bool
					items, ok = temporaryBigmaps[v.SourceId]

					if !ok || len(items) == 0 {
						return fmt.Errorf("etl.bigmap.copy: missing temporary bigmap %d", v.SourceId)
					}
					// update destination bigmap identity and ownership
					for _, vv := range items {
						vv.BigMapId = v.DestId
						vv.AccountId = contract.AccountId
						vv.ContractId = contract.RowId
						vv.OpId = op.RowId
					}

				} else {
					// find the source alloc and all current bigmap entries
					// order matters, because alloc is always first
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
						source := &BigMapItem{}
						if err := r.Decode(source); err != nil {
							return err
						}
						// copy the item
						if source.Action == micheline.DiffActionAlloc {
							// log.Debugf("BigMap %d %s alloc in block %d", v.DestId, v.Action, block.Height)
							item := CopyBigMapAlloc(source, op, contract, v.DestId, counter+1, 0)
							items = append(items, item)
							last = item
						} else {
							// log.Debugf("BigMap %d %s item key %s in block %d", v.DestId, v.Action, v.KeyHash, block.Height)
							item := CopyBigMapValue(source, op, contract, v.DestId, counter+1, counter)
							items = append(items, item)
							last = item
						}
						counter++
						return nil
					})
					if err != nil {
						return fmt.Errorf("etl.bigmap.copy: %v", err)
					}
				}

				if v.DestId < 0 {
					// keep temp bigmaps around, lazy create map
					if temporaryBigmaps == nil {
						temporaryBigmaps = make(map[int64][]*BigMapItem)
					}
					temporaryBigmaps[v.DestId] = items
				} else {
					// target for regular copies are stored, need type conversion
					ins := make([]pack.Item, len(items))
					for i, vv := range items {
						ins[i] = vv
					}
					if err := idx.table.Insert(ctx, ins); err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
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
