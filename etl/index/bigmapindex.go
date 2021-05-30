// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/cache"
	"blockwatch.cc/packdb/cache/lru"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	. "blockwatch.cc/tzindex/etl/model"
)

const (
	BigMapPackSizeLog2         = 15 // 32k packs
	BigMapJournalSizeLog2      = 16 // 64k
	BigMapCacheSize            = 128
	BigMapFillLevel            = 100
	BigMapIndexPackSizeLog2    = 14 // 16k packs (32k split size) ~256kB
	BigMapIndexJournalSizeLog2 = 16 // 64k
	BigMapIndexCacheSize       = 1024
	BigMapIndexFillLevel       = 90
	BigMapIndexKey             = "bigmap"
	BigMapTableKey             = "bigmap"
)

var (
	ErrNoBigmapEntry = errors.New("bigmap not indexed")
)

type BigMapIndex struct {
	db         *pack.DB
	opts       pack.Options
	iopts      pack.Options
	table      *pack.Table
	allocCache cache.Cache // cache bigmap alloc operations (for fast type access)
	lastCache  cache.Cache // cache most recent bigmap item (for fast counter access)
}

var _ BlockIndexer = (*BigMapIndex)(nil)

func NewBigMapIndex(opts, iopts pack.Options) *BigMapIndex {
	ac, _ := lru.New(1024)
	lc, _ := lru.New(1024)
	return &BigMapIndex{opts: opts, iopts: iopts, allocCache: ac, lastCache: lc}
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
	fields, err := pack.Fields(BigmapItem{})
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
		"key",                 // name
		fields.Find("K"),      // HashId (uint64 xxhash(bigmap_id + expr-hash)
		pack.IndexTypeInteger, // sorted int, index stores uint64 -> pk value
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
	idx.lastCache.Purge()
	idx.allocCache.Purge()
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

// assumes op ids are already set (must run after OpIndex)
// Note: zero is a valid bigmap id in all protocols
func (idx *BigMapIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	contracts, err := builder.Table(ContractIndexKey)
	if err != nil {
		return err
	}
	// contracts can pass temporary bigmaps to internal calls
	var temporaryBigmaps map[int64][]*BigmapItem

	contract := &Contract{}
	for _, op := range block.Ops {
		if len(op.BigmapDiff) == 0 || !op.IsSuccess {
			continue
		}
		// load rpc op
		o, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
		if !ok {
			return fmt.Errorf("missing bigmap transaction op [%d:%d]", op.OpL, op.OpP)
		}

		// reset temp bigmap after a batch of internal ops has been processed
		if !op.IsInternal {
			// log.Infof("%s %d/%d/%d/%d Clearing %d temp bigmaps", op.Hash, op.OpL, op.OpP, op.OpC, op.OpI, len(temporaryBigmaps))
			temporaryBigmaps = nil
		}

		// extract deserialized bigmap diff
		var bmd micheline.BigmapDiff
		switch op.Type {
		case tezos.OpTypeTransaction:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap transaction op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigmapDiff
			} else {
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("contract bigmap transaction op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = top.Metadata.Result.BigmapDiff
			}
		case tezos.OpTypeOrigination:
			if op.IsInternal {
				// on internal tx, find corresponding internal op
				top, ok := o.(*rpc.TransactionOp)
				if !ok {
					return fmt.Errorf("internal bigmap origination op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = top.Metadata.InternalResults[op.OpI].Result.BigmapDiff
			} else {
				oop, ok := o.(*rpc.OriginationOp)
				if !ok {
					return fmt.Errorf("contract bigmap origination op [%d:%d]: unexpected type %T ",
						op.OpL, op.OpP, o)
				}
				bmd = oop.Metadata.Result.BigmapDiff
			}
		}

		// load corresponding contract
		if contract.AccountId != op.ReceiverId {
			contract, ok = builder.ContractById(op.ReceiverId)
			if !ok {
				// when a contract was just originated, it is not yet known
				// by the builder, we fallback to loading the contract here
				contract = &Contract{}
				err := pack.NewQuery("etl.bigmap.lookup", contracts).
					WithDesc().
					WithLimit(1). // there should only be one match anyways
					AndEqual("account_id", op.ReceiverId).
					Execute(ctx, contract)
				if err != nil {
					return fmt.Errorf("bigmap update op [%d:%d] type=%s internal=%t: missing contract account %d: %v",
						op.OpL, op.OpP, op.Type, op.IsInternal, op.ReceiverId, err)
				}
			}
		}

		// process bigmapdiffs
		alloc := &BigmapItem{}
		last := &BigmapItem{}
		for _, v := range bmd {
			// find and update previous key if any
			switch v.Action {
			case micheline.DiffActionUpdate, micheline.DiffActionRemove:
				// Temporary Bigmaps
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
					if alloc.BigmapId != v.Id {
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
						var item *BigmapItem
						for _, vv := range items {
							if vv.Action == micheline.DiffActionAlloc {
								continue
							}
							if vv.IsReplaced || vv.IsDeleted {
								continue
							}
							if !vv.GetKeyHash().Equal(v.KeyHash) {
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
						last = NewBigmapItem(op, contract, v, prevId, last.Counter+1, nkeys)
						// extend list (may create new list)
						items = append(items, last)
						// re-add potentially new list to map
						temporaryBigmaps[v.Id] = items
						// for _, vv := range items {
						// 	log.Infof("  %s %d %x", vv.Action, vv.BigmapId, vv.Key)
						// }
					}
				} else {
					// Regular bigmaps (non-temporary)
					// find bigmap allocation (required for key type)
					if alloc.RowId == 0 || alloc.BigmapId != v.Id {
						cachedAlloc, ok := idx.allocCache.Get(v.Id)
						if ok {
							alloc = cachedAlloc.(*BigmapItem)
						} else {
							alloc = &BigmapItem{}
							err := pack.NewQuery("etl.bigmap.find_alloc", idx.table).
								WithDesc().
								WithLimit(1). // there should only be one match anyways
								AndEqual("bigmap_id", v.Id).
								AndEqual("action", micheline.DiffActionAlloc).
								Execute(ctx, alloc)
							if err != nil {
								return fmt.Errorf("etl.bigmap.alloc decode: %v", err)
							}
							idx.allocCache.Add(alloc.BigmapId, alloc)
						}
					}
					// find last bigmap entry for counters
					if last.RowId == 0 || last.BigmapId != alloc.BigmapId {
						cachedLast, ok := idx.lastCache.Get(v.Id)
						if ok {
							last = cachedLast.(*BigmapItem)
						} else {
							last = &BigmapItem{}
							err := pack.NewQuery("etl.bigmap.find_last", idx.table).
								WithDesc().
								WithLimit(1). // there should only be one match anyways
								AndEqual("bigmap_id", v.Id).
								Execute(ctx, last)
							if err != nil {
								return fmt.Errorf("etl.bigmap.last decode: %v", err)
							}
							idx.lastCache.Add(last.BigmapId, last)
						}
					}

					// find previous bigmap item at key
					if v.Key.OpCode == micheline.I_EMPTY_BIG_MAP {
						// log.Debugf("BigMap emptying %d (%d entries) in block %d",
						// 	v.Id, last.NKeys, block.Height)
						// remove all keys from the bigmap including the bigmap itself
						for last.NKeys > 0 {
							// find the next entry to remove
							prev := &BigmapItem{}
							err := pack.NewQuery("etl.bigmap.empty", idx.table).
								WithDesc().
								WithLimit(1). // limit to one match
								AndEqual("bigmap_id", v.Id).
								AndEqual("action", micheline.DiffActionUpdate).
								AndEqual("is_replaced", false).
								AndEqual("is_deleted", false).
								Execute(ctx, prev)
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
								prevdiff := prev.BigmapDiff()
								prevdiff.Action = micheline.DiffActionRemove
								item := NewBigmapItem(op, contract, prevdiff, prev.RowId, last.Counter+1, last.NKeys-1)
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
						idx.lastCache.Remove(last.BigmapId)

					} else {
						// update/remove a single key

						// find the previous entry at this key if exists
						// don't filter by deleted flag so we find any deletion entry
						prev := &BigmapItem{}
						err := pack.NewQuery("etl.bigmap.update", idx.table).
							WithDesc().
							WithLimit(1).   // there should only be one match anyways
							WithoutIndex(). // index may contain many duplicates, skip
							AndEqual("bigmap_id", v.Id).
							AndEqual("action", micheline.DiffActionUpdate).
							AndEqual("is_replaced", false).
							AndEqual("key_id", GetKeyId(v.Id, v.KeyHash)).
							Execute(ctx, prev)

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
						item := NewBigmapItem(op, contract, v, prev.RowId, last.Counter+1, nkeys)
						if err := idx.table.Insert(ctx, item); err != nil {
							return fmt.Errorf("etl.bigmap.insert: %v", err)
						}
						last = item
						idx.lastCache.Add(last.BigmapId, last)
					}
				}

			case micheline.DiffActionAlloc:
				// insert immediately to allow sequence of updates
				// log.Debugf("BigMap %s %d in block %d", v.Action, v.Id, block.Height)
				if v.Id < 0 {
					// alloc temp bigmap
					item := NewBigmapItem(op, contract, v, 0, 0, 0)
					if temporaryBigmaps == nil {
						temporaryBigmaps = make(map[int64][]*BigmapItem)
					}
					temporaryBigmaps[v.Id] = []*BigmapItem{item}
				} else {
					// alloc real bigmap
					item := NewBigmapItem(op, contract, v, 0, 0, 0)
					if err := idx.table.Insert(ctx, item); err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
					last = item
					idx.lastCache.Add(last.BigmapId, last)
				}

			case micheline.DiffActionCopy:
				// copy the alloc and all current keys to new entries, set is_copied
				items := make([]*BigmapItem, 0)

				if v.SourceId < 0 {
					// use temporary bigmap as source
					var ok bool
					items, ok = temporaryBigmaps[v.SourceId]

					// log.Infof("%s %d/%d/%d/%d Bigmap copy from temp id %d to %d (%d items)",
					// 	op.Hash, op.OpL, op.OpP, op.OpC, op.OpI, v.SourceId, v.DestId, len(items))

					if !ok || len(items) == 0 {
						return fmt.Errorf("etl.bigmap.copy: missing temporary bigmap %d", v.SourceId)
					}
					// update destination bigmap identity and ownership
					for _, vv := range items {
						vv.BigmapId = v.DestId
						vv.AccountId = contract.AccountId
						vv.ContractId = contract.RowId
						vv.OpId = op.RowId
					}

					// for _, vv := range items {
					// 	log.Infof("  %s %d %x", vv.Action, vv.BigmapId, vv.Key)
					// }

				} else {
					// find the source alloc and all current bigmap entries
					// order matters, because alloc is always first
					var counter int64
					err := pack.NewQuery("etl.bigmap.copy", idx.table).
						AndEqual("bigmap_id", v.SourceId). // copy from source map
						AndEqual("is_replaced", false).
						AndEqual("is_deleted", false).
						Stream(ctx, func(r pack.Row) error {
							source := &BigmapItem{}
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
								// log.Debugf("BigMap %d %s item key %x in block %d", v.DestId, v.Action, source.KeyHash, block.Height)
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
					idx.lastCache.Add(last.BigmapId, last)
				}

				if v.DestId < 0 {
					// keep temp bigmaps around
					// log.Infof("%s %d/%d/%d/%d Bigmap copy from %d to temp id %d (%d items)",
					// 	op.Hash, op.OpL, op.OpP, op.OpC, op.OpI, v.SourceId, v.DestId, len(items))
					// lazy create map
					if temporaryBigmaps == nil {
						temporaryBigmaps = make(map[int64][]*BigmapItem)
					}
					temporaryBigmaps[v.DestId] = items
					// for _, vv := range items {
					// 	log.Infof("  %s %d %x", vv.Action, vv.BigmapId, vv.Key)
					// }
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
	idx.allocCache.Purge()
	idx.lastCache.Purge()
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BigMapIndex) DeleteBlock(ctx context.Context, height int64) error {
	// log.Debugf("Rollback deleting bigmap updates at height %d", height)

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
	err := idx.table.Stream(ctx,
		pack.NewQuery("etl.bigmap.delete_scan", idx.table).
			WithFields("I").
			AndEqual("height", height),
		func(r pack.Row) error {
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
		item := AllocBigmapItem()
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
