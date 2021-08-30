// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"io"

	"blockwatch.cc/packdb/cache"
	"blockwatch.cc/packdb/cache/lru"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/model"
)

const (
	BigmapPackSizeLog2         = 15 // 32k packs
	BigmapJournalSizeLog2      = 16 // 64k
	BigmapCacheSize            = 128
	BigmapFillLevel            = 100
	BigmapIndexPackSizeLog2    = 14 // 16k packs (32k split size) ~256kB
	BigmapIndexJournalSizeLog2 = 16 // 64k
	BigmapIndexCacheSize       = 1024
	BigmapIndexFillLevel       = 90

	// TODO: separate config options
	// BigmapAllocPackSizeLog2  = 15 // 32k
	// BigmapAllocCacheSize     = 16
	// BigmapLivePackSizeLog2   = 15 // 32k
	// BigmapLiveCacheSize      = 256
	// BigmapUpdatePackSizeLog2 = 16 // 64k
	// BigmapUpdateCacheSize    = 2  // minimum

	BigmapIndexKey       = "bigmap"
	BigmapAllocTableKey  = "bigmaps"
	BigmapUpdateTableKey = "bigmap_updates"
	BigmapValueTableKey  = "bigmap_values"
)

var (
	ErrNoBigmapAlloc = errors.New("bigmap not indexed")
)

type BigmapIndex struct {
	db          *pack.DB
	opts        pack.Options
	iopts       pack.Options
	allocTable  *pack.Table
	updateTable *pack.Table
	valueTable  *pack.Table
	allocCache  cache.Cache // cache bigmap allocs (for fast type access)
}

var _ model.BlockIndexer = (*BigmapIndex)(nil)

func NewBigmapIndex(opts, iopts pack.Options) *BigmapIndex {
	ac, _ := lru.New(1024)
	return &BigmapIndex{opts: opts, iopts: iopts, allocCache: ac}
}

func (idx *BigmapIndex) DB() *pack.DB {
	return idx.db
}

func (idx *BigmapIndex) Tables() []*pack.Table {
	return []*pack.Table{
		idx.allocTable,
		idx.updateTable,
		idx.valueTable,
	}
}

func (idx *BigmapIndex) Key() string {
	return BigmapIndexKey
}

func (idx *BigmapIndex) Name() string {
	return BigmapIndexKey + " index"
}

func (idx *BigmapIndex) Create(path, label string, opts interface{}) error {
	allocFields, err := pack.Fields(model.BigmapAlloc{})
	if err != nil {
		return err
	}
	updateFields, err := pack.Fields(model.BigmapUpdate{})
	if err != nil {
		return err
	}
	liveFields, err := pack.Fields(model.BigmapKV{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating database: %v", err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		BigmapAllocTableKey,
		allocFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BigmapPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigmapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigmapCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, BigmapFillLevel),
		})
	if err != nil {
		return err
	}
	_, err = db.CreateTableIfNotExists(
		BigmapUpdateTableKey,
		updateFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BigmapPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigmapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigmapCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, BigmapFillLevel),
		})
	if err != nil {
		return err
	}
	live, err := db.CreateTableIfNotExists(
		BigmapValueTableKey,
		liveFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, BigmapPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigmapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigmapCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, BigmapFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = live.CreateIndexIfNotExists(
		"key",                 // name
		liveFields.Find("K"),  // HashId (uint64 xxhash(bigmap_id + expr-hash)
		pack.IndexTypeInteger, // sorted int, index stores uint64 -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, BigmapIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, BigmapIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, BigmapIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, BigmapIndexFillLevel),
		})
	if err != nil {
		return err
	}

	return nil
}

func (idx *BigmapIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.allocTable, err = idx.db.Table(
		BigmapAllocTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigmapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigmapCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	idx.updateTable, err = idx.db.Table(
		BigmapUpdateTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigmapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigmapCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	idx.valueTable, err = idx.db.Table(
		BigmapValueTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, BigmapJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, BigmapCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, BigmapIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, BigmapIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *BigmapIndex) Close() error {
	idx.allocCache.Purge()
	for _, v := range idx.Tables() {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %v", v.Name(), err)
			}
		}
	}
	idx.allocTable = nil
	idx.updateTable = nil
	idx.valueTable = nil
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func getBigmapDiffs(op *model.Op, block *model.Block) (micheline.BigmapDiff, error) {
	// implicit ops (from block metadata) can only contain originations
	if op.IsImplicit {
		// load and unpack from op
		var bmd micheline.BigmapDiff
		if err := bmd.UnmarshalBinary(op.BigmapDiff); err != nil {
			return nil, fmt.Errorf("decoding implicit bigmap origination op [%d:%d]: %v",
				op.OpL, op.OpP, err)
		}
		return bmd, nil
	}

	// load rpc op
	o, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
	if !ok {
		return nil, fmt.Errorf("missing bigmap transaction op [%d:%d]", op.OpL, op.OpP)
	}

	// extract deserialized bigmap diff
	switch op.Type {
	case tezos.OpTypeTransaction:
		if op.IsInternal {
			// on internal tx, find corresponding internal op
			top, ok := o.(*rpc.TransactionOp)
			if !ok {
				return nil, fmt.Errorf("internal bigmap transaction op [%d:%d]: unexpected type %T",
					op.OpL, op.OpP, o)
			}
			return top.Metadata.InternalResults[op.OpI].Result.BigmapDiff, nil
		} else {
			top, ok := o.(*rpc.TransactionOp)
			if !ok {
				return nil, fmt.Errorf("contract bigmap transaction op [%d:%d]: unexpected type %T",
					op.OpL, op.OpP, o)
			}
			return top.Metadata.Result.BigmapDiff, nil
		}
	case tezos.OpTypeOrigination:
		switch true {
		case op.IsInternal:
			// on internal tx, find corresponding internal op
			top, ok := o.(*rpc.TransactionOp)
			if !ok {
				return nil, fmt.Errorf("internal bigmap origination op [%d:%d]: unexpected type %T",
					op.OpL, op.OpP, o)
			}
			return top.Metadata.InternalResults[op.OpI].Result.BigmapDiff, nil
		default:
			oop, ok := o.(*rpc.OriginationOp)
			if !ok {
				return nil, fmt.Errorf("contract bigmap origination op [%d:%d]: unexpected type %T",
					op.OpL, op.OpP, o)
			}
			return oop.Metadata.Result.BigmapDiff, nil
		}
	}
	return nil, nil
}

type InMemoryBigmap struct {
	Alloc   *model.BigmapAlloc
	Updates []*model.BigmapUpdate
	Live    []*model.BigmapKV
}

func NewInMemoryBigmap(alloc *model.BigmapAlloc) InMemoryBigmap {
	return InMemoryBigmap{
		Alloc:   alloc,
		Updates: make([]*model.BigmapUpdate, 0),
		Live:    make([]*model.BigmapKV, 0),
	}
}

func (idx *BigmapIndex) loadAlloc(ctx context.Context, id int64) (*model.BigmapAlloc, error) {
	cachedAlloc, ok := idx.allocCache.Get(id)
	if ok {
		return cachedAlloc.(*model.BigmapAlloc), nil
	}
	alloc := &model.BigmapAlloc{}
	err := pack.NewQuery("etl.bigmap.find_alloc", idx.allocTable).
		WithLimit(1). // there should only be one match anyways
		AndEqual("bigmap_id", id).
		Execute(ctx, alloc)
	if err != nil {
		return nil, fmt.Errorf("etl.bigmap.alloc decode: %v", err)
	}
	idx.allocCache.Add(id, alloc)
	return alloc, nil
}

// assumes op ids are already set (must run after OpIndex)
func (idx *BigmapIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	tmp := make(map[int64]InMemoryBigmap)
	for _, op := range block.Ops {
		// skip non-bigmap ops
		if len(op.BigmapDiff) == 0 || !op.IsSuccess {
			continue
		}

		// reset temp bigmap after a batch of internal ops has been processed
		if !op.IsInternal {
			for k := range tmp {
				delete(tmp, k)
			}
		}

		bmd, err := getBigmapDiffs(op, block)
		if err != nil {
			log.Error(err)
			continue
		}

		// process bigmapdiffs
		for _, diff := range bmd {
			switch diff.Action {
			case micheline.DiffActionAlloc:
				// insert immediately to allow sequence of updates
				// log.Debugf("Bigmap %s %d in block %d", v.Action, v.Id, block.Height)
				if diff.Id < 0 {
					// alloc temp bigmap
					alloc := model.NewBigmapAlloc(op, diff)
					tmp[diff.Id] = NewInMemoryBigmap(alloc)
				} else {
					// alloc real bigmap
					alloc := model.NewBigmapAlloc(op, diff)
					if err := idx.allocTable.Insert(ctx, alloc); err != nil {
						return fmt.Errorf("etl.bigmap_alloc.insert: %v", err)
					}
					idx.allocCache.Add(alloc.BigmapId, alloc)

					// store as update
					if err := idx.updateTable.Insert(ctx, alloc.ToUpdate(op)); err != nil {
						return fmt.Errorf("etl.bigmap_alloc.insert: %v", err)
					}
				}

			case micheline.DiffActionCopy:
				// copy the alloc
				// copy and all live keys
				// generate updates for inserting all live keys
				var (
					alloc   *model.BigmapAlloc
					updates = make([]*model.BigmapUpdate, 0)
					live    = make([]*model.BigmapKV, 0)
				)

				if diff.SourceId < 0 {
					// use temporary bigmap as source
					bm, ok := tmp[diff.SourceId]
					if !ok {
						return fmt.Errorf("etl.bigmap.copy: missing temporary bigmap %d", diff.SourceId)
					}

					// create new alloc
					alloc = model.CopyBigmapAlloc(bm.Alloc, op, diff.DestId)

					// copy live keys and create updates
					for _, v := range bm.Live {
						copied := model.CopyBigmapKV(v, diff.DestId)
						live = append(live, copied)
						updates = append(updates, copied.ToUpdateCopy(op))
					}
					alloc.NKeys = int64(len(live))
					alloc.NUpdates = int64(len(live))

				} else {
					// find the source alloc
					srcAlloc, err := idx.loadAlloc(ctx, diff.SourceId)
					if err != nil {
						return fmt.Errorf("etl.bigmap.copy: %v", err)
					}
					alloc = model.CopyBigmapAlloc(srcAlloc, op, diff.DestId)

					// add an alloc update
					updates = append(updates, alloc.ToUpdateCopy(op, diff.SourceId))

					// load all currently live bigmap entries from source
					err = pack.NewQuery("etl.bigmap.copy", idx.valueTable).
						AndEqual("bigmap_id", diff.SourceId).
						Stream(ctx, func(r pack.Row) error {
							source := &model.BigmapKV{}
							if err := r.Decode(source); err != nil {
								return err
							}
							copied := model.CopyBigmapKV(source, diff.DestId)
							live = append(live, copied)
							updates = append(updates, copied.ToUpdateCopy(op))
							return nil
						})
					if err != nil {
						return fmt.Errorf("etl.bigmap.copy: %v", err)
					}
					alloc.NKeys = int64(len(live))
					alloc.NUpdates = int64(len(live))
				}

				if diff.DestId < 0 {
					// keep temp bigmaps around
					bm := NewInMemoryBigmap(alloc)
					bm.Updates = updates
					bm.Live = live
					tmp[diff.DestId] = bm
				} else {
					// store copied data
					if err := idx.allocTable.Insert(ctx, alloc); err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
					idx.allocCache.Add(alloc.BigmapId, alloc)
					ins := make([]pack.Item, len(live))
					for i, v := range live {
						ins[i] = v
					}
					if err := idx.valueTable.Insert(ctx, ins); err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
					ins = ins[:0]
					for _, v := range updates {
						ins = append(ins, v)
					}
					if err := idx.updateTable.Insert(ctx, ins); err != nil {
						return fmt.Errorf("etl.bigmap.insert: %v", err)
					}
				}

			case micheline.DiffActionRemove:
				// full bigmap removal
				if diff.Key.OpCode == micheline.I_EMPTY_BIG_MAP {
					if diff.Id < 0 {
						// clear temp bigmap
						delete(tmp, diff.Id)

						// done, next bigmap diff
						continue
					}

					// for regular bigmaps, update alloc
					alloc, err := idx.loadAlloc(ctx, diff.Id)
					if err != nil {
						return fmt.Errorf("etl.bigmap.empty: %v", err)
					}

					// list all live keys and schedule for deletion
					ids := make([]uint64, 0, int(alloc.NKeys))
					updates := make([]pack.Item, 0, int(alloc.NKeys))
					err = pack.NewQuery("etl.bigmap.empty", idx.valueTable).
						AndEqual("bigmap_id", diff.Id).
						Stream(ctx, func(r pack.Row) error {
							source := &model.BigmapKV{}
							if err := r.Decode(source); err != nil {
								return err
							}
							ids = append(ids, source.RowId)
							updates = append(updates, source.ToUpdateRemove(op))
							return nil
						})
					if err != nil {
						return fmt.Errorf("etl.bigmap.empty decode: %v", err)
					}
					alloc.NKeys = 0
					alloc.NUpdates += int64(len(updates))
					alloc.Updated = op.Height

					if err := idx.allocTable.Update(ctx, alloc); err != nil {
						return fmt.Errorf("etl.bigmap.empty: %v", err)
					}
					if err := idx.updateTable.Insert(ctx, updates); err != nil {
						return fmt.Errorf("etl.bigmap.empty: %v", err)
					}
					if err := idx.valueTable.DeleteIds(ctx, ids); err != nil {
						return fmt.Errorf("etl.bigmap.empty: %v", err)
					}

					// done, next bigmap diff
					continue
				}

				// single key removal from temp bigmap
				if diff.Id < 0 {
					// use temporary bigmap as source
					bm, ok := tmp[diff.Id]
					if !ok {
						return fmt.Errorf("etl.bigmap.remove: missing temporary bigmap %d", diff.Id)
					}

					// find the key to remove and remove from live & update lists
					var pos int = -1
					for i, v := range bm.Live {
						if !v.GetKeyHash().Equal(diff.KeyHash) {
							continue
						}
						pos = i
						break
					}
					if pos > -1 {
						bm.Alloc.NKeys--
						bm.Live = append(bm.Live[:pos], bm.Live[pos+1:]...)
					}
					pos = -1
					for i, v := range bm.Updates {
						if !v.GetKeyHash().Equal(diff.KeyHash) {
							continue
						}
						pos = i
						break
					}
					if pos > -1 {
						bm.Updates = append(bm.Updates[:pos], bm.Updates[pos+1:]...)
					}

					// done, next bigmap diff
					continue
				}

				// single key removal from regular bigmap
				alloc, err := idx.loadAlloc(ctx, diff.Id)
				if err != nil {
					return fmt.Errorf("etl.bigmap.remove: %v", err)
				}

				// find the previous entry, key should exist
				var prev *model.BigmapKV
				err = pack.NewQuery("etl.bigmap.remove", idx.valueTable).
					AndEqual("key_id", model.GetKeyId(diff.Id, diff.KeyHash)).
					AndEqual("bigmap_id", diff.Id).
					Stream(ctx, func(r pack.Row) error {
						source := &model.BigmapKV{}
						if err := r.Decode(source); err != nil {
							return err
						}
						// additional check for hash collision safety
						if source.GetKeyHash().Equal(diff.KeyHash) {
							prev = source
							return io.EOF
						}
						return nil
					})
				if err != nil && err != io.EOF {
					return fmt.Errorf("etl.bigmap.remove decode: %v", err)
				}

				if prev != nil {
					if err := idx.valueTable.DeleteIds(ctx, []uint64{prev.RowId}); err != nil {
						return fmt.Errorf("etl.bigmap.remove: %v", err)
					}
					alloc.NKeys--
				} else {
					// double remove is possible, actually its double update
					// with empty value (which we translate into remove)
					log.Debugf("bigmap: remove on non-existing key %d %s in %s", diff.Id, diff.KeyHash, op.Hash)
				}
				alloc.Updated = op.Height
				alloc.NUpdates++
				if err := idx.updateTable.Insert(ctx, model.NewBigmapUpdate(op, diff)); err != nil {
					return fmt.Errorf("etl.bigmap.remove: %v", err)
				}
				if err := idx.allocTable.Update(ctx, alloc); err != nil {
					return fmt.Errorf("etl.bigmap.remove: %v", err)
				}

			case micheline.DiffActionUpdate:
				// update on temp bigmap
				if diff.Id < 0 {
					// use temporary bigmap as source
					bm, ok := tmp[diff.Id]
					if !ok {
						return fmt.Errorf("etl.bigmap.update: missing temporary bigmap %d", diff.Id)
					}

					// find the key to update in live list
					// create live item if none exists
					// always create update item
					var pos int = -1
					for i, v := range bm.Live {
						if !v.GetKeyHash().Equal(diff.KeyHash) {
							continue
						}
						pos = i
						break
					}
					if pos < 0 {
						// add
						bm.Alloc.NKeys++
						bm.Alloc.NUpdates++
						bm.Live = append(bm.Live, model.NewBigmapKV(diff))
						bm.Updates = append(bm.Updates, model.NewBigmapUpdate(op, diff))
					} else {
						// replace
						bm.Alloc.NUpdates++
						bm.Live[pos] = model.NewBigmapKV(diff)
						bm.Updates = append(bm.Updates, model.NewBigmapUpdate(op, diff))
					}

					// done, next bigmap diff
					continue
				}

				// regular bigmaps
				alloc, err := idx.loadAlloc(ctx, diff.Id)
				if err != nil {
					return fmt.Errorf("etl.bigmap.update: %v", err)
				}

				// find the previous entry, key should exist
				var prev *model.BigmapKV
				err = pack.NewQuery("etl.bigmap.update", idx.valueTable).
					AndEqual("key_id", model.GetKeyId(diff.Id, diff.KeyHash)).
					AndEqual("bigmap_id", diff.Id).
					Stream(ctx, func(r pack.Row) error {
						source := &model.BigmapKV{}
						if err := r.Decode(source); err != nil {
							return err
						}
						// additional check for hash collision safety
						if source.GetKeyHash().Equal(diff.KeyHash) {
							prev = source
						}
						return nil
					})
				if err != nil {
					return fmt.Errorf("etl.bigmap.update decode: %v", err)
				}

				live := model.NewBigmapKV(diff)
				if prev != nil {
					// replace
					live.RowId = prev.RowId
					if err := idx.valueTable.Update(ctx, live); err != nil {
						return fmt.Errorf("etl.bigmap.update: %v", err)
					}
				} else {
					// add
					if err := idx.valueTable.Insert(ctx, live); err != nil {
						return fmt.Errorf("etl.bigmap.update: %v", err)
					}
					alloc.NKeys++
				}
				alloc.Updated = op.Height
				alloc.NUpdates++

				if err := idx.updateTable.Insert(ctx, model.NewBigmapUpdate(op, diff)); err != nil {
					return fmt.Errorf("etl.bigmap.update: %v", err)
				}
				if err := idx.allocTable.Update(ctx, alloc); err != nil {
					return fmt.Errorf("etl.bigmap.update: %v", err)
				}
			}
		}
	}

	return nil
}

func (idx *BigmapIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	idx.allocCache.Purge()
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *BigmapIndex) DeleteBlock(ctx context.Context, height int64) error {
	// reconstruct live keys by rolling back updates
	updates := make([]*model.BigmapUpdate, 0)
	err := pack.NewQuery("etl.bigmap.delete_scan", idx.updateTable).
		WithDesc().
		AndEqual("height", height).
		Execute(ctx, &updates)
	if err != nil {
		return err
	}

	// load allocs for rollback along the way, update all at once at the end
	allocs := make(map[int64]*model.BigmapAlloc)

	// identify list of updated keys
	for _, v := range updates {
		hash := v.GetKeyHash()
		key := model.GetKeyId(v.BigmapId, hash)

		// load alloc first
		alloc, ok := allocs[v.BigmapId]
		if !ok {
			alloc, err = idx.loadAlloc(ctx, v.BigmapId)
			if err != nil {
				return fmt.Errorf("rollback: missing alloc for bigmap %d: %v", v.BigmapId, err)
			}
			alloc.Updated = 0
			allocs[v.BigmapId] = alloc
		}

		// find the previous live entry, may not exist, may be from same block(!)
		var (
			prev *model.BigmapUpdate
			live *model.BigmapKV
		)
		err = pack.NewQuery("etl.bigmap.rollback", idx.updateTable).
			AndEqual("key_id", key).
			AndEqual("bigmap_id", v.BigmapId).
			AndLt("I", v.RowId).            // exclude self
			AndLte("height", height).       // limit search scope
			AndGte("height", alloc.Height). // limit search scope
			Stream(ctx, func(r pack.Row) error {
				source := &model.BigmapUpdate{}
				if err := r.Decode(source); err != nil {
					return err
				}
				// additional check for hash collision safety
				if source.GetKeyHash().Equal(hash) {
					prev = source
					return io.EOF
				}
				return nil
			})
		if err != nil && err != io.EOF {
			return fmt.Errorf("etl.bigmap.rollback decode: %v", err)
		}

		// rollback update
		switch v.Action {
		case micheline.DiffActionRemove:
			// sanity checks
			if prev == nil {
				log.Warnf("rollback: missing previous update for bigmap %d key %s", v.BigmapId, v.GetKeyHash())
				continue
			}
			if prev.Action != micheline.DiffActionUpdate {
				// just skip if this was a double remove
				alloc.NUpdates--
				log.Debugf("rollback: unexpected prev action %s update for bigmap %d key %s", prev.Action, v.BigmapId, v.GetKeyHash())
			} else {
				// this was a remove after update, insert previous live key
				live = prev.ToKV()
				alloc.NKeys++
				alloc.NUpdates--
				if err := idx.valueTable.Insert(ctx, live); err != nil {
					return fmt.Errorf("etl.bigmap.rollback insert live key: %v", err)
				}
			}
			// beware of same-block updates when resetting alloc update
			if prev.Height < height {
				alloc.Updated = util.Max64(alloc.Updated, prev.Height)
			}

		case micheline.DiffActionUpdate, micheline.DiffActionCopy:
			// load current live key, may not exist
			err = pack.NewQuery("etl.bigmap.rollback", idx.valueTable).
				AndEqual("key_id", key).
				AndEqual("bigmap_id", v.BigmapId).
				Stream(ctx, func(r pack.Row) error {
					source := &model.BigmapKV{}
					if err := r.Decode(source); err != nil {
						return err
					}
					// additional check for hash collision safety
					if source.GetKeyHash().Equal(hash) {
						live = source
						return io.EOF
					}
					return nil
				})
			if err != nil && err != io.EOF {
				return fmt.Errorf("etl.bigmap.rollback decode: %v", err)
			}
			if prev == nil {
				// sanity check
				if live == nil {
					log.Warnf("rollback: missing live key in bigmap %d key %s", v.BigmapId, v.GetKeyHash())
					continue
				}

				// this was a first-time insert, delete current live key
				if err := idx.valueTable.DeleteIds(ctx, []uint64{live.RowId}); err != nil {
					return fmt.Errorf("etl.bigmap.rollback delete live key: %v", err)
				}
				alloc.NKeys--
				alloc.NUpdates--

				// FIXME: we don't know previous update height here, but its ok
				// since alloc.Updated field is not relevant for correctness

			} else {
				if prev.Action == micheline.DiffActionRemove {
					// this was an insert after remove, remove current live key
					if err := idx.valueTable.DeleteIds(ctx, []uint64{live.RowId}); err != nil {
						return fmt.Errorf("etl.bigmap.rollback delete live key: %v", err)
					}
					alloc.NKeys--
					alloc.NUpdates--

				} else {
					// sanity check
					if live == nil {
						return fmt.Errorf("rollback: missing live key in bigmap %d key %s", v.BigmapId, v.GetKeyHash())
					}

					// this was an update after update, replace current live key
					lastLive := prev.ToKV()
					lastLive.RowId = live.RowId
					if err := idx.valueTable.Update(ctx, lastLive); err != nil {
						return fmt.Errorf("etl.bigmap.rollback replace live key: %v", err)
					}
					alloc.NUpdates--
				}

				// beware of same-block updates when resetting alloc update
				if prev.Height < height {
					alloc.Updated = util.Max64(alloc.Updated, prev.Height)
				}
			}
		}
	}

	// delete all updates at height
	_, err = pack.NewQuery("etl.bigmap.delete", idx.updateTable).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}

	// update rolled back allocs
	upd := make([]pack.Item, 0)
	for _, v := range allocs {
		if v.Height != height {
			continue
		}
		upd = append(upd, v)
	}
	if err := idx.allocTable.Update(ctx, upd); err != nil {
		return err
	}

	// delete all allocs from this block
	_, err = pack.NewQuery("etl.bigmap.delete", idx.allocTable).
		AndEqual("h", height). // alloc height
		Delete(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (idx *BigmapIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}
