// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	// "fmt"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	. "blockwatch.cc/tzindex/etl/model"
)

type IndexerConfig struct {
	DBPath  string
	DBOpts  interface{}
	StateDB store.DB
	Indexes []BlockIndexer
}

// Indexer defines an index manager that manages and stores multiple indexes.
type Indexer struct {
	mu      sync.Mutex
	times   atomic.Value
	ranks   atomic.Value
	dbpath  string
	dbopts  interface{}
	statedb store.DB
	reg     *Registry
	indexes []BlockIndexer
	tips    map[string]*IndexTip
	tables  map[string]*pack.Table
}

func NewIndexer(cfg IndexerConfig) *Indexer {
	return &Indexer{
		dbpath:  cfg.DBPath,
		dbopts:  cfg.DBOpts,
		statedb: cfg.StateDB,
		indexes: cfg.Indexes,
		reg:     NewRegistry(),
		tips:    make(map[string]*IndexTip),
		tables:  make(map[string]*pack.Table),
	}
}

func (m *Indexer) Init(ctx context.Context, tip *ChainTip) error {
	// Nothing to do when no indexes are enabled.
	if len(m.indexes) == 0 {
		return nil
	}

	// load tips
	var needCreate bool
	err := m.statedb.View(func(dbTx store.Tx) error {
		for _, t := range m.indexes {
			// load tip
			key := t.Key()
			tip, err := dbLoadIndexTip(dbTx, key)
			if err != nil {
				needCreate = err == ErrNoTable
				return err
			}
			m.tips[string(key)] = tip
		}

		// load known protocol deployment parameters
		deps, err := dbLoadDeployments(dbTx, tip)
		if err != nil {
			return err
		}
		for _, v := range deps {
			m.reg.Register(v)
		}
		return nil
	})
	if err != nil && !needCreate {
		return err
	}

	// Create the initial state for the indexes as needed.
	if needCreate {
		err := m.statedb.Update(func(dbTx store.Tx) error {
			// create buckets for index tips in the respecive databases
			for _, t := range m.indexes {
				if err := m.maybeCreateIndex(ctx, dbTx, t, tip.Symbol); err != nil {
					return err
				}
				key := t.Key()
				ttip, err := dbLoadIndexTip(dbTx, key)
				if err != nil {
					return err
				}
				m.tips[string(key)] = ttip
			}
			// create deployments index
			_, err := dbTx.Root().CreateBucketIfNotExists(deploymentsBucketName)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	// Initialize each of the enabled indexes.
	for _, t := range m.indexes {
		log.Infof("Initializing %s.", t.Name())
		if err := t.Init(m.dbpath, tip.Symbol, m.dbopts); err != nil {
			return err
		}
	}

	// cache indexer tables for fast lookups by API
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			m.tables[t.Name()] = t
		}
	}
	return nil
}

func (m *Indexer) Close() error {
	m.tables = nil
	for _, idx := range m.indexes {
		log.Infof("Closing %s.", idx.Name())
		if err := idx.Close(); err != nil {
			return err
		}
		if err := m.storeTip(idx.Key()); err != nil {
			return err
		}
	}
	return nil
}

func (m *Indexer) ConnectProtocol(ctx context.Context, params *chain.Params) error {
	prev := m.reg.GetParamsLatest()
	err := m.statedb.Update(func(dbTx store.Tx) error {
		if prev != nil {
			// update previous protocol end height
			prev.EndHeight = params.StartHeight - 1
			if err := dbStoreDeployment(dbTx, prev); err != nil {
				return err
			}
		}
		return dbStoreDeployment(dbTx, params)
	})
	if err != nil {
		return err
	}
	return m.reg.Register(params)
}

func (m *Indexer) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// update block time when synchronized
	if err := m.updateBlockTime(block); err != nil {
		return err
	}

	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[string(key)]
		if !ok {
			log.Errorf("missing tip for table %s", string(key))
			continue
		}

		// skip when the block is already known
		if tip.Hash != nil && tip.Hash.String() == block.Hash.String() {
			continue
		}

		if err := t.ConnectBlock(ctx, block, builder); err != nil {
			return err
		}

		// Update the current tip.
		cloned := block.Hash.Clone()
		tip.Hash = &cloned
		tip.Height = block.Height
	}
	return nil
}

func (m *Indexer) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder, ignoreErrors bool) error {
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[string(key)]
		if !ok {
			log.Errorf("missing tip for table %s", string(key))
			continue
		}
		if block.Height > 0 && !tip.Hash.IsEqual(block.Hash) {
			continue
		}

		if err := t.DisconnectBlock(ctx, block, builder); err != nil && !ignoreErrors {
			return err
		}

		// Update the current tip.
		cloned := block.TZ.Parent().Clone()
		tip.Hash = &cloned
		tip.Height = block.Height - 1
	}
	return nil
}

func (m *Indexer) DeleteBlock(ctx context.Context, tz *Bundle) error {
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[string(key)]
		if !ok {
			log.Errorf("missing tip for table %s", string(key))
			continue
		}
		if tz.Height() != tip.Height {
			continue
		}
		if err := t.DeleteBlock(ctx, tz.Height()); err != nil {
			return err
		}
		// Update the current tip.
		cloned := tz.Parent().Clone()
		tip.Hash = &cloned
		tip.Height = tz.Height() - 1
	}
	return nil
}

// maybeCreateIndex determines if each of the enabled index indexes has already
// been created and creates them if not.
func (m *Indexer) maybeCreateIndex(ctx context.Context, dbTx store.Tx, idx BlockIndexer, sym string) error {
	// Create the bucket for the current tips as needed.
	b, err := dbTx.Root().CreateBucketIfNotExists(tipsBucketName)
	if err != nil {
		return err
	}

	// Nothing to do if the tip already exists.
	key := idx.Key()
	if b.Get([]byte(key)) != nil {
		return nil
	}

	// The tip for the index does not exist, so create it.
	log.Infof("Creating %s.", idx.Name())
	if err := idx.Create(m.dbpath, sym, m.dbopts); err != nil {
		return err
	}

	// start with zero hash on create (genesis is inserted next)
	return dbStoreIndexTip(dbTx, key, &IndexTip{
		Hash:   nil,
		Height: 0,
	})
}

// Store idx tip
func (m *Indexer) storeTip(key string) error {
	tip, ok := m.tips[key]
	if !ok {
		return nil
	}
	log.Debugf("Storing %s idx tip.", key)
	return m.statedb.Update(func(dbTx store.Tx) error {
		return dbStoreIndexTip(dbTx, key, tip)
	})
}

// FIXME: uint32 overflows in year 2083
// - first timestamp is actual time in unix secs, remainder are offsets in seconds
// - each entry uses 4 bytes per block, safe until June 2017 + 66 years;
func (m *Indexer) buildBlockTimes(ctx context.Context) ([]uint32, error) {
	times := make([]uint32, 0, 1<<20)
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	type XBlock struct {
		Timestamp time.Time `pack:"T,snappy"`
	}
	q := pack.Query{
		Name:    "init.block_time",
		NoCache: true,
		Fields:  blocks.Fields().Select("T"),
		Conditions: pack.ConditionList{pack.Condition{
			Field: blocks.Fields().Find("h"), // all blocks with h >= 0
			Mode:  pack.FilterModeGte,
			Value: int64(0),
		}},
	}
	b := XBlock{}
	err = blocks.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(&b); err != nil {
			return err
		}
		if len(times) == 0 {
			// safe, it's before 2036
			times = append(times, uint32(b.Timestamp.Unix()))
		} else {
			// make sure there's no rounding error
			tsdiff := b.Timestamp.Unix() - int64(times[0])
			times = append(times, uint32(tsdiff))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return times, nil
}

// only called from single thread in crawler, no locking required
func (m *Indexer) updateBlockTime(block *Block) error {
	av := m.times.Load()
	if av == nil {
		// not initialized yet
		return nil
	}
	oldTimes := av.([]uint32)
	newTimes := make([]uint32, len(oldTimes), util.Max(cap(oldTimes), int(block.Height+1)))
	copy(newTimes, oldTimes)
	// extend slice and patch time-diff into position
	newTimes = newTimes[:int(block.Height+1)]
	tsdiff := block.Timestamp.Unix() - int64(newTimes[0])
	newTimes[int(block.Height)] = uint32(tsdiff)
	m.times.Store(newTimes)
	return nil
}
