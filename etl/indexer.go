// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
	"blockwatch.cc/tzindex/rpc"
)

type IndexerConfig struct {
	DBPath    string
	DBOpts    interface{}
	StateDB   store.DB
	Indexes   []model.BlockIndexer
	LightMode bool
}

// Indexer defines an index manager that manages and stores multiple indexes.
type Indexer struct {
	mu             sync.Mutex
	blocks         atomic.Value              // cache for all block hashes and timestamps
	ranks          atomic.Value              // top addresses (>10tez, 100k = 10 MB)
	rights         atomic.Value              // bitset 400 (bakers) * 6 (cycles) * 4096 (blocks) * 33 (rights)
	addrs          atomic.Value              // all on-chain address hashes by id
	proposals      atomic.Value              // gov proposals/protocol hashes by id
	bigmap_values  *cache.BigmapHistoryCache // bigmap history cache
	bigmap_types   *cache.BigmapCache        // bigmap allocs
	contract_types *cache.ContractTypeCache  // contract type data
	ticket_types   *cache.TicketTypeCache    // ticket type data
	dbpath         string
	dbopts         interface{}
	statedb        store.DB
	reg            *Registry
	indexes        []model.BlockIndexer
	tips           map[string]*IndexTip
	tables         map[string]*pack.Table
	sched          *task.Scheduler
	taskdb         *pack.DB
	tasks          *pack.Table
	lightMode      bool
}

func NewIndexer(cfg IndexerConfig) *Indexer {
	return &Indexer{
		dbpath:         cfg.DBPath,
		dbopts:         cfg.DBOpts,
		statedb:        cfg.StateDB,
		indexes:        cfg.Indexes,
		bigmap_values:  cache.NewBigmapHistoryCache(0),
		bigmap_types:   cache.NewBigmapCache(0),
		contract_types: cache.NewContractTypeCache(0),
		ticket_types:   cache.NewTicketTypeCache(0),
		reg:            NewRegistry(),
		tips:           make(map[string]*IndexTip),
		tables:         make(map[string]*pack.Table),
		lightMode:      cfg.LightMode,
	}
}

func (m *Indexer) ParamsByHeight(height int64) *rpc.Params {
	return m.reg.GetParamsByHeight(height)
}

func (m *Indexer) ParamsByCycle(cycle int64) *rpc.Params {
	return m.reg.GetParamsByCycle(cycle)
}

func (m *Indexer) ParamsByProtocol(proto tezos.ProtocolHash) (*rpc.Params, error) {
	return m.reg.GetParams(proto)
}

func (m *Indexer) ParamsByDeployment(v int) (*rpc.Params, error) {
	return m.reg.GetParamsByDeployment(v)
}

func (m *Indexer) IsLightMode() bool {
	return m.lightMode
}

func (m *Indexer) Sched() *task.Scheduler {
	return m.sched
}

func (m *Indexer) Table(key string) (*pack.Table, error) {
	t, ok := m.tables[key]
	if !ok {
		return nil, ErrNoTable
	}
	return t, nil
}

func (m *Indexer) Index(key string) (model.BlockIndexer, error) {
	for _, v := range m.indexes {
		if v.Key() == key {
			return v, nil
		}
	}
	return nil, ErrNoIndex
}

func (m *Indexer) TableStats() []pack.TableStats {
	stats := make([]pack.TableStats, 0)
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			stats = append(stats, t.Stats()...)
		}
	}
	return append(stats, m.tasks.Stats()...)
}

func (m *Indexer) Init(ctx context.Context, tip *model.ChainTip, mode Mode) error {
	// Nothing to do when no indexes are enabled.
	if len(m.indexes) == 0 {
		return nil
	}

	// load tips
	var needCreate bool
	err := m.statedb.View(func(dbTx store.Tx) error {
		for _, t := range m.indexes {
			key := t.Key()
			tip, err := dbLoadIndexTip(dbTx, key)
			if err != nil {
				needCreate = needCreate || err == ErrNoTable
			}
			m.tips[key] = tip
		}
		return nil
	})
	if err != nil {
		return err
	}

	// load known protocol deployment parameters
	err = m.statedb.View(func(dbTx store.Tx) error {
		deps, err := dbLoadDeployments(dbTx, tip)
		if err != nil {
			return err
		}
		for _, v := range deps {
			m.reg.Register(v)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Create the initial state for the indexes as needed.
	nError := 0
	nMissing := 0
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
				m.tips[key] = ttip
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
	} else {
		// check all indexes are at same height as chain tip
		for n, v := range m.tips {
			if tip.BestHeight > 0 && v.Height != tip.BestHeight {
				log.Errorf("%s index with unexpected height %d/%d", n, v.Height, tip.BestHeight)
				nError++
				if v.Height == 0 {
					nMissing++
				}
			}
		}
	}

	switch {
	case nMissing > 0 && !m.lightMode:
		return fmt.Errorf("Missing database files! Looks like you used --light mode before or you deleted a database file.")
	case nMissing > 0 && m.lightMode:
		return fmt.Errorf("Missing database files! Looks like you deleted a database file.")
	case nError > 0 && mode != MODE_ROLLBACK:
		return fmt.Errorf("Corrupted database! Looks like you need to rebuild your database.")
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

	// open tasks db/table
	var tasks task.TaskRequest
	key := tasks.TableKey()
	fields, err := pack.Fields(tasks)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, tasks, err)
	}
	m.taskdb, err = pack.CreateDatabaseIfNotExists(m.dbpath, key, tip.Symbol, m.dbopts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", key, err)
	}
	m.tasks, err = m.taskdb.CreateTableIfNotExists(key, fields, tasks.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		return fmt.Errorf("creating %s table: %w", key, err)
	}

	// start scheduler
	m.sched = task.NewScheduler()
	m.sched.WithTable(m.tasks).
		WithCallback(m.OnTaskComplete).
		WithLogger(log).
		Start()

	return nil
}

func (m *Indexer) Finalize(ctx context.Context) error {
	for _, idx := range m.indexes {
		if err := idx.FinalizeSync(ctx); err != nil {
			return err
		}
	}

	// compact task table every 8192 blocks
	if m.tips[index.BlockIndexKey].Height%8129 == 0 {
		if err := m.tasks.Compact(ctx); err != nil {
			log.Errorf("compacting tasks: %v", err)
		}
	}

	return nil
}

func (m *Indexer) Flush(ctx context.Context) error {
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			// log.Debugf("Flushing %s.", t.Name())
			if err := t.Flush(ctx); err != nil {
				return err
			}
		}
	}
	return m.tasks.Flush(ctx)
}

func (m *Indexer) FlushJournals(ctx context.Context) error {
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			// log.Debugf("Flushing %s.", t.Name())
			if err := t.FlushJournal(ctx); err != nil {
				return err
			}
		}
	}
	return m.tasks.FlushJournal(ctx)
}

func (m *Indexer) GC(ctx context.Context, ratio float64) error {
	if err := m.Flush(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			log.Infof("Compacting %s.", t.Name())
			if err := t.Compact(ctx); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		db := idx.DB()
		log.Infof("Garbage collecting %s (%s).", idx.Name(), db.Path())
		if err := db.GC(ctx, ratio); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	log.Info("Compacting tasks table.")
	if err := m.tasks.Compact(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Infof("Garbage collecting tasks db (%s).", m.taskdb.Path())
	if err := m.taskdb.GC(ctx, ratio); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	return nil
}

func (m *Indexer) Close() error {
	// shutdown task scheduler
	if m.sched != nil {
		m.sched.Stop()
		m.sched = nil
	}
	if m.tasks != nil {
		m.tasks.Close()
		m.tasks = nil
	}
	if m.taskdb != nil {
		m.taskdb.Close()
		m.taskdb = nil
	}

	// close indexes
	m.tables = nil
	for _, idx := range m.indexes {
		log.Infof("Closing %s.", idx.Name())
		if err := idx.Close(); err != nil {
			return err
		}
		// if err := m.storeTip(idx.Key()); err != nil {
		// 	return err
		// }
	}
	return nil
}

func (m *Indexer) ConnectProtocol(ctx context.Context, next, prev *rpc.Params) error {
	err := m.statedb.Update(func(dbTx store.Tx) error {
		if prev != nil {
			if prev.EndHeight < 0 {
				prev.EndHeight = max(next.StartHeight-1, 0)
			}
			if err := dbStoreDeployment(dbTx, prev); err != nil {
				return err
			}
			m.reg.Register(prev)
		}
		return dbStoreDeployment(dbTx, next)
	})
	if err != nil {
		return err
	}
	m.reg.Register(next)
	return nil
}

func (m *Indexer) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// insert block into all indexes
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[key]
		if !ok {
			log.Errorf("missing tip for table %s", key)
			continue
		}

		// skip when the block is already known
		if tip.Hash != nil && *tip.Hash == block.Hash {
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

	// check context
	if err := ctx.Err(); err != nil {
		return err
	}

	// update live caches
	if err := m.updateBlocks(ctx, block); err != nil {
		return err
	}
	if err := m.updateAddrs(ctx, builder.Accounts()); err != nil {
		return err
	}
	if err := m.updateProposals(ctx, block); err != nil {
		return err
	}

	return nil
}

func (m *Indexer) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder, ignoreErrors bool) error {
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[key]
		if !ok {
			log.Errorf("missing tip for table %s", key)
			continue
		}
		if block.Height > 0 && *tip.Hash != block.Hash {
			continue
		}

		if err := t.DisconnectBlock(ctx, block, builder); err != nil && !ignoreErrors {
			return err
		}

		// Update the current tip.
		cloned := block.TZ.ParentHash().Clone()
		tip.Hash = &cloned
		tip.Height = block.Height - 1
	}

	// we don't roll-back caches here because cached data will be overwritten by
	// roll-forward

	return nil
}

func (m *Indexer) DeleteBlock(ctx context.Context, tz *rpc.Bundle) error {
	for _, t := range m.indexes {
		key := t.Key()
		tip, ok := m.tips[key]
		if !ok {
			log.Errorf("missing tip for table %s", key)
			continue
		}
		if tz.Height() != tip.Height {
			continue
		}
		if err := t.DeleteBlock(ctx, tz.Height()); err != nil {
			return err
		}
		// Update the current tip.
		cloned := tz.ParentHash().Clone()
		tip.Hash = &cloned
		tip.Height = tz.Height() - 1
	}
	return nil
}

// maybeCreateIndex determines if each of the enabled index indexes has already
// been created and creates them if not.
func (m *Indexer) maybeCreateIndex(_ context.Context, dbTx store.Tx, idx model.BlockIndexer, sym string) error {
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

func (m *Indexer) storeTips(dbTx store.Tx) error {
	for key, tip := range m.tips {
		if err := dbStoreIndexTip(dbTx, key, tip); err != nil {
			return err
		}
	}
	return nil
}

func (m *Indexer) OnTaskComplete(ctx context.Context, res *task.TaskResult) error {
	// identify target indexer
	idx, err := m.Index(res.Index)
	if err != nil {
		return err
	}
	return idx.OnTaskComplete(ctx, res)
}
