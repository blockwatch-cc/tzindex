// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

const (
	StateDBName = "state.db"

	// state database schema
	stateDBSchemaName    = "2022-04-16"
	stateDBSchemaVersion = 6
	stateDBKey           = "statedb"
)

type Mode string

const (
	MODE_SYNC     Mode = "sync"
	MODE_INFO     Mode = "info"
	MODE_LIGHT    Mode = "light"
	MODE_ROLLBACK Mode = "rollback"
)

const (
	MONITOR_DISABLE = true
	MONITOR_KEEP    = false
)

type State string

const (
	STATE_LOADING       State = "loading"    // index init in progress
	STATE_CONNECTING    State = "connecting" // RPC disconnected
	STATE_STOPPING      State = "stopping"   // shutting down
	STATE_STOPPED       State = "stopped"    // auto-sync disabled, serving stale blockchain state
	STATE_WAITING       State = "waiting"    // ready to sync, but RPC server is not ready
	STATE_SYNCHRONIZING State = "syncing"    // sync in progress
	STATE_SYNCHRONIZED  State = "synced"     // in sync with blockchain
	STATE_FAILED        State = "failed"     // sync stopped due to index error
	STATE_STALLED       State = "stalled"    // temporary state when no new block in 1 min
)

type CrawlerConfig struct {
	DB            store.DB
	Indexer       *Indexer
	Client        *rpc.Client
	Queue         int
	Delay         int
	StopBlock     int64
	Snapshot      *SnapshotConfig
	EnableMonitor bool
	Validate      bool
}

type SnapshotConfig struct {
	Path          string
	Blocks        []int64
	BlockInterval int64
}

// Crawler loads blocks from blockchain client via RPC and informs
// indexers to connect/disconnect blocks when chaintip changes.
// It also handles chain reorganizations and API calls.
type Crawler struct {
	sync.RWMutex
	state         State
	mode          Mode
	snap          *SnapshotConfig
	useMonitor    bool
	enableMonitor bool
	stopHeight    int64

	db        store.DB
	rpc       *rpc.Client
	builder   *Builder
	indexer   *Indexer
	finalized chan *rpc.Bundle
	filter    *ReorgDelayFilter
	plog      *BlockProgressLogger
	chainId   tezos.ChainIdHash
	delay     int64
	wasInSync bool
	head      int64

	// read-mostly thread-safe access
	tipStore atomic.Value

	// coordinated shutdown
	quit   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// coordinated snapshot
	snapch chan error
}

func NewCrawler(cfg CrawlerConfig) *Crawler {
	queue := make(chan *rpc.Bundle, cfg.Queue)
	return &Crawler{
		state:         STATE_LOADING,
		mode:          MODE_SYNC,
		snap:          cfg.Snapshot,
		useMonitor:    false,
		enableMonitor: cfg.EnableMonitor,
		stopHeight:    cfg.StopBlock,
		db:            cfg.DB,
		rpc:           cfg.Client,
		builder:       NewBuilder(cfg.Indexer, cfg.Client, cfg.Validate),
		indexer:       cfg.Indexer,
		finalized:     queue,
		filter:        NewReorgDelayFilter(cfg.Delay, queue),
		delay:         int64(cfg.Delay),
		plog:          NewBlockProgressLogger("Processed"),
		quit:          make(chan struct{}),
	}
}

func (c *Crawler) Tip() *model.ChainTip {
	return c.tipStore.Load().(*model.ChainTip)
}

func (c *Crawler) Height() int64 {
	return c.Tip().BestHeight
}

func (c *Crawler) Time() time.Time {
	return c.Tip().BestTime
}

func (c *Crawler) updateTip(tip *model.ChainTip) {
	c.tipStore.Store(tip)
}

func (c *Crawler) setState(state State, disableMonitor bool, args ...string) bool {
	var logStr string
	c.Lock()
	isMonActive := c.useMonitor && !disableMonitor
	if state == STATE_SYNCHRONIZED {
		if c.enableMonitor {
			// log only when we're coming out of unsync
			if c.useMonitor && !c.wasInSync {
				logStr = "Fully synchronized. Switching to monitor mode."
				if len(args) > 0 {
					logStr = args[0]
				}
				c.wasInSync = true
			}
			// reset new target state
			isMonActive = !disableMonitor
		} else {
			isMonActive = false
		}
	}
	c.state = state
	c.useMonitor = isMonActive
	c.Unlock()
	if logStr != "" {
		log.Info(logStr)
	}
	return isMonActive
}

func (c *Crawler) getState() (State, bool) {
	var (
		s State
		m bool
	)
	c.RLock()
	s, m = c.state, c.useMonitor
	c.RUnlock()
	return s, m
}

func (c *Crawler) disableMonitor() {
	c.Lock()
	c.useMonitor = false
	c.Unlock()
}

func (c *Crawler) ParamsByHeight(height int64) *rpc.Params {
	if height < 0 {
		height = c.Height()
	}
	return c.indexer.ParamsByHeight(height)
}

func (c *Crawler) ParamsByCycle(cycle int64) *rpc.Params {
	return c.indexer.ParamsByCycle(cycle)
}

func (c *Crawler) ParamsByProtocol(proto tezos.ProtocolHash) *rpc.Params {
	p, _ := c.indexer.ParamsByProtocol(proto)
	return p
}

func (c *Crawler) Table(key string) (*pack.Table, error) {
	return c.indexer.Table(key)
}

func (c *Crawler) ChainByHeight(ctx context.Context, height int64) (*model.Chain, error) {
	return c.indexer.ChainByHeight(ctx, height)
}

func (c *Crawler) SupplyByHeight(ctx context.Context, height int64) (*model.Supply, error) {
	return c.indexer.SupplyByHeight(ctx, height)
}

func (c *Crawler) BlockByHeight(ctx context.Context, height int64) (*model.Block, error) {
	return c.indexer.BlockByHeight(ctx, height)
}

func (c *Crawler) BlockHeightFromTime(ctx context.Context, tm time.Time) int64 {
	return c.indexer.LookupBlockHeightFromTime(ctx, tm)
}

func (c *Crawler) CacheStats() map[string]interface{} {
	return c.builder.CacheStats()
}

type CrawlerStatus struct {
	Mode       Mode      `json:"mode"`
	Status     State     `json:"status"`
	Blocks     int64     `json:"blocks"`
	Finalized  int64     `json:"finalized"`
	Indexed    int64     `json:"indexed"`
	Progress   float64   `json:"progress"`
	LastUpdate time.Time `json:"last_update"`
}

func (c *Crawler) Status() CrawlerStatus {
	tip := c.Tip()
	state, _ := c.getState()
	s := CrawlerStatus{
		Mode:       c.mode,
		Status:     state,
		Blocks:     -1,
		Finalized:  -1,
		Indexed:    tip.BestHeight,
		LastUpdate: tip.BestTime,
	}
	if c.indexer.lightMode {
		s.Mode = MODE_LIGHT
	}
	if tip.BestHeight > 0 && c.head > 0 {
		s.Blocks = c.head
		s.Finalized = c.head - 1
		s.Progress = float64(s.Indexed) / float64(s.Finalized)
		if s.Progress == 1.0 && s.Indexed < s.Finalized {
			s.Progress = 0.999999
		}
	}
	if time.Since(s.LastUpdate) > time.Minute {
		s.Status = STATE_STALLED
	}
	return s
}

// Init is invoked when the block manager is first initializing.
func (c *Crawler) Init(ctx context.Context, mode Mode) error {
	log.Infof("Initializing blockchain crawler in %s mode.", mode)
	var firstRun bool
	c.mode = mode

	// init chain state
	err := c.db.View(func(dbTx store.Tx) error {
		// read chain tip
		tip, err := dbLoadChainTip(dbTx)
		firstRun = err == ErrNoChainTip
		if firstRun {
			return nil
		}
		if err != nil {
			return err
		}
		c.updateTip(tip)
		c.chainId = tip.ChainId.Clone()
		// check manifest, allow empty
		mft, err := dbTx.Manifest()
		if err != nil {
			return fmt.Errorf("Reading database manifest: %v", err)
		}
		if have, want := mft.Name, stateDBKey; have != want {
			return fmt.Errorf("Invalid database name %s (expected %s)", have, want)
		}
		if have, want := mft.Version, stateDBSchemaVersion; have < want {
			return fmt.Errorf("Deprecated database version %d detected. This software only supports version %d. Please rebuild the database from scratch.", have, want)
		} else if have > want {
			return fmt.Errorf("Newer database version %d detected. This software only supports up to version %d. Please upgrade.", have, want)
		}
		if have, want := mft.Schema, stateDBSchemaName; have < want {
			return fmt.Errorf("Deprecated database schema %s detected. This software only supports schema version %s. Please rebuild the database from scratch.", have, want)
		} else if have > want {
			return fmt.Errorf("Newer database schema %s detected. This software only supports schema %s. Please upgrade.", have, want)
		}
		if have, want := mft.Label, tezos.Symbol; have != want {
			return fmt.Errorf("Invalid database label %s, expected %s.", have, want)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if firstRun {
		// create initial state
		log.Info("Creating blockchain storage.")
		tip := &model.ChainTip{
			BestHeight: -1,
			Name:       tezos.Name,
			Symbol:     tezos.Symbol,
		}
		c.updateTip(tip)
		err = c.db.Update(func(dbTx store.Tx) error {
			// indexer manifest
			err := dbTx.SetManifest(store.Manifest{
				Name:    stateDBKey,
				Label:   tezos.Symbol,
				Version: stateDBSchemaVersion,
				Schema:  stateDBSchemaName,
			})
			if err != nil {
				return err
			}

			// create state bucket
			_, err = dbTx.Root().CreateBucketIfNotExists(tipBucketName)
			return err
		})
		if err != nil {
			return err
		}
	}

	tip := c.Tip()
	// log.Tracef("Chain tip: %s", util.JsonString(tip))

	// init table manager (this will init all registered indexers in order)
	if c.indexer != nil {
		// open databases and tables
		if err = c.indexer.Init(ctx, tip, mode); err != nil {
			return fmt.Errorf("indexer init: %v", err)
		}
	}

	// skip RPC init if not required
	if c.rpc == nil || mode == MODE_INFO {
		c.setState(STATE_STOPPED, MONITOR_DISABLE)
		return nil
	}

	// wait for RPC to become ready
	c.setState(STATE_CONNECTING, MONITOR_KEEP)
	log.Info("Connecting to RPC server.")
	for {
		if err := c.fetchBlockchainInfo(ctx); err != nil {
			if err == context.Canceled {
				c.setState(STATE_STOPPED, MONITOR_DISABLE)
				return err
			}
			log.Errorf("Connection failed: %s", err)
			select {
			case <-ctx.Done():
				c.setState(STATE_STOPPED, MONITOR_DISABLE)
				return ctx.Err()
			case <-time.After(5 * time.Second):
				c.setState(STATE_CONNECTING, MONITOR_KEEP)
			}
		} else {
			break
		}
	}

	// fetch and index genesis block
	if firstRun {
		log.Info("Fetching genesis block.")
		tzblock, err := c.fetchBlock(ctx, rpc.Genesis)
		if err != nil {
			c.setState(STATE_FAILED, MONITOR_DISABLE)
			return err
		}

		// build a new genesis block from rpc.Block
		genesis, err := c.builder.Build(ctx, tzblock)
		if err != nil {
			c.setState(STATE_FAILED, MONITOR_DISABLE)
			return err
		}
		log.Infof("Crawling Tezos %s.", genesis.Params.Network)

		// Note: block index lives in separate DB
		if c.indexer != nil {
			// register genesis protocol
			genesis.Params.StartHeight = 0
			genesis.Params.Version = -1
			if err := c.indexer.ConnectProtocol(ctx, genesis.Params, nil); err != nil {
				return err
			}

			// add to all indexes
			if err := c.indexer.ConnectBlock(ctx, genesis, c.builder); err != nil {
				c.setState(STATE_FAILED, MONITOR_DISABLE)
				return err
			}
			// keep as best block
			newTip := tip.Clone()
			newTip.BestHash = genesis.Hash
			newTip.BestHeight = genesis.Height
			newTip.BestId = genesis.RowId
			newTip.BestTime = genesis.Timestamp
			newTip.GenesisTime = genesis.Timestamp
			newTip.ChainId = genesis.Params.ChainId
			newTip.AddDeployment(genesis.Params)
			c.updateTip(newTip)
			c.chainId = genesis.Params.ChainId.Clone()
		}

		c.builder.Clean()

	} else {
		// p := c.ParamsByHeight(0).WithChainId(c.chainId)
		log.Infof("Crawling Tezos %s.", c.ParamsByHeight(0).Network)

		// init block builder state
		if err = c.builder.Init(ctx, tip, c.rpc); err != nil {
			return fmt.Errorf("builder init: %v", err)
		}

		// retry reporting in case the last try failed
		if mode == MODE_SYNC {
			// retry database snapshot in case it failed last time
			if err := c.MaybeSnapshot(ctx); err != nil {
				c.setState(STATE_FAILED, MONITOR_DISABLE)
				return fmt.Errorf("Snapshot failed at block %d: %v", c.Height(), err)
			}
		}
	}

	return nil
}

// run goroutine
func (c *Crawler) Start() {
	log.Info("Starting blockchain crawler.")
	go c.syncBlockchain()
}

// close quit channel
func (c *Crawler) Stop(ctx context.Context) {
	// run only once
	select {
	case <-c.quit:
		return
	default:
	}
	log.Info("Stopping blockchain crawler.")
	// signal close to ingest thread
	close(c.quit)

	// convert wait group end into channel
	done := make(chan struct{})
	go func() {
		// wait for sync, ingest and monitor goroutines to exit
		c.wg.Wait()
		// done may have been closed
		select {
		case <-done:
			return
		default:
		}
		// signal we're done
		done <- struct{}{}
	}()

	// prepare shutdown timeout
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	select {
	// wait until timeout
	case <-ctx.Done():
	// or until all goroutines have exited
	case <-done:
		close(done)
	}

	// force-shutdown blocking tasks, i.e. unblock downstream report buffers
	// and cancel long-running downstream RPC/HTTP calls
	c.cancel()

	// wait for done channel to become readable or closed,
	// meaning all goroutines have exited by now
	<-done
	c.setState(STATE_STOPPED, MONITOR_DISABLE)
	log.Info("Stopped blockchain crawler.")
}

func (c *Crawler) runMonitor(next chan<- tezos.BlockHash) {
	log.Infof("Starting blockchain monitor.")
	c.wg.Add(1)
	defer c.wg.Done()
	var mon *rpc.BlockHeaderMonitor
	defer func() {
		c.disableMonitor()
		if mon != nil {
			mon.Close()
		}
	}()
	for {
		// check context cancellation
		select {
		case <-c.quit:
			log.Infof("Exiting monitor loop on quit.")
			return
		case <-c.ctx.Done():
			log.Infof("Exiting monitor loop on cancelled context.")
			return
		default:
		}

		// (re)connect
		if mon == nil {
			mon = rpc.NewBlockHeaderMonitor()
			// may block when no HTTP headers are sent
			if err := c.rpc.MonitorBlockHeader(c.ctx, mon); err != nil {
				if err != context.Canceled {
					log.Errorf("monitor error: %s", err)
				}
				c.disableMonitor()
				mon.Close()
				mon = nil
				// wait 5 sec, but also return on shutdown
				select {
				case <-c.quit:
					log.Infof("Exiting monitor loop on quit.")
					return
				case <-c.ctx.Done():
					log.Infof("Exiting monitor loop on cancel.")
					return
				case <-time.After(5 * time.Second):
				}
				continue
			}
		}

		// wait for message
		head, err := mon.Recv(c.ctx)
		// reconnect on error unless context was cancelled
		if err != nil {
			if err == context.Canceled {
				log.Infof("Exiting monitor loop on cancel.")
				return
			}
			// prepare for reconnect
			mon.Close()
			mon = nil
			log.Debugf("Exiting monitor mode on error: %v", err)
			c.disableMonitor()
			continue
		}

		// skip messages until we have caught up with chain head
		if _, ok := c.getState(); !ok {
			// log.Infof("Monitor skipping block %d %s (not synchronized)", head.Level, head.Hash)
			continue
		}

		// in any case, update blockchain info, ignore error
		_ = c.fetchBlockchainInfo(c.ctx)

		// check for shutdown again
		select {
		case <-c.quit:
			return
		case <-c.ctx.Done():
			return
		default:
		}

		// then forward to avoid send on closed channel
		select {
		case next <- head.Hash:
			log.Debugf("crawler: monitor new block %d %s", head.Level, head.Hash)
		default:
			log.Debugf("crawler: monitor send on full channel, skipping block")
		}
	}
}

func (c *Crawler) runIngest(next chan tezos.BlockHash) {
	// on shutdown wait for this goroutine to stop
	log.Infof("Starting blockchain ingest.")
	c.wg.Add(1)
	defer c.wg.Done()
	defer close(next)
	defer close(c.finalized)

	// init current state
	var nextHash tezos.BlockHash
	lastblock := c.Tip().BestHeight

	// setup periodic updates
	tick := util.NewWallTicker(10*time.Second, 0)
	defer func() {
		tick.Stop()
	}()

	for {
		var (
			state  State
			useMon bool
		)
		select {
		case <-c.quit:
			log.Infof("Stopping blockchain ingest on quit.")
			return
		case <-c.ctx.Done():
			log.Infof("Stopping blockchain ingest on cancel.")
			return
		case <-tick.C:
			// log.Debugf("Tick %s", c.useMonitor)
			_, useMon = c.getState()
			if !useMon {
				// this helps survive a broken monitoring channel
				if err := c.fetchBlockchainInfo(c.ctx); err != nil {
					atomic.StoreInt64(&c.head, 0)
				}
				select {
				case next <- tezos.BlockHash{}:
				default:
				}
			}
			continue
		case nextHash = <-next:
			state, useMon = c.getState()
			// on startup, check if we're already synchronized even
			// without having processed a block
			if !nextHash.IsValid() && state == STATE_CONNECTING {
				if c.Height()+c.delay == c.head {
					c.setState(STATE_SYNCHRONIZED, MONITOR_KEEP, "Already synchronized. Starting in monitor mode.")
				}
			}
			// process next block
		}

		// on missing bchead, wait and retry
		if c.head == 0 {
			log.Warn("crawler: broken RPC connection. Trying again in 5s...")
			// keep going
			select {
			case <-c.quit:
			case <-c.ctx.Done():
			case <-time.After(5 * time.Second):
				c.setState(STATE_CONNECTING, MONITOR_KEEP)
				if err := c.fetchBlockchainInfo(c.ctx); err == nil {
					log.Info("crawler: RPC connection OK.")
				}
				select {
				case next <- nextHash:
				default:
				}
			}
			continue
		}

		// update bchead when last block is higher
		if lastblock > c.head {
			if err := c.fetchBlockchainInfo(c.ctx); err != nil {
				continue
			}
		}

		// prefetch block
		if lastblock < c.head || nextHash.IsValid() {
			if c.ctx.Err() != nil {
				continue
			}

			var (
				tzblock *rpc.Bundle
				err     error
			)
			if nextHash.IsValid() {
				// log.Debugf("crawler: fetching next block %s", nextHash)
				tzblock, err = c.fetchBlock(c.ctx, nextHash)
			} else {
				// log.Debugf("crawler: fetching next block %d", lastblock+1)
				tzblock, err = c.fetchBlock(c.ctx, rpc.BlockLevel(lastblock+1))
			}
			if err != nil {
				tzblock = nil
			}

			// be resilient to network errors
			if tzblock != nil {
				// push block through reorg/delay filter and into finalized queue; may block
				// log.Debugf("crawler: queuing block %d %s q=%d", tzblock.Height(), tzblock.Hash(), len(c.finalized))
				err := c.filter.Push(c.ctx, tzblock, c.quit)
				if err != nil {
					switch err {
					case ErrGapDetected:
						// leave monitor mode and fall back to crawling
						log.Debugf("crawler: gap size %d detected. Falling back to crawl mode.", tzblock.Height()-lastblock-1)
						useMon = c.setState(STATE_SYNCHRONIZING, MONITOR_DISABLE)

					case ErrReorgDetected:
						// clear filter, leave monitor mode and fall back to crawling
						log.Debugf("crawler: reorg detected at %d. Falling back to crawl mode.", tzblock.Height())
						c.filter.Reset()
						useMon = c.setState(STATE_SYNCHRONIZING, MONITOR_DISABLE)
						lastblock -= c.delay
						// give the node/proxy some time to catch up
						time.Sleep(time.Second)

					default:
						// quit signal
						continue
					}
				} else {
					// continue with next block (may be empty when at tip)
					lastblock = tzblock.Height()
				}

				// stop request
				if c.stopHeight > 0 && lastblock >= c.stopHeight {
					log.Infof("crawler: stopping ingest at requested block %d", c.stopHeight)
					tick.Stop()
					return
				}

				// continue with next block unless we used to be synchronized once;
				// Note that on Tezos there are no forward links to newer blocks,
				// so we always fetch by height
				if !useMon {
					select {
					case next <- tezos.BlockHash{}:
					default:
					}
				}
			} else {
				// handle the case where we did not receive a new block from the node
				// log.Debugf("Block %d download failed: %s", lastblock+1, err)
				if c.ctx.Err() != nil {
					continue
				}

				// reset last block
				lastblock = c.Height()
				nextHash = tezos.ZeroBlockHash

				// handle RPC errors (wait and retry)
				switch e := err.(type) {
				case rpc.RPCError:
					log.Warnf("crawler: RPC: %s", e.Error())
					c.setState(STATE_WAITING, MONITOR_KEEP)
				default:
					c.setState(STATE_CONNECTING, MONITOR_KEEP)
					log.Warnf("crawler: RPC connection error: %s", err)
				}
				// wait 5 sec (also stop on shutdown)
				select {
				case <-c.quit:
				case <-c.ctx.Done():
				case <-time.After(5 * time.Second):
				}
				// on error, retry with current hash
				select {
				case next <- nextHash:
				default:
				}
			}
		}
	}
}

func (c *Crawler) ingest(_ context.Context) {
	// internal next block signal to prefetch blocks; may hold an empty
	// hash (initially, after errors, and on ticks) or a block hash
	// when received via monitoring (monitor may break, so we don't rely on it)
	next := make(chan tezos.BlockHash, cap(c.finalized))

	if c.enableMonitor {
		// run monitor loop in go-routine
		go c.runMonitor(next)
	}

	// run ingest loop in go-routine
	go c.runIngest(next)

	// kick off ingest loop
	next <- tezos.BlockHash{}
}

func drain(c chan *rpc.Bundle) {
	if len(c) == 0 {
		return
	}
	for {
		select {
		case <-c:
			if len(c) > 0 {
				// keep going until flushed
				continue
			}
			return
		default:
			return
		}
	}
}

func (c *Crawler) syncBlockchain() {
	c.wg.Add(1)
	defer c.wg.Done()

	// derive private context
	c.ctx, c.cancel = context.WithCancel(context.Background())
	ctx := c.ctx
	defer c.cancel()
	tip := c.Tip()

	// do not sync when after requested stop
	if c.stopHeight > 0 && tip.BestHeight >= c.stopHeight {
		log.Infof("Skipping blockchain sync. Already after block height %d.", c.stopHeight)
		return
	}

	// run ingest goroutine
	c.ingest(ctx)
	defer drain(c.finalized)

	var (
		tzblock    *rpc.Bundle
		ctxNonStop = context.Background()
	)

	log.Infof("Starting blockchain sync from height %d.", tip.BestHeight+1)
	c.setState(STATE_SYNCHRONIZING, MONITOR_KEEP)

	// recover from panic
	defer func() {
		if e := recover(); e != nil {
			// e might not be error type, e.g. when panic is thrown by Go Std Library
			// (e.g. from reflect package)
			log.Errorf("Unrecoverable error: %s", e)
			// generate stack trace as json
			trace := debug.Stack()
			// log.Debugf("%s", string(trace))
			lines := make([]string, 0, bytes.Count(trace, []byte("\n"))+1)
			for _, v := range bytes.Split(trace, []byte("\n")) {
				if len(v) == 0 {
					continue
				}
				lines = append(lines, string(v))
			}
			js := struct {
				Stack []string `json:"stack"`
			}{
				Stack: lines,
			}
			log.Error(util.JsonString(js))
			log.Infof("Stopping blockchain sync at height %d.", tip.BestHeight)
			c.setState(STATE_FAILED, MONITOR_DISABLE)
		}

		// flush indexer journals on failure (may take some time)
		if state, _ := c.getState(); state == STATE_FAILED {
			if err := c.indexer.FlushJournals(ctx); err != nil {
				log.Errorf("flushing tables: %s", err)
			}
		}

		// store last chain state
		err := c.db.Update(func(dbTx store.Tx) error {
			return dbStoreChainTip(dbTx, tip)
		})
		if err != nil {
			log.Errorf("Updating database for block %d: %s", tip.BestHeight, err)
		}
		c.cancel()
	}()

	// process new blocks as they arrive
	for {
		select {
		case <-c.quit:
			c.setState(STATE_STOPPING, MONITOR_DISABLE)
			log.Infof("Stopping blockchain sync at height %d.", tip.BestHeight)
			return
		case <-ctx.Done():
			c.setState(STATE_STOPPING, MONITOR_DISABLE)
			log.Infof("Context aborted. Stopping blockchain sync at height %d.", tip.BestHeight)
			return
		case tzblock = <-c.finalized:
			// process finalized blocks, all blocks arriving from this queue
			// are expected to be in order, gap-free and reorg-free; we may
			// drop all reorg handling code here and in builder/indexers
			// when Tenderbake goes live and we set a sufficient margin
			if tzblock == nil {
				log.Infof("Stopping blockchain sync at height %d.", tip.BestHeight)
				return
			}
			// log.Tracef("Processing block %d %s", tzblock.Height(), tzblock.Hash())
			if c.head > tzblock.Height()+c.delay {
				c.setState(STATE_SYNCHRONIZING, MONITOR_KEEP)
			}
		}

		// under very rare conditions (tick and monitor triggered the same block download,
		// one via height, the other via hash) we may see a duplicate block in the
		// ingest queue; check and discard
		if tip.BestHeight > 1 && tip.BestHash == tzblock.Hash() {
			continue
		}

		// time block processing
		blockstart := time.Now()

		// detect and process reorg (skip for genesis block)
		if tip.BestHeight > 0 && tip.BestHash != tzblock.ParentHash() {
			log.Infof("Reorg at height %d: parent %s is not on our main chain, have %s",
				tzblock.Block.Header.Level, tzblock.Block.Header.Predecessor, tip.BestHash)

			// those are the two blocks between the reorg will switch
			tipblock := c.builder.parent
			bestblock, err := model.NewBlock(tzblock, nil)
			if err != nil {
				log.Errorf("Reorg failed: %s", err)
				break
			}

			// run reorg
			if err = c.reorganize(ctx, tipblock, bestblock, false, false); err != nil {
				log.Errorf("Reorg failed: %s", err)
				break
			}

			// update local tip copy after reorg was successful
			newtip := c.Tip()
			// safety check for non zero parent id
			if newtip.BestId == 0 {
				log.Errorf("Zero parent id after reorg for parent block %d %s", newtip.BestHeight, newtip.BestHash)
				break
			}
			tip = newtip
		}

		// shutdown check
		if ctx.Err() != nil {
			continue
		}

		// assemble block data and statistics; will lookup and create new accounts
		block, err := c.builder.Build(ctx, tzblock)
		if err != nil {
			// rollback inserted accounts, ops and flows here
			log.Errorf("Processing block %d %s: %s", tzblock.Height(), tzblock.Hash(), err)
			if err = c.indexer.DeleteBlock(ctx, tzblock); err != nil {
				log.Errorf("Rollback of data for failed block %d: %s", tzblock.Height(), err)
			}
			break
		}

		//
		// CRITICAL SECTION BEGIN (execute atomic to protect DB state)
		//

		// update indexes
		if err = c.indexer.ConnectBlock(ctxNonStop, block, c.builder); err != nil {
			log.Errorf("Connecting block %d: %s", block.Height, err)
			break
		}

		// update chain tip
		newTip := &model.ChainTip{
			Name:        tip.Name,
			Symbol:      tip.Symbol,
			ChainId:     tip.ChainId,
			BestHash:    block.Hash,
			BestId:      block.RowId,
			BestHeight:  block.Height,
			BestTime:    block.Timestamp,
			GenesisTime: tip.GenesisTime,
			NYEveBlocks: tip.NYEveBlocks,
			Deployments: tip.Deployments,
		}

		if newTip.GenesisTime.AddDate(len(newTip.NYEveBlocks)+1, 0, 0).Before(block.Timestamp) {
			newTip.NYEveBlocks = append(newTip.NYEveBlocks, block.Height)
			log.Infof("Happy New Blockchain Year %d at block %d!", len(newTip.NYEveBlocks)+1, block.Height)
		}
		// update deployments on protocol upgrade
		if block.IsProtocolUpgrade() {
			newTip.AddDeployment(block.Params)
		}

		// update chainstate with new version
		c.updateTip(newTip)
		tip = newTip

		//
		// CRITICAL SECTION END
		//

		// check for shutdown signal
		if ctx.Err() != nil {
			continue
		}

		// current block may be ahead of bcinfo by one
		if block.Height+c.delay >= c.head {
			c.setState(STATE_SYNCHRONIZED, MONITOR_KEEP)
		}
		state, _ := c.getState()

		if state == STATE_SYNCHRONIZED {
			// flush journals every block when synchronized
			if err := c.indexer.FlushJournals(ctx); err != nil {
				log.Errorf("flushing tables: %s", err)
			}

			// tell indexers to finalize (i.e. build table indexes)
			if err := c.indexer.Finalize(ctx); err != nil {
				log.Errorf("finalizing tables: %v", err)
			}

			// update rights cache at cycle start (or later when we we're not in sync then)
			if err := c.indexer.updateRights(ctx, block.Height); err != nil {
				log.Errorf("updating rights cache: %s", err)
			}
		}

		// log progress once every 10sec
		c.plog.LogBlockHeight(block, len(c.finalized), state, time.Since(blockstart), state == STATE_SYNCHRONIZED)

		// update state every 256 blocks or every block when synchronized
		if state == STATE_SYNCHRONIZED || block.Height&0xff == 0 {
			err := c.db.Update(func(dbTx store.Tx) error {
				return dbStoreChainTip(dbTx, tip)
			})
			if err != nil {
				log.Errorf("Updating state database for block %d: %s", tip.BestHeight, err)
				break
			}
		}

		// database snapshots
		if err := c.MaybeSnapshot(ctx); err != nil {
			log.Errorf("Snapshot failed at block %d: %s", tip.BestHeight, err)
		}

		if c.stopHeight > 0 && tip.BestHeight >= c.stopHeight {
			log.Infof("Stopping blockchain sync after block height %d.", tip.BestHeight)
			return
		}

		// clean builder state
		c.builder.Clean()
	}

	// handle failures
	c.builder.Purge()
	log.Infof("Stopping blockchain sync due to too many errors at %d.", tip.BestHeight)
	c.setState(STATE_FAILED, MONITOR_DISABLE)
}

func (c *Crawler) fetchBlock(ctx context.Context, id rpc.BlockID) (b *rpc.Bundle, err error) {
	p := c.indexer.reg.GetParamsLatest()
	if c.indexer.lightMode {
		b, err = c.rpc.GetLightBundle(ctx, id, p)
	} else {
		b, err = c.rpc.GetFullBundle(ctx, id, p)
	}
	if err != nil {
		return
	}

	// check chain match
	if c.chainId.IsValid() && c.chainId != b.Block.ChainId {
		err = fmt.Errorf("fetch: invalid chain %s (expected %s)", b.Block.ChainId, c.chainId)
		return
	}

	// check for protocol update
	if p == nil || p.Version < b.Params.Version {
		// register params with indexer
		// this will make them available when fetching next blocks,
		// but not yet registered as deployment
		log.Debugf("New params for proto %s v%03d from %d",
			b.Params.Protocol, b.Params.Version, b.Params.StartHeight)
		c.indexer.reg.Register(b.Params)
	}
	return b, nil
}

func (c *Crawler) fetchBlockchainInfo(ctx context.Context) error {
	head, err := c.rpc.GetTipHeader(ctx)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&c.head, head.Level)
	return nil
}
