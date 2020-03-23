// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

const (
	StateDBName = "state.db"

	// state database schema
	stateDBSchemaVersion = 2
	stateDBKey           = "statedb"
)

type Mode string

const (
	MODE_SYNC     Mode = "sync"
	MODE_INFO     Mode = "info"
	MODE_ROLLBACK Mode = "rollback"
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
)

type CrawlerConfig struct {
	DB            store.DB
	Indexer       *Indexer
	Client        *rpc.Client
	Queue         int
	StopBlock     int64
	Snapshot      *SnapshotConfig
	EnableMonitor bool
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

	db      store.DB
	rpc     *rpc.Client
	builder *Builder
	indexer *Indexer
	queue   chan *Bundle
	plog    *BlockProgressLogger
	params  *chain.Params
	tip     *ChainTip
	bchead  *rpc.BlockHeader

	// coordinated shutdown
	quit   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// coordinated snapshot
	snapch chan error
}

func NewCrawler(cfg CrawlerConfig) *Crawler {
	return &Crawler{
		state:         STATE_LOADING,
		mode:          MODE_SYNC,
		snap:          cfg.Snapshot,
		useMonitor:    false,
		enableMonitor: cfg.EnableMonitor,
		stopHeight:    cfg.StopBlock,
		db:            cfg.DB,
		rpc:           cfg.Client,
		builder:       NewBuilder(cfg.Indexer),
		indexer:       cfg.Indexer,
		queue:         make(chan *Bundle, cfg.Queue),
		params:        chain.NewParams(),
		plog:          NewBlockProgressLogger("Processed"),
		quit:          make(chan struct{}),
	}
}

func (c *Crawler) Tip() *ChainTip {
	return c.tip
}

func (c *Crawler) Height() int64 {
	return c.tip.BestHeight
}

func (c *Crawler) Time() time.Time {
	return c.tip.BestTime
}

func (c *Crawler) ParamsByHeight(height int64) *chain.Params {
	if height < 0 {
		height = c.Height()
	}
	return c.indexer.ParamsByHeight(height)
}

func (c *Crawler) ParamsByProtocol(proto chain.ProtocolHash) *chain.Params {
	p, _ := c.indexer.ParamsByProtocol(proto)
	return p
}

func (c *Crawler) Table(key string) (*pack.Table, error) {
	return c.indexer.Table(key)
}

func (c *Crawler) ChainByHeight(ctx context.Context, height int64) (*Chain, error) {
	return c.indexer.ChainByHeight(ctx, height)
}

func (c *Crawler) SupplyByHeight(ctx context.Context, height int64) (*Supply, error) {
	return c.indexer.SupplyByHeight(ctx, height)
}

func (c *Crawler) BlockByHeight(ctx context.Context, height int64) (*Block, error) {
	return c.indexer.BlockByHeight(ctx, height)
}

type CrawlerStatus struct {
	Status   State   `json:"status"`
	Blocks   int64   `json:"blocks"`
	Indexed  int64   `json:"indexed"`
	Progress float64 `json:"progress"`
}

func (c *Crawler) Status() CrawlerStatus {
	tip := c.Tip()
	s := CrawlerStatus{
		Status:  c.state,
		Blocks:  -1,
		Indexed: tip.BestHeight,
	}
	if tip.BestHeight > 0 && c.bchead != nil && c.bchead.Level > 0 {
		s.Blocks = c.bchead.Level
		s.Progress = float64(s.Indexed) / float64(s.Blocks)
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
		var err error
		c.tip, err = dbLoadChainTip(dbTx)
		firstRun = err == ErrNoChainTip
		if firstRun {
			return nil
		}
		if err != nil {
			return err
		}
		// check manifest, allow empty
		mft, err := dbTx.Manifest()
		if err != nil {
			return fmt.Errorf("reading manifest: %v", err)
		}
		if have, want := mft.Name, stateDBKey; have != want {
			return fmt.Errorf("invalid state DB name %s (expected %s)", have, want)
		}
		if have, want := mft.Version, stateDBSchemaVersion; have != want {
			return fmt.Errorf("invalid state DB schema version %d (expected version %d)", have, want)
		}
		if have, want := mft.Label, c.params.Symbol; have != want {
			return fmt.Errorf("invalid state DB label %s (expected %s)", have, want)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if firstRun {
		// create initial state
		log.Info("Creating blockchain storage.")
		c.tip = &ChainTip{
			BestHeight: -1,
			Name:       c.params.Name,
			Symbol:     c.params.Symbol,
		}
		err = c.db.Update(func(dbTx store.Tx) error {
			// indexer manifest
			err := dbTx.SetManifest(store.Manifest{
				Name:    stateDBKey,
				Version: stateDBSchemaVersion,
				Label:   c.params.Symbol,
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

	// init table manager (this will init all registered indexers in order)
	if c.indexer != nil {
		// open databases and tables
		if err = c.indexer.Init(ctx, tip); err != nil {
			return err
		}
	}

	// skip RPC init if not required
	if c.rpc == nil || mode == MODE_INFO {
		c.state = STATE_STOPPED
		return nil
	}

	// wait for RPC to become ready
	c.state = STATE_CONNECTING
	log.Info("Connecting to RPC server.")
	for {
		if err := c.fetchBlockchainInfo(ctx); err != nil {
			if err == context.Canceled {
				c.state = STATE_STOPPED
				return err
			}
			log.Errorf("Connection failed: %v", err)
			select {
			case <-ctx.Done():
				c.state = STATE_STOPPED
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		} else {
			break
		}
	}

	// fetch and index genesis block
	if firstRun {
		log.Info("Fetching genesis block.")
		tzblock, err := c.fetchBlockByHeight(ctx, 0)
		if err != nil {
			c.state = STATE_FAILED
			return err
		}

		// build a new genesis block from rpc.Block
		genesis, err := c.builder.Build(ctx, tzblock)
		if err != nil {
			c.state = STATE_FAILED
			return err
		}
		log.Infof("Crawling %s %s.", genesis.Params.Name, genesis.Params.Network)

		// Note: block index lives in separate DB
		if c.indexer != nil {
			// register genesis protocol
			genesis.Params.StartHeight = 0
			c.indexer.ConnectProtocol(ctx, genesis.Params)

			// add to all indexes
			if err := c.indexer.ConnectBlock(ctx, genesis, c.builder); err != nil {
				c.state = STATE_FAILED
				return err
			}
			// keep as best block
			c.tip.BestHash = genesis.Hash
			c.tip.BestHeight = genesis.Height
			c.tip.BestId = genesis.RowId
			c.tip.BestTime = genesis.Timestamp
			c.tip.GenesisTime = genesis.Timestamp
			c.tip.ChainId = genesis.Params.ChainId
			c.tip.AddDeployment(genesis.Params)
		}

		c.builder.Clean()

	} else {
		p := c.ParamsByHeight(0).ForNetwork(c.tip.ChainId)
		log.Infof("Crawling %s %s.", p.Name, p.Network)

		// init block builder state
		if err = c.builder.Init(ctx, tip, c.rpc); err != nil {
			return err
		}

		if mode == MODE_SYNC {
			// retry database snapshot in case it failed last time
			if err := c.MaybeSnapshot(ctx); err != nil {
				c.state = STATE_FAILED
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

	// force-shutdown blocking tasks, i.e. unblock downstream indexers
	// and cancel long-running RPC/HTTP calls
	c.cancel()

	// wait for done channel to become readable or closed,
	// meaning all goroutines have exited by now
	<-done
	c.state = STATE_STOPPED
	log.Info("Stopped blockchain crawler.")
}

func (c *Crawler) runMonitor(next chan<- chain.BlockHash) {
	log.Infof("Starting blockchain monitor.")
	c.wg.Add(1)
	defer c.wg.Done()
	var mon *rpc.BlockHeaderMonitor
	defer func() {
		c.useMonitor = false
		if mon != nil {
			mon.Close()
		}
	}()
	for {
		// check context cancellation
		select {
		case <-c.quit:
			log.Errorf("Exiting monitor loop on quit.")
			return
		case <-c.ctx.Done():
			log.Errorf("Exiting monitor loop on cancelled context.")
			return
		default:
		}

		// (re)connect
		if mon == nil {
			mon = rpc.NewBlockHeaderMonitor()
			// may block when no HTTP headers are sent
			if err := c.rpc.MonitorBlockHeader(c.ctx, mon); err != nil {
				if err != context.Canceled {
					log.Errorf("monitor error: %v", err)
				}
				c.useMonitor = false
				mon.Close()
				mon = nil
				// wait 5 sec, but also return on shutdown
				select {
				case <-c.quit:
					log.Errorf("Exiting monitor loop on quit.")
					return
				case <-c.ctx.Done():
					log.Errorf("Exiting monitor loop on cancelled context.")
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
				log.Errorf("Exiting monitor loop on cancelled context")
				return
			}
			// prepare for reconnect
			mon.Close()
			mon = nil
			c.useMonitor = false
			continue
		}

		// skip messages until we have caught up with chain head
		if !c.useMonitor {
			log.Debugf("Monitor skipping block %d %s (not synchronized)", head.Level, head.Hash)
			continue
		}

		// in any case, update blockchain info, ignore error
		c.fetchBlockchainInfo(c.ctx)

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
			log.Debugf("Monitor new block %d %s", head.Level, head.Hash)
		default:
			log.Debugf("Monitor send on full channel, skipping block")
		}
	}
}

func (c *Crawler) runIngest(next chan chain.BlockHash) {
	// on shutdown wait for this goroutine to stop
	log.Infof("Starting blockchain ingest.")
	c.wg.Add(1)
	defer c.wg.Done()
	defer close(next)
	defer close(c.queue)

	// init current state
	var nextHash chain.BlockHash
	lastblock := c.Tip().BestHeight

	// setup periodic updates
	tick := util.NewWallTicker(20*time.Second, 0)
	defer func() {
		tick.Stop()
	}()

	for {
		select {
		case <-tick.C:
			if !c.useMonitor {
				// this helps survive a broken monitoring channel
				if err := c.fetchBlockchainInfo(c.ctx); err != nil {
					c.bchead = nil
				}
				select {
				case next <- chain.BlockHash{}:
				default:
				}
			}
			continue
		case <-c.quit:
			log.Infof("Stopping blockchain ingest.")
			return
		case <-c.ctx.Done():
			log.Infof("Context cancelled. Stopping blockchain ingest.")
			return
		case nextHash = <-next:
			// process next block
			if !nextHash.IsValid() {
				c.Lock()
				// on startup, check if we're already synchronized even
				// without having processed a block
				if c.state == STATE_CONNECTING && c.bchead != nil && c.tip != nil {
					if c.tip.BestHeight == c.bchead.Level {
						c.state = STATE_SYNCHRONIZED
						if c.enableMonitor {
							c.useMonitor = true
							log.Info("Already synchronized. Starting in monitor mode.")
						}
					}
				}
				c.Unlock()
			}
		}

		// on missing bchead, wait and retry
		if c.bchead == nil {
			c.Lock()
			c.state = STATE_CONNECTING
			c.Unlock()
			log.Warn("Broken RPC connection. Trying again in 5s...")
			// keep going
			select {
			case <-c.quit:
			case <-c.ctx.Done():
			case <-time.After(5 * time.Second):
				if err := c.fetchBlockchainInfo(c.ctx); err == nil {
					log.Info("RPC connection OK.")
				}
				select {
				case next <- nextHash:
				default:
				}
			}
			continue
		}

		// update bchead when last block is higher
		if lastblock > c.bchead.Level {
			if err := c.fetchBlockchainInfo(c.ctx); err != nil {
				continue
			}
		}

		// prefetch block
		if lastblock < c.bchead.Level || nextHash.IsValid() {
			if util.InterruptRequested(c.ctx) {
				continue
			}
			log.Tracef("Fetching next block %d %s", lastblock+1, nextHash)

			var (
				tzblock *Bundle
				err     error
			)
			if nextHash.IsValid() {
				tzblock, err = c.fetchBlockByHash(c.ctx, nextHash)
			} else {
				tzblock, err = c.fetchBlockByHeight(c.ctx, lastblock+1)
			}

			// be resilient to network errors
			if tzblock != nil {
				// push block into queue; may block
				log.Tracef("Queuing block %d %s", tzblock.Height(), tzblock.Hash())
				select {
				case <-c.quit:
					continue
				case <-c.ctx.Done():
					continue
				case c.queue <- tzblock:
				}

				// continue with next block (may be empty when at tip)
				lastblock = tzblock.Height()

				// stop request
				if c.stopHeight > 0 && lastblock >= c.stopHeight {
					log.Infof("Stopping ingest at requested block %d", c.stopHeight)
					tick.Stop()
					return
				}

				// continue with next block unless we used to be synchronized once;
				// Note that on Tezos there are no forward links to newer blocks,
				// so we always fetch by height
				if !c.useMonitor {
					select {
					case next <- chain.BlockHash{}:
					default:
					}
				}
			} else {
				log.Debugf("Block %d download failed: %v", lastblock+1, err)
				if util.InterruptRequested(c.ctx) {
					continue
				}
				// reset last block
				lastblock = c.Tip().BestHeight

				// handle RPC errors (wait and retry)
				switch e := err.(type) {
				case rpc.RPCError:
					log.Warnf("RPC: %s", e.Error())
					c.Lock()
					c.state = STATE_WAITING
					c.Unlock()
				default:
					c.Lock()
					c.state = STATE_CONNECTING
					c.Unlock()
					log.Warnf("RPC connection error: %v", err)
				}
				time.Sleep(5 * time.Second)
				// on error, retry with current hash
				select {
				case next <- nextHash:
				default:
				}
			}
		}
	}
}

func (c *Crawler) ingest(ctx context.Context) {
	// internal next block signal to prefetch blocks; may hold an empty
	// hash (initially, after errors, and on ticks) or a block hash
	// when received via monitoring (monitor may break, so we don't rely on it)
	next := make(chan chain.BlockHash, cap(c.queue))

	if c.enableMonitor {
		// run monitor loop in go-routine
		go c.runMonitor(next)
	}

	// run ingest loop in go-routine
	go c.runIngest(next)

	// kick off ingest loop
	next <- chain.BlockHash{}
}

func drain(c chan *Bundle) {
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
	defer drain(c.queue)

	var (
		tzblock  *Bundle
		errCount int
	)

	log.Infof("Starting blockchain sync from height %d.", tip.BestHeight+1)

	// recover from panic
	defer func() {
		if e := recover(); e != nil {
			// e might not be error type, e.g. when panic is thrown by Go Std Library
			// (e.g. from reflect package)
			log.Errorf("Unrecoverable error: %v", e)
			// generate stack trace as json
			trace := debug.Stack()
			log.Debugf("%s", string(trace))
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
			buf, _ := json.Marshal(js)
			log.Error(string(buf))
			log.Infof("Stopping blockchain sync at height %d.", tip.BestHeight)
			c.state = STATE_FAILED
		}

		// flush indexer journals on failure (may take some time)
		if c.state == STATE_FAILED {
			if err := c.indexer.FlushJournals(ctx); err != nil {
				log.Errorf("flushing tables: %v", err)
			}
		}

		// store last chain state
		err := c.db.Update(func(dbTx store.Tx) error {
			return dbStoreChainTip(dbTx, tip)
		})
		if err != nil {
			log.Errorf("Updating database for block %d: %v", tip.BestHeight, err)
		}
		c.cancel()
	}()

	// process new blocks as they arrive
	for {
		select {
		case <-c.quit:
			c.Lock()
			c.state = STATE_STOPPING
			c.Unlock()
			log.Infof("Stopping blockchain sync at height %d.", tip.BestHeight)
			return
		case <-ctx.Done():
			c.Lock()
			c.state = STATE_STOPPING
			c.Unlock()
			log.Infof("Context aborted. Stopping blockchain sync at height %d.", tip.BestHeight)
			return
		case tzblock = <-c.queue:
			if tzblock == nil {
				log.Infof("Stopping blockchain sync at height %d.", tip.BestHeight)
				return
			}
			c.Lock()
			c.state = STATE_SYNCHRONIZING
			c.Unlock()
			log.Tracef("Processing block %d %s", tzblock.Height(), tzblock.Hash())
		}

	again:
		if errCount > 1 {
			log.Infof("Stopping blockchain sync due to too many errors at %d.", tip.BestHeight)
			c.Lock()
			c.state = STATE_FAILED
			c.Unlock()
			return
		}

		// under very rare conditions (tick and monitor triggered the same block download,
		// one via height, the other via hash) we may see a duplicate block in the
		// ingest queue; check and discard
		if tip.BestHeight > 1 && tip.BestHash.String() == tzblock.Hash().String() {
			continue
		}

		// time block processing
		blockstart := time.Now()

		// detect and process reorg (skip for genesis block)
		if tip.BestHeight > 0 && tip.BestHash.String() != tzblock.Parent().String() {
			log.Infof("Reorg at height %d: parent %s is not on our main chain, have %s",
				tzblock.Block.Header.Level, tzblock.Block.Header.Predecessor, tip.BestHash)

			// those are the two blocks between the reorg will switch
			tipblock := c.builder.parent
			bestblock, err := NewBlock(tzblock, nil)
			if err != nil {
				log.Errorf("Reorg failed: %v", err)
				errCount++
				goto again
			}

			// run reorg
			if err = c.reorganize(ctx, tipblock, bestblock, false, false); err != nil {
				log.Errorf("Reorg failed: %v", err)
				c.builder.Purge()
				if err := c.builder.Init(ctx, tip, c.rpc); err != nil {
					log.Errorf("Reinit failed: %v", err)
					errCount += 10
				} else {
					errCount++
				}
				goto again
			}

			// update local tip copy after reorg was successful
			newtip := c.Tip()
			// safety check for non zero parent id
			if newtip.BestId == 0 {
				log.Errorf("Zero parent id after reorg for parent block %d %s", newtip.BestHeight, newtip.BestHash)
				c.builder.Purge()
				if err := c.builder.Init(ctx, tip, c.rpc); err != nil {
					log.Errorf("Reinit failed: %v", err)
					errCount += 10
				} else {
					errCount++
				}
				goto again
			}
			tip = newtip
		}

		// shutdown check
		if util.InterruptRequested(ctx) {
			continue
		}

		// POINT OF NO RETURN

		// assemble block data and statistics; will lookup and create new accounts
		block, err := c.builder.Build(ctx, tzblock)
		if err != nil {
			// rollback inserted accounts, ops and flows here
			log.Errorf("Processing block %d %s: %v", tzblock.Height(), tzblock.Hash(), err)
			if err = c.indexer.DeleteBlock(ctx, tzblock); err != nil {
				log.Errorf("Rollback of data for failed block %d: %v", tzblock.Height(), err)
			}
			// pruge and reinit builder state to last successful block
			c.builder.Purge()
			if err := c.builder.Init(ctx, tip, c.rpc); err != nil {
				log.Errorf("Reinit failed: %v", err)
				errCount += 10
			} else {
				errCount++
			}
			errCount++
			goto again
		}

		// update indexes; will insert or update rows & generate unique ids
		// for blocks and flows
		if err = c.indexer.ConnectBlock(ctx, block, c.builder); err != nil {
			log.Errorf("Connecting block %d: %v", block.Height, err)
			errCount++
			goto again
		}

		// update chain tip
		newTip := &ChainTip{
			Name:          tip.Name,
			Symbol:        tip.Symbol,
			ChainId:       tip.ChainId,
			BestHash:      block.Hash,
			BestId:        block.RowId,
			BestHeight:    block.Height,
			BestTime:      block.Timestamp,
			GenesisTime:   tip.GenesisTime,
			NYEveBlocks:   tip.NYEveBlocks,
			QuarterBlocks: tip.QuarterBlocks,
			Deployments:   tip.Deployments,
		}

		// update blockchain years
		if newTip.GenesisTime.AddDate(0, 3*(len(newTip.QuarterBlocks)+1), 0).Before(block.Timestamp) {
			newTip.QuarterBlocks = append(newTip.QuarterBlocks, block.Height)
			log.Infof("Happy New Blockchain Quarter %d at block %d!", len(newTip.QuarterBlocks)+1, block.Height)
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
		c.Lock()
		c.params = block.Params
		c.tip = newTip
		tip = newTip
		c.Unlock()

		// trace progress
		log.Tracef("block %d ts=%s tx=%d/%d acc=%d/%d vol=%d rwd=%d fee=%d burn=%d q=%d\n",
			block.Height,
			block.Timestamp.Format(time.RFC3339),
			block.NOps,
			block.Chain.TotalOps,
			block.SeenAccounts,
			block.Chain.TotalAccounts,
			block.Volume,
			block.Rewards,
			block.Fees,
			block.BurnedSupply,
			len(c.queue),
		)

		// current block may be ahead of bcinfo by one
		c.Lock()
		if c.bchead != nil && block.Height >= c.bchead.Level {
			c.state = STATE_SYNCHRONIZED
			if c.enableMonitor {
				if !c.useMonitor {
					log.Info("Fully synchronized. Switching to monitor mode.")
				}
				c.useMonitor = true
			}
		}
		state := c.state
		c.Unlock()

		// flush journals every block when synchronized
		if state == STATE_SYNCHRONIZED {
			if err := c.indexer.FlushJournals(ctx); err != nil {
				log.Errorf("flushing tables: %v", err)
			}

			// rebuild ranking data
			if err := c.indexer.UpdateRanking(ctx, block.Timestamp); err != nil {
				log.Errorf("updating ranking: %v", err)
			}
		}

		// log progress once every 10sec
		c.plog.LogBlockHeight(block, len(c.queue), state, time.Since(blockstart))

		// update state every 256 blocks or every block when synchronized
		if state == STATE_SYNCHRONIZED || block.Height&0xff == 0 {
			err := c.db.Update(func(dbTx store.Tx) error {
				return dbStoreChainTip(dbTx, tip)
			})
			if err != nil {
				log.Errorf("Updating state database for block %d: %v", tip.BestHeight, err)
				c.Lock()
				c.state = STATE_FAILED
				c.Unlock()
				return
			}
		}

		// database snapshots
		if err := c.MaybeSnapshot(ctx); err != nil {
			log.Errorf("Snapshot failed at block %d: %v", tip.BestHeight, err)
			// only fail on configured snapshot blocks
			if c.snap != nil && (len(c.snap.Blocks) > 0 || c.snap.BlockInterval > 0) {
				c.Lock()
				c.state = STATE_FAILED
				c.Unlock()
				return
			}
		}

		if c.stopHeight > 0 && tip.BestHeight >= c.stopHeight {
			log.Infof("Stopping blockchain sync after block height %d.", tip.BestHeight)
			return
		}

		// remove build state
		c.builder.Clean()
		errCount = 0
	}
}

func (c *Crawler) fetchBlockByHash(ctx context.Context, blockID chain.BlockHash) (*Bundle, error) {
	b := &Bundle{}
	var err error
	if b.Block, err = c.rpc.GetBlock(ctx, blockID); err != nil {
		return nil, err
	}
	if c.tip.ChainId.IsValid() && !c.tip.ChainId.IsEqual(b.Block.ChainId) {
		return nil, fmt.Errorf("block init: invalid chain %s (expected %s)",
			b.Block.ChainId, c.tip.ChainId)
	}
	height := b.Block.Header.Level
	b.Params, err = c.indexer.reg.GetParams(b.Block.Protocol)
	needUpdate := b.Params != nil && b.Params.IsCycleStart(height)
	if err != nil || needUpdate {
		// fetch params from chain
		if height > 0 {
			cons, err := c.rpc.GetConstantsHeight(ctx, height)
			if err != nil {
				return nil, fmt.Errorf("block init: %v", err)
			}
			b.Params = cons.MapToChainParams()
		} else {
			b.Params = chain.NewParams()
		}
		// changes will be updated during build
		b.Params = b.Params.
			ForProtocol(b.Block.Protocol).
			ForNetwork(b.Block.ChainId)
		b.Params.Deployment = b.Block.Header.Proto
	}
	b.Cycle = b.Params.CycleFromHeight(height)

	// in monitor mode we are live, so we don't have to check for early cycles
	// still max look-ahead is 5 (e.g. PreservedCycles)
	if b.Params.IsCycleStart(height) {
		// snapshot index and rights for future cycle N; the snapshot index
		// refers to a snapshot block taken in cycle N-7 and randomness
		// collected from seed_nonce_revelations during cycle N-6; N is the
		// farthest future cycle that exists.
		//
		// Note that for consistency and due to an off-by-one error in Tezos RPC
		// nodes we fetch snapshot index and rights at the start of cycle N-5 even
		// though they are created at the end of N-6!
		cycle := b.Cycle + b.Params.PreservedCycles
		br, er, snap, err := c.fetchRightsByCycle(ctx, height, cycle)
		if err != nil {
			return nil, fmt.Errorf("fetching rights for cycle %d: %v", cycle, err)
		}
		b.Baking = br
		b.Endorsing = er
		b.Snapshot = snap
	}
	return b, nil
}

func (c *Crawler) fetchBlockByHeight(ctx context.Context, height int64) (*Bundle, error) {
	b := &Bundle{}
	var err error
	if b.Block, err = c.rpc.GetBlockHeight(ctx, height); err != nil {
		return nil, err
	}
	if c.tip.ChainId.IsValid() && !c.tip.ChainId.IsEqual(b.Block.ChainId) {
		return nil, fmt.Errorf("block init: invalid chain %s (expected %s)",
			b.Block.ChainId, c.tip.ChainId)
	}
	b.Params, err = c.indexer.reg.GetParams(b.Block.Protocol)
	needUpdate := b.Params != nil && b.Params.IsCycleStart(height)
	if err != nil || needUpdate {
		// fetch params from chain
		if height > 0 {
			cons, err := c.rpc.GetConstantsHeight(ctx, height)
			if err != nil {
				return nil, fmt.Errorf("block init: %v", err)
			}
			b.Params = cons.MapToChainParams()
		} else {
			b.Params = chain.NewParams()
		}
		// changes will be updated during build
		b.Params = b.Params.
			ForProtocol(b.Block.Protocol).
			ForNetwork(b.Block.ChainId)
		b.Params.Deployment = b.Block.Header.Proto
	}
	b.Cycle = b.Params.CycleFromHeight(height)

	// on first block after genesis, fetch rights for first 7 cycles [0..6]
	// cycle 7 rights are then processed at block 4096+1
	if height == 1 {
		log.Infof("Fetching bootstrap rights for %d(+1) preserved cycles", b.Params.PreservedCycles)
		for cycle := int64(0); cycle < b.Params.PreservedCycles+1; cycle++ {
			// fetch using current height (context stores from [n-5, n+5])
			br, er, _, err := c.fetchRightsByCycle(ctx, height, cycle)
			if err != nil {
				return nil, fmt.Errorf("fetching rights for cycle %d: %v", cycle, err)
			}
			b.Baking = append(b.Baking, br...)
			b.Endorsing = append(b.Endorsing, er...)
		}
		return b, nil
	}

	// start fetching more rights after bootstrap (max look-ahead is 5 on mainnet)
	if b.Cycle > 0 && b.Params.IsCycleStart(height) {
		// snapshot index and rights for future cycle N; the snapshot index
		// refers to a snapshot block taken in cycle N-7 and randomness
		// collected from seed_nonce_revelations during cycle N-6; N is the
		// farthest future cycle that we can fetch data for.
		//
		// Note that for consistency and due to an off-by-one error in Tezos RPC
		// we fetch snapshot index and rights at the START of cycle N-5 even
		// though they are created at the end of N-6!
		cycle := b.Cycle + b.Params.PreservedCycles
		br, er, snap, err := c.fetchRightsByCycle(ctx, height, cycle)
		if err != nil {
			return nil, fmt.Errorf("fetching rights for cycle %d: %v", cycle, err)
		}
		b.Baking = br
		b.Endorsing = er
		b.Snapshot = snap
	}
	return b, nil
}

func (c *Crawler) fetchRightsByCycle(ctx context.Context, height, cycle int64) ([]rpc.BakingRight, []rpc.EndorsingRight, *rpc.SnapshotIndex, error) {
	br, err := c.rpc.GetBakingRightsCycle(ctx, height, cycle)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(br) == 0 {
		return nil, nil, nil, fmt.Errorf("empty baking rights, make sure your Tezos node runs in archive mode")
	}
	er, err := c.rpc.GetEndorsingRightsCycle(ctx, height, cycle)
	if err != nil {
		return br, nil, nil, err
	}
	if len(er) == 0 {
		return nil, nil, nil, fmt.Errorf("empty endorsing rights, make sure your Tezos node runs in archive mode")
	}
	snap, err := c.rpc.GetSnapshotIndexCycle(ctx, height, cycle)
	if err != nil {
		return br, er, nil, err
	}
	return br, er, snap, nil
}

func (c *Crawler) fetchBlockchainInfo(ctx context.Context) error {
	head, err := c.rpc.GetTipHeader(ctx)
	if err != nil {
		return err
	}
	c.Lock()
	c.bchead = head
	c.Unlock()
	return nil
}
