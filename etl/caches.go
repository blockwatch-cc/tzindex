// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) CacheStats() map[string]interface{} {
	stats := make(map[string]interface{})
	if b := m.blocks.Load(); b != nil {
		stats["blocks"] = b.(*cache.BlockCache).Stats()
	}

	if b := m.ranks.Load(); b != nil {
		stats["ranks"] = b.(*cache.RankCache).Stats()
	}

	if b := m.rights.Load(); b != nil {
		stats["rights"] = b.(*cache.RightsCache).Stats()
	}

	if b := m.addrs.Load(); b != nil {
		stats["addresses"] = b.(*cache.AddressCache).Stats()
	}
	stats["bigmaps"] = m.bigmaps.Stats()
	return stats
}

func (m *Indexer) PurgeCaches() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks = atomic.Value{}
	m.ranks = atomic.Value{}
	m.rights = atomic.Value{}
	m.addrs = atomic.Value{}
	m.bigmaps.Purge()
}

func (m *Indexer) NextRights(ctx context.Context, a model.AccountID, height int64) (int64, int64) {
	cache, err := m.getRights(ctx, height)
	if err != nil {
		// ignore this error, can only happen in --light mode
		return 0, 0
	}
	return cache.Lookup(a, height)
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) LookupBlockTimePtr(ctx context.Context, height int64) *time.Time {
	if height == 0 {
		return nil
	}
	t := m.LookupBlockTime(ctx, height)
	return &t
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) LookupBlockTime(ctx context.Context, height int64) time.Time {
	cc, err := m.getBlocks(ctx)
	if err != nil {
		return time.Time{}
	}
	l := int64(cc.Len())
	if height < l {
		return cc.GetTime(height)
	}
	last := cc.GetTime(l - 1)
	p := m.reg.GetParamsLatest()
	return last.Add(time.Duration(height-l+1) * p.BlockTime())
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) LookupBlockTimeMs(ctx context.Context, height int64) int64 {
	if height == 0 {
		return 0
	}
	tm := m.LookupBlockTime(ctx, height)
	return tm.Unix() * 1000
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) LookupBlockHeightFromTime(ctx context.Context, tm time.Time) int64 {
	cc, err := m.getBlocks(ctx)
	if err != nil {
		return 0
	}
	return cc.GetHeight(tm)
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) LookupBlockHash(ctx context.Context, height int64) tezos.BlockHash {
	cc, err := m.getBlocks(ctx)
	if err != nil {
		return tezos.BlockHash{}
	}
	return cc.GetHash(height)
}

func (m *Indexer) LookupAddress(ctx context.Context, id model.AccountID) tezos.Address {
	if id == 0 {
		return tezos.InvalidAddress
	}
	cc, err := m.getAddrs(ctx)
	if err != nil {
		log.Errorf("addr cache build failed: %v", err)
		return tezos.InvalidAddress
	}
	return cc.GetAddress(id)
}

func (m *Indexer) LookupRanking(ctx context.Context, id model.AccountID) (*model.AccountRank, bool) {
	if id == 0 {
		return nil, false
	}
	ranks, err := m.getRanks(ctx)
	if err != nil {
		log.Errorf("rank build failed: %v", err)
		return nil, false
	}
	r, ok := ranks.GetAccount(id)
	return r, ok
}

func (m *Indexer) TopRich(ctx context.Context, n, o int) ([]*model.AccountRank, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid negative top value %d", n)
	}
	ranks, err := m.getRanks(ctx)
	if err != nil {
		return nil, err
	}
	return ranks.TopRich(n, o), nil
}

func (m *Indexer) TopTraffic(ctx context.Context, n, o int) ([]*model.AccountRank, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid negative top value %d", n)
	}
	ranks, err := m.getRanks(ctx)
	if err != nil {
		return nil, err
	}
	return ranks.TopTraffic(n, o), nil
}

func (m *Indexer) TopVolume(ctx context.Context, n, o int) ([]*model.AccountRank, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid negative top value %d", n)
	}
	ranks, err := m.getRanks(ctx)
	if err != nil {
		return nil, err
	}
	return ranks.TopVolume(n, o), nil
}

func (m *Indexer) clearRights() {
	m.rights.Store(cache.NewRightsCache(0, 0, 0))
}

func (m *Indexer) getRights(ctx context.Context, height int64) (*cache.RightsCache, error) {
	// lazy-load on first call
	rights := m.rights.Load()
	if rights == nil || rights.(*cache.RightsCache).End() < height {
		// grab lock
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again
		rights = m.rights.Load()
		// build if still not updated by other goroutine
		if rights == nil {
			if err := m.updateRights(ctx, height); err != nil {
				return nil, err
			}
			rights = m.rights.Load()
		}
	}
	return rights.(*cache.RightsCache), nil
}

func (m *Indexer) updateRights(ctx context.Context, height int64) error {
	startTime := time.Now()
	table, err := m.Table(index.RightsTableKey)
	if err != nil {
		return err
	}
	params := m.ParamsByHeight(height)
	startCycle := params.CycleFromHeight(height)
	next := cache.NewRightsCache(params.BlocksPerCycle, params.PreservedCycles+1, params.CycleStartHeight(startCycle))
	if err := next.Build(ctx, height, startCycle, table); err != nil {
		return err
	}
	m.rights.Store(next)
	log.Infof("Rights cache with %d entries built in %s", next.Len(), time.Since(startTime))
	return nil
}

func (m *Indexer) getBlocks(ctx context.Context) (*cache.BlockCache, error) {
	// lazy-load on first call
	blocks := m.blocks.Load()
	if blocks == nil {
		// grab lock
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again
		blocks = m.blocks.Load()
		// build if still not updated by other goroutine
		if blocks == nil {
			if err := m.updateBlocks(ctx, nil); err != nil {
				return nil, err
			}
			blocks = m.blocks.Load()
		}
	}
	return blocks.(*cache.BlockCache), nil
}

func (m *Indexer) updateBlocks(ctx context.Context, block *model.Block) error {
	blocks := m.blocks.Load()
	if blocks != nil {
		return blocks.(*cache.BlockCache).Update(block)
	}
	startTime := time.Now()
	table, err := m.Table(index.BlockTableKey)
	if err != nil {
		return err
	}
	next := cache.NewBlockCache(0)
	if err := next.Build(ctx, table); err != nil {
		return err
	}
	m.blocks.Store(next)
	log.Infof("Block cache with %d entries built in %s", next.Len(), time.Since(startTime))
	return nil
}

func (m *Indexer) getRanks(ctx context.Context) (*cache.RankCache, error) {
	// lazy-load on first call
	ranks := m.ranks.Load()
	if ranks == nil || ranks.(*cache.RankCache).Expired() {
		// grab lock
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again
		ranks = m.ranks.Load()
		// build if still not updated by other goroutine
		if ranks == nil || ranks.(*cache.RankCache).Expired() {
			if err := m.updateRanks(ctx); err != nil {
				return nil, err
			}
			ranks = m.ranks.Load()
		}
	}
	return ranks.(*cache.RankCache), nil
}

func (m *Indexer) updateRanks(ctx context.Context) error {
	startTime := time.Now()
	accounts, err := m.Table(index.AccountTableKey)
	if err != nil {
		return err
	}
	ops, err := m.Table(index.OpTableKey)
	if err != nil {
		return err
	}
	ranks := cache.NewRankCache()
	if err := ranks.Build(ctx, accounts, ops); err != nil {
		return err
	}
	m.ranks.Store(ranks)
	log.Infof("Ranks cache with %d entries built in %s", ranks.Len(), time.Since(startTime))
	return nil
}

func (m *Indexer) getAddrs(ctx context.Context) (*cache.AddressCache, error) {
	// lazy-load on first call
	addrs := m.addrs.Load()
	if addrs == nil {
		// grab lock
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again
		addrs = m.addrs.Load()
		// build if still not updated by other goroutine
		if addrs == nil {
			if err := m.updateAddrs(ctx, nil); err != nil {
				return nil, err
			}
			addrs = m.addrs.Load()
		}
	}
	return addrs.(*cache.AddressCache), nil
}

func (m *Indexer) updateAddrs(ctx context.Context, accounts map[model.AccountID]*model.Account) error {
	addrs := m.addrs.Load()
	if addrs != nil {
		return addrs.(*cache.AddressCache).Update(accounts)
	}
	startTime := time.Now()
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return err
	}
	next := cache.NewAddressCache(0)
	if err := next.Build(ctx, table); err != nil {
		return err
	}
	if next.Len() == 0 {
		return nil
	}
	m.addrs.Store(next)
	log.Infof("Address cache with %d entries built in %s", next.Len(), time.Since(startTime))
	return nil
}

func (m *Indexer) reloadAddrs(ctx context.Context) error {
	m.addrs.Store(nil)
	return m.updateAddrs(ctx, nil)
}
