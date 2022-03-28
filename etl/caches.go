// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
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

	if b := m.addrs.Load(); b != nil {
		stats["addresses"] = b.(*cache.AddressCache).Stats()
	}

	stats["bigmap_values"] = m.bigmap_values.Stats()
	stats["bigmap_types"] = m.bigmap_types.Stats()
	stats["contract_types"] = m.contract_types.Stats()
	return stats
}

func (m *Indexer) PurgeCaches() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks = atomic.Value{}
	m.addrs = atomic.Value{}
	m.bigmap_values.Purge()
	m.bigmap_types.Purge()
	m.contract_types.Purge()
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			t.PurgeCache()
		}
	}
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) LookupBlockTimePtr(ctx context.Context, height int64) *time.Time {
	if height == 0 {
		return nil
	}
	t := m.LookupBlockTime(ctx, height)
	return &t
}

func (m *Indexer) BestHeight() int64 {
	cc, err := m.getBlocks(context.Background())
	if err != nil {
		return 0
	}
	return int64(cc.Len())
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
		log.Errorf("addr cache build failed: %s", err)
		return tezos.InvalidAddress
	}
	return cc.GetAddress(id)
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
