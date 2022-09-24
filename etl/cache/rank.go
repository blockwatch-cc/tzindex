// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
	"context"
	"sort"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

const rankDefaultSize = 1 << 16

type RankCache struct {
	idmap   map[model.AccountID]*model.AccountRank // map for log-N lookups and updates
	rich    ByRichRank
	traffic ByTrafficRank
	volume  ByVolumeRank
	ts      time.Time
	stats   Stats
}

func NewRankCache() *RankCache {
	return &RankCache{
		idmap:   make(map[model.AccountID]*model.AccountRank),
		rich:    make(ByRichRank, 0, rankDefaultSize),
		traffic: make(ByTrafficRank, 0, rankDefaultSize),
		volume:  make(ByVolumeRank, 0, rankDefaultSize),
		ts:      time.Now(),
	}
}

func (c RankCache) Cap() int {
	return cap(c.rich)
}

func (c RankCache) Len() int {
	return len(c.rich)
}

func (c RankCache) Size() int {
	return (len(c.rich)+len(c.traffic)+len(c.volume))*8 + // pointers
		len(c.idmap)*(8+64) // not counting map bucket overheads
}

func (c RankCache) Stats() Stats {
	s := c.stats.Get()
	s.Size = c.Len()
	s.Bytes = int64(c.Size())
	return s
}

func (h RankCache) Time() time.Time {
	return h.ts
}

func (c *RankCache) Expire() *RankCache {
	c.ts = time.Time{}
	return c
}

func (h RankCache) Expired() bool {
	return h.ts.Add(time.Minute).Before(time.Now())
}

func (h *RankCache) TopRich(n, o int) []*model.AccountRank {
	l := len(h.rich)
	if o > l {
		return nil
	}
	h.stats.CountHits(1)
	return h.rich[o:util.Min(o+n, l)]
}

func (h *RankCache) TopTraffic(n, o int) []*model.AccountRank {
	l := len(h.traffic)
	if o > l {
		return nil
	}
	h.stats.CountHits(1)
	return h.traffic[o:util.Min(o+n, l)]
}

func (h *RankCache) TopVolume(n, o int) []*model.AccountRank {
	l := len(h.volume)
	if o > l {
		return nil
	}
	h.stats.CountHits(1)
	return h.volume[o:util.Min(o+n, l)]
}

func (h *RankCache) GetAccount(id model.AccountID) (*model.AccountRank, bool) {
	r, ok := h.idmap[id]
	h.stats.CountHits(1)
	return r, ok
}

type ByRichRank []*model.AccountRank

func (h ByRichRank) Len() int           { return len(h) }
func (h ByRichRank) Less(i, j int) bool { return h[i].Balance > h[j].Balance }
func (h ByRichRank) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type ByTrafficRank []*model.AccountRank

func (h ByTrafficRank) Len() int           { return len(h) }
func (h ByTrafficRank) Less(i, j int) bool { return h[i].TxTraffic24h > h[j].TxTraffic24h }
func (h ByTrafficRank) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type ByVolumeRank []*model.AccountRank

func (h ByVolumeRank) Len() int           { return len(h) }
func (h ByVolumeRank) Less(i, j int) bool { return h[i].TxVolume24h > h[j].TxVolume24h }
func (h ByVolumeRank) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *RankCache) Build(ctx context.Context, accounts, ops *pack.Table) error {
	// Step 1: capture accounts
	type XAcc struct {
		RowId            model.AccountID `pack:"I,pk"`
		Hash             tezos.Address   `pack:"H"`
		SpendableBalance int64           `pack:"s"`
	}
	a := &XAcc{}
	err := pack.NewQuery("build_ranks").
		WithTable(accounts).
		WithoutCache().
		WithFields("I", "H", "s").
		AndEqual("is_funded", true).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(a); err != nil {
				return err
			}
			bal := a.SpendableBalance
			if bal < 1 {
				return nil
			}
			acc := &model.AccountRank{
				AccountId: a.RowId,
				Balance:   bal,
			}
			h.idmap[a.RowId] = acc
			h.rich = append(h.rich, acc)
			h.volume = append(h.volume, acc)
			h.traffic = append(h.traffic, acc)
			return nil
		})
	if err != nil {
		return err
	}

	// Step 2: fetch 24h transactions
	type XOp struct {
		SenderId   model.AccountID `pack:"S"`
		ReceiverId model.AccountID `pack:"R"`
		Volume     int64           `pack:"v"`
	}
	o := &XOp{}
	err = pack.NewQuery("rank_ops_24h").
		WithTable(ops).
		WithoutCache().
		WithFields("S", "R", "v").
		AndGte("time", h.ts.Add(-24*time.Hour)).
		AndEqual("type", tezos.OpTypeTransaction).
		AndEqual("is_success", true).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(o); err != nil {
				return err
			}
			if sender, ok := h.idmap[o.SenderId]; ok {
				sender.TxVolume24h += o.Volume
				sender.TxTraffic24h++
			}
			if receiver, ok := h.idmap[o.ReceiverId]; ok {
				receiver.TxVolume24h += o.Volume
				receiver.TxTraffic24h++
			}
			return nil
		})
	if err != nil {
		return err
	}

	// Step 3: sort embedded lists and assign rank order
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		sort.Stable(h.rich)
		rank := 1
		var last int64
		for _, v := range h.rich {
			if v.Balance == 0 {
				continue
			}
			v.RichRank = rank
			if v.Balance != last {
				last = v.Balance
				rank++
			}
		}
		wg.Done()
	}()
	go func() {
		sort.Stable(h.volume)
		rank := 1
		var last int64
		for _, v := range h.volume {
			if v.TxVolume24h == 0 {
				continue
			}
			v.VolumeRank = rank
			if v.TxVolume24h != last {
				last = v.TxVolume24h
				rank++
			}
		}
		wg.Done()
	}()
	go func() {
		sort.Stable(h.traffic)
		rank := 1
		var last int64
		for _, v := range h.traffic {
			if v.TxTraffic24h == 0 {
				continue
			}
			v.TrafficRank = rank
			if v.TxTraffic24h != last {
				last = v.TxTraffic24h
				rank++
			}
		}
		wg.Done()
	}()
	wg.Wait()
	h.stats.CountUpdates(1)
	return nil
}
