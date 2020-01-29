// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"sort"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

const rankDefaultSize = 1 << 16

type AccountRankingEntry struct {
	AccountId    model.AccountID
	Address      chain.Address
	Balance      int64 // total balance
	TxFlow24h    int64 // tx volume in+out
	TxTraffic24h int64 // number of tx in+out
	RichRank     int   // assigned rank based on balance
	FlowRank     int   // assigned rank based on flow
	TrafficRank  int   // assigned rank based on traffic
}

type AccountRanking struct {
	idmap   map[model.AccountID]*AccountRankingEntry // map for log-N lookups and updates
	rich    ByRichRank
	traffic ByTrafficRank
	flow    ByFlowRank
}

func NewAccountRanking() *AccountRanking {
	return &AccountRanking{
		idmap:   make(map[model.AccountID]*AccountRankingEntry),
		rich:    make(ByRichRank, 0, rankDefaultSize),
		traffic: make(ByTrafficRank, 0, rankDefaultSize),
		flow:    make(ByFlowRank, 0, rankDefaultSize),
	}
}

func (h *AccountRanking) TopRich(n int) []*AccountRankingEntry {
	return h.rich[:util.Min(n, len(h.flow))]
}

func (h *AccountRanking) TopTraffic(n int) []*AccountRankingEntry {
	return h.traffic[:util.Min(n, len(h.flow))]
}

func (h *AccountRanking) TopFlows(n int) []*AccountRankingEntry {
	return h.flow[:util.Min(n, len(h.flow))]
}

func (h *AccountRanking) GetAccount(id model.AccountID) (*AccountRankingEntry, bool) {
	r, ok := h.idmap[id]
	return r, ok
}

type ByRichRank []*AccountRankingEntry

func (h ByRichRank) Len() int           { return len(h) }
func (h ByRichRank) Less(i, j int) bool { return h[i].Balance > h[j].Balance }
func (h ByRichRank) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type ByTrafficRank []*AccountRankingEntry

func (h ByTrafficRank) Len() int           { return len(h) }
func (h ByTrafficRank) Less(i, j int) bool { return h[i].TxTraffic24h > h[j].TxTraffic24h }
func (h ByTrafficRank) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type ByFlowRank []*AccountRankingEntry

func (h ByFlowRank) Len() int           { return len(h) }
func (h ByFlowRank) Less(i, j int) bool { return h[i].TxFlow24h > h[j].TxFlow24h }
func (h ByFlowRank) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (m *Indexer) GetRanking(ctx context.Context, now time.Time) (*AccountRanking, error) {
	// lazy-load on first call
	ranks := m.ranks.Load()
	if ranks == nil {
		// grab lock
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again
		ranks = m.ranks.Load()
		// build if still not updated by other goroutine
		if ranks == nil {
			var err error
			ranks, err = m.BuildAccountRanking(ctx, now)
			if err != nil {
				return nil, err
			}
			m.ranks.Store(ranks)
		}
	}
	return ranks.(*AccountRanking), nil
}

func (m *Indexer) UpdateRanking(ctx context.Context, now time.Time) error {
	ranks, err := m.BuildAccountRanking(ctx, now)
	if err != nil {
		return err
	}
	m.ranks.Store(ranks)
	return nil
}

func (m *Indexer) BuildAccountRanking(ctx context.Context, now time.Time) (*AccountRanking, error) {
	start := time.Now()
	accounts, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	ops, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	ranks := NewAccountRanking()

	// Step 1: capture accounts
	q := pack.Query{
		Name:    "rank.accounts",
		NoCache: true,
		Fields:  accounts.Fields().Select("A", "H", "t", "z", "Z", "Y", "U", "s"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: accounts.Fields().Find("f"), // filter is_funded
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
	type XAcc struct {
		RowId            model.AccountID   `pack:"I,pk,snappy"`
		Hash             []byte            `pack:"H"`
		Type             chain.AddressType `pack:"t,snappy"`
		FrozenDeposits   int64             `pack:"z,snappy"`
		FrozenRewards    int64             `pack:"Z,snappy"`
		FrozenFees       int64             `pack:"Y,snappy"`
		UnclaimedBalance int64             `pack:"U,snappy"`
		SpendableBalance int64             `pack:"s,snappy"`
	}
	a := &XAcc{}
	err = accounts.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(a); err != nil {
			return err
		}
		bal := a.FrozenDeposits + a.FrozenFees + a.FrozenRewards + a.SpendableBalance
		if bal < 1 {
			return nil
		}
		acc := &AccountRankingEntry{
			AccountId: a.RowId,
			Address:   chain.NewAddress(a.Type, a.Hash),
			Balance:   bal,
		}
		ranks.idmap[a.RowId] = acc
		ranks.rich = append(ranks.rich, acc)
		ranks.flow = append(ranks.flow, acc)
		ranks.traffic = append(ranks.traffic, acc)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Step 2: fetch 24h transactions
	q = pack.Query{
		Name:    "rank.ops_24h",
		NoCache: true,
		Fields:  ops.Fields().Select("S", "R", "v"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: ops.Fields().Find("T"), // timestamp
				Mode:  pack.FilterModeGte,
				Value: now.Add(-24 * time.Hour),
			},
			pack.Condition{
				Field: ops.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.OpTypeTransaction),
			},
			pack.Condition{
				Field: ops.Fields().Find("!"), // success
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
	type XOp struct {
		SenderId   model.AccountID `pack:"S,snappy"`
		ReceiverId model.AccountID `pack:"R,snappy"`
		Volume     int64           `pack:"v,snappy"`
	}
	o := &XOp{}
	err = ops.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(o); err != nil {
			return err
		}
		if sender, ok := ranks.idmap[o.SenderId]; ok {
			sender.TxFlow24h += o.Volume
			sender.TxTraffic24h++
		}
		if receiver, ok := ranks.idmap[o.ReceiverId]; ok {
			receiver.TxFlow24h += o.Volume
			receiver.TxTraffic24h++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Step 3: sort embedded lists and assign rank order
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		sort.Stable(ranks.rich)
		rank := 1
		var last int64
		for _, v := range ranks.rich {
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
		sort.Stable(ranks.flow)
		rank := 1
		var last int64
		for _, v := range ranks.flow {
			if v.TxFlow24h == 0 {
				continue
			}
			v.FlowRank = rank
			if v.TxFlow24h != last {
				last = v.TxFlow24h
				rank++
			}
		}
		wg.Done()
	}()
	go func() {
		sort.Stable(ranks.traffic)
		rank := 1
		var last int64
		for _, v := range ranks.rich {
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
	log.Debugf("Ranks built in %s", time.Since(start))
	return ranks, nil
}
