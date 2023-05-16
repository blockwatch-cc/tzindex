// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
)

type Growth struct {
	NewAccounts     int64
	NewContracts    int64
	ClearedAccounts int64
	FundedAccounts  int64
}

func (m *Indexer) GrowthByDuration(ctx context.Context, to time.Time, d time.Duration) (*Growth, error) {
	table, err := m.Table(model.BlockTableKey)
	if err != nil {
		return nil, err
	}
	type XBlock struct {
		NewAccounts     int64 `pack:"A"`
		NewContracts    int64 `pack:"C"`
		ClearedAccounts int64 `pack:"E"`
		FundedAccounts  int64 `pack:"J"`
	}
	from := to.Add(-d)
	x := &XBlock{}
	g := &Growth{}
	err = pack.NewQuery("api.aggregate_growth").
		WithTable(table).
		WithFields("A", "C", "E", "J").
		AndRange("time", from, to). // search for timestamp
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(x); err != nil {
				return err
			}
			g.NewAccounts += x.NewAccounts
			g.NewContracts += x.NewContracts
			g.ClearedAccounts += x.ClearedAccounts
			g.FundedAccounts += x.FundedAccounts
			return nil
		})
	if err != nil {
		return nil, err
	}
	return g, nil
}

// luck, performance, contribution (reliability)
func (m *Indexer) BakerPerformance(ctx context.Context, id model.AccountID, fromCycle, toCycle int64) ([3]int64, error) {
	perf := [3]int64{}
	table, err := m.Table(model.IncomeTableKey)
	if err != nil {
		return perf, err
	}
	q := pack.NewQuery("api.baker_income").
		WithTable(table).
		AndEqual("account_id", id).
		AndRange("cycle", fromCycle, toCycle)
	var count int64
	income := &model.Income{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(income); err != nil {
			return err
		}
		perf[0] += income.LuckPct
		perf[1] += income.PerformancePct
		perf[2] += income.ContributionPct
		count++
		return nil
	})
	if err != nil {
		return perf, err
	}
	if count > 0 {
		perf[0] /= count
		perf[1] /= count
		perf[2] /= count
	}
	return perf, nil
}
