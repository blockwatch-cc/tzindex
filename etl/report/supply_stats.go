// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

type SupplyStats struct {
	model.Supply

	// 3-month dormant and transacting supply
	ThreeMonthTransacting   int64   `pack:"x,snappy" json:"m3_tx"` // should not overflow
	ThreeMonthDaysDestroyed float64 `pack:"y,convert,precision=6,snappy" json:"m3_tdd"`
}

func (r *SupplyReport) BuildStats(ctx context.Context, now time.Time, supply *model.Supply) (*SupplyStats, error) {
	s := &SupplyStats{
		Supply: *supply,
	}
	// overwrite block timestamp with stats creation time (EOD)
	s.Timestamp = now
	s.RowId = 0

	log.Debugf("Collecting supply statistics at height %d time %s.", s.Height, now)
	start := time.Now()

	//
	// Part 1: Moving supply
	//
	if err := s.Collect3MStats(ctx, r.crawler, now); err != nil {
		return nil, err
	}

	if d := time.Since(start); d > 10*time.Second {
		log.Infof("Supply statistics collection took %s.", d)
	} else {
		log.Debugf("Successfully collected supply statistics in %s.", d)
	}

	return s, nil
}

// 3M transacting supply = sum(tokens) WHERE LAST_MOVED_AGE <= 3M
// 3M dormant supply = sum(tokens) WHERE age > 3M
func (s *SupplyStats) Collect3MStats(ctx context.Context, c model.BlockCrawler, now time.Time) error {
	// sub 3 months from now
	cutoff := now.AddDate(0, -3, 0)
	params := c.ParamsByHeight(-1)

	flows, err := c.Table(index.FlowTableKey)
	if err != nil {
		return err
	}

	type XFlow struct {
		AccountId uint64 `pack:"A"` // unique acount id
		AmountIn  int64  `pack:"i"` // sum flowing in to the account
		AmountOut int64  `pack:"o"` // sum flowing out of the account
		TokenAge  int64  `pack:"a"` // time since last move in seconds
	}

	// find transaction flows (outputs only) from the most recent 3 months
	// that move tokens older than 3 months (avoid double counting of supply
	// that moved multiple times during the time-frame)
	//
	q := pack.Query{
		Name:    "stats.supply_tx3M",
		NoCache: true,
		Fields:  flows.Fields().Select("A", "i", "o", "a"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: flows.Fields().Find("T"), // flow creation time >= now-3M
				Mode:  pack.FilterModeGte,
				Value: cutoff,
			},
			pack.Condition{
				Field: flows.Fields().Find("C"), // flow category = FlowCategoryBalance
				Mode:  pack.FilterModeEqual,
				Value: int64(model.FlowCategoryBalance), // as int64! due to pack restrictions
			},
			pack.Condition{
				Field: flows.Fields().Find("O"), // flow type = FlowTypeTransaction
				Mode:  pack.FilterModeEqual,
				Value: int64(model.FlowTypeTransaction), // as int64!
			},
			pack.Condition{
				Field: flows.Fields().Find("e"), // ignore fees
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
			pack.Condition{
				Field: flows.Fields().Find("b"), // ignore burns
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
			pack.Condition{
				Field: flows.Fields().Find("u"), // ignore unfreeze
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
		},
	}
	f := &XFlow{}
	volMap := make(map[uint64]int64) // store out-in volume per account
	err = flows.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(f); err != nil {
			return err
		}

		// to count supply moved we add all out flows and subtract flows at accounts
		// that received and sent tokens during the time frame (if such an account
		// sent more tokens than it received, this adds to the moved supply as well)
		v, _ := volMap[f.AccountId]
		volMap[f.AccountId] = v + f.AmountOut - f.AmountIn

		// convert seconds to days and volume from atomic units to coins
		if f.AmountOut > 0 {
			s.ThreeMonthDaysDestroyed += float64(f.TokenAge) / 86400 * params.ConvertValue(f.AmountOut)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// only sum positive (out flows and out-in diffs) across all active accounts
	for _, vol := range volMap {
		if vol > 0 {
			s.ThreeMonthTransacting += vol
		}
	}
	return nil
}
