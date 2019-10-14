// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
)

type OpStats struct {
	Height  int64
	Time    time.Time
	Buckets map[chain.OpType][]OpStatsBucket
}

type OpStatsBucket struct {
	Name   string // fee, gas_used, gas_price, vol, tdd, add
	Type   string
	N      int
	Sum    float64
	Min    float64
	Max    float64
	Mean   float64
	Median float64
}

func NewOpStatsBucket(n string, typ chain.OpType, values interface{}) OpStatsBucket {
	b := OpStatsBucket{
		Name: n,
		Type: typ.String(),
	}
	switch slice := values.(type) {
	case []int64:
		if l := len(slice); l > 0 {
			red := vec.NewWindowIntegerReducer(0)
			red.UseSlice(slice)
			b.N = red.Len()
			b.Min = float64(red.Min())
			b.Max = float64(red.Max())
			b.Sum = float64(red.Sum())
			b.Mean = red.Mean()
			b.Median = red.Median()
		}
	case []uint64:
		if l := len(slice); l > 0 {
			red := vec.NewWindowUnsignedReducer(0)
			red.UseSlice(slice)
			b.N = red.Len()
			b.Min = float64(red.Min())
			b.Max = float64(red.Max())
			b.Sum = float64(red.Sum())
			b.Mean = red.Mean()
			b.Median = red.Median()
		}
	case []float64:
		if l := len(slice); l > 0 {
			red := vec.NewWindowFloatReducer(0)
			red.UseSlice(slice)
			b.N = red.Len()
			b.Min = float64(red.Min())
			b.Max = float64(red.Max())
			b.Sum = float64(red.Sum())
			b.Mean = red.Mean()
			b.Median = red.Median()
		}
	}
	return b
}

func (r *OpReport) BuildStats(ctx context.Context, now time.Time, height int64) (*OpStats, error) {
	s := &OpStats{
		Time:    now,
		Height:  height,
		Buckets: make(map[chain.OpType][]OpStatsBucket),
	}
	params := r.crawler.ParamsByHeight(height)

	log.Debugf("Collecting op statistics at height %d time %s.", height, now)
	start := time.Now()

	table, err := r.crawler.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}

	// manuall group/scan all of today's ops and aggregate results per type
	for _, group := range []chain.OpType{
		chain.OpTypeActivateAccount,
		chain.OpTypeDoubleBakingEvidence,
		chain.OpTypeDoubleEndorsementEvidence,
		chain.OpTypeSeedNonceRevelation,
		chain.OpTypeTransaction,
		chain.OpTypeOrigination,
		chain.OpTypeDelegation,
		chain.OpTypeReveal,
		chain.OpTypeEndorsement,
		chain.OpTypeProposals,
		chain.OpTypeBallot,
	} {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		q := pack.Query{
			Name:   "stats." + group.String() + ".query",
			Fields: table.Fields().Select("f", "G", "g", "v", "x"),
			Conditions: pack.ConditionList{
				pack.Condition{
					Field: table.Fields().Find("T"), // search for op timestamp
					Mode:  pack.FilterModeRange,
					From:  now.Truncate(24 * time.Hour), // start of day
					To:    now,                          // end of day
				},
				pack.Condition{
					Field: table.Fields().Find("!"), // limit to successful ops
					Mode:  pack.FilterModeEqual,
					Value: true,
				},
				pack.Condition{
					Field: table.Fields().Find("t"), // select type group
					Mode:  pack.FilterModeEqual,
					Value: int64(group),
				},
			},
		}
		res, err := table.Query(ctx, q)
		if err != nil {
			return nil, err
		}
		if res.Rows() == 0 {
			res.Close()
			continue
		}
		// fee, gas_used, gas_price, vol, tdd
		buckets := make([]OpStatsBucket, 0, 8)
		for _, kind := range []string{
			"fee",
			"gas_used",
			"gas_price",
			"vol",
			"tdd", // token days destroyed
			"add", // average days destroyed per token = tdd / vol
		} {
			switch kind {
			case "fee":
				col, _ := res.Column("f") // keep in atomic units
				buckets = append(buckets, NewOpStatsBucket(kind, group, col))
			case "gas_used":
				col, _ := res.Column("G")
				buckets = append(buckets, NewOpStatsBucket(kind, group, col))
			case "gas_price":
				col, _ := res.Column("g")
				buckets = append(buckets, NewOpStatsBucket(kind, group, col))
			case "vol":
				// in coins, convert volume to float64
				vol, _ := res.Int64Column("v")
				conv := make([]float64, len(vol))
				for i, v := range vol {
					conv[i] = params.ConvertValue(v)
				}
				buckets = append(buckets, NewOpStatsBucket(kind, group, conv))
			case "tdd":
				col, _ := res.Column("x")
				buckets = append(buckets, NewOpStatsBucket(kind, group, col))
			case "add":
				// per-op average coin age transacted
				tdd, _ := res.Float64Column("x")
				vol, _ := res.Int64Column("v")
				fee, _ := res.Int64Column("f")
				conv := make([]float64, len(vol))
				for i, v := range vol {
					volume := params.ConvertValue(v + fee[i])
					if volume > 0 {
						conv[i] = tdd[i] / volume
					}
				}
				buckets = append(buckets, NewOpStatsBucket(kind, group, conv))
			}
		}
		s.Buckets[group] = buckets
		res.Close()
	}

	if d := time.Since(start); d > 10*time.Second {
		log.Infof("Op statistics collection took %s.", d)
	} else {
		log.Debugf("Successfully collected op statistics in %s.", d)
	}

	return s, nil
}
