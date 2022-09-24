// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	balanceSeriesNames = util.StringList([]string{"time", "balance"})
)

type BalanceModel struct {
	model.Balance
	idx *etl.Indexer
	ctx context.Context
}

func (m *BalanceModel) Time() time.Time {
	return m.idx.LookupBlockTime(m.ctx, m.ValidUntil)
}

// configurable marshalling helper
type BalanceSeries struct {
	Timestamp time.Time `json:"time"`
	Balance   int64     `json:"balance"`

	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params
	verbose bool
	null    bool
}

var _ SeriesBucket = (*BalanceSeries)(nil)

func (s *BalanceSeries) Init(params *tezos.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *BalanceSeries) IsEmpty() bool {
	return s.null
}

// Aggregation func is `last()` instead of `sum()`
func (s *BalanceSeries) Add(m SeriesModel) {
	o := m.(*BalanceModel)
	s.Balance = o.Balance.Balance
}

func (s *BalanceSeries) Reset() {
	s.Timestamp = time.Time{}
	s.Balance = 0
	s.null = false
}

func (s *BalanceSeries) Null(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	s.null = true
	return s
}

func (s *BalanceSeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *BalanceSeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *BalanceSeries) Time() time.Time {
	return s.Timestamp
}

func (s *BalanceSeries) Clone() SeriesBucket {
	return &BalanceSeries{
		Timestamp: s.Timestamp,
		Balance:   s.Balance,
		columns:   s.columns,
		params:    s.params,
		verbose:   s.verbose,
		null:      s.null,
	}
}

func (s *BalanceSeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	return s
}

func (s *BalanceSeries) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (s *BalanceSeries) MarshalJSONVerbose() ([]byte, error) {
	balance := struct {
		Timestamp time.Time `json:"time"`
		Balance   float64   `json:"balance"`
	}{
		Timestamp: s.Timestamp,
		Balance:   s.params.ConvertValue(s.Balance),
	}
	return json.Marshal(balance)
}

func (s *BalanceSeries) MarshalJSONBrief() ([]byte, error) {
	dec := s.params.Decimals
	buf := make([]byte, 0, 128)
	buf = append(buf, '[')
	for i, v := range s.columns {
		if s.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(s.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(s.Timestamp), 10)
			case "balance":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Balance), 'f', dec, 64)
			default:
				continue
			}
		}
		if i < len(s.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (s *BalanceSeries) MarshalCSV() ([]string, error) {
	dec := s.params.Decimals
	res := make([]string, len(s.columns))
	for i, v := range s.columns {
		if s.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(s.Timestamp.Format(time.RFC3339))
			default:
				continue
			}
		} else {
			switch v {
			case "time":
				res[i] = strconv.Quote(s.Timestamp.Format(time.RFC3339))
			case "balance":
				res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Balance), 'f', dec, 64)
			default:
				continue
			}
		}
	}
	return res, nil
}

func (s *BalanceSeries) BuildQuery(ctx *server.Context, args *SeriesRequest) pack.Query {
	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Series), err))
	}

	// translate long column names to short names used in pack tables
	var hasAddres bool

	// time is auto-added from parser
	if len(args.Columns) == 1 {
		// use all series columns
		args.Columns = balanceSeriesNames
	}
	// resolve short column names
	srcNames := make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore non-series columns
		if !balanceSeriesNames.Contains(v) {
			panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		srcNames = append(srcNames, v)
	}

	// build table query
	from := ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, args.From.Time())
	to := ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, args.To.Time())
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
		WithFields(srcNames...).
		WithOrder(args.Order).
		AndGt("valid_until", from).
		AndLt("valid_from", to)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		mode := pack.FilterModeEqual
		if len(keys) > 1 {
			mode = pack.ParseFilterMode(keys[1])
			if !mode.IsValid() {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s'", keys[1]), nil))
			}
		}
		switch prefix {
		case "columns", "collapse", "start_date", "end_date", "limit", "order", "verbose", "filename", "fill":
			// skip these fields
			continue

		case "address":
			field := "account_id" // account
			if mode == pack.FilterModeEqual {
				// single-address lookup and compile condition
				addr, err := tezos.ParseAddress(val[0])
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != index.ErrNoAccountEntry {
					panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("address not found '%s'", val[0]), err))
				}
				// Note: when not found we insert an always false condition
				if acc == nil || acc.RowId == 0 {
					q = q.And(field, mode, uint64(math.MaxUint64))
				} else {
					// add id as extra condition
					q = q.And(field, mode, acc.RowId)
				}
				hasAddres = true
			} else {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q = q.AndCondition(cond)
				}
			}
		}
	}

	if !hasAddres {
		panic(server.EBadRequest(server.EC_PARAM_REQUIRED, "missing address", nil))

	}
	return q
}
