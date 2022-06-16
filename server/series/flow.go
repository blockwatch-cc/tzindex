// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	flowSeriesNames = util.StringList([]string{"time", "amount_in", "amount_out", "count"})
)

// configurable marshalling helper
type FlowSeries struct {
	Timestamp time.Time `json:"time"`
	Count     int       `json:"count"`
	AmountIn  int64     `json:"amount_in"`
	AmountOut int64     `json:"amount_out"`

	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params
	verbose bool
	null    bool
}

var _ SeriesBucket = (*FlowSeries)(nil)

func (s *FlowSeries) Init(params *tezos.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *FlowSeries) IsEmpty() bool {
	return s.Count == 0
}

func (s *FlowSeries) Add(m SeriesModel) {
	o := m.(*model.Flow)
	s.AmountIn += o.AmountIn
	s.AmountOut += o.AmountOut
	s.Count++
}

func (s *FlowSeries) Reset() {
	s.Timestamp = time.Time{}
	s.AmountIn = 0
	s.AmountOut = 0
	s.Count = 0
	s.null = false
}

func (s *FlowSeries) Null(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	s.null = true
	return s
}

func (s *FlowSeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *FlowSeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *FlowSeries) Time() time.Time {
	return s.Timestamp
}

func (s *FlowSeries) Clone() SeriesBucket {
	return &FlowSeries{
		Timestamp: s.Timestamp,
		AmountIn:  s.AmountIn,
		AmountOut: s.AmountOut,
		Count:     s.Count,
		columns:   s.columns,
		params:    s.params,
		verbose:   s.verbose,
		null:      s.null,
	}
}

func (s *FlowSeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	o := m.(*FlowSeries)
	weight := float64(ts.Sub(s.Timestamp)) / float64(o.Timestamp.Sub(s.Timestamp))
	if math.IsInf(weight, 1) {
		weight = 1
	}
	switch weight {
	case 0:
		return s
	default:
		return &FlowSeries{
			Timestamp: ts,
			AmountIn:  s.AmountIn + int64(weight*float64(o.AmountIn-s.AmountIn)),
			AmountOut: s.AmountOut + int64(weight*float64(o.AmountOut-s.AmountOut)),
			Count:     0,
			columns:   s.columns,
			params:    s.params,
			verbose:   s.verbose,
			null:      false,
		}
	}
}

func (f *FlowSeries) MarshalJSON() ([]byte, error) {
	if f.verbose {
		return f.MarshalJSONVerbose()
	} else {
		return f.MarshalJSONBrief()
	}
}

func (f *FlowSeries) MarshalJSONVerbose() ([]byte, error) {
	flow := struct {
		Timestamp time.Time `json:"time"`
		Count     int       `json:"count"`
		AmountIn  float64   `json:"amount_in"`
		AmountOut float64   `json:"amount_out"`
	}{
		Timestamp: f.Timestamp,
		Count:     f.Count,
		AmountIn:  f.params.ConvertValue(f.AmountIn),
		AmountOut: f.params.ConvertValue(f.AmountOut),
	}
	return json.Marshal(flow)
}

func (f *FlowSeries) MarshalJSONBrief() ([]byte, error) {
	dec := f.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range f.columns {
		if f.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(f.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(f.Timestamp), 10)
			case "count":
				buf = strconv.AppendInt(buf, int64(f.Count), 10)
			case "amount_in":
				buf = strconv.AppendFloat(buf, f.params.ConvertValue(f.AmountIn), 'f', dec, 64)
			case "amount_out":
				buf = strconv.AppendFloat(buf, f.params.ConvertValue(f.AmountOut), 'f', dec, 64)
			default:
				continue
			}
		}
		if i < len(f.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (f *FlowSeries) MarshalCSV() ([]string, error) {
	dec := f.params.Decimals
	res := make([]string, len(f.columns))
	for i, v := range f.columns {
		if f.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(f.Timestamp.Format(time.RFC3339))
			default:
				continue
			}

		}
		switch v {
		case "time":
			res[i] = strconv.Quote(f.Timestamp.Format(time.RFC3339))
		case "count":
			res[i] = strconv.FormatInt(int64(f.Count), 10)
		case "amount_in":
			res[i] = strconv.FormatFloat(f.params.ConvertValue(f.AmountIn), 'f', dec, 64)
		case "amount_out":
			res[i] = strconv.FormatFloat(f.params.ConvertValue(f.AmountOut), 'f', dec, 64)
		default:
			continue
		}
	}
	return res, nil
}

func (s *FlowSeries) BuildQuery(ctx *server.Context, args *SeriesRequest) pack.Query {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Series), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	// time is auto-added from parser
	if len(args.Columns) == 1 {
		// use all series columns
		args.Columns = flowSeriesNames
	}
	// resolve short column names
	srcNames = make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore count column
		if v == "count" {
			continue
		}
		// ignore non-series columns
		if !flowSeriesNames.Contains(v) {
			panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		srcNames = append(srcNames, v)
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID, table).
		WithFields(srcNames...).
		WithOrder(args.Order).
		AndRange("time", args.From.Time(), args.To.Time())

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

		case "address", "counterparty":
			field := "A" // account
			if prefix == "counterparty" {
				field = "R"
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := tezos.ParseAddress(val[0])
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != index.ErrNoAccountEntry {
					panic(err)
				}
				// Note: when not found we insert an always false condition
				if acc == nil || acc.RowId == 0 {
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: uint64(math.MaxUint64),
						Raw:   "account not found", // debugging aid
					})
				} else {
					// add id as extra condition
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: acc.RowId,
						Raw:   val[0], // debugging aid
					})
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(err)
					}
					// skip not found account
					if acc == nil || acc.RowId == 0 {
						continue
					}
					// collect list of account ids
					ids = append(ids, acc.RowId.Value())
				}
				// Note: when list is empty (no accounts were found, the match will
				//       always be false and return no result as expected)
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find(field), // account id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "amount_in", "amount_out":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
				case "category":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]uint8, 0)
					for _, t := range strings.Split(v, ",") {
						typ := model.ParseFlowCategory(t)
						if !typ.IsValid() {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid category '%s'", val[0]), nil))
						}
						typs = append(typs, uint8(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueUint8Slice(typs) {
						styps = append(styps, strconv.FormatUint(uint64(i), 10))
					}
					v = strings.Join(styps, ",")
				case "operation":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]uint8, 0)
					for _, t := range strings.Split(v, ",") {
						typ := model.ParseFlowType(t)
						if !typ.IsValid() {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation '%s'", val[0]), nil))
						}
						typs = append(typs, uint8(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueUint8Slice(typs) {
						styps = append(styps, strconv.FormatUint(uint64(i), 10))
					}
					v = strings.Join(styps, ",")
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions.AddAndCondition(&cond)
				}
			}
		}
	}

	return q
}
