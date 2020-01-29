// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	flowSeriesNames = util.StringList([]string{"time", "amount_in", "amount_out"})
)

// configurable marshalling helper
type FlowSeries struct {
	Timestamp time.Time     `json:"time"`
	AmountIn  int64         `json:"amount_in"`
	AmountOut int64         `json:"amount_out"`
	params    *chain.Params `csv:"-" pack:"-"`
	verbose   bool          `csv:"-" pack:"-"`
}

func (f *FlowSeries) Reset() {
	f.Timestamp = time.Time{}
	f.AmountIn = 0
	f.AmountOut = 0
}

func (f *FlowSeries) Add(m *model.Flow) {
	f.AmountIn += m.AmountIn
	f.AmountOut += m.AmountOut
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
		AmountIn  float64   `json:"amount_in"`
		AmountOut float64   `json:"amount_out"`
	}{
		Timestamp: f.Timestamp,
		AmountIn:  f.params.ConvertValue(f.AmountIn),
		AmountOut: f.params.ConvertValue(f.AmountOut),
	}
	return json.Marshal(flow)
}

func (f *FlowSeries) MarshalJSONBrief() ([]byte, error) {
	dec := f.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range flowSeriesNames {
		switch v {
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(f.Timestamp), 10)
		case "amount_in":
			buf = strconv.AppendFloat(buf, f.params.ConvertValue(f.AmountIn), 'f', dec, 64)
		case "amount_out":
			buf = strconv.AppendFloat(buf, f.params.ConvertValue(f.AmountOut), 'f', dec, 64)
		default:
			continue
		}
		if i < len(flowSeriesNames)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (f *FlowSeries) MarshalCSV() ([]string, error) {
	dec := f.params.Decimals
	res := make([]string, len(flowSeriesNames))
	for i, v := range flowSeriesNames {
		switch v {
		case "time":
			res[i] = strconv.FormatInt(util.UnixMilliNonZero(f.Timestamp), 10)
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

func StreamFlowSeries(ctx *ApiContext, args *SeriesRequest) (interface{}, int) {
	// fetch chain params at current height
	params := ctx.Crawler.ParamsByHeight(-1)

	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access series source table '%s'", args.Series), err))
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
		// ignore non-series columns
		if !flowSeriesNames.Contains(v) {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		n, ok := flowSourceNames[v]
		if !ok {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
		}
		if n != "-" {
			srcNames = append(srcNames, n)
		}
	}

	// build table query
	q := pack.Query{
		Name:   ctx.RequestID,
		Fields: table.Fields().Select(srcNames...),
		Order:  args.Order,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("T"), // time
				Mode:  pack.FilterModeRange,
				From:  args.From.Time(),
				To:    args.To.Time(),
				Raw:   args.From.String() + " - " + args.To.String(), // debugging aid
			},
		},
	}

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		mode := pack.FilterModeEqual
		if len(keys) > 1 {
			mode = pack.ParseFilterMode(keys[1])
			if !mode.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s'", keys[1]), nil))
			}
		}
		switch prefix {
		case "columns", "collapse", "start_date", "end_date", "limit", "order", "verbose":
			// skip these fields
			continue

		case "address", "origin":
			field := "A" // account
			if prefix == "origin" {
				field = "R"
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := chain.ParseAddress(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != index.ErrNoAccountEntry {
					panic(err)
				}
				// Note: when not found we insert an always false condition
				if acc == nil || acc.RowId == 0 {
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: uint64(math.MaxUint64),
						Raw:   "account not found", // debugging aid
					})
				} else {
					// add id as extra condition
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: acc.RowId.Value(),
						Raw:   val[0], // debugging aid
					})
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := chain.ParseAddress(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
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
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field), // account id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := flowSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "amount_in", "amount_out":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
				case "address_type":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := chain.ParseAddressType(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid account type '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
				case "category":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := model.ParseFlowCategory(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid category '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
				case "operation":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := model.ParseFlowType(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid operation '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions = append(q.Conditions, cond)
				}
			}
		}
	}

	var count int
	start := time.Now()
	ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Series)
	defer func() {
		ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	}()

	// prepare for source and return type marshalling
	fs := &FlowSeries{params: params, verbose: args.Verbose}
	fm := &model.Flow{}
	window := args.Collapse.Duration()
	nextBucketTime := args.From.Add(window).Time()
	mul := 1
	if args.Order == pack.OrderDesc {
		mul = 0
	}

	// prepare response stream
	ctx.StreamResponseHeaders(http.StatusOK, mimetypes[args.Format])

	switch args.Format {
	case "json":
		enc := json.NewEncoder(ctx.ResponseWriter)
		enc.SetIndent("", "")
		enc.SetEscapeHTML(false)

		// open JSON array
		io.WriteString(ctx.ResponseWriter, "[")
		// close JSON array on panic
		defer func() {
			if e := recover(); e != nil {
				io.WriteString(ctx.ResponseWriter, "]")
				panic(e)
			}
		}()

		// run query and stream results
		var needComma bool

		// stream from database, result order is assumed to be in timestamp order
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if err := r.Decode(fm); err != nil {
				return err
			}

			// output FlowSeries when valid and time has crossed next boundary
			if !fs.Timestamp.IsZero() && (fm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
				// output accumulated data
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}
				if err := enc.Encode(fs); err != nil {
					return err
				}
				count++
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				fs.Reset()
			}

			// init next time window from data
			if fs.Timestamp.IsZero() {
				fs.Timestamp = fm.Timestamp.Truncate(window)
				nextBucketTime = fs.Timestamp.Add(window * time.Duration(mul))
			}

			// accumulate data in FlowSeries
			fs.Add(fm)
			return nil
		})
		// don't handle error here, will be picked up by trailer
		if err == nil {
			// output last series element
			if !fs.Timestamp.IsZero() {
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				}
				err = enc.Encode(fs)
				if err == nil {
					count++
				}
			}
		}

		// close JSON bracket
		io.WriteString(ctx.ResponseWriter, "]")
		ctx.Log.Tracef("JSON encoded %d rows", count)

	case "csv":
		enc := csv.NewEncoder(ctx.ResponseWriter)
		// use custom header columns and order
		if len(args.Columns) > 0 {
			err = enc.EncodeHeader(args.Columns, nil)
		}
		if err == nil {
			// stream from database, result order is assumed to be in timestamp order
			err = table.Stream(ctx, q, func(r pack.Row) error {
				if err := r.Decode(fm); err != nil {
					return err
				}

				// output FlowSeries when valid and time has crossed next boundary
				if !fs.Timestamp.IsZero() && (fm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
					// output accumulated data
					if err := enc.EncodeRecord(fs); err != nil {
						return err
					}
					count++
					if args.Limit > 0 && count == int(args.Limit) {
						return io.EOF
					}
					fs.Reset()
				}

				// init next time window from data
				if fs.Timestamp.IsZero() {
					fs.Timestamp = fm.Timestamp.Truncate(window)
					nextBucketTime = fs.Timestamp.Add(window * time.Duration(mul))
				}

				// accumulate data in FlowSeries
				fs.Add(fm)
				return nil
			})
			if err == nil {
				// output last series element
				if !fs.Timestamp.IsZero() {
					err = enc.EncodeRecord(fs)
					if err == nil {
						count++
					}
				}
			}
		}
		ctx.Log.Tracef("CSV Encoded %d rows", count)
	}

	// write error (except EOF), cursor and count as http trailer
	ctx.StreamTrailer("", count, err)

	// streaming return
	return nil, -1
}
