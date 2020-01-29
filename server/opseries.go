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
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	opSeriesNames = util.StringList([]string{
		"time",
		"gas_used",
		"storage_size",
		"storage_paid",
		"volume",
		"fee",
		"reward",
		"deposit",
		"burned",
		"days_destroyed",
	})
)

// Only use fields that can be summed over time
// configurable marshalling helper
type OpSeries struct {
	Timestamp   time.Time       `json:"time"`
	GasUsed     int64           `json:"gas_used"`
	StorageSize int64           `json:"storage_size"`
	StoragePaid int64           `json:"storage_paid"`
	Volume      int64           `json:"volume"`
	Fee         int64           `json:"fee"`
	Reward      int64           `json:"reward"`
	Deposit     int64           `json:"deposit"`
	Burned      int64           `json:"burned"`
	TDD         float64         `json:"days_destroyed"`
	columns     util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params      *chain.Params   `csv:"-" pack:"-"`
	verbose     bool            `csv:"-" pack:"-"`
}

func (s *OpSeries) Add(o *model.Op) {
	s.GasUsed += o.GasUsed
	s.StorageSize += o.StorageSize
	s.StoragePaid += o.StoragePaid
	s.Volume += o.Volume
	s.Fee += o.Fee
	s.Reward += o.Reward
	s.Deposit += o.Deposit
	s.Burned += o.Burned
	s.TDD += o.TDD
}

func (s *OpSeries) Reset() {
	s.Timestamp = time.Time{}
	s.GasUsed = 0
	s.StorageSize = 0
	s.StoragePaid = 0
	s.Volume = 0
	s.Fee = 0
	s.Reward = 0
	s.Deposit = 0
	s.Burned = 0
	s.TDD = 0
}

func (s *OpSeries) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (o *OpSeries) MarshalJSONVerbose() ([]byte, error) {
	op := struct {
		Timestamp   time.Time `json:"time"`
		GasUsed     int64     `json:"gas_used"`
		StorageSize int64     `json:"storage_size"`
		StoragePaid int64     `json:"storage_paid"`
		Volume      float64   `json:"volume"`
		Fee         float64   `json:"fee"`
		Reward      float64   `json:"reward"`
		Deposit     float64   `json:"deposit"`
		Burned      float64   `json:"burned"`
		TDD         float64   `json:"days_destroyed"`
	}{
		Timestamp:   o.Timestamp,
		GasUsed:     o.GasUsed,
		StorageSize: o.StorageSize,
		StoragePaid: o.StoragePaid,
		Volume:      o.params.ConvertValue(o.Volume),
		Fee:         o.params.ConvertValue(o.Fee),
		Reward:      o.params.ConvertValue(o.Reward),
		Deposit:     o.params.ConvertValue(o.Deposit),
		Burned:      o.params.ConvertValue(o.Burned),
		TDD:         o.TDD,
	}
	return json.Marshal(op)
}

func (o *OpSeries) MarshalJSONBrief() ([]byte, error) {
	dec := o.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range o.columns {
		switch v {
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(o.Timestamp), 10)
		case "gas_used":
			buf = strconv.AppendInt(buf, o.GasUsed, 10)
		case "storage_size":
			buf = strconv.AppendInt(buf, o.StorageSize, 10)
		case "storage_paid":
			buf = strconv.AppendInt(buf, o.StoragePaid, 10)
		case "volume":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Volume), 'f', dec, 64)
		case "fee":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Fee), 'f', dec, 64)
		case "reward":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Reward), 'f', dec, 64)
		case "deposit":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Deposit), 'f', dec, 64)
		case "burned":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Burned), 'f', dec, 64)
		case "days_destroyed":
			buf = strconv.AppendFloat(buf, o.TDD, 'f', -1, 64)
		default:
			continue
		}
		if i < len(o.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (o *OpSeries) MarshalCSV() ([]string, error) {
	dec := o.params.Decimals
	res := make([]string, len(o.columns))
	for i, v := range o.columns {
		switch v {
		case "time":
			res[i] = strconv.FormatInt(util.UnixMilliNonZero(o.Timestamp), 10)
		case "gas_used":
			res[i] = strconv.FormatInt(o.GasUsed, 10)
		case "storage_size":
			res[i] = strconv.FormatInt(o.StorageSize, 10)
		case "storage_paid":
			res[i] = strconv.FormatInt(o.StoragePaid, 10)
		case "volume":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Volume), 'f', dec, 64)
		case "fee":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Fee), 'f', dec, 64)
		case "reward":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Reward), 'f', dec, 64)
		case "deposit":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Deposit), 'f', dec, 64)
		case "burned":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Burned), 'f', dec, 64)
		case "days_destroyed":
			res[i] = strconv.FormatFloat(o.TDD, 'f', -1, 64)
		default:
			continue
		}
	}
	return res, nil
}

func StreamOpSeries(ctx *ApiContext, args *SeriesRequest) (interface{}, int) {
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
		args.Columns = opSeriesNames
	}
	// resolve short column names
	srcNames = make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore non-series columns
		if !opSeriesNames.Contains(v) {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		n, ok := opSourceNames[v]
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

		case "type":
			// parse only the first value
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				typ := chain.ParseOpType(val[0])
				if !typ.IsValid() {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", val[0]), nil))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("t"),
					Mode:  mode,
					Value: int64(typ),
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				typs := make([]int64, 0)
				for _, t := range strings.Split(val[0], ",") {
					typ := chain.ParseOpType(t)
					if !typ.IsValid() {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", t), nil))
					}
					typs = append(typs, int64(typ))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("t"),
					Mode:  mode,
					Value: typs,
					Raw:   val[0], // debugging aid
				})

			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "sender", "receiver":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			field := "S" // sender
			if prefix == "receiver" {
				field = "R"
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  pack.FilterModeEqual,
						Value: uint64(0),
						Raw:   val[0], // debugging aid
					})
				} else {
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
			if short, ok := opSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "volume", "reward", "fee", "deposit", "burned":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
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
	os := &OpSeries{params: params, verbose: args.Verbose, columns: args.Columns}
	om := &model.Op{}
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
			if err := r.Decode(om); err != nil {
				return err
			}

			// output OpSeries when valid and time has crossed next boundary
			if !os.Timestamp.IsZero() && (om.Timestamp.Before(nextBucketTime) != (mul == 1)) {
				// output accumulated data
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}
				if err := enc.Encode(os); err != nil {
					return err
				}
				count++
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				os.Reset()
			}

			// init next time window from data
			if os.Timestamp.IsZero() {
				os.Timestamp = om.Timestamp.Truncate(window)
				nextBucketTime = os.Timestamp.Add(window * time.Duration(mul))
			}

			// accumulate data in OpSeries
			os.Add(om)
			return nil
		})
		// don't handle error here, will be picked up by trailer
		if err == nil {
			// output last series element
			if !os.Timestamp.IsZero() {
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				}
				err = enc.Encode(os)
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
				if err := r.Decode(om); err != nil {
					return err
				}

				// output OpSeries when valid and time has crossed next boundary
				if !os.Timestamp.IsZero() && (om.Timestamp.Before(nextBucketTime) != (mul == 1)) {
					// output accumulated data
					if err := enc.EncodeRecord(os); err != nil {
						return err
					}
					count++
					if args.Limit > 0 && count == int(args.Limit) {
						return io.EOF
					}
					os.Reset()
				}

				// init next time window from data
				if os.Timestamp.IsZero() {
					os.Timestamp = om.Timestamp.Truncate(window)
					nextBucketTime = os.Timestamp.Add(window * time.Duration(mul))
				}

				// accumulate data in OpSeries
				os.Add(om)
				return nil
			})
			if err == nil {
				// output last series element
				if !os.Timestamp.IsZero() {
					err = enc.EncodeRecord(os)
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
