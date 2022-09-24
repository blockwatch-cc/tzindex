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
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	opSeriesNames = util.StringList([]string{
		"time",
		"gas_used",
		"storage_paid",
		"volume",
		"fee",
		"reward",
		"deposit",
		"burned",
		"days_destroyed",
		"count",
	})
)

// Only use fields that can be summed over time
// configurable marshalling helper
type OpSeries struct {
	Timestamp   time.Time `json:"time"`
	Count       int       `json:"count"`
	GasUsed     int64     `json:"gas_used"`
	StoragePaid int64     `json:"storage_paid"`
	Volume      int64     `json:"volume"`
	Fee         int64     `json:"fee"`
	Reward      int64     `json:"reward"`
	Deposit     int64     `json:"deposit"`
	Burned      int64     `json:"burned"`

	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params
	verbose bool
	null    bool
}

var _ SeriesBucket = (*OpSeries)(nil)

func (s *OpSeries) Init(params *tezos.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *OpSeries) IsEmpty() bool {
	return s.Count == 0
}

func (s *OpSeries) Add(m SeriesModel) {
	o := m.(*model.Op)
	s.GasUsed += o.GasUsed
	s.StoragePaid += o.StoragePaid
	s.Volume += o.Volume
	s.Fee += o.Fee
	s.Reward += o.Reward
	s.Deposit += o.Deposit
	s.Burned += o.Burned
	s.Count++
}

func (s *OpSeries) Reset() {
	s.Timestamp = time.Time{}
	s.GasUsed = 0
	s.StoragePaid = 0
	s.Volume = 0
	s.Fee = 0
	s.Reward = 0
	s.Deposit = 0
	s.Burned = 0
	s.Count = 0
	s.null = false
}

func (s *OpSeries) Null(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	s.null = true
	return s
}

func (s *OpSeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *OpSeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *OpSeries) Time() time.Time {
	return s.Timestamp
}

func (s *OpSeries) Clone() SeriesBucket {
	return &OpSeries{
		Timestamp:   s.Timestamp,
		GasUsed:     s.GasUsed,
		StoragePaid: s.StoragePaid,
		Volume:      s.Volume,
		Fee:         s.Fee,
		Reward:      s.Reward,
		Deposit:     s.Deposit,
		Burned:      s.Burned,
		Count:       s.Count,
		columns:     s.columns,
		params:      s.params,
		verbose:     s.verbose,
		null:        s.null,
	}
}

func (s *OpSeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	o := m.(*OpSeries)
	weight := float64(ts.Sub(s.Timestamp)) / float64(o.Timestamp.Sub(s.Timestamp))
	if math.IsInf(weight, 1) {
		weight = 1
	}
	// log.Infof("INTERPOLATE %s -> %s %f = %d .. %s / %s", o.Timestamp, ts, weight,
	// 	s.Volume+int64(weight*float64(o.Volume-s.Volume)),
	// 	ts.Sub(s.Timestamp),
	// 	o.Timestamp, //.Truncate(window).Sub(s.Timestamp),
	// )
	switch weight {
	case 0:
		return s
	default:
		return &OpSeries{
			Timestamp:   ts,
			GasUsed:     s.GasUsed + int64(weight*float64(o.GasUsed-s.GasUsed)),
			StoragePaid: s.StoragePaid + int64(weight*float64(o.StoragePaid-s.StoragePaid)),
			Volume:      s.Volume + int64(weight*float64(o.Volume-s.Volume)),
			Fee:         s.Fee + int64(weight*float64(o.Fee-s.Fee)),
			Reward:      s.Reward + int64(weight*float64(o.Reward-s.Reward)),
			Deposit:     s.Deposit + int64(weight*float64(o.Deposit-s.Deposit)),
			Burned:      s.Burned + int64(weight*float64(o.Burned-s.Burned)),
			Count:       0,
			columns:     s.columns,
			params:      s.params,
			verbose:     s.verbose,
			null:        false,
		}
	}
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
		Count       int       `json:"count"`
		GasUsed     int64     `json:"gas_used"`
		StoragePaid int64     `json:"storage_paid"`
		Volume      float64   `json:"volume"`
		Fee         float64   `json:"fee"`
		Reward      float64   `json:"reward"`
		Deposit     float64   `json:"deposit"`
		Burned      float64   `json:"burned"`
	}{
		Timestamp:   o.Timestamp,
		Count:       o.Count,
		GasUsed:     o.GasUsed,
		StoragePaid: o.StoragePaid,
		Volume:      o.params.ConvertValue(o.Volume),
		Fee:         o.params.ConvertValue(o.Fee),
		Reward:      o.params.ConvertValue(o.Reward),
		Deposit:     o.params.ConvertValue(o.Deposit),
		Burned:      o.params.ConvertValue(o.Burned),
	}
	return json.Marshal(op)
}

func (o *OpSeries) MarshalJSONBrief() ([]byte, error) {
	dec := o.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range o.columns {
		if o.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(o.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(o.Timestamp), 10)
			case "count":
				buf = strconv.AppendInt(buf, int64(o.Count), 10)
			case "gas_used":
				buf = strconv.AppendInt(buf, o.GasUsed, 10)
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
			default:
				continue
			}
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
		if o.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(o.Timestamp.Format(time.RFC3339))
			default:
				continue
			}
		}
		switch v {
		case "time":
			res[i] = strconv.Quote(o.Timestamp.Format(time.RFC3339))
		case "count":
			res[i] = strconv.FormatInt(int64(o.Count), 10)
		case "gas_used":
			res[i] = strconv.FormatInt(o.GasUsed, 10)
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
		default:
			continue
		}
	}
	return res, nil
}

func (s *OpSeries) BuildQuery(ctx *server.Context, args *SeriesRequest) pack.Query {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Series), err))
	}

	// time is auto-added from parser
	if len(args.Columns) == 1 {
		// use all series columns
		args.Columns = opSeriesNames
	}
	// resolve short column names
	srcNames := make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore count column
		if v == "count" {
			continue
		}
		// ignore non-series columns
		if !opSeriesNames.Contains(v) {
			panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		srcNames = append(srcNames, v)
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
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

		case "type":
			// parse only the first value
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				typ := model.ParseOpType(val[0])
				if !typ.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", val[0]), nil))
				}
				q = q.And("type", mode, typ)
			case pack.FilterModeIn, pack.FilterModeNotIn:
				typs := make([]uint8, 0)
				for _, t := range strings.Split(val[0], ",") {
					typ := model.ParseOpType(t)
					if !typ.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", t), nil))
					}
					typs = append(typs, uint8(typ))
				}
				q = q.And("type", mode, typs)

			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "sender", "receiver":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			field := "sender_id"
			if prefix == "receiver" {
				field = "receiver_id"
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q = q.AndEqual(field, 0)
				} else {
					// single-address lookup and compile condition
					addr, err := tezos.ParseAddress(val[0])
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					// Note: when not found we insert an always false condition
					if acc == nil || acc.RowId == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						// add id as extra condition
						q = q.And(field, mode, acc.RowId)
					}
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
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
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
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "volume", "reward", "fee", "deposit", "burned":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q = q.AndCondition(cond)
				}
			}
		}
	}

	return q
}
