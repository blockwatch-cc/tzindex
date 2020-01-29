// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"context"
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
	// long -> short form
	rightSourceNames map[string]string
	// short -> long form
	rightAliasNames map[string]string
	// all aliases as list
	rightAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Right{})
	if err != nil {
		log.Fatalf("rights field type error: %v\n", err)
	}
	rightSourceNames = fields.NameMapReverse()
	rightAllAliases = fields.Aliases()

	// add extra transalations
	rightSourceNames["address"] = "A"
	rightSourceNames["time"] = "-"
	rightAllAliases = append(rightAllAliases, "address")
	rightAllAliases = append(rightAllAliases, "time")
}

// configurable marshalling helper
type Right struct {
	model.Right
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"` // blockchain amount conversion
	height  int64           `csv:"-" pack:"-"` // best chain height
	ctx     *ApiContext     `csv:"-" pack:"-"`
}

func (r *Right) MarshalJSON() ([]byte, error) {
	if r.verbose {
		return r.MarshalJSONVerbose()
	} else {
		return r.MarshalJSONBrief()
	}
}

func (r *Right) TimestampMs() int64 {
	if diff := r.Height - r.height; diff > 0 {
		return r.ctx.Now.Add(time.Duration(diff)*r.params.TimeBetweenBlocks[0]).Unix() * 1000
	}
	// blocktime cache is lazy initialzed on first use by querying block table
	return r.ctx.Indexer.BlockTimeMs(context.Background(), r.Height)
}

func (r *Right) Timestamp() time.Time {
	if diff := r.Height - r.height; diff > 0 {
		return r.ctx.Now.Add(time.Duration(diff) * r.params.TimeBetweenBlocks[0])
	}
	// blocktime cache is lazy initialzed on first use by querying block table
	return r.ctx.Indexer.BlockTime(context.Background(), r.Height)
}

func (r *Right) MarshalJSONVerbose() ([]byte, error) {
	right := struct {
		RowId          uint64 `json:"row_id"`
		Height         int64  `json:"height"`
		Cycle          int64  `json:"cycle"`
		Timestamp      int64  `json:"time"`
		Type           string `json:"type"`
		Priority       int    `json:"priority"`
		AccountId      uint64 `json:"account_id"`
		Address        string `json:"address"`
		IsLost         bool   `json:"is_lost"`
		IsStolen       bool   `json:"is_stolen"`
		IsMissed       bool   `json:"is_missed"`
		IsSeedRequired bool   `json:"is_seed_required"`
		IsSeedRevealed bool   `json:"is_seed_revealed"`
	}{
		RowId:          r.RowId,
		Timestamp:      r.TimestampMs(),
		Height:         r.Height,
		Cycle:          r.Cycle,
		Type:           r.Type.String(),
		Priority:       r.Priority,
		AccountId:      r.AccountId.Value(),
		Address:        lookupAddress(r.ctx, r.AccountId).String(),
		IsLost:         r.IsLost,
		IsStolen:       r.IsStolen,
		IsMissed:       r.IsMissed,
		IsSeedRequired: r.IsSeedRequired,
		IsSeedRevealed: r.IsSeedRevealed,
	}
	return json.Marshal(right)
}

func (r *Right) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range r.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, r.RowId, 10)
		case "height":
			buf = strconv.AppendInt(buf, r.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, r.Cycle, 10)
		case "time":
			buf = strconv.AppendInt(buf, r.TimestampMs(), 10)
		case "type":
			buf = strconv.AppendQuote(buf, r.Type.String())
		case "priority":
			buf = strconv.AppendInt(buf, int64(r.Priority), 10)
		case "account_id":
			buf = strconv.AppendUint(buf, r.AccountId.Value(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, lookupAddress(r.ctx, r.AccountId).String())
		case "is_lost":
			if r.IsLost {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_stolen":
			if r.IsStolen {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_missed":
			if r.IsMissed {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_seed_required":
			if r.IsSeedRequired {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_seed_revealed":
			if r.IsSeedRevealed {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		default:
			continue
		}
		if i < len(r.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (r *Right) MarshalCSV() ([]string, error) {
	res := make([]string, len(r.columns))
	for i, v := range r.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(r.RowId, 10)
		case "height":
			res[i] = strconv.FormatInt(r.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(r.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(r.Timestamp().Format(time.RFC3339))
		case "type":
			res[i] = strconv.Quote(r.Type.String())
		case "priority":
			res[i] = strconv.Itoa(r.Priority)
		case "account_id":
			res[i] = strconv.FormatUint(r.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(lookupAddress(r.ctx, r.AccountId).String())
		case "is_lost":
			res[i] = strconv.FormatBool(r.IsLost)
		case "is_stolen":
			res[i] = strconv.FormatBool(r.IsStolen)
		case "is_missed":
			res[i] = strconv.FormatBool(r.IsMissed)
		case "is_seed_required":
			res[i] = strconv.FormatBool(r.IsSeedRequired)
		case "is_seed_revealed":
			res[i] = strconv.FormatBool(r.IsSeedRevealed)
		default:
			continue
		}
	}
	return res, nil
}

func StreamRightsTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// fetch chain params at current height
	params := ctx.Crawler.ParamsByHeight(-1)

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := rightSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = rightAllAliases
	}

	// build table query
	q := pack.Query{
		Name:       ctx.RequestID,
		Fields:     table.Fields().Select(srcNames...),
		Limit:      int(args.Limit),
		Conditions: make(pack.ConditionList, 0),
		Order:      args.Order,
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
		case "columns", "limit", "order", "verbose":
			// skip these fields
		case "cursor":
			// add row id condition: id > cursor (new cursor == last row id)
			id, err := strconv.ParseUint(val[0], 10, 64)
			if err != nil {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid cursor value '%s'", val), err))
			}
			cursorMode := pack.FilterModeGt
			if args.Order == pack.OrderDesc {
				cursorMode = pack.FilterModeLt
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Pk(),
				Mode:  cursorMode,
				Value: id,
				Raw:   val[0], // debugging aid
			})
		case "time":
			// translate time into height, use val[0] only
			bestTime := ctx.Crawler.Time()
			bestHeight := ctx.Crawler.Height()
			cond, err := pack.ParseCondition(key, val[0], pack.FieldList{
				pack.Field{
					Name: "time",
					Type: pack.FieldTypeDatetime,
				},
			})
			if err != nil {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, val[0]), err))
			}
			// re-use the block height -> time slice because it's already loaded
			// into memory, the binary search should be faster than a block query
			switch cond.Mode {
			case pack.FilterModeRange:
				// use cond.From and con.To
				from, to := cond.From.(time.Time), cond.To.(time.Time)
				var fromBlock, toBlock int64
				if !from.After(bestTime) {
					fromBlock = ctx.Indexer.BlockHeightFromTime(ctx.Context, from)
				} else {
					nDiff := int64(from.Sub(bestTime) / params.TimeBetweenBlocks[0])
					fromBlock = bestHeight + nDiff
				}
				if !to.After(bestTime) {
					toBlock = ctx.Indexer.BlockHeightFromTime(ctx.Context, to)
				} else {
					nDiff := int64(to.Sub(bestTime) / params.TimeBetweenBlocks[0])
					toBlock = bestHeight + nDiff
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("h"), // height
					Mode:  cond.Mode,
					From:  fromBlock,
					To:    toBlock,
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// cond.Value is slice
				valueBlocks := make([]int64, 0)
				for _, v := range cond.Value.([]time.Time) {
					if !v.After(bestTime) {
						valueBlocks = append(valueBlocks, ctx.Indexer.BlockHeightFromTime(ctx.Context, v))
					} else {
						nDiff := int64(v.Sub(bestTime) / params.TimeBetweenBlocks[0])
						valueBlocks = append(valueBlocks, bestHeight+nDiff)
					}
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("h"), // height
					Mode:  cond.Mode,
					Value: valueBlocks,
					Raw:   val[0], // debugging aid
				})

			default:
				// cond.Value is time.Time
				valueTime := cond.Value.(time.Time)
				var valueBlock int64
				if !valueTime.After(bestTime) {
					valueBlock = ctx.Indexer.BlockHeightFromTime(ctx.Context, valueTime)
				} else {
					nDiff := int64(valueTime.Sub(bestTime) / params.TimeBetweenBlocks[0])
					valueBlock = bestHeight + nDiff
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("h"), // height
					Mode:  cond.Mode,
					Value: valueBlock,
					Raw:   val[0], // debugging aid
				})
			}

		case "type":
			// parse only the first value
			typ := chain.ParseRightType(val[0])
			if !typ.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid right type '%s'", val[0]), nil))
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Find("t"),
				Mode:  pack.FilterModeEqual,
				Value: int64(typ),
				Raw:   val[0], // debugging aid
			})
		case "address":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
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
						Field: table.Fields().Find("A"), // acc id
						Mode:  mode,
						Value: uint64(math.MaxUint64),
						Raw:   "account not found", // debugging aid
					})
				} else {
					// add addr id as extra fund_flow condition
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find("A"), // acc id
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
					Field: table.Fields().Find("A"), // acc id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := rightSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				switch prefix {
				case "cycle":
					if v == "head" {
						currentCycle := params.CycleFromHeight(ctx.Crawler.Height())
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions = append(q.Conditions, cond)
				}
			}
		}
	}

	var (
		count  int
		lastId uint64
	)

	start := time.Now()
	ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Table)
	defer func() {
		ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	}()

	// prepare return type marshalling
	right := &Right{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  ctx.Crawler.ParamsByHeight(-1),
		height:  ctx.Crawler.Height(),
		ctx:     ctx,
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
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(right); err != nil {
				return err
			}
			if err := enc.Encode(right); err != nil {
				return err
			}
			count++
			lastId = right.RowId
			if args.Limit > 0 && count == int(args.Limit) {
				return io.EOF
			}
			return nil
		})
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
			// run query and stream results
			err = table.Stream(ctx, q, func(r pack.Row) error {
				if err := r.Decode(right); err != nil {
					return err
				}
				if err := enc.EncodeRecord(right); err != nil {
					return err
				}
				count++
				lastId = right.RowId
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				return nil
			})
		}
		ctx.Log.Tracef("CSV Encoded %d rows", count)
	}

	// without new records, cursor remains the same as input (may be empty)
	cursor := args.Cursor
	if lastId > 0 {
		cursor = strconv.FormatUint(lastId, 10)
	}

	// write error (except EOF), cursor and count as http trailer
	ctx.StreamTrailer(cursor, count, err)

	// streaming return
	return nil, -1
}
