// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

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
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	snapSourceNames map[string]string
	// short -> long form
	snapAliasNames map[string]string
	// all aliases as list
	snapAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Snapshot{})
	if err != nil {
		log.Fatalf("account field type error: %v\n", err)
	}
	snapSourceNames = fields.NameMapReverse()
	snapAllAliases = fields.Aliases()

	// add extra transalations for accounts
	snapSourceNames["address"] = "a"
	snapSourceNames["baker"] = "d"
	snapSourceNames["since_time"] = "S"
	snapAllAliases = append(snapAllAliases, "address")
	snapAllAliases = append(snapAllAliases, "baker")
	snapAllAliases = append(snapAllAliases, "since_time")
}

// configurable marshalling helper
type Snapshot struct {
	model.Snapshot
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	ctx     *server.Context
}

func (s *Snapshot) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (s *Snapshot) MarshalJSONVerbose() ([]byte, error) {
	snap := struct {
		RowId        uint64  `json:"row_id"`
		Height       int64   `json:"height"`
		Cycle        int64   `json:"cycle"`
		IsSelected   bool    `json:"is_selected"`
		Timestamp    int64   `json:"time"` // convert to UNIX milliseconds
		Index        int     `json:"index"`
		Rolls        int64   `json:"rolls"`
		ActiveStake  float64 `json:"active_stake"`
		Account      string  `json:"address"`
		AccountId    uint64  `json:"account_id"`
		Baker        string  `json:"baker"`
		BakerId      uint64  `json:"baker_id"`
		IsBaker      bool    `json:"is_baker"`
		IsActive     bool    `json:"is_active"`
		Balance      float64 `json:"balance"`
		Delegated    float64 `json:"delegated"`
		NDelegations int64   `json:"n_delegations"`
		Since        int64   `json:"since"`
		SinceTime    int64   `json:"since_time"`
	}{
		RowId:        s.RowId,
		Height:       s.Height,
		Cycle:        s.Cycle,
		IsSelected:   s.IsSelected,
		Timestamp:    util.UnixMilliNonZero(s.Timestamp),
		Index:        s.Snapshot.Index,
		Rolls:        s.Snapshot.Rolls,
		ActiveStake:  s.params.ConvertValue(s.ActiveStake),
		AccountId:    s.AccountId.Value(),
		Account:      s.ctx.Indexer.LookupAddress(s.ctx, s.AccountId).String(),
		BakerId:      s.BakerId.Value(),
		Baker:        s.ctx.Indexer.LookupAddress(s.ctx, s.BakerId).String(),
		IsBaker:      s.IsBaker,
		IsActive:     s.IsActive,
		Balance:      s.params.ConvertValue(s.Balance),
		Delegated:    s.params.ConvertValue(s.Delegated),
		NDelegations: s.NDelegations,
		Since:        s.Since,
		SinceTime:    s.ctx.Indexer.LookupBlockTimeMs(s.ctx.Context, s.Since),
	}
	return json.Marshal(snap)
}

func (s *Snapshot) MarshalJSONBrief() ([]byte, error) {
	dec := s.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range s.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, s.RowId, 10)
		case "height":
			buf = strconv.AppendInt(buf, s.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, s.Cycle, 10)
		case "is_selected":
			if s.IsSelected {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(s.Timestamp), 10)
		case "index":
			buf = strconv.AppendInt(buf, int64(s.Snapshot.Index), 10)
		case "rolls":
			buf = strconv.AppendInt(buf, s.Snapshot.Rolls, 10)
		case "active_stake":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.ActiveStake), 'f', dec, 64)
		case "account_id":
			buf = strconv.AppendUint(buf, s.AccountId.Value(), 10)
		case "address":
			if s.AccountId > 0 {
				buf = strconv.AppendQuote(buf, s.ctx.Indexer.LookupAddress(s.ctx, s.AccountId).String())
			} else {
				buf = append(buf, null...)
			}
		case "baker_id":
			buf = strconv.AppendUint(buf, s.BakerId.Value(), 10)
		case "baker":
			if s.BakerId > 0 {
				buf = strconv.AppendQuote(buf, s.ctx.Indexer.LookupAddress(s.ctx, s.BakerId).String())
			} else {
				buf = append(buf, null...)
			}
		case "is_baker":
			if s.IsBaker {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_active":
			if s.IsActive {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "balance":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Balance), 'f', dec, 64)
		case "delegated":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Delegated), 'f', dec, 64)
		case "n_delegations":
			buf = strconv.AppendInt(buf, s.NDelegations, 10)
		case "since":
			buf = strconv.AppendInt(buf, s.Since, 10)
		case "since_time":
			buf = strconv.AppendInt(buf, s.ctx.Indexer.LookupBlockTimeMs(s.ctx.Context, s.Since), 10)
		default:
			continue
		}
		if i < len(s.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (s *Snapshot) MarshalCSV() ([]string, error) {
	dec := s.params.Decimals
	res := make([]string, len(s.columns))
	for i, v := range s.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(s.RowId, 10)
		case "height":
			res[i] = strconv.FormatInt(s.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(s.Cycle, 10)
		case "is_selected":
			res[i] = strconv.FormatBool(s.IsSelected)
		case "time":
			res[i] = strconv.Quote(s.Timestamp.Format(time.RFC3339))
		case "index":
			res[i] = strconv.FormatInt(int64(s.Snapshot.Index), 10)
		case "rolls":
			res[i] = strconv.FormatInt(s.Snapshot.Rolls, 10)
		case "active_stake":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.ActiveStake), 'f', dec, 64)
		case "account_id":
			res[i] = strconv.FormatUint(s.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(s.ctx.Indexer.LookupAddress(s.ctx, s.AccountId).String())
		case "baker_id":
			res[i] = strconv.FormatUint(s.BakerId.Value(), 10)
		case "baker":
			res[i] = strconv.Quote(s.ctx.Indexer.LookupAddress(s.ctx, s.BakerId).String())
		case "is_baker":
			res[i] = strconv.FormatBool(s.IsBaker)
		case "is_active":
			res[i] = strconv.FormatBool(s.IsActive)
		case "balance":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Balance), 'f', dec, 64)
		case "delegated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Delegated), 'f', dec, 64)
		case "n_delegations":
			res[i] = strconv.FormatInt(s.NDelegations, 10)
		case "since":
			res[i] = strconv.FormatInt(s.Since, 10)
		case "since_time":
			res[i] = strconv.Quote(s.ctx.Indexer.LookupBlockTime(s.ctx.Context, s.Since).Format(time.RFC3339))
		default:
			continue
		}
	}
	return res, nil
}

func StreamSnapshotTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames []string
		// needAccountT bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := snapSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = snapAllAliases
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID, table).
		WithFields(srcNames...).
		WithLimit(int(args.Limit)).
		WithOrder(args.Order)

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
		case "columns", "limit", "order", "verbose", "filename":
			// skip these fields
		case "cursor":
			// add row id condition: id > cursor (new cursor == last row id)
			id, err := strconv.ParseUint(val[0], 10, 64)
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid cursor value '%s'", val), err))
			}
			cursorMode := pack.FilterModeGt
			if args.Order == pack.OrderDesc {
				cursorMode = pack.FilterModeLt
			}
			q.Conditions.AddAndCondition(&pack.Condition{
				Field: table.Fields().Pk(),
				Mode:  cursorMode,
				Value: id,
				Raw:   val[0], // debugging aid
			})
		case "address", "baker":
			field := "a" // account
			if prefix == "baker" {
				field = "d"
			}
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== no delegate/manager set)
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: 0,
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-account lookup and compile condition
					addr, err := tezos.ParseAddress(val[0])
					if err != nil || !addr.IsValid() {
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
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil || !addr.IsValid() {
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

		case "since_time":
			// translate time into height, use val[0] only
			bestTime := ctx.Tip.BestTime
			bestHeight := ctx.Tip.BestHeight
			cond, err := pack.ParseCondition(key, val[0], pack.FieldList{
				pack.Field{
					Name: "since_time",
					Type: pack.FieldTypeDatetime,
				},
			})
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, val[0]), err))
			}
			// re-use the block height -> time slice because it's already loaded
			// into memory, the binary search should be faster than a block query
			switch cond.Mode {
			case pack.FilterModeRange:
				// use cond.From and con.To
				from, to := cond.From.(time.Time), cond.To.(time.Time)
				var fromBlock, toBlock int64
				if !from.After(bestTime) {
					fromBlock = ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, from)
				} else {
					nDiff := int64(from.Sub(bestTime) / params.BlockTime())
					fromBlock = bestHeight + nDiff
				}
				if !to.After(bestTime) {
					toBlock = ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, to)
				} else {
					nDiff := int64(to.Sub(bestTime) / params.BlockTime())
					toBlock = bestHeight + nDiff
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("S"), // since
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
						valueBlocks = append(valueBlocks, ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, v))
					} else {
						nDiff := int64(v.Sub(bestTime) / params.BlockTime())
						valueBlocks = append(valueBlocks, bestHeight+nDiff)
					}
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("S"), // since
					Mode:  cond.Mode,
					Value: valueBlocks,
					Raw:   val[0], // debugging aid
				})

			default:
				// cond.Value is time.Time
				valueTime := cond.Value.(time.Time)
				var valueBlock int64
				if !valueTime.After(bestTime) {
					valueBlock = ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, valueTime)
				} else {
					nDiff := int64(valueTime.Sub(bestTime) / params.BlockTime())
					valueBlock = bestHeight + nDiff
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("S"), // since
					Mode:  cond.Mode,
					Value: valueBlock,
					Raw:   val[0], // debugging aid
				})
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := snapSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "cycle":
					if v == "head" {
						currentCycle := params.CycleFromHeight(ctx.Tip.BestHeight)
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				case "balance", "delegated", "active_stake":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions.AddAndCondition(&cond)
				}
			}
		}
	}

	var (
		count  int
		lastId uint64
	)

	// start := time.Now()
	// ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Table)
	// defer func() {
	// 	ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

	// Step 1: query database
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "query failed", err))
	}
	// ctx.Log.Tracef("Processing result with %d rows %d cols", res.Rows(), res.Cols())
	defer res.Close()

	// prepare return type marshalling
	snap := &Snapshot{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  params,
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
		err = res.Walk(func(r pack.Row) error {
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(snap); err != nil {
				return err
			}
			if err := enc.Encode(snap); err != nil {
				return err
			}
			count++
			lastId = snap.RowId
			if args.Limit > 0 && count == int(args.Limit) {
				return io.EOF
			}
			return nil
		})
		// close JSON bracket
		io.WriteString(ctx.ResponseWriter, "]")
		// ctx.Log.Tracef("JSON encoded %d rows", count)

	case "csv":
		enc := csv.NewEncoder(ctx.ResponseWriter)
		// use custom header columns and order
		if len(args.Columns) > 0 {
			err = enc.EncodeHeader(args.Columns, nil)
		}
		if err == nil {
			// run query and stream results
			err = res.Walk(func(r pack.Row) error {
				if err := r.Decode(snap); err != nil {
					return err
				}
				if err := enc.EncodeRecord(snap); err != nil {
					return err
				}
				count++
				lastId = snap.RowId
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				return nil
			})
		}
		// ctx.Log.Tracef("CSV Encoded %d rows", count)
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
