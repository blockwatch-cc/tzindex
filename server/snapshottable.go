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
	snapSourceNames["delegate"] = "d"
	snapSourceNames["since_time"] = "S"
	snapAllAliases = append(snapAllAliases, "address")
	snapAllAliases = append(snapAllAliases, "delegate")
	snapAllAliases = append(snapAllAliases, "since_time")
}

// configurable marshalling helper
type Snapshot struct {
	model.Snapshot
	verbose bool                              `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList                   `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params                     `csv:"-" pack:"-"` // blockchain amount conversion
	addrs   map[model.AccountID]chain.Address `csv:"-" pack:"-"` // address map
	ctx     *ApiContext                       `csv:"-" pack:"-"`
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
		Index        int64   `json:"index"`
		Rolls        int64   `json:"rolls"`
		Account      string  `json:"address"`
		AccountId    uint64  `json:"account_id"`
		Delegate     string  `json:"delegate"`
		DelegateId   uint64  `json:"delegate_id"`
		IsDelegate   bool    `json:"is_delegate"`
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
		AccountId:    s.AccountId.Value(),
		Account:      s.addrs[s.AccountId].String(),
		DelegateId:   s.DelegateId.Value(),
		Delegate:     lookupAddress(s.ctx, s.DelegateId).String(),
		IsDelegate:   s.IsDelegate,
		IsActive:     s.IsActive,
		Balance:      s.params.ConvertValue(s.Balance),
		Delegated:    s.params.ConvertValue(s.Delegated),
		NDelegations: s.NDelegations,
		Since:        s.Since,
		SinceTime:    s.ctx.Indexer.BlockTimeMs(s.ctx.Context, s.Since),
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
			buf = strconv.AppendInt(buf, s.Snapshot.Index, 10)
		case "rolls":
			buf = strconv.AppendInt(buf, s.Snapshot.Rolls, 10)
		case "account_id":
			buf = strconv.AppendUint(buf, s.AccountId.Value(), 10)
		case "address":
			if s.AccountId > 0 {
				buf = strconv.AppendQuote(buf, s.addrs[s.AccountId].String())
			} else {
				buf = append(buf, "null"...)
			}
		case "delegate_id":
			buf = strconv.AppendUint(buf, s.DelegateId.Value(), 10)
		case "delegate":
			if s.DelegateId > 0 {
				buf = strconv.AppendQuote(buf, lookupAddress(s.ctx, s.DelegateId).String())
			} else {
				buf = append(buf, "null"...)
			}
		case "is_delegate":
			if s.IsDelegate {
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
			buf = strconv.AppendInt(buf, s.ctx.Indexer.BlockTimeMs(s.ctx.Context, s.Since), 10)
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
			res[i] = strconv.FormatInt(s.Snapshot.Index, 10)
		case "rolls":
			res[i] = strconv.FormatInt(s.Snapshot.Rolls, 10)
		case "account_id":
			res[i] = strconv.FormatUint(s.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(s.addrs[s.AccountId].String())
		case "delegate_id":
			res[i] = strconv.FormatUint(s.DelegateId.Value(), 10)
		case "delegate":
			res[i] = strconv.Quote(lookupAddress(s.ctx, s.DelegateId).String())
		case "is_delegate":
			res[i] = strconv.FormatBool(s.IsDelegate)
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
			res[i] = strconv.Quote(s.ctx.Indexer.BlockTime(s.ctx.Context, s.Since).Format(time.RFC3339))
		default:
			continue
		}
	}
	return res, nil
}

func StreamSnapshotTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// fetch chain params at current height
	params := ctx.Crawler.ParamsByHeight(-1)

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}
	accountT, err := ctx.Indexer.Table(index.AccountTableKey)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access table '%s'", index.AccountTableKey), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames     []string
		needAccountT bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := snapSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			switch v {
			case "address":
				needAccountT = true
			}
			if args.Verbose {
				needAccountT = true
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = snapAllAliases
		needAccountT = true
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
		case "address", "delegate":
			field := "a" // account
			if prefix == "delegate" {
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
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: uint64(0),
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-account lookup and compile condition
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

		case "since_time":
			// translate time into height, use val[0] only
			bestTime := ctx.Crawler.Time()
			bestHeight := ctx.Crawler.Height()
			cond, err := pack.ParseCondition(key, val[0], pack.FieldList{
				pack.Field{
					Name: "since_time",
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
						valueBlocks = append(valueBlocks, ctx.Indexer.BlockHeightFromTime(ctx.Context, v))
					} else {
						nDiff := int64(v.Sub(bestTime) / params.TimeBetweenBlocks[0])
						valueBlocks = append(valueBlocks, bestHeight+nDiff)
					}
				}
				q.Conditions = append(q.Conditions, pack.Condition{
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
					valueBlock = ctx.Indexer.BlockHeightFromTime(ctx.Context, valueTime)
				} else {
					nDiff := int64(valueTime.Sub(bestTime) / params.TimeBetweenBlocks[0])
					valueBlock = bestHeight + nDiff
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("S"), // since
					Mode:  cond.Mode,
					Value: valueBlock,
					Raw:   val[0], // debugging aid
				})
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := snapSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
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
						currentCycle := params.CycleFromHeight(ctx.Crawler.Height())
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				case "balance", "delegated":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
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

	// Step 1: query database
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(EInternal(EC_DATABASE, "query failed", err))
	}
	ctx.Log.Tracef("Processing result with %d rows %d cols", res.Rows(), res.Cols())
	defer res.Close()

	// Step 2: resolve related accounts using lookup (when requested)
	accMap := make(map[model.AccountID]chain.Address)
	if needAccountT && res.Rows() > 0 {
		// get a unique copy of account and delegate id columns (clip on request limit)
		acol, _ := res.Uint64Column("a")
		find := vec.UniqueUint64Slice(acol[:util.Min(len(acol), int(args.Limit))])

		// lookup accounts from id
		q := pack.Query{
			Name:   ctx.RequestID + ".account_lookup",
			Fields: accountT.Fields().Select("I", "H", "t"),
			Conditions: pack.ConditionList{pack.Condition{
				Field: accountT.Fields().Find("I"),
				Mode:  pack.FilterModeIn,
				Value: find,
			}},
		}
		type XAcc struct {
			Id   model.AccountID   `pack:"I,pk"`
			Hash []byte            `pack:"H"`
			Type chain.AddressType `pack:"t"`
		}
		acc := &XAcc{}
		err := accountT.Stream(ctx, q, func(r pack.Row) error {
			if err := r.Decode(acc); err != nil {
				return err
			}
			accMap[acc.Id] = chain.NewAddress(acc.Type, acc.Hash)
			return nil
		})
		if err != nil {
			// non-fatal error
			ctx.Log.Errorf("Account lookup failed: %v", err)
		}
	}

	// prepare return type marshalling
	snap := &Snapshot{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  params,
		addrs:   accMap,
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
		ctx.Log.Tracef("JSON encoded %d rows", count)

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
