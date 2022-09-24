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
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	flowSourceNames map[string]string
	// all aliases as list
	flowAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Flow{})
	if err != nil {
		log.Fatalf("flow field type error: %v\n", err)
	}
	flowSourceNames = fields.NameMapReverse()
	flowAllAliases = fields.Aliases()

	// add extra translations
	flowSourceNames["address"] = "A"
	flowSourceNames["counterparty"] = "R"
	flowAllAliases = append(flowAllAliases, "address")
	flowAllAliases = append(flowAllAliases, "counterparty")
}

// configurable marshalling helper
type Flow struct {
	model.Flow
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	ctx     *server.Context
}

func (f *Flow) MarshalJSON() ([]byte, error) {
	if f.verbose {
		return f.MarshalJSONVerbose()
	} else {
		return f.MarshalJSONBrief()
	}
}

func (f *Flow) MarshalJSONVerbose() ([]byte, error) {
	flow := struct {
		RowId          uint64  `json:"row_id"`
		Height         int64   `json:"height"`
		Cycle          int64   `json:"cycle"`
		Timestamp      int64   `json:"time"`
		OpN            int     `json:"op_n"`
		OpC            int     `json:"op_c"`
		OpI            int     `json:"op_i"`
		AccountId      uint64  `json:"account_id"`
		Account        string  `json:"address"`
		CounterParty   string  `json:"counterparty"`
		CounterPartyId uint64  `json:"counterparty_id"`
		Category       string  `json:"category"`
		Operation      string  `json:"operation"`
		AmountIn       float64 `json:"amount_in"`
		AmountOut      float64 `json:"amount_out"`
		IsFee          bool    `json:"is_fee"`
		IsBurned       bool    `json:"is_burned"`
		IsFrozen       bool    `json:"is_frozen"`
		IsUnfrozen     bool    `json:"is_unfrozen"`
		IsShielded     bool    `json:"is_shielded"`
		IsUnshielded   bool    `json:"is_unshielded"`
		TokenAge       int64   `json:"token_age"`
	}{
		RowId:          f.RowId,
		Height:         f.Height,
		Cycle:          f.Cycle,
		Timestamp:      util.UnixMilliNonZero(f.Timestamp),
		OpN:            f.OpN,
		OpC:            f.OpC,
		OpI:            f.OpI,
		AccountId:      f.AccountId.Value(),
		Account:        f.ctx.Indexer.LookupAddress(f.ctx, f.AccountId).String(),
		CounterPartyId: f.CounterPartyId.Value(),
		CounterParty:   f.ctx.Indexer.LookupAddress(f.ctx, f.CounterPartyId).String(),
		Category:       f.Category.String(),
		Operation:      f.Operation.String(),
		AmountIn:       f.params.ConvertValue(f.AmountIn),
		AmountOut:      f.params.ConvertValue(f.AmountOut),
		IsFee:          f.IsFee,
		IsBurned:       f.IsBurned,
		IsFrozen:       f.IsFrozen,
		IsUnfrozen:     f.IsUnfrozen,
		IsShielded:     f.IsShielded,
		IsUnshielded:   f.IsUnshielded,
		TokenAge:       f.TokenAge,
	}
	return json.Marshal(flow)
}

func (f *Flow) MarshalJSONBrief() ([]byte, error) {
	dec := f.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range f.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, f.RowId, 10)
		case "height":
			buf = strconv.AppendInt(buf, f.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, f.Cycle, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(f.Timestamp), 10)
		case "op_n":
			buf = strconv.AppendInt(buf, int64(f.OpN), 10)
		case "op_c":
			buf = strconv.AppendInt(buf, int64(f.OpC), 10)
		case "op_i":
			buf = strconv.AppendInt(buf, int64(f.OpI), 10)
		case "account_id":
			buf = strconv.AppendUint(buf, f.AccountId.Value(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, f.ctx.Indexer.LookupAddress(f.ctx, f.AccountId).String())
		case "counterparty_id":
			buf = strconv.AppendUint(buf, f.CounterPartyId.Value(), 10)
		case "counterparty":
			buf = strconv.AppendQuote(buf, f.ctx.Indexer.LookupAddress(f.ctx, f.CounterPartyId).String())
		case "category":
			buf = strconv.AppendQuote(buf, f.Category.String())
		case "operation":
			buf = strconv.AppendQuote(buf, f.Operation.String())
		case "amount_in":
			buf = strconv.AppendFloat(buf, f.params.ConvertValue(f.AmountIn), 'f', dec, 64)
		case "amount_out":
			buf = strconv.AppendFloat(buf, f.params.ConvertValue(f.AmountOut), 'f', dec, 64)
		case "is_fee":
			if f.IsFee {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_burned":
			if f.IsBurned {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_frozen":
			if f.IsFrozen {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_unfrozen":
			if f.IsUnfrozen {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_shielded":
			if f.IsShielded {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_unshielded":
			if f.IsUnshielded {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "token_age":
			buf = strconv.AppendInt(buf, f.TokenAge, 10)
		default:
			continue
		}
		if i < len(f.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (f *Flow) MarshalCSV() ([]string, error) {
	dec := f.params.Decimals
	res := make([]string, len(f.columns))
	for i, v := range f.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(f.RowId, 10)
		case "height":
			res[i] = strconv.FormatInt(f.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(f.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(f.Timestamp.Format(time.RFC3339))
		case "op_n":
			res[i] = strconv.FormatInt(int64(f.OpN), 10)
		case "op_c":
			res[i] = strconv.FormatInt(int64(f.OpC), 10)
		case "op_ni":
			res[i] = strconv.FormatInt(int64(f.OpI), 10)
		case "account_id":
			res[i] = strconv.FormatUint(f.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(f.ctx.Indexer.LookupAddress(f.ctx, f.AccountId).String())
		case "counterparty_id":
			res[i] = strconv.FormatUint(f.CounterPartyId.Value(), 10)
		case "counterparty":
			res[i] = strconv.Quote(f.ctx.Indexer.LookupAddress(f.ctx, f.CounterPartyId).String())
		case "category":
			res[i] = strconv.Quote(f.Category.String())
		case "operation":
			res[i] = strconv.Quote(f.Operation.String())
		case "amount_in":
			res[i] = strconv.FormatFloat(f.params.ConvertValue(f.AmountIn), 'f', dec, 64)
		case "amount_out":
			res[i] = strconv.FormatFloat(f.params.ConvertValue(f.AmountOut), 'f', dec, 64)
		case "is_fee":
			res[i] = strconv.FormatBool(f.IsFee)
		case "is_burned":
			res[i] = strconv.FormatBool(f.IsBurned)
		case "is_frozen":
			res[i] = strconv.FormatBool(f.IsFrozen)
		case "is_unfrozen":
			res[i] = strconv.FormatBool(f.IsUnfrozen)
		case "is_shielded":
			res[i] = strconv.FormatBool(f.IsShielded)
		case "is_unshielded":
			res[i] = strconv.FormatBool(f.IsUnshielded)
		case "token_age":
			res[i] = strconv.FormatInt(f.TokenAge, 10)
		default:
			continue
		}
	}
	return res, nil
}

func StreamFlowTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := flowSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			if v == "op" {
				srcNames = append(srcNames, "1") // op_n
				srcNames = append(srcNames, "2") // op_c
				srcNames = append(srcNames, "3") // op_i
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = flowAllAliases
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
		WithFields(srcNames...).
		WithLimit(int(args.Limit)).
		WithOrder(args.Order)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		field := flowSourceNames[prefix]
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
			q = q.And("I", cursorMode, id)

		case "address", "counterparty":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := tezos.ParseAddress(val[0])
				if err != nil || !addr.IsValid() {
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
		case "op":
			// parse op hash and lookup op_*
			// valid filter modes: eq, in
			// 1 resolve from op table
			// 2 add eq/in cond: for height + op_n's
			switch mode {
			case pack.FilterModeEqual:
				if val[0] != "" {
					// single-op lookup and compile condition
					// ignore endorsements
					ops, err := ctx.Indexer.LookupOp(ctx, val[0], etl.ListRequest{})
					if err != nil {
						switch err {
						case index.ErrNoOpEntry:
							panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("no such op '%s'", val[0]), nil))
						case etl.ErrInvalidHash:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", val[0]), err))
						default:
							panic(server.EInternal(server.EC_DATABASE, fmt.Sprintf("cannot lookup op hash '%s'", val[0]), err))
						}
					}
					height := ops[0].Height
					opns := make([]int, 0)
					for _, v := range ops {
						opns = append(opns, v.OpN)
					}
					q = q.AndEqual("h", height)
					if len(opns) == 1 {
						q = q.AndEqual("1", opns[0])
					} else {
						q = q.AndIn("1", opns)
					}
				}
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := flowSourceNames[prefix]; !ok {
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
						v = strconv.FormatInt(currentCycle, 10)
					}
				case "amount_in", "amount_out":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				case "address_type":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]uint8, 0)
					for _, t := range strings.Split(v, ",") {
						typ := tezos.ParseAddressType(t)
						if !typ.IsValid() {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid account type '%s'", val[0]), nil))
						}
						typs = append(typs, uint8(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueUint8Slice(typs) {
						styps = append(styps, strconv.FormatUint(uint64(i), 10))
					}
					v = strings.Join(styps, ",")
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
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", val[0]), nil))
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
					q = q.AndCondition(cond)
				}
			}
		}
	}

	var (
		count  int
		lastId uint64
	)

	// prepare return type marshalling
	flow := &Flow{
		verbose: args.Verbose,
		columns: args.Columns,
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
		_, _ = io.WriteString(ctx.ResponseWriter, "[")
		// close JSON array on panic
		defer func() {
			if e := recover(); e != nil {
				_, _ = io.WriteString(ctx.ResponseWriter, "]")
				panic(e)
			}
		}()

		// run query and stream results
		var needComma bool
		err = table.Stream(ctx.Context, q, func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(flow); err != nil {
				return err
			}
			if err := enc.Encode(flow); err != nil {
				return err
			}
			count++
			lastId = flow.RowId
			if args.Limit > 0 && count == int(args.Limit) {
				return io.EOF
			}
			return nil
		})
		// close JSON bracket
		_, _ = io.WriteString(ctx.ResponseWriter, "]")
		// ctx.Log.Tracef("JSON encoded %d rows", count)

	case "csv":
		enc := csv.NewEncoder(ctx.ResponseWriter)
		// use custom header columns and order
		if len(args.Columns) > 0 {
			err = enc.EncodeHeader(args.Columns, nil)
		}
		if err == nil {
			// run query and stream results
			err = table.Stream(ctx.Context, q, func(r pack.Row) error {
				if err := r.Decode(flow); err != nil {
					return err
				}
				if err := enc.EncodeRecord(flow); err != nil {
					return err
				}
				count++
				lastId = flow.RowId
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
