// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	rightSourceNames map[string]string
	// all aliases as list
	rightAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Right{})
	if err != nil {
		fmt.Printf("rights field type error: %v\n", err)
		// log.Fatalf("rights field type error: %v\n", err)
	}
	rightSourceNames = fields.NameMapReverse()
	rightAllAliases = fields.Aliases()

	// add extra translations
	rightSourceNames["address"] = "A"
	rightAllAliases = append(rightAllAliases, "address")
}

// configurable marshalling helper
type Right struct {
	model.Right
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	ctx     *server.Context
	params  *rpc.Params
}

func (r *Right) MarshalJSON() ([]byte, error) {
	if r.verbose {
		return r.MarshalJSONVerbose()
	} else {
		return r.MarshalJSONBrief()
	}
}

func (r *Right) MarshalJSONVerbose() ([]byte, error) {
	right := struct {
		RowId     uint64 `json:"row_id"`
		Cycle     int64  `json:"cycle"`
		Height    int64  `json:"height"`
		AccountId uint64 `json:"account_id"`
		Address   string `json:"address"`
		Bake      string `json:"baking_rights"`
		Endorse   string `json:"endorsing_rights"`
		Baked     string `json:"blocks_baked"`
		Endorsed  string `json:"blocks_endorsed"`
		Seed      string `json:"seeds_required"`
		Seeded    string `json:"seeds_revealed"`
	}{
		RowId:     r.RowId,
		Cycle:     r.Cycle,
		Height:    r.Height,
		AccountId: r.AccountId.U64(),
		Address:   r.ctx.Indexer.LookupAddress(r.ctx, r.AccountId).String(),
		Bake:      hex.EncodeToString(r.Bake.Bytes()),
		Endorse:   hex.EncodeToString(r.Endorse.Bytes()),
		Baked:     hex.EncodeToString(r.Baked.Bytes()),
		Endorsed:  hex.EncodeToString(r.Endorsed.Bytes()),
		Seed:      hex.EncodeToString(r.Seed.Bytes()),
		Seeded:    hex.EncodeToString(r.Seeded.Bytes()),
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
		case "cycle":
			buf = strconv.AppendInt(buf, r.Cycle, 10)
		case "height":
			buf = strconv.AppendInt(buf, r.Height, 10)
		case "account_id":
			buf = strconv.AppendUint(buf, r.AccountId.U64(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, r.ctx.Indexer.LookupAddress(r.ctx, r.AccountId).String())
		case "baking_rights":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(r.Bake.Bytes()))
		case "endorsing_rights":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(r.Endorse.Bytes()))
		case "blocks_baked":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(r.Baked.Bytes()))
		case "blocks_endorsed":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(r.Endorsed.Bytes()))
		case "seeds_required":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(r.Seed.Bytes()))
		case "seeds_revealed":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(r.Seeded.Bytes()))
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
		case "cycle":
			res[i] = strconv.FormatInt(r.Cycle, 10)
		case "height":
			res[i] = strconv.FormatInt(r.Height, 10)
		case "account_id":
			res[i] = strconv.FormatUint(r.AccountId.U64(), 10)
		case "address":
			res[i] = strconv.Quote(r.ctx.Indexer.LookupAddress(r.ctx, r.AccountId).String())
		case "baking_rights":
			res[i] = strconv.Quote(hex.EncodeToString(r.Bake.Bytes()))
		case "endorsing_rights":
			res[i] = strconv.Quote(hex.EncodeToString(r.Endorse.Bytes()))
		case "blocks_baked":
			res[i] = strconv.Quote(hex.EncodeToString(r.Baked.Bytes()))
		case "blocks_endorsed":
			res[i] = strconv.Quote(hex.EncodeToString(r.Endorsed.Bytes()))
		case "seeds_required":
			res[i] = strconv.Quote(hex.EncodeToString(r.Seed.Bytes()))
		case "seeds_revealed":
			res[i] = strconv.Quote(hex.EncodeToString(r.Seeded.Bytes()))
		default:
			continue
		}
	}
	return res, nil
}

func StreamRightsTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// fetch chain params at current height
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
			n, ok := rightSourceNames[v]
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
		args.Columns = rightAllAliases
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
		field := rightSourceNames[prefix]
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

		case "address":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := tezos.ParseAddress(val[0])
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != model.ErrNoAccount {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				// Note: when not found we insert an always false condition
				if acc == nil || acc.RowId == 0 {
					q = q.And(field, mode, uint64(math.MaxUint64))
				} else {
					// add addr id as extra fund_flow condition
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
					if err != nil && err != model.ErrNoAccount {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					// skip not found account
					if acc == nil || acc.RowId == 0 {
						continue
					}
					// collect list of account ids
					ids = append(ids, acc.RowId.U64())
				}
				// Note: when list is empty (no accounts were found, the match will
				//       always be false and return no result as expected)
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := rightSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

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

	var (
		count  int
		lastId uint64
	)

	// start := time.Now()
	// ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Table)
	// defer func() {
	// 	ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

	// prepare return type marshalling
	right := &Right{
		verbose: args.Verbose,
		columns: args.Columns,
		ctx:     ctx,
		params:  params,
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
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
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
