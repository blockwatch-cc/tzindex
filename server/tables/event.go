// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	eventSourceNames map[string]string
	// all aliases as list
	eventAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Event{})
	if err != nil {
		log.Fatalf("event field type error: %v\n", err)
	}
	eventSourceNames = fields.NameMapReverse()
	eventAllAliases = fields.Aliases()
	eventSourceNames["contract"] = "C"
	eventAllAliases = append(eventAllAliases, "contract")
}

// configurable marshalling helper
type Event struct {
	model.Event
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	ctx     *server.Context
}

func (c *Event) MarshalJSON() ([]byte, error) {
	if c.verbose {
		return c.MarshalJSONVerbose()
	} else {
		return c.MarshalJSONBrief()
	}
}

func (e *Event) MarshalJSONVerbose() ([]byte, error) {
	ev := struct {
		RowId     uint64 `json:"row_id"`
		Contract  string `json:"contract"`
		AccountId uint64 `json:"account_id"`
		Height    int64  `json:"height"`
		OpId      uint64 `json:"op_id"`
		Type      string `json:"type"`
		Payload   string `json:"payload"`
		Tag       string `json:"tag"`
		TypeHash  string `json:"type_hash"`
	}{
		RowId:     e.RowId.Value(),
		Contract:  e.ctx.Indexer.LookupAddress(e.ctx, e.AccountId).String(),
		AccountId: e.AccountId.Value(),
		Height:    e.Height,
		OpId:      e.OpId,
		Type:      hex.EncodeToString(e.Type),
		Payload:   hex.EncodeToString(e.Payload),
		Tag:       e.Tag,
		TypeHash:  util.U64String(e.TypeHash).Hex(),
	}
	return json.Marshal(ev)
}

func (e *Event) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range e.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, e.RowId.Value(), 10)
		case "contract":
			buf = strconv.AppendQuote(buf, e.ctx.Indexer.LookupAddress(e.ctx, e.AccountId).String())
		case "account_id":
			buf = strconv.AppendUint(buf, e.AccountId.Value(), 10)
		case "height":
			buf = strconv.AppendInt(buf, e.Height, 10)
		case "op_id":
			buf = strconv.AppendUint(buf, e.OpId, 10)
		case "type":
			if e.Type != nil {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(e.Type))
			} else {
				buf = append(buf, null...)
			}
		case "payload":
			if e.Payload != nil {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(e.Payload))
			} else {
				buf = append(buf, null...)
			}
		case "tag":
			buf = strconv.AppendQuote(buf, e.Tag)
		case "type_hash":
			buf = strconv.AppendQuote(buf, util.U64String(e.TypeHash).Hex())
		default:
			continue
		}
		if i < len(e.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (e *Event) MarshalCSV() ([]string, error) {
	res := make([]string, len(e.columns))
	for i, v := range e.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(e.RowId.Value(), 10)
		case "contract":
			res[i] = strconv.Quote(e.ctx.Indexer.LookupAddress(e.ctx, e.AccountId).String())
		case "account_id":
			res[i] = strconv.FormatUint(e.AccountId.Value(), 10)
		case "height":
			res[i] = strconv.FormatInt(e.Height, 10)
		case "op_id":
			res[i] = strconv.FormatUint(e.OpId, 10)
		case "type":
			res[i] = strconv.Quote(hex.EncodeToString(e.Type))
		case "payload":
			res[i] = strconv.Quote(hex.EncodeToString(e.Payload))
		case "type_hash":
			res[i] = strconv.Quote(util.U64String(e.TypeHash).Hex())
		default:
			continue
		}
	}
	return res, nil
}

func StreamEventTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// params := ctx.Params
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
			n, ok := eventSourceNames[v]
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
		args.Columns = eventAllAliases
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
		field := eventSourceNames[prefix]
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
		case "contract":
			addrs := make([]model.AccountID, 0)
			for _, v := range strings.Split(val[0], ",") {
				addr, err := tezos.ParseAddress(v)
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
				}
				if err == nil && acc.RowId > 0 {
					addrs = append(addrs, acc.RowId)
				}
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if len(addrs) > 0 {
					q = q.And(field, mode, addrs[0])
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				if len(addrs) > 0 {
					q = q.And(field, mode, addrs)
				}
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "type_hash":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				u, err := util.DecodeU64String(val[0])
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid hash '%s'", val[0]), err))
				}
				q = q.And(field, mode, u)
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-hash lookup
				hashes := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					u, err := util.DecodeU64String(v)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid hash '%s'", v), err))
					}
					hashes = append(hashes, u.U64())
				}
				q = q.And(field, mode, hashes)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := eventSourceNames[prefix]; !ok {
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
	//  ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

	// prepare return type marshalling
	val := &Event{
		verbose: args.Verbose,
		columns: args.Columns,
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
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(val); err != nil {
				return err
			}
			if err := enc.Encode(val); err != nil {
				return err
			}
			count++
			lastId = val.RowId.Value()
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
				if err := r.Decode(val); err != nil {
					return err
				}
				if err := enc.EncodeRecord(val); err != nil {
					return err
				}
				count++
				lastId = val.RowId.Value()
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
