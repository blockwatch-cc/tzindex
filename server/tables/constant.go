// Copyright (c) 2020-2024 Blockwatch Data Inc.
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
	constantSourceNames map[string]string
	// all aliases as list
	constantAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Constant{})
	if err != nil {
		log.Fatalf("constant field type error: %v\n", err)
	}
	constantSourceNames = fields.NameMapReverse()
	constantAllAliases = fields.Aliases()
	constantSourceNames["creator"] = "C"
	constantSourceNames["time"] = "h"
	constantAllAliases = append(constantAllAliases, "creator", "time")
}

// configurable marshalling helper
type Constant struct {
	model.Constant
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	ctx     *server.Context
}

func (c *Constant) MarshalJSON() ([]byte, error) {
	if c.verbose {
		return c.MarshalJSONVerbose()
	} else {
		return c.MarshalJSONBrief()
	}
}

func (c *Constant) MarshalJSONVerbose() ([]byte, error) {
	val := struct {
		RowId       uint64 `json:"row_id"`
		Address     string `json:"address"`
		CreatorId   uint64 `json:"creator_id"`
		Creator     string `json:"creator"`
		Value       string `json:"value"`
		Height      int64  `json:"height"`
		Time        int64  `json:"time"`
		StorageSize int64  `json:"storage_size"`
		Features    string `json:"features"`
	}{
		RowId:       c.RowId.U64(),
		Address:     c.Address.String(),
		CreatorId:   c.CreatorId.U64(),
		Creator:     c.ctx.Indexer.LookupAddress(c.ctx, c.CreatorId).String(),
		Value:       hex.EncodeToString(c.Value),
		Height:      c.Height,
		Time:        c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.Height),
		StorageSize: c.StorageSize,
		Features:    c.Features.String(),
	}
	return json.Marshal(val)
}

func (c *Constant) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range c.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, c.RowId.U64(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, c.Address.String())
		case "creator_id":
			buf = strconv.AppendUint(buf, c.CreatorId.U64(), 10)
		case "creator":
			buf = strconv.AppendQuote(buf, c.ctx.Indexer.LookupAddress(c.ctx, c.CreatorId).String())
		case "value":
			if c.Value != nil {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(c.Value))
			} else {
				buf = append(buf, null...)
			}
		case "height":
			buf = strconv.AppendInt(buf, c.Height, 10)
		case "time":
			buf = strconv.AppendInt(buf, c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.Height), 10)
		case "storage_size":
			buf = strconv.AppendInt(buf, c.StorageSize, 10)
		case "features":
			buf = strconv.AppendQuote(buf, c.Features.String())
		default:
			continue
		}
		if i < len(c.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (c *Constant) MarshalCSV() ([]string, error) {
	res := make([]string, len(c.columns))
	for i, v := range c.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(c.RowId.U64(), 10)
		case "address":
			res[i] = strconv.Quote(c.Address.String())
		case "creator_id":
			res[i] = strconv.FormatUint(c.CreatorId.U64(), 10)
		case "creator":
			res[i] = strconv.Quote(c.ctx.Indexer.LookupAddress(c.ctx, c.CreatorId).String())
		case "value":
			res[i] = strconv.Quote(hex.EncodeToString(c.Value))
		case "height":
			res[i] = strconv.FormatInt(c.Height, 10)
		case "time":
			res[i] = strconv.FormatInt(c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.Height), 10)
		case "storage_size":
			res[i] = strconv.FormatInt(c.StorageSize, 10)
		case "features":
			res[i] = strconv.Quote(c.Features.String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamConstantTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
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
			n, ok := constantSourceNames[v]
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
		args.Columns = constantAllAliases
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
		field := constantSourceNames[prefix]
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
		case "creator":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := tezos.ParseAddress(val[0])
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				q = q.And(field, mode, addr[:])
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup (Note: does not check for address type so may
				// return duplicates)
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					hashes = append(hashes, addr[:])
				}
				q = q.And(field, mode, hashes)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "address": // expr-hash (!)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-hash lookup and compile condition
				hash, err := tezos.ParseExprHash(val[0])
				if err != nil || !hash.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid exprhash '%s'", val[0]), err))
				}
				q = q.And(field, mode, hash.Bytes())
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-hash lookup
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseExprHash(v)
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid exprhash '%s'", v), err))
					}
					hashes = append(hashes, addr.Bytes())
				}
				q = q.And(field, mode, hashes)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := constantSourceNames[prefix]; !ok {
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
	val := &Constant{
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
			lastId = val.RowId.U64()
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
				lastId = val.RowId.U64()
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
