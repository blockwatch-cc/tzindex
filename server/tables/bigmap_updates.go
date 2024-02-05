// Copyright (c) 2020-2024 Blockwatch Data Inc.
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
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	bigmapUpdateSourceNames map[string]string
	// all aliases as list
	bigmapUpdateAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.BigmapUpdate{})
	if err != nil {
		log.Fatalf("bigmap update field type error: %v\n", err)
	}
	bigmapUpdateSourceNames = fields.NameMapReverse()
	bigmapUpdateAllAliases = fields.Aliases()

	// add extra transalations for accounts
	bigmapUpdateSourceNames["op"] = "o"
	bigmapUpdateSourceNames["hash"] = "-"
	bigmapUpdateAllAliases = append(bigmapUpdateAllAliases, "op")
	bigmapUpdateAllAliases = append(bigmapUpdateAllAliases, "hash")
}

// configurable marshalling helper
type BigmapUpdateItem struct {
	model.BigmapUpdate
	verbose bool                        // cond. marshal
	columns util.StringList             // cond. cols & order when brief
	ops     map[model.OpID]tezos.OpHash // op map
	ctx     *server.Context
}

func (b *BigmapUpdateItem) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *BigmapUpdateItem) MarshalJSONVerbose() ([]byte, error) {
	bigmap := struct {
		RowId     uint64    `json:"row_id"`
		BigmapId  int64     `json:"bigmap_id"`
		KeyId     uint64    `json:"key_id"`
		Action    string    `json:"action"`
		OpId      uint64    `json:"op_id"`
		Op        string    `json:"op"`
		Height    int64     `json:"height"`
		Timestamp time.Time `json:"time"`
		Hash      string    `json:"hash,omitempty"`
		Key       string    `json:"key,omitempty"`
		Value     string    `json:"value,omitempty"`
	}{
		RowId:     b.RowId,
		BigmapId:  b.BigmapId,
		KeyId:     b.KeyId,
		Action:    b.Action.String(),
		OpId:      b.OpId.U64(),
		Op:        b.ops[b.OpId].String(),
		Height:    b.Height,
		Timestamp: b.ctx.Indexer.LookupBlockTime(b.ctx, b.Height),
		Hash:      b.GetKeyHash().String(),
		Key:       hex.EncodeToString(b.Key),
		Value:     hex.EncodeToString(b.Value),
	}
	return json.Marshal(bigmap)
}

func (b *BigmapUpdateItem) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, b.RowId, 10)
		case "bigmap_id":
			buf = strconv.AppendInt(buf, b.BigmapId, 10)
		case "key_id":
			buf = strconv.AppendUint(buf, b.KeyId, 10)
		case "action":
			buf = strconv.AppendQuote(buf, b.Action.String())
		case "op_id":
			buf = strconv.AppendUint(buf, b.OpId.U64(), 10)
		case "op":
			buf = strconv.AppendQuote(buf, b.ops[b.OpId].String())
		case "height":
			buf = strconv.AppendInt(buf, b.Height, 10)
		case "time":
			buf = strconv.AppendInt(buf, b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.Height), 10)
		case "hash":
			buf = strconv.AppendQuote(buf, b.GetKeyHash().String())
		case "key":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.Key))
		case "value":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.Value))
		default:
			continue
		}
		if i < len(b.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (b *BigmapUpdateItem) MarshalCSV() ([]string, error) {
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(b.RowId, 10)
		case "bigmap_id":
			res[i] = strconv.FormatInt(b.BigmapId, 10)
		case "key_id":
			res[i] = strconv.FormatUint(b.KeyId, 10)
		case "action":
			res[i] = strconv.Quote(b.Action.String())
		case "op_id":
			res[i] = strconv.FormatUint(b.OpId.U64(), 10)
		case "op":
			res[i] = strconv.Quote(b.ops[b.OpId].String())
		case "height":
			res[i] = strconv.FormatInt(b.Height, 10)
		case "time":
			res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
		case "hash":
			res[i] = strconv.Quote(b.GetKeyHash().String())
		case "key":
			res[i] = strconv.Quote(hex.EncodeToString(b.Key))
		case "value":
			res[i] = strconv.Quote(hex.EncodeToString(b.Value))
		default:
			continue
		}
	}
	return res, nil
}

func StreamBigmapUpdateTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}
	opT, err := ctx.Indexer.Table(model.OpTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", model.OpTableKey), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames     []string
		needOpT      bool
		needBigmapId bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := bigmapUpdateSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			if v == "op" {
				needOpT = true
			}
			if args.Verbose {
				needOpT = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = bigmapUpdateAllAliases
		needOpT = true
	}

	// prepare lookup caches
	opMap := make(map[model.OpID]tezos.OpHash)
	bigmapIds := make([]int64, 0)

	// pre-parse bigmap ids required for hash lookups
	// this only works for EQ/IN, panic on other conditions
	for key := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		// hash requires bigmap_id(s) to genrate key_id(s)
		if keys[0] == "hash" {
			needBigmapId = true
			break
		}
	}
	if needBigmapId {
		for key, val := range ctx.Request.URL.Query() {
			keys := strings.Split(key, ".")
			if keys[0] != "bigmap_id" {
				continue
			}
			mode := pack.FilterModeEqual
			if len(keys) > 1 {
				mode = pack.ParseFilterMode(keys[1])
			}
			switch mode {
			case pack.FilterModeEqual:
				// single-bigmap
				id, err := strconv.ParseInt(val[0], 10, 64)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid bigmap id '%s'", val[0]), err))
				}
				bigmapIds = append(bigmapIds, id)
			case pack.FilterModeIn:
				// multi-bigmap
				for _, v := range strings.Split(val[0], ",") {
					id, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid bigmap id '%s'", v), err))
					}
					bigmapIds = append(bigmapIds, id)
				}
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for bigmap_id on hash query", mode), nil))
			}
		}
		if len(bigmapIds) == 0 {
			panic(server.EBadRequest(server.EC_PARAM_INVALID, "bigmap_id.[eq|in] required for hash query", nil))
		}
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
		mode := pack.FilterModeEqual
		field := bigmapUpdateSourceNames[prefix]
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

		case "op":
			// parse op hash and lookup id
			// valid filter modes: eq, in
			// 1 resolve op_id from op table
			// 2 add eq/in cond: op_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty op matches id 0 (== missing baker)
					q = q.And(field, mode, 0)
				} else {
					// single-op lookup and compile condition
					op, err := ctx.Indexer.LookupOp(ctx, val[0], etl.ListRequest{})
					if err != nil {
						switch err {
						case model.ErrNoOp:
							// expected
						case model.ErrInvalidOpHash:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", val[0]), err))
						default:
							panic(server.EInternal(server.EC_DATABASE, fmt.Sprintf("cannot lookup op hash '%s'", val[0]), err))
						}
					}
					// Note: when not found we insert an always false condition
					if len(op) == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						// add op id as extra condition
						opMap[op[0].RowId] = op[0].Hash.Clone()
						q = q.And(field, mode, op[0].RowId)
						if mode == pack.FilterModeEqual {
							needOpT = false
							opMap[op[0].RowId] = op[0].Hash.Clone()
						}
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					op, err := ctx.Indexer.LookupOp(ctx, v, etl.ListRequest{})
					if err != nil {
						switch err {
						case model.ErrNoOp:
							// expected
						case model.ErrInvalidOpHash:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", v), err))
						default:
							panic(server.EInternal(server.EC_DATABASE, fmt.Sprintf("cannot lookup op hash '%s'", v), err))
						}
					}
					// skip not found ops
					if len(op) == 0 {
						continue
					}
					// collect list of op ids (use first slice value only)
					if mode == pack.FilterModeIn {
						needOpT = false
						opMap[op[0].RowId] = op[0].Hash.Clone()
						ids = append(ids, op[0].RowId.U64())
					}
				}
				// Note: when list is empty (no ops were found, the match will
				//       always be false and return no result as expected)
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "action":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-action
				action, err := micheline.ParseDiffAction(val[0])
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid action '%s'", val[0]), err))
				}
				q = q.And(field, mode, action)
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-action
				actions := make([]int, 0)
				for _, v := range strings.Split(val[0], ",") {
					action, err := micheline.ParseDiffAction(v)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid action '%s'", v), err))
					}
					actions = append(actions, int(action))
				}
				q = q.And(field, mode, actions)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "hash":
			// this requires a list of bigmaps to be present on the query
			// we will use these bigmaps to derive key_ids
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-hash
				h, err := tezos.ParseExprHash(val[0])
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid key hash '%s'", val[0]), err))
				}
				if len(bigmapIds) == 1 {
					// single bigmap
					q = q.And("key_id", mode, model.GetKeyId(bigmapIds[0], h))
				} else {
					// multiple bigmaps
					if mode == pack.FilterModeEqual {
						mode = pack.FilterModeIn
					} else {
						mode = pack.FilterModeNotIn
					}
					keyIds := make([]uint64, 0)
					for _, bi := range bigmapIds {
						keyIds = append(keyIds, model.GetKeyId(bi, h))
					}
					q = q.And("key_id", mode, keyIds)
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-hash lookup
				hashes := make([]tezos.ExprHash, 0)
				for _, v := range strings.Split(val[0], ",") {
					h, err := tezos.ParseExprHash(v)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid key hash '%s'", v), err))
					}
					hashes = append(hashes, h)
				}
				// query hashes x bigmapids
				keyIds := make([]uint64, 0)
				for _, bi := range bigmapIds {
					for _, hh := range hashes {
						keyIds = append(keyIds, model.GetKeyId(bi, hh))
					}
				}
				q = q.And(field, mode, keyIds)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := bigmapUpdateSourceNames[prefix]; !ok {
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

	// Step 1: query database
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "query failed", err))
	}
	// ctx.Log.Tracef("Processing result with %d rows %d cols", res.Rows(), res.Cols())
	defer res.Close()

	// Step 2: resolve related op hashes using lookup (when requested)
	if res.Rows() > 0 && needOpT {
		// get a unique copy of account id column (clip on request limit)
		col, _ := res.Uint64Column("o")
		find := vec.UniqueUint64Slice(col[:util.Min(len(col), int(args.Limit))])

		// lookup accounts from id
		// ctx.Log.Tracef("Looking up %d ops", len(find))
		type XOp struct {
			Id   model.OpID   `pack:"I,pk"`
			Hash tezos.OpHash `pack:"H"`
		}
		op := &XOp{}
		err = pack.NewQuery(ctx.RequestID+".bigmap_lookup").
			WithTable(opT).
			WithFields("I", "H").
			AndIn("I", find).
			Stream(ctx, func(r pack.Row) error {
				if err := r.Decode(op); err != nil {
					return err
				}
				opMap[op.Id] = op.Hash.Clone()
				return nil
			})
		if err != nil {
			// non-fatal error
			ctx.Log.Errorf("Op hash lookup failed: %w", err)
		}
	}

	// prepare return type marshalling
	bigmap := &BigmapUpdateItem{
		verbose: args.Verbose,
		columns: args.Columns,
		ops:     opMap,
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
		err = res.Walk(func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(bigmap); err != nil {
				return err
			}
			if err := enc.Encode(bigmap); err != nil {
				return err
			}
			count++
			lastId = bigmap.RowId
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
			err = res.Walk(func(r pack.Row) error {
				if err := r.Decode(bigmap); err != nil {
					return err
				}
				if err := enc.EncodeRecord(bigmap); err != nil {
					return err
				}
				count++
				lastId = bigmap.RowId
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
