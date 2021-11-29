// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// long -> short form
	bigmapValueSourceNames map[string]string
	// short -> long form
	bigmapValueAliasNames map[string]string
	// all aliases as list
	bigmapValueAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.BigmapKV{})
	if err != nil {
		log.Fatalf("bigmap value field type error: %v\n", err)
	}
	bigmapValueSourceNames = fields.NameMapReverse()
	bigmapValueAllAliases = fields.Aliases()

	// add extra translations for accounts
	bigmapValueSourceNames["key_hash"] = "-"
	bigmapValueSourceNames["time"] = "h"
	bigmapValueAllAliases = append(bigmapValueAllAliases, "key_hash")
	bigmapValueAllAliases = append(bigmapValueAllAliases, "time")
}

// configurable marshalling helper
type BigmapValueItem struct {
	model.BigmapKV
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	ctx     *ApiContext
}

func (b *BigmapValueItem) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *BigmapValueItem) MarshalJSONVerbose() ([]byte, error) {
	bigmap := struct {
		RowId    uint64    `json:"row_id"`
		BigmapId int64     `json:"bigmap_id"`
		Height   int64     `json:"height"`
		KeyId    uint64    `json:"key_id"`
		KeyHash  string    `json:"key_hash"`
		Key      string    `json:"key"`
		Value    string    `json:"value"`
		Time     time.Time `json:"time"`
	}{
		RowId:    b.RowId,
		BigmapId: b.BigmapId,
		Height:   b.Height,
		KeyId:    b.KeyId,
		KeyHash:  b.GetKeyHash().String(),
		Key:      hex.EncodeToString(b.Key),
		Value:    hex.EncodeToString(b.Value),
		Time:     b.ctx.Indexer.LookupBlockTime(b.ctx.Context, b.Height),
	}
	return json.Marshal(bigmap)
}

func (b *BigmapValueItem) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, b.RowId, 10)
		case "bigmap_id":
			buf = strconv.AppendInt(buf, b.BigmapId, 10)
		case "height":
			buf = strconv.AppendInt(buf, b.Height, 10)
		case "key_id":
			buf = strconv.AppendUint(buf, b.KeyId, 10)
		case "key_hash":
			buf = strconv.AppendQuote(buf, b.GetKeyHash().String())
		case "key":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.Key))
		case "value":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.Value))
		case "time":
			buf = strconv.AppendInt(buf, b.ctx.Indexer.LookupBlockTimeMs(b.ctx.Context, b.Height), 10)
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

func (b *BigmapValueItem) MarshalCSV() ([]string, error) {
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(b.RowId, 10)
		case "bigmap_id":
			res[i] = strconv.FormatInt(b.BigmapId, 10)
		case "height":
			res[i] = strconv.FormatInt(b.Height, 10)
		case "key_id":
			res[i] = strconv.FormatUint(b.KeyId, 10)
		case "key_hash":
			res[i] = strconv.Quote(b.GetKeyHash().String())
		case "key":
			res[i] = strconv.Quote(hex.EncodeToString(b.Key))
		case "value":
			res[i] = strconv.Quote(hex.EncodeToString(b.Value))
		case "time":
			res[i] = strconv.FormatInt(b.ctx.Indexer.LookupBlockTimeMs(b.ctx.Context, b.Height), 10)
		default:
			continue
		}
	}
	return res, nil
}

func StreamBigmapValueTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames     []string
		needBigmapId bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := bigmapValueSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			switch v {
			case "key_hash":
				// key_hash requires bigmap_id(s) to genrate key_id(s)
				needBigmapId = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = bigmapValueAllAliases
	}

	// prepare lookup caches
	// accMap := make(map[model.AccountID]tezos.Address)
	bigmapIds := make([]int64, 0)

	// pre-parse bigmap ids required for key_hash lookups
	// this only works for EQ/IN, panic on other conditions
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
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid bigmap id '%s'", val[0]), err))
				}
				bigmapIds = append(bigmapIds, id)
			case pack.FilterModeIn:
				// multi-bigmap
				for _, v := range strings.Split(val[0], ",") {
					id, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid bigmap id '%s'", v), err))
					}
					bigmapIds = append(bigmapIds, id)
				}
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for bigmap_id on key_hash query", mode), nil))
			}
		}
		if len(bigmapIds) == 0 {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("bigmap_id.[eq|in] required for key_hash query"), nil))
		}
	}

	// build table query
	q := pack.Query{
		Name:   ctx.RequestID,
		Fields: table.Fields().Select(srcNames...),
		Limit:  int(args.Limit),
		Order:  args.Order,
	}

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		mode := pack.FilterModeEqual
		field := bigmapValueSourceNames[prefix]
		if len(keys) > 1 {
			mode = pack.ParseFilterMode(keys[1])
			if !mode.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s'", keys[1]), nil))
			}
		}
		switch prefix {
		case "columns", "limit", "order", "verbose", "filename":
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
			q.Conditions.AddAndCondition(&pack.Condition{
				Field: table.Fields().Pk(),
				Mode:  cursorMode,
				Value: id,
				Raw:   val[0], // debugging aid
			})

		case "key_hash":
			// this requires a list of bigmaps to be present on the query
			// we will use these bigmaps to derive key_ids
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-hash
				h, err := tezos.ParseExprHash(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key hash '%s'", val[0]), err))
				}
				if len(bigmapIds) == 1 {
					// single bigmap
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find("key_id"),
						Mode:  mode,
						Value: model.GetKeyId(bigmapIds[0], h),
						Raw:   val[0], // debugging aid
					})
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
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find("key_id"),
						Mode:  mode,
						Value: keyIds,
						Raw:   val[0], // debugging aid
					})
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-hash lookup
				hashes := make([]tezos.ExprHash, 0)
				for _, v := range strings.Split(val[0], ",") {
					h, err := tezos.ParseExprHash(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key hash '%s'", v), err))
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
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: keyIds,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "time":
			// translate time into height, use val[0] only
			bestTime := ctx.Tip.BestTime
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
				if from.After(bestTime) {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid future time %s", from), nil))
				}
				if to.After(bestTime) {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid future time %s", to), nil))
				}
				fromBlock = ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, from)
				toBlock = ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, to)
				q.Conditions.AddAndCondition(&pack.Condition{
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
					if v.After(bestTime) {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid future time %s", v), nil))
					}
					valueBlocks = append(valueBlocks, ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, v))
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("h"), // height
					Mode:  cond.Mode,
					Value: valueBlocks,
					Raw:   val[0], // debugging aid
				})

			default:
				// cond.Value is time.Time
				valueTime := cond.Value.(time.Time)
				var valueBlock int64
				if valueTime.After(bestTime) {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid future time %s", valueTime), nil))
				}
				valueBlock = ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, valueTime)
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("h"), // height
					Mode:  cond.Mode,
					Value: valueBlock,
					Raw:   val[0], // debugging aid
				})
			}

		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := bigmapValueSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
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
	//  ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

	// prepare return type marshalling
	bigmap := &BigmapValueItem{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
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
		err = q.Stream(ctx, func(r pack.Row) error {
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
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
			err = q.Stream(ctx, func(r pack.Row) error {
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
