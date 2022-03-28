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
	bigmapAllocSourceNames map[string]string
	// short -> long form
	bigmapAllocAliasNames map[string]string
	// all aliases as list
	bigmapAllocAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.BigmapAlloc{})
	if err != nil {
		log.Fatalf("bigmap alloc field type error: %v\n", err)
	}
	bigmapAllocSourceNames = fields.NameMapReverse()
	bigmapAllocAllAliases = fields.Aliases()

	// add extra transalations for accounts
	bigmapAllocSourceNames["contract"] = "A"
	bigmapAllocSourceNames["update_time"] = "-"
	bigmapAllocSourceNames["update_block"] = "-"
	bigmapAllocSourceNames["alloc_time"] = "-"
	bigmapAllocSourceNames["alloc_block"] = "-"
	bigmapAllocSourceNames["key_type"] = "d"
	bigmapAllocSourceNames["value_type"] = "d"
	bigmapAllocAllAliases = append(bigmapAllocAllAliases, []string{
		"contract",
		"update_time",
		"update_block",
		"alloc_time",
		"alloc_block",
		"key_type",
		"value_type",
	}...)
}

// configurable marshalling helper
type BigmapAllocItem struct {
	model.BigmapAlloc
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	ctx     *server.Context
}

func (b *BigmapAllocItem) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *BigmapAllocItem) MarshalJSONVerbose() ([]byte, error) {
	bigmap := struct {
		RowId        uint64          `json:"row_id"`
		BigmapId     int64           `json:"bigmap_id"`
		AccountId    uint64          `json:"account_id"`
		Contract     string          `json:"contract"`
		AllocHeight  int64           `json:"alloc_height"`
		AllocTime    time.Time       `json:"alloc_time"`
		AllocBlock   tezos.BlockHash `json:"alloc_block"`
		KeyType      string          `json:"key_type,omitempty"`
		ValueType    string          `json:"value_type,omitempty"`
		NUpdates     int64           `json:"n_updates"`
		NKeys        int64           `json:"n_keys"`
		UpdateHeight int64           `json:"update_height"`
		UpdateTime   time.Time       `json:"update_time"`
		UpdateBlock  tezos.BlockHash `json:"update_block"`
	}{
		RowId:        b.RowId,
		BigmapId:     b.BigmapId,
		AccountId:    b.AccountId.Value(),
		Contract:     b.ctx.Indexer.LookupAddress(b.ctx, b.AccountId).String(),
		AllocHeight:  b.Height,
		AllocTime:    b.ctx.Indexer.LookupBlockTime(b.ctx, b.Height),
		AllocBlock:   b.ctx.Indexer.LookupBlockHash(b.ctx, b.Height),
		KeyType:      hex.EncodeToString(b.GetKeyTypeBytes()),
		ValueType:    hex.EncodeToString(b.GetValueTypeBytes()),
		NUpdates:     b.NUpdates,
		NKeys:        b.NKeys,
		UpdateHeight: b.Updated,
		UpdateTime:   b.ctx.Indexer.LookupBlockTime(b.ctx, b.Updated),
		UpdateBlock:  b.ctx.Indexer.LookupBlockHash(b.ctx, b.Updated),
	}
	return json.Marshal(bigmap)
}

func (b *BigmapAllocItem) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, b.RowId, 10)
		case "bigmap_id":
			buf = strconv.AppendInt(buf, b.BigmapId, 10)
		case "account_id":
			buf = strconv.AppendUint(buf, b.AccountId.Value(), 10)
		case "contract":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.AccountId).String())
		case "alloc_height":
			buf = strconv.AppendInt(buf, b.Height, 10)
		case "alloc_time":
			buf = strconv.AppendInt(buf, b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.Height), 10)
		case "alloc_block":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupBlockHash(b.ctx, b.Height).String())
		case "key_type":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.GetKeyTypeBytes()))
		case "value_type":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.GetValueTypeBytes()))
		case "n_updates":
			buf = strconv.AppendInt(buf, b.NUpdates, 10)
		case "n_keys":
			buf = strconv.AppendInt(buf, b.NKeys, 10)
		case "update_height":
			buf = strconv.AppendInt(buf, b.Updated, 10)
		case "update_time":
			buf = strconv.AppendInt(buf, b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.Updated), 10)
		case "update_block":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupBlockHash(b.ctx, b.Updated).String())
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

func (b *BigmapAllocItem) MarshalCSV() ([]string, error) {
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(b.RowId, 10)
		case "bigmap_id":
			res[i] = strconv.FormatInt(b.BigmapId, 10)
		case "account_id":
			res[i] = strconv.FormatUint(b.AccountId.Value(), 10)
		case "contract":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.AccountId).String())
		case "alloc_height":
			res[i] = strconv.FormatInt(b.Height, 10)
		case "alloc_time":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupBlockTime(b.ctx, b.Height).Format(time.RFC3339))
		case "alloc_block":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupBlockHash(b.ctx, b.Height).String())
		case "key_type":
			res[i] = strconv.Quote(hex.EncodeToString(b.GetKeyTypeBytes()))
		case "value_type":
			res[i] = strconv.Quote(hex.EncodeToString(b.GetValueTypeBytes()))
		case "n_updates":
			res[i] = strconv.FormatInt(b.NUpdates, 10)
		case "n_keys":
			res[i] = strconv.FormatInt(b.NKeys, 10)
		case "update_height":
			res[i] = strconv.FormatInt(b.Updated, 10)
		case "update_time":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupBlockTime(b.ctx, b.Updated).Format(time.RFC3339))
		case "update_block":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupBlockHash(b.ctx, b.Updated).String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamBigmapAllocTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
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
			n, ok := bigmapAllocSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			switch n {
			case "update_time", "update_block":
				srcNames = append(srcNames, "u") // updated
				continue
			case "alloc_time", "alloc_block":
				srcNames = append(srcNames, "h") // height
				continue
			case "contract":
				srcNames = append(srcNames, "A") // account_id
			case "key_type", "value_type":
				srcNames = append(srcNames, "d") // data
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = bigmapAllocAllAliases
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
		field := bigmapAllocSourceNames[prefix]
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
		case "contract":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  pack.FilterModeEqual,
						Value: 0,
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-address lookup and compile condition
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

		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := bigmapAllocSourceNames[prefix]; !ok {
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
	bigmap := &BigmapAllocItem{
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
		err = table.Stream(ctx, q, func(r pack.Row) error {
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
			err = table.Stream(ctx, q, func(r pack.Row) error {
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
