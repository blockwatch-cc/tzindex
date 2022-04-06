// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"encoding/binary"
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
	contractSourceNames map[string]string
	// short -> long form
	contractAliasNames map[string]string
	// all aliases as list
	contractAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Contract{})
	if err != nil {
		log.Fatalf("contract field type error: %v\n", err)
	}
	contractSourceNames = fields.NameMapReverse()
	contractAllAliases = fields.Aliases()

	// add extra transalations for accounts
	contractSourceNames["creator"] = "C"
	contractSourceNames["first_seen_time"] = "f"
	contractSourceNames["last_seen_time"] = "l"
	contractAllAliases = append(contractAllAliases, "creator")
}

// configurable marshalling helper
type Contract struct {
	model.Contract
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	ctx     *server.Context
}

func (c *Contract) MarshalJSON() ([]byte, error) {
	if c.verbose {
		return c.MarshalJSONVerbose()
	} else {
		return c.MarshalJSONBrief()
	}
}

func (c *Contract) MarshalJSONVerbose() ([]byte, error) {
	cc := struct {
		RowId         uint64 `json:"row_id"`
		AccountId     uint64 `json:"account_id"`
		Address       string `json:"address"`
		CreatorId     uint64 `json:"creator_id"`
		Creator       string `json:"creator"`
		FirstSeen     int64  `json:"first_seen"`
		LastSeen      int64  `json:"last_seen"`
		FirstSeenTime int64  `json:"first_seen_time"`
		LastSeenTime  int64  `json:"last_seen_time"`
		StorageSize   int64  `json:"storage_size"`
		StoragePaid   int64  `json:"storage_paid"`
		Script        string `json:"script"`
		Storage       string `json:"storage"`
		InterfaceHash string `json:"iface_hash"`
		CodeHash      string `json:"code_hash"`
		StorageHash   string `json:"storage_hash"`
		CallStats     string `json:"call_stats"`
		Features      string `json:"features"`
		Interfaces    string `json:"interfaces"`
	}{
		RowId:         c.RowId.Value(),
		AccountId:     c.AccountId.Value(),
		Address:       c.String(),
		CreatorId:     c.CreatorId.Value(),
		Creator:       c.ctx.Indexer.LookupAddress(c.ctx, c.CreatorId).String(),
		FirstSeen:     c.FirstSeen,
		LastSeen:      c.LastSeen,
		FirstSeenTime: c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.FirstSeen),
		LastSeenTime:  c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.LastSeen),
		StorageSize:   c.StorageSize,
		StoragePaid:   c.StoragePaid,
		Script:        hex.EncodeToString(c.Script),
		Storage:       hex.EncodeToString(c.Storage),
		CallStats:     hex.EncodeToString(c.CallStats),
		Features:      c.Features.String(),
		Interfaces:    c.Interfaces.String(),
	}

	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], c.InterfaceHash)
	cc.InterfaceHash = hex.EncodeToString(tmp[:])
	binary.BigEndian.PutUint64(tmp[:], c.CodeHash)
	cc.CodeHash = hex.EncodeToString(tmp[:])
	binary.BigEndian.PutUint64(tmp[:], c.StorageHash)
	cc.StorageHash = hex.EncodeToString(tmp[:])

	return json.Marshal(cc)
}

func (c *Contract) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range c.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, c.RowId.Value(), 10)
		case "account_id":
			buf = strconv.AppendUint(buf, c.AccountId.Value(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, c.String())
		case "creator_id":
			buf = strconv.AppendUint(buf, c.CreatorId.Value(), 10)
		case "creator":
			buf = strconv.AppendQuote(buf, c.ctx.Indexer.LookupAddress(c.ctx, c.CreatorId).String())
		case "first_seen":
			buf = strconv.AppendInt(buf, c.FirstSeen, 10)
		case "last_seen":
			buf = strconv.AppendInt(buf, c.LastSeen, 10)
		case "first_seen_time":
			buf = strconv.AppendInt(buf, c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.FirstSeen), 10)
		case "last_seen_time":
			buf = strconv.AppendInt(buf, c.ctx.Indexer.LookupBlockTimeMs(c.ctx.Context, c.LastSeen), 10)
		case "storage_size":
			buf = strconv.AppendInt(buf, c.StorageSize, 10)
		case "storage_paid":
			buf = strconv.AppendInt(buf, c.StoragePaid, 10)
		case "script":
			// code is binary
			if c.Script != nil {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(c.Script))
			} else {
				buf = append(buf, "null"...)
			}
		case "storage":
			// code is binary
			if c.Storage != nil {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(c.Storage))
			} else {
				buf = append(buf, "null"...)
			}
		case "iface_hash":
			if c.InterfaceHash != 0 {
				var tmp [8]byte
				binary.BigEndian.PutUint64(tmp[:], c.InterfaceHash)
				buf = strconv.AppendQuote(buf, hex.EncodeToString(tmp[:]))
			} else {
				buf = append(buf, "null"...)
			}
		case "code_hash":
			if c.CodeHash != 0 {
				var tmp [8]byte
				binary.BigEndian.PutUint64(tmp[:], c.CodeHash)
				buf = strconv.AppendQuote(buf, hex.EncodeToString(tmp[:]))
			} else {
				buf = append(buf, "null"...)
			}
		case "storage_hash":
			if c.StorageHash != 0 {
				var tmp [8]byte
				binary.BigEndian.PutUint64(tmp[:], c.StorageHash)
				buf = strconv.AppendQuote(buf, hex.EncodeToString(tmp[:]))
			} else {
				buf = append(buf, "null"...)
			}
		case "call_stats":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(c.CallStats))
		case "features":
			buf = strconv.AppendQuote(buf, c.Features.String())
		case "interfaces":
			buf = strconv.AppendQuote(buf, c.Interfaces.String())
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

func (c *Contract) MarshalCSV() ([]string, error) {
	res := make([]string, len(c.columns))
	for i, v := range c.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(c.RowId.Value(), 10)
		case "account_id":
			res[i] = strconv.FormatUint(c.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(c.String())
		case "creator_id":
			res[i] = strconv.FormatUint(c.CreatorId.Value(), 10)
		case "creator":
			res[i] = strconv.Quote(c.ctx.Indexer.LookupAddress(c.ctx, c.CreatorId).String())
		case "first_seen":
			res[i] = strconv.FormatInt(c.FirstSeen, 10)
		case "last_seen":
			res[i] = strconv.FormatInt(c.LastSeen, 10)
		case "first_seen_time":
			res[i] = strconv.Quote(c.ctx.Indexer.LookupBlockTime(c.ctx.Context, c.FirstSeen).Format(time.RFC3339))
		case "last_seen_time":
			res[i] = strconv.Quote(c.ctx.Indexer.LookupBlockTime(c.ctx.Context, c.LastSeen).Format(time.RFC3339))
		case "storage_size":
			res[i] = strconv.FormatInt(c.StorageSize, 10)
		case "storage_paid":
			res[i] = strconv.FormatInt(c.StoragePaid, 10)
		case "script":
			res[i] = strconv.Quote(hex.EncodeToString(c.Script))
		case "storage":
			res[i] = strconv.Quote(hex.EncodeToString(c.Storage))
		case "iface_hash":
			var tmp [8]byte
			binary.BigEndian.PutUint64(tmp[:], c.InterfaceHash)
			res[i] = strconv.Quote(hex.EncodeToString(tmp[:]))
		case "code_hash":
			var tmp [8]byte
			binary.BigEndian.PutUint64(tmp[:], c.CodeHash)
			res[i] = strconv.Quote(hex.EncodeToString(tmp[:]))
		case "storage_hash":
			var tmp [8]byte
			binary.BigEndian.PutUint64(tmp[:], c.StorageHash)
			res[i] = strconv.Quote(hex.EncodeToString(tmp[:]))
		case "call_stats":
			res[i] = strconv.Quote(hex.EncodeToString(c.CallStats))
		case "features":
			res[i] = strconv.Quote(c.Features.String())
		case "interfaces":
			res[i] = strconv.Quote(c.Interfaces.String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamContractTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
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
			n, ok := contractSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n == "-" {
				continue
			}
			switch v {
			case "first_seen_time":
				srcNames = append(srcNames, "f")
			case "last_seen_time":
				srcNames = append(srcNames, "l")
			}
			srcNames = append(srcNames, n)
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = contractAllAliases
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
		field := contractSourceNames[prefix]
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
		case "address":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := tezos.ParseAddress(val[0])
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				if addr.Type != tezos.AddressTypeContract {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid contract address '%s'", val[0]), err))
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  mode,
					Value: addr.Bytes22(),
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup (Note: does not check for address type so may
				// return duplicates)
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					if addr.Type != tezos.AddressTypeContract {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid contract address '%s'", v), err))
					}
					hashes = append(hashes, addr.Bytes22())
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  mode,
					Value: hashes,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "first_seen_time", "last_seen_time":
			// translate time into height, use val[0] only
			bestTime := ctx.Tip.BestTime
			bestHeight := ctx.Tip.BestHeight
			cond, err := pack.ParseCondition(key, val[0], pack.FieldList{
				pack.Field{
					Name: "time",
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
					Field: table.Fields().Find(field),
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
					Field: table.Fields().Find(field),
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
					Field: table.Fields().Find(field),
					Mode:  cond.Mode,
					Value: valueBlock,
					Raw:   val[0], // debugging aid
				})
			}

		case "creator":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
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
						Field: table.Fields().Find(field), // creator account id
						Mode:  mode,
						Value: uint64(math.MaxUint64),
						Raw:   "account not found", // debugging aid
					})
				} else {
					// add id as extra condition
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find(field), // creator account id
						Mode:  mode,
						Value: acc.RowId,
						Raw:   val[0], // debugging aid
					})
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
					Field: table.Fields().Find(contractSourceNames[prefix]), // creator account id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "iface_hash", "code_hash", "storage_hash":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				buf, err := hex.DecodeString(val[0])
				if err != nil || len(buf) != 8 {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid hash '%s'", val[0]), err))
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: binary.BigEndian.Uint64(buf[:8]),
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-hash lookup
				hashes := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					buf, err := hex.DecodeString(v)
					if err != nil || len(buf) != 8 {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid hash '%s'", v), err))
					}
					hashes = append(hashes, binary.BigEndian.Uint64(buf[:8]))
				}
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: hashes,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := contractSourceNames[prefix]; !ok {
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
	contract := &Contract{
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
			contract.Contract.Reset()
			if err := r.Decode(contract); err != nil {
				return err
			}
			if err := enc.Encode(contract); err != nil {
				return err
			}
			count++
			lastId = contract.RowId.Value()
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
				contract.Contract.Reset()
				if err := r.Decode(contract); err != nil {
					return err
				}
				if err := enc.EncodeRecord(contract); err != nil {
					return err
				}
				count++
				lastId = contract.RowId.Value()
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
