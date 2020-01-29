// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

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
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

var (
	// long -> short form
	bigmapSourceNames map[string]string
	// short -> long form
	bigmapAliasNames map[string]string
	// all aliases as list
	bigmapAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.BigMapItem{})
	if err != nil {
		log.Fatalf("bigmap field type error: %v\n", err)
	}
	bigmapSourceNames = fields.NameMapReverse()
	bigmapAllAliases = fields.Aliases()

	// add extra transalations for accounts
	bigmapSourceNames["address"] = "A"
	bigmapSourceNames["op"] = "O"
	bigmapAllAliases = append(bigmapAllAliases, "address")
	bigmapAllAliases = append(bigmapAllAliases, "op")

	// hide some internal fields (don't let CSV encoder pick them up)
	for _, v := range []string{"", "", ""} {
		for i, n := range bigmapAllAliases {
			if n == v {
				bigmapAllAliases = append(bigmapAllAliases[:i], bigmapAllAliases[i+1:]...)
				break
			}
		}
	}
}

// configurable marshalling helper
type BigMapItem struct {
	model.BigMapItem
	verbose bool                               `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList                    `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params                      `csv:"-" pack:"-"` // blockchain amount conversion
	addrs   map[model.AccountID]chain.Address  `csv:"-" pack:"-"` // address map
	ops     map[model.OpID]chain.OperationHash `csv:"-" pack:"-"` // op map
}

func (b *BigMapItem) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *BigMapItem) MarshalJSONVerbose() ([]byte, error) {
	bigmap := struct {
		RowId       uint64 `json:"row_id"`
		PrevId      uint64 `json:"prev_id"`
		Address     string `json:"address"`
		AccountId   uint64 `json:"account_id"`
		ContractId  uint64 `json:"contract_id"`
		OpId        uint64 `json:"op_id"`
		Op          string `json:"op"`
		Height      int64  `json:"height"`
		Timestamp   int64  `json:"time"`
		BigMapId    int64  `json:"bigmap_id"`
		Action      string `json:"action"`
		KeyHash     string `json:"key_hash"`
		KeyType     string `json:"key_type"`
		KeyEncoding string `json:"key_encoding"`
		Key         string `json:"key"`
		Value       string `json:"value"`
		IsReplaced  bool   `json:"is_replaced"`
		IsDeleted   bool   `json:"is_deleted"`
		IsCopied    bool   `json:"is_copied"`
	}{
		RowId:       b.RowId,
		PrevId:      b.PrevId,
		Address:     b.addrs[b.AccountId].String(),
		AccountId:   b.AccountId.Value(),
		ContractId:  b.ContractId,
		OpId:        b.OpId.Value(),
		Op:          b.ops[b.OpId].String(),
		Height:      b.Height,
		Timestamp:   util.UnixMilliNonZero(b.Timestamp),
		BigMapId:    b.BigMapId,
		Action:      b.Action.String(),
		KeyHash:     chain.NewExprHash(b.KeyHash).String(),
		KeyType:     b.KeyType.String(),
		KeyEncoding: b.KeyEncoding.String(),
		Value:       hex.EncodeToString(b.Value),
		IsReplaced:  b.IsReplaced,
		IsDeleted:   b.IsDeleted,
		IsCopied:    b.IsCopied,
	}
	if len(b.Key) > 0 {
		switch b.KeyEncoding {
		case micheline.PrimInt:
			var z micheline.Z
			if err := z.UnmarshalBinary(b.Key); err != nil {
				return nil, err
			}
			bigmap.Key = z.Big().Text(10)
		case micheline.PrimBytes:
			bigmap.Key = hex.EncodeToString(b.Key)
		case micheline.PrimString:
			bigmap.Key = string(b.Key)
		}
	}
	return json.Marshal(bigmap)
}

func (b *BigMapItem) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, b.RowId, 10)
		case "prev_id":
			buf = strconv.AppendUint(buf, b.PrevId, 10)
		case "address":
			buf = strconv.AppendQuote(buf, b.addrs[b.AccountId].String())
		case "account_id":
			buf = strconv.AppendUint(buf, b.AccountId.Value(), 10)
		case "contract_id":
			buf = strconv.AppendUint(buf, b.ContractId, 10)
		case "op_id":
			buf = strconv.AppendUint(buf, b.OpId.Value(), 10)
		case "op":
			buf = strconv.AppendQuote(buf, b.ops[b.OpId].String())
		case "height":
			buf = strconv.AppendInt(buf, b.Height, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
		case "bigmap_id":
			buf = strconv.AppendInt(buf, b.BigMapId, 10)
		case "action":
			buf = strconv.AppendQuote(buf, b.Action.String())
		case "key_hash":
			buf = strconv.AppendQuote(buf, chain.NewExprHash(b.KeyHash).String())
		case "key_type":
			buf = strconv.AppendQuote(buf, b.KeyType.String())
		case "key_encoding":
			buf = strconv.AppendQuote(buf, b.KeyEncoding.String())
		case "key":
			if len(b.Key) > 0 {
				switch b.KeyEncoding {
				case micheline.PrimInt:
					var z micheline.Z
					if err := z.UnmarshalBinary(b.Key); err != nil {
						return nil, err
					}
					buf = strconv.AppendQuote(buf, z.Big().Text(10))
				case micheline.PrimBytes:
					buf = strconv.AppendQuote(buf, hex.EncodeToString(b.Key))
				case micheline.PrimString:
					buf = strconv.AppendQuote(buf, string(b.Key))
				}
			} else {
				buf = strconv.AppendQuote(buf, "")
			}
		case "value":
			buf = strconv.AppendQuote(buf, hex.EncodeToString(b.Value))
		case "is_replaced":
			if b.IsReplaced {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_deleted":
			if b.IsDeleted {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_copied":
			if b.IsCopied {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
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

func (b *BigMapItem) MarshalCSV() ([]string, error) {
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(b.RowId, 10)
		case "prev_id":
			res[i] = strconv.FormatUint(b.PrevId, 10)
		case "address":
			res[i] = strconv.Quote(b.addrs[b.AccountId].String())
		case "account_id":
			res[i] = strconv.FormatUint(b.AccountId.Value(), 10)
		case "contract_id":
			res[i] = strconv.FormatUint(b.ContractId, 10)
		case "op_id":
			res[i] = strconv.FormatUint(b.OpId.Value(), 10)
		case "op":
			res[i] = strconv.Quote(b.ops[b.OpId].String())
		case "height":
			res[i] = strconv.FormatInt(b.Height, 10)
		case "time":
			res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
		case "bigmap_id":
			res[i] = strconv.FormatInt(b.BigMapId, 10)
		case "action":
			res[i] = strconv.Quote(b.Action.String())
		case "key_hash":
			res[i] = strconv.Quote(chain.NewExprHash(b.KeyHash).String())
		case "key_type":
			res[i] = strconv.Quote(b.KeyType.String())
		case "key_encoding":
			res[i] = strconv.Quote(b.KeyEncoding.String())
		case "key":
			if len(b.Key) > 0 {
				switch b.KeyEncoding {
				case micheline.PrimInt:
					var z micheline.Z
					if err := z.UnmarshalBinary(b.Key); err != nil {
						return nil, err
					}
					res[i] = strconv.Quote(z.Big().Text(10))
				case micheline.PrimBytes:
					res[i] = strconv.Quote(hex.EncodeToString(b.Key))
				case micheline.PrimString:
					res[i] = strconv.Quote(string(b.Key))
				}
			} else {
				res[i] = strconv.Quote("")
			}
		case "value":
			res[i] = strconv.Quote(hex.EncodeToString(b.Value))
		case "is_replaced":
			res[i] = strconv.FormatBool(b.IsReplaced)
		case "is_deleted":
			res[i] = strconv.FormatBool(b.IsDeleted)
		case "is_copied":
			res[i] = strconv.FormatBool(b.IsCopied)
		default:
			continue
		}
	}
	return res, nil
}

func StreamBigMapItemTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
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
	opT, err := ctx.Indexer.Table(index.OpTableKey)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access table '%s'", index.OpTableKey), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames     []string
		needAccountT bool
		needOpT      bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := bigmapSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			switch v {
			case "address":
				needAccountT = true
			case "op":
				needOpT = true
			case "key":
				srcNames = append(srcNames, "e") // require key_encoding
			}
			if args.Verbose {
				needAccountT = true
				needOpT = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = bigmapAllAliases
		needAccountT = true
		needOpT = true
	}

	// prepare lookup caches
	accMap := make(map[model.AccountID]chain.Address)
	opMap := make(map[model.OpID]chain.OperationHash)

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
		field := bigmapSourceNames[prefix]
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
		case "address":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  pack.FilterModeEqual,
						Value: uint64(0),
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-address lookup and compile condition
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
						// add to map
						if mode == pack.FilterModeEqual {
							needAccountT = false
							accMap[acc.RowId] = acc.Address()
						}
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
					// add to map
					if mode == pack.FilterModeIn {
						needAccountT = false
						accMap[acc.RowId] = acc.Address()
					}
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
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // op id
						Mode:  mode,
						Value: uint64(0),
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-op lookup and compile condition
					op, err := ctx.Indexer.LookupOp(ctx, val[0])
					if err != nil {
						switch err {
						case index.ErrNoOpEntry:
							// expected
						case etl.ErrInvalidHash:
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", val[0]), err))
						default:
							panic(err)
						}
					}
					// Note: when not found we insert an always false condition
					if op == nil || len(op) == 0 {
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find(field), // op id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "op not found", // debugging aid
						})
					} else {
						// add op id as extra condition
						opMap[op[0].RowId] = op[0].Hash.Clone()
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find(field), // op id
							Mode:  mode,
							Value: op[0].RowId.Value(), // op slice may contain internal ops
							Raw:   val[0],              // debugging aid
						})
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
					op, err := ctx.Indexer.LookupOp(ctx, v)
					if err != nil {
						switch err {
						case index.ErrNoOpEntry:
							// expected
						case etl.ErrInvalidHash:
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", v), err))
						default:
							panic(err)
						}
					}
					// skip not found ops
					if op == nil || len(op) == 0 {
						continue
					}
					// collect list of op ids (use first slice value only)
					if mode == pack.FilterModeIn {
						needOpT = false
						opMap[op[0].RowId] = op[0].Hash.Clone()
						ids = append(ids, op[0].RowId.Value())
					}
				}
				// Note: when list is empty (no ops were found, the match will
				//       always be false and return no result as expected)
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field), // op id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "action":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-action
				action, err := micheline.ParseBigMapDiffAction(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid action '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: uint64(action), // it's a byte, which translates to uint64 in packdb
					Raw:   val[0],         // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-action
				actions := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					action, err := micheline.ParseBigMapDiffAction(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid action '%s'", v), err))
					}
					actions = append(actions, uint64(action))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: actions,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "key_hash":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-hash
				h, err := chain.ParseExprHash(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key hash '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: h.Hash.Hash,
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-hash lookup
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					h, err := chain.ParseExprHash(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key hash '%s'", v), err))
					}
					hashes = append(hashes, h.Hash.Hash)
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: hashes,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "key_encoding":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-key
				typ, err := micheline.ParsePrimType(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key encoding '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: uint64(typ), // byte -> uint64
					Raw:   val[0],      // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-key lookup
				typs := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					typ, err := micheline.ParsePrimType(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key encoding '%s'", v), err))
					}
					typs = append(typs, uint64(typ))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: typs,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "key_type":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				typ, err := micheline.ParseBigMapKeyType(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid bigmap key type '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: uint64(typ), // byte -> uint
					Raw:   val[0],      // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				typs := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					typ, err := micheline.ParseBigMapKeyType(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key type '%s'", v), err))
					}
					typs = append(typs, uint64(typ))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: typs,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "key":
			// try to detect what the key is and convert appropriately
			keyType := ctx.Request.URL.Query().Get("key_type")
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-key
				key, err := micheline.ParseBigMapKey(keyType, val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid bigmap key '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: key.Bytes(),
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-key lookup
				keys := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					key, err := micheline.ParseBigMapKey(keyType, v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid key '%s'", v), err))
					}
					keys = append(keys, key.Bytes())
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find(field),
					Mode:  mode,
					Value: keys,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := bigmapSourceNames[prefix]; !ok {
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
	if res.Rows() > 0 && needAccountT {
		// get a unique copy of account id column (clip on request limit)
		col, _ := res.Uint64Column("A")
		find := vec.UniqueUint64Slice(col[:util.Min(len(col), int(args.Limit))])

		// lookup accounts from id
		q := pack.Query{
			Name:   ctx.RequestID + ".bigmap_lookup",
			Fields: accountT.Fields().Select("I", "H", "t"),
			Conditions: pack.ConditionList{pack.Condition{
				Field: accountT.Fields().Find("I"),
				Mode:  pack.FilterModeIn,
				Value: find,
			}},
		}
		ctx.Log.Tracef("Looking up %d accounts", len(find))
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

	// Step 3: resolve related op hashes using lookup (when requested)
	if res.Rows() > 0 && needOpT {
		// get a unique copy of account id column (clip on request limit)
		col, _ := res.Uint64Column("O")
		find := vec.UniqueUint64Slice(col[:util.Min(len(col), int(args.Limit))])

		// lookup accounts from id
		q := pack.Query{
			Name:   ctx.RequestID + ".bigmap_lookup",
			Fields: opT.Fields().Select("I", "H"),
			Conditions: pack.ConditionList{pack.Condition{
				Field: opT.Fields().Find("I"),
				Mode:  pack.FilterModeIn,
				Value: find,
			}},
		}
		ctx.Log.Tracef("Looking up %d ops", len(find))
		type XOp struct {
			Id   model.OpID          `pack:"I,pk"`
			Hash chain.OperationHash `pack:"H"`
		}
		op := &XOp{}
		err := opT.Stream(ctx, q, func(r pack.Row) error {
			if err := r.Decode(op); err != nil {
				return err
			}
			opMap[op.Id] = op.Hash.Clone()
			return nil
		})
		if err != nil {
			// non-fatal error
			ctx.Log.Errorf("Op hash lookup failed: %v", err)
		}
	}

	// prepare return type marshalling
	bigmap := &BigMapItem{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  params,
		addrs:   accMap,
		ops:     opMap,
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
