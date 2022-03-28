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
	opSourceNames map[string]string
	// short -> long form
	opAliasNames map[string]string
	// all aliases as list
	opAllAliases []string

	// long -> short form
	endSourceNames map[string]string
	// short -> long form
	endAliasNames map[string]string
	// all aliases as list
	endAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Op{})
	if err != nil {
		log.Fatalf("op field type error: %v\n", err)
	}
	opSourceNames = fields.NameMapReverse()
	opAllAliases = fields.Aliases()

	fields, err = pack.Fields(&model.Endorsement{})
	if err != nil {
		log.Fatalf("endorsement field type error: %v\n", err)
	}
	endSourceNames = fields.NameMapReverse()
	endAllAliases = fields.Aliases()

	// add extra translations for related accounts
	opSourceNames["sender"] = "S"
	opSourceNames["receiver"] = "R"
	opSourceNames["creator"] = "M"
	opSourceNames["baker"] = "D"
	opSourceNames["block"] = "h"
	opSourceNames["entrypoint"] = "a"    // stored in data field
	opSourceNames["entrypoint_id"] = "-" // ignore, internal
	opSourceNames["address"] = "-"       // any address
	opSourceNames["id"] = "-"            // height + op_p
	opSourceNames["row_id"] = "-"        // suppress, internal only
	opAllAliases = append(opAllAliases,
		"id",
		"sender",
		"receiver",
		"creator",
		"baker",
		"block",
		"entrypoint",
	)
}

type OpSorter []*model.Op

func (o OpSorter) Len() int { return len(o) }

func (o OpSorter) Less(i, j int) bool {
	return o[i].Height < o[j].Height ||
		(o[i].Height == o[j].Height && o[i].OpN < o[j].OpN)
}

func (o OpSorter) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// configurable marshalling helper
type Op struct {
	model.Op
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	ctx     *server.Context
}

func (o *Op) MarshalJSON() ([]byte, error) {
	if o.verbose {
		return o.MarshalJSONVerbose()
	} else {
		return o.MarshalJSONBrief()
	}
}

func (o *Op) MarshalJSONVerbose() ([]byte, error) {
	op := struct {
		Id           uint64          `json:"id"`
		Type         string          `json:"type"`
		Hash         string          `json:"hash"`
		Block        string          `json:"block"`
		Timestamp    int64           `json:"time"`
		Height       int64           `json:"height"`
		Cycle        int64           `json:"cycle"`
		OpN          int             `json:"op_n"`
		OpP          int             `json:"op_p"`
		Status       string          `json:"status"`
		IsSuccess    bool            `json:"is_success"`
		IsContract   bool            `json:"is_contract"`
		IsEvent      bool            `json:"is_event"`
		IsInternal   bool            `json:"is_internal"`
		Counter      int64           `json:"counter"`
		GasLimit     int64           `json:"gas_limit"`
		GasUsed      int64           `json:"gas_used"`
		StorageLimit int64           `json:"storage_limit"`
		StoragePaid  int64           `json:"storage_paid"`
		Volume       float64         `json:"volume"`
		Fee          float64         `json:"fee"`
		Reward       float64         `json:"reward"`
		Deposit      float64         `json:"deposit"`
		Burned       float64         `json:"burned"`
		SenderId     uint64          `json:"sender_id"`
		Sender       string          `json:"sender"`
		ReceiverId   uint64          `json:"receiver_id"`
		Receiver     string          `json:"receiver"`
		CreatorId    uint64          `json:"creator_id"`
		Creator      string          `json:"creator"`
		BakerId      uint64          `json:"baker_id"`
		Baker        string          `json:"baker"`
		Data         string          `json:"data,omitempty"`
		Parameters   string          `json:"parameters,omitempty"`
		Storage      string          `json:"storage,omitempty"`
		BigmapDiff   string          `json:"big_map_diff,omitempty"`
		Errors       json.RawMessage `json:"errors,omitempty"`
		Entrypoint   string          `json:"entrypoint"`
	}{
		Id:           o.Id(),
		Type:         o.Type.String(),
		Hash:         "",
		Block:        o.ctx.Indexer.LookupBlockHash(o.ctx.Context, o.Height).String(),
		Height:       o.Height,
		Cycle:        o.Cycle,
		Timestamp:    util.UnixMilliNonZero(o.Timestamp),
		OpN:          o.OpN,
		OpP:          o.OpP,
		Status:       o.Status.String(),
		IsSuccess:    o.IsSuccess,
		IsContract:   o.IsContract,
		IsEvent:      o.IsEvent,
		IsInternal:   o.IsInternal,
		Counter:      o.Counter,
		GasLimit:     o.GasLimit,
		GasUsed:      o.GasUsed,
		StorageLimit: o.StorageLimit,
		StoragePaid:  o.StoragePaid,
		Volume:       o.params.ConvertValue(o.Volume),
		Fee:          o.params.ConvertValue(o.Fee),
		Reward:       o.params.ConvertValue(o.Reward),
		Deposit:      o.params.ConvertValue(o.Deposit),
		Burned:       o.params.ConvertValue(o.Burned),
		SenderId:     o.SenderId.Value(),
		Sender:       o.ctx.Indexer.LookupAddress(o.ctx, o.SenderId).String(),
		ReceiverId:   o.ReceiverId.Value(),
		Receiver:     o.ctx.Indexer.LookupAddress(o.ctx, o.ReceiverId).String(),
		CreatorId:    o.CreatorId.Value(),
		Creator:      o.ctx.Indexer.LookupAddress(o.ctx, o.CreatorId).String(),
		BakerId:      o.BakerId.Value(),
		Baker:        o.ctx.Indexer.LookupAddress(o.ctx, o.BakerId).String(),
		Data:         o.Data,
		Parameters:   "",
		Storage:      "",
		BigmapDiff:   "",
		Errors:       nil,
	}
	if o.Type.ListId() >= 0 {
		op.Hash = o.Hash.String()
	}
	if len(o.Parameters) > 0 {
		op.Parameters = hex.EncodeToString(o.Parameters)
	}
	if len(o.Storage) > 0 {
		op.Storage = hex.EncodeToString(o.Storage)
	}
	if len(o.BigmapDiff) > 0 {
		op.BigmapDiff = hex.EncodeToString(o.Diff)
	}
	if o.Errors != nil {
		op.Errors = json.RawMessage(o.Errors)
	}
	if o.IsContract {
		op.Entrypoint = o.Data
		op.Data = ""
	}

	return json.Marshal(op)
}

func (o *Op) MarshalJSONBrief() ([]byte, error) {
	dec := o.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range o.columns {
		switch v {
		case "id":
			buf = strconv.AppendUint(buf, o.Id(), 10)
		case "type":
			buf = strconv.AppendQuote(buf, o.Type.String())
		case "hash":
			if o.Type.ListId() >= 0 {
				buf = strconv.AppendQuote(buf, o.Hash.String())
			} else {
				buf = append(buf, []byte(`""`)...)
			}
		case "block":
			buf = strconv.AppendQuote(buf, o.ctx.Indexer.LookupBlockHash(o.ctx.Context, o.Height).String())
		case "height":
			buf = strconv.AppendInt(buf, o.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, o.Cycle, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(o.Timestamp), 10)
		case "op_n":
			buf = strconv.AppendInt(buf, int64(o.OpN), 10)
		case "op_p":
			buf = strconv.AppendInt(buf, int64(o.OpP), 10)
		case "status":
			buf = strconv.AppendQuote(buf, o.Status.String())
		case "is_success":
			if o.IsSuccess {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_contract":
			if o.IsContract {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_event":
			if o.IsEvent {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_internal":
			if o.IsInternal {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "counter":
			buf = strconv.AppendInt(buf, o.Counter, 10)
		case "gas_limit":
			buf = strconv.AppendInt(buf, o.GasLimit, 10)
		case "gas_used":
			buf = strconv.AppendInt(buf, o.GasUsed, 10)
		case "storage_limit":
			buf = strconv.AppendInt(buf, o.StorageLimit, 10)
		case "storage_paid":
			buf = strconv.AppendInt(buf, o.StoragePaid, 10)
		case "volume":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Volume), 'f', dec, 64)
		case "fee":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Fee), 'f', dec, 64)
		case "reward":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Reward), 'f', dec, 64)
		case "deposit":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Deposit), 'f', dec, 64)
		case "burned":
			buf = strconv.AppendFloat(buf, o.params.ConvertValue(o.Burned), 'f', dec, 64)
		case "sender_id":
			buf = strconv.AppendUint(buf, o.SenderId.Value(), 10)
		case "sender":
			buf = strconv.AppendQuote(buf, o.ctx.Indexer.LookupAddress(o.ctx, o.SenderId).String())
		case "receiver_id":
			buf = strconv.AppendUint(buf, o.ReceiverId.Value(), 10)
		case "receiver":
			if o.ReceiverId > 0 {
				buf = strconv.AppendQuote(buf, o.ctx.Indexer.LookupAddress(o.ctx, o.ReceiverId).String())
			} else {
				buf = append(buf, null...)
			}
		case "creator_id":
			buf = strconv.AppendUint(buf, o.CreatorId.Value(), 10)
		case "creator":
			if o.CreatorId > 0 {
				buf = strconv.AppendQuote(buf, o.ctx.Indexer.LookupAddress(o.ctx, o.CreatorId).String())
			} else {
				buf = append(buf, null...)
			}
		case "baker_id":
			buf = strconv.AppendUint(buf, o.BakerId.Value(), 10)
		case "baker":
			if o.BakerId > 0 {
				buf = strconv.AppendQuote(buf, o.ctx.Indexer.LookupAddress(o.ctx, o.BakerId).String())
			} else {
				buf = append(buf, null...)
			}
		case "data":
			if o.Data != "" && !o.IsContract {
				buf = strconv.AppendQuote(buf, o.Data)
			} else {
				buf = append(buf, null...)
			}
		case "parameters":
			// parameters is binary
			if len(o.Parameters) > 0 {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(o.Parameters))
			} else {
				buf = append(buf, null...)
			}
		case "storage":
			// storage is binary
			if len(o.Storage) > 0 {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(o.Storage))
			} else {
				buf = append(buf, null...)
			}
		case "big_map_diff":
			// big_map_diff is binary
			if len(o.Diff) > 0 {
				buf = strconv.AppendQuote(buf, hex.EncodeToString(o.Diff))
			} else {
				buf = append(buf, null...)
			}
		case "errors":
			// errors is raw json
			if len(o.Errors) > 0 {
				buf = strconv.AppendQuote(buf, string(o.Errors))
			} else {
				buf = append(buf, null...)
			}
		case "entrypoint":
			if o.IsContract {
				buf = strconv.AppendQuote(buf, o.Data)
			} else {
				buf = append(buf, null...)
			}
		default:
			continue
		}
		if i < len(o.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (o *Op) MarshalCSV() ([]string, error) {
	dec := o.params.Decimals
	res := make([]string, len(o.columns))
	for i, v := range o.columns {
		switch v {
		case "id":
			res[i] = strconv.FormatUint(o.Id(), 10)
		case "type":
			res[i] = strconv.Quote(o.Type.String())
		case "hash":
			if o.Type.ListId() >= 0 {
				res[i] = strconv.Quote(o.Hash.String())
			} else {
				res[i] = `""`
			}
		case "block":
			res[i] = strconv.Quote(o.ctx.Indexer.LookupBlockHash(o.ctx.Context, o.Height).String())
		case "height":
			res[i] = strconv.FormatInt(o.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(o.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(o.Timestamp.Format(time.RFC3339))
		case "op_n":
			res[i] = strconv.FormatInt(int64(o.OpN), 10)
		case "op_p":
			res[i] = strconv.FormatInt(int64(o.OpP), 10)
		case "status":
			res[i] = strconv.Quote(o.Status.String())
		case "is_success":
			res[i] = strconv.FormatBool(o.IsSuccess)
		case "is_contract":
			res[i] = strconv.FormatBool(o.IsContract)
		case "is_event":
			res[i] = strconv.FormatBool(o.IsEvent)
		case "is_internal":
			res[i] = strconv.FormatBool(o.IsInternal)
		case "counter":
			res[i] = strconv.FormatInt(o.Counter, 10)
		case "gas_limit":
			res[i] = strconv.FormatInt(o.GasLimit, 10)
		case "gas_used":
			res[i] = strconv.FormatInt(o.GasUsed, 10)
		case "storage_limit":
			res[i] = strconv.FormatInt(o.StorageLimit, 10)
		case "storage_paid":
			res[i] = strconv.FormatInt(o.StoragePaid, 10)
		case "volume":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Volume), 'f', dec, 64)
		case "fee":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Fee), 'f', dec, 64)
		case "reward":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Reward), 'f', dec, 64)
		case "deposit":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Deposit), 'f', dec, 64)
		case "burned":
			res[i] = strconv.FormatFloat(o.params.ConvertValue(o.Burned), 'f', dec, 64)
		case "sender_id":
			res[i] = strconv.FormatUint(o.SenderId.Value(), 10)
		case "sender":
			res[i] = strconv.Quote(o.ctx.Indexer.LookupAddress(o.ctx, o.SenderId).String())
		case "receiver_id":
			res[i] = strconv.FormatUint(o.ReceiverId.Value(), 10)
		case "receiver":
			res[i] = strconv.Quote(o.ctx.Indexer.LookupAddress(o.ctx, o.ReceiverId).String())
		case "creator_id":
			res[i] = strconv.FormatUint(o.CreatorId.Value(), 10)
		case "creator":
			res[i] = strconv.Quote(o.ctx.Indexer.LookupAddress(o.ctx, o.CreatorId).String())
		case "baker_id":
			res[i] = strconv.FormatUint(o.BakerId.Value(), 10)
		case "baker":
			res[i] = strconv.Quote(o.ctx.Indexer.LookupAddress(o.ctx, o.BakerId).String())
		case "data":
			if !o.IsContract {
				res[i] = strconv.Quote(o.Data)
			} else {
				res[i] = `""`
			}
		case "parameters":
			res[i] = strconv.Quote(hex.EncodeToString(o.Parameters))
		case "storage":
			res[i] = strconv.Quote(hex.EncodeToString(o.Storage))
		case "big_map_diff":
			res[i] = strconv.Quote(hex.EncodeToString(o.Diff))
		case "errors":
			res[i] = strconv.Quote(string(o.Errors))
		case "entrypoint":
			if o.IsContract {
				res[i] = strconv.Quote(o.Data)
			} else {
				res[i] = `""`
			}
		default:
			continue
		}
	}
	return res, nil
}

func StreamOpTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames []string
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := opSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			switch v {
			case "address":
				srcNames = append(srcNames, "sender_id", "receiver_id", "baker_id", "creator_id")
			case "id":
				srcNames = append(srcNames, "height", "op_n")
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = opAllAliases
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID, table).
		WithFields(srcNames...).
		WithLimit(int(args.Limit)).
		WithOrder(args.Order)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
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
			id, err := strconv.ParseUint(val[0], 10, 64)
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid cursor value '%s'", val), err))
			}
			height := int64(id >> 16)
			opn := int64(id & 0xFFFF)
			if args.Order == pack.OrderDesc {
				q = q.Or(
					pack.Lt("height", height),
					pack.And(
						pack.Equal("height", height),
						pack.Lt("op_n", opn),
					),
				)
			} else {
				q = q.Or(
					pack.Gt("height", height),
					pack.And(
						pack.Equal("height", height),
						pack.Gt("op_n", opn),
					),
				)
			}
		case "id":
			switch mode {
			case pack.FilterModeEqual:
				id, err := strconv.ParseUint(val[0], 10, 64)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid id value '%s'", val[0]), err))
				}
				height := int64(id >> 16)
				opn := int64(id & 0xFFFF)
				q = q.AndCondition("height", mode, height).AndCondition("op_n", mode, opn)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "hash":
			// special hash type to []byte conversion
			hashes := make([][]byte, len(val))
			for i, v := range val {
				h, err := tezos.ParseOpHash(v)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation hash '%s'", v), err))
				}
				hashes[i] = h.Hash.Hash
			}
			if len(hashes) == 1 {
				q = q.AndEqual("hash", hashes[0])
			} else {
				q = q.AndIn("hash", hashes)
			}
		case "block":
			// special hash type to []byte conversion
			heights := make([]int64, len(val))
			for i, v := range val {
				b, err := ctx.Indexer.LookupBlock(ctx.Context, v)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid block '%s'", v), err))
				}
				heights[i] = b.Height
			}
			if len(heights) == 1 {
				q = q.AndEqual("height", heights[0])
			} else {
				q = q.AndIn("height", heights)
			}
		case "type":
			// parse only the first value
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				typ := model.ParseOpType(val[0])
				if !typ.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", val[0]), nil))
				}
				q = q.AndCondition("type", mode, typ)
			case pack.FilterModeIn, pack.FilterModeNotIn:
				typs := make([]uint8, 0)
				for _, t := range strings.Split(val[0], ",") {
					typ := model.ParseOpType(t)
					if !typ.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", t), nil))
					}
					typs = append(typs, uint8(typ))
				}
				q = q.AndCondition("type", mode, typs)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "status":
			// parse only the first value
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				stat := tezos.ParseOpStatus(val[0])
				if !stat.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation status '%s'", val[0]), nil))
				}
				q = q.AndCondition("status", mode, stat)
			case pack.FilterModeIn, pack.FilterModeNotIn:
				stats := make([]uint8, 0)
				for _, t := range strings.Split(val[0], ",") {
					stat := tezos.ParseOpStatus(t)
					if !stat.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation status '%s'", t), nil))
					}
					stats = append(stats, uint8(stat))
				}
				q = q.AndCondition("status", mode, stats)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "address":
			// any address, use OR cond
			// parse address and lookup id
			addrs := make([]model.AccountID, 0)
			for _, v := range strings.Split(val[0], ",") {
				addr, err := tezos.ParseAddress(v)
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != index.ErrNoAccountEntry {
					panic(err)
				}
				if err == nil && acc.RowId > 0 {
					addrs = append(addrs, acc.RowId)
				}
			}

			switch mode {
			case pack.FilterModeEqual: // OR
				if len(addrs) == 1 {
					q = q.Or(
						pack.Equal("sender_id", addrs[0]),
						pack.Equal("receiver_id", addrs[0]),
						pack.Equal("baker_id", addrs[0]),
						pack.Equal("creator_id", addrs[0]),
					)
				}
				fallthrough

			case pack.FilterModeIn: // OR
				if len(addrs) > 1 {
					q = q.Or(
						pack.In("sender_id", addrs),
						pack.In("receiver_id", addrs),
						pack.In("baker_id", addrs),
						pack.In("creator_id", addrs),
					)
				}

			case pack.FilterModeNotEqual: // AND
				if len(addrs) == 1 {
					q = q.And(
						pack.NotEqual("sender_id", addrs[0]),
						pack.NotEqual("receiver_id", addrs[0]),
						pack.NotEqual("baker_id", addrs[0]),
						pack.NotEqual("creator_id", addrs[0]),
					)
				}
				fallthrough

			case pack.FilterModeNotIn: // AND
				if len(addrs) > 1 {
					q = q.And(
						pack.NotIn("sender_id", addrs),
						pack.NotIn("receiver_id", addrs),
						pack.NotIn("baker_id", addrs),
						pack.NotIn("creator_id", addrs),
					)
				}

			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "sender", "receiver", "creator", "baker":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			field := opSourceNames[prefix]
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q = q.AndEqual(field, 0)
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
						q = q.AndCondition(field, mode, uint64(math.MaxUint64))
					} else {
						// add id as extra condition
						q = q.AndCondition(field, mode, acc.RowId)
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, a := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(a)
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
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
				q = q.AndCondition(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := opSourceNames[prefix]; !ok {
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
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				case "volume", "reward", "fee", "deposit", "burned":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				}
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

	// run queries
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read ops", err))
	}
	ops := make([]*model.Op, 0, res.Rows())
	res.Walk(func(r pack.Row) error {
		o := model.AllocOp()
		r.Decode(o)
		ops = append(ops, o)
		return nil
	})
	res.Close()

	defer func() {
		for _, v := range ops {
			v.Free()
		}
	}()

	// prepare return type marshalling
	op := &Op{
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
		for _, v := range ops {
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			op.Op = *v
			if err = enc.Encode(op); err != nil {
				break
			}
			count++
			lastId = op.Id()
			if args.Limit > 0 && count == int(args.Limit) {
				err = io.EOF
				break
			}
		}
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
			for _, v := range ops {
				op.Op = *v
				if err = enc.EncodeRecord(op); err != nil {
					break
				}
				count++
				lastId = op.Id()
				if args.Limit > 0 && count == int(args.Limit) {
					err = io.EOF
					break
				}
			}
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
