// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

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
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// long -> short form
	flowSourceNames map[string]string
	// short -> long form
	flowAliasNames map[string]string
	// all aliases as list
	flowAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Flow{})
	if err != nil {
		log.Fatalf("block field type error: %v\n", err)
	}
	flowSourceNames = fields.NameMapReverse()
	flowAllAliases = fields.Aliases()

	// add extra translations
	flowSourceNames["address"] = "A"
	flowSourceNames["counterparty"] = "R"
	flowSourceNames["op"] = "D"
	flowAllAliases = append(flowAllAliases, "address")
	flowAllAliases = append(flowAllAliases, "counterparty")
	flowAllAliases = append(flowAllAliases, "op")
}

// configurable marshalling helper
type Flow struct {
	model.Flow
	verbose bool                               `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList                    `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params                      `csv:"-" pack:"-"` // blockchain amount conversion
	addrs   map[model.AccountID]chain.Address  `csv:"-" pack:"-"` // address map
	ops     map[model.OpID]chain.OperationHash `csv:"-" pack:"-"` // op map
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
		OpId           uint64  `json:"op_id"`
		Op             string  `json:"op"`
		OpN            int     `json:"op_n"`
		OpC            int     `json:"op_c"`
		OpI            int     `json:"op_i"`
		OpL            int     `json:"op_l"`
		OpP            int     `json:"op_p"`
		AccountId      uint64  `json:"account_id"`
		Account        string  `json:"address"`
		AccountType    string  `json:"address_type"`
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
		TokenGenMin    int64   `json:"token_gen_min"`
		TokenGenMax    int64   `json:"token_gen_max"`
		TokenAge       int64   `json:"token_age"`
	}{
		RowId:          f.RowId,
		Height:         f.Height,
		Cycle:          f.Cycle,
		Timestamp:      util.UnixMilliNonZero(f.Timestamp),
		OpId:           f.OpId.Value(),
		Op:             f.ops[f.OpId].String(),
		OpN:            f.OpN,
		OpC:            f.OpC,
		OpI:            f.OpI,
		OpL:            f.OpL,
		OpP:            f.OpP,
		AccountId:      f.AccountId.Value(),
		Account:        f.addrs[f.AccountId].String(),
		AccountType:    f.AddressType.String(),
		CounterPartyId: f.CounterPartyId.Value(),
		CounterParty:   f.addrs[f.CounterPartyId].String(),
		Category:       f.Category.String(),
		Operation:      f.Operation.String(),
		AmountIn:       f.params.ConvertValue(f.AmountIn),
		AmountOut:      f.params.ConvertValue(f.AmountOut),
		IsFee:          f.IsFee,
		IsBurned:       f.IsBurned,
		IsFrozen:       f.IsFrozen,
		IsUnfrozen:     f.IsUnfrozen,
		TokenGenMin:    f.TokenGenMin,
		TokenGenMax:    f.TokenGenMax,
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
		case "op_id":
			buf = strconv.AppendUint(buf, f.OpId.Value(), 10)
		case "op":
			buf = strconv.AppendQuote(buf, f.ops[f.OpId].String())
		case "op_n":
			buf = strconv.AppendInt(buf, int64(f.OpN), 10)
		case "op_c":
			buf = strconv.AppendInt(buf, int64(f.OpC), 10)
		case "op_i":
			buf = strconv.AppendInt(buf, int64(f.OpI), 10)
		case "op_l":
			buf = strconv.AppendInt(buf, int64(f.OpL), 10)
		case "op_p":
			buf = strconv.AppendInt(buf, int64(f.OpP), 10)
		case "account_id":
			buf = strconv.AppendUint(buf, f.AccountId.Value(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, f.addrs[f.AccountId].String())
		case "address_type":
			buf = strconv.AppendQuote(buf, f.AddressType.String())
		case "counterparty_id":
			buf = strconv.AppendUint(buf, f.CounterPartyId.Value(), 10)
		case "counterparty":
			buf = strconv.AppendQuote(buf, f.addrs[f.CounterPartyId].String())
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
		case "token_gen_min":
			buf = strconv.AppendInt(buf, f.TokenGenMin, 10)
		case "token_gen_max":
			buf = strconv.AppendInt(buf, f.TokenGenMax, 10)
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
		case "op_id":
			res[i] = strconv.FormatUint(f.OpId.Value(), 10)
		case "op":
			res[i] = strconv.Quote(f.ops[f.OpId].String())
		case "op_n":
			res[i] = strconv.Itoa(f.OpN)
		case "op_c":
			res[i] = strconv.Itoa(f.OpC)
		case "op_i":
			res[i] = strconv.Itoa(f.OpI)
		case "op_l":
			res[i] = strconv.Itoa(f.OpL)
		case "op_p":
			res[i] = strconv.Itoa(f.OpP)
		case "account_id":
			res[i] = strconv.FormatUint(f.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(f.addrs[f.AccountId].String())
		case "address_type":
			res[i] = strconv.Quote(f.AddressType.String())
		case "counterparty_id":
			res[i] = strconv.FormatUint(f.CounterPartyId.Value(), 10)
		case "counterparty":
			res[i] = strconv.Quote(f.addrs[f.CounterPartyId].String())
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
		case "token_gen_min":
			res[i] = strconv.FormatInt(f.TokenGenMin, 10)
		case "token_gen_max":
			res[i] = strconv.FormatInt(f.TokenGenMax, 10)
		case "token_age":
			res[i] = strconv.FormatInt(f.TokenAge, 10)
		default:
			continue
		}
	}
	return res, nil
}

func StreamFlowTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

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
		needAccountT bool
		needOpT      bool
		srcNames     []string
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := flowSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			if args.Verbose || v == "address" || v == "counterparty" {
				needAccountT = true
			}
			if args.Verbose || v == "op" {
				needOpT = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = flowAllAliases
		needAccountT = true
		needOpT = true
	}

	// build table query
	q := pack.Query{
		Name:       ctx.RequestID,
		Fields:     table.Fields().Select(srcNames...),
		Limit:      int(args.Limit),
		Conditions: make(pack.ConditionList, 0),
		Order:      args.Order,
	}
	accMap := make(map[model.AccountID]chain.Address)
	opMap := make(map[model.OpID]chain.OperationHash)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		mode := pack.FilterModeEqual
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

		case "address", "counterparty":
			field := "A" // account
			if prefix == "counterparty" {
				field = "R"
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
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
						Field: table.Fields().Find("D"), // op id
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
							Field: table.Fields().Find("D"), // op id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "op not found", // debugging aid
						})
					} else {
						opMap[op[0].RowId] = op[0].Hash.Clone()
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find("D"), // op id
							Mode:  mode,
							Value: op[0].RowId.Value(), // op slice may contain internal ops
							Raw:   val[0],              // debugging aid
						})
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
					// collect list of op ids (use first slice balue only since
					// we're looking for ballots which are always single-op)
					opMap[op[0].RowId] = op[0].Hash.Clone()
					ids = append(ids, op[0].RowId.Value())
				}
				// Note: when list is empty (no ops were found, the match will
				//       always be false and return no result as expected)
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("D"), // op id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := flowSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
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
				case "amount_in", "amount_out":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				case "address_type":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := chain.ParseAddressType(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid account type '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
				case "category":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := model.ParseFlowCategory(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid category '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
				case "operation":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := model.ParseFlowType(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid operation '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
				}
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

	// Step 2: resolve accounts using lookup (when requested)
	if needAccountT && res.Rows() > 0 {
		// get a unique copy of account and origin id columns (clip on request limit)
		acol, _ := res.Uint64Column("A")
		ocol, _ := res.Uint64Column("R")
		find := vec.UniqueUint64Slice(acol[:util.Min(len(acol), int(args.Limit))])
		find = vec.UniqueUint64Slice(append(find, ocol[:util.Min(len(ocol), int(args.Limit))]...))

		// lookup accounts from id
		q := pack.Query{
			Name:   ctx.RequestID + ".flow_account_lookup",
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

	// Step 3: resolve ops using lookup (when requested)
	if needOpT && res.Rows() > 0 {
		// get a unique copy of op id column (clip on request limit)
		ucol, _ := res.Uint64Column("D")
		find := vec.UniqueUint64Slice(ucol[:util.Min(len(ucol), int(args.Limit))])

		// filter already known ops
		var n int
		for _, v := range find {
			if _, ok := opMap[model.OpID(v)]; !ok {
				find[n] = v
				n++
			}
		}
		find = find[:n]

		if len(find) > 0 {
			// lookup ops from id
			q := pack.Query{
				Name:   ctx.RequestID + ".flow_op_lookup",
				Fields: opT.Fields().Select("I", "H"),
				Conditions: pack.ConditionList{pack.Condition{
					Field: accountT.Fields().Find("I"),
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
				ctx.Log.Errorf("Op lookup failed: %v", err)
			}
		}
	}

	// prepare return type marshalling
	flow := &Flow{
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
