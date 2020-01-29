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
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
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
	contractSourceNames["address"] = "A"
	contractSourceNames["manager"] = "M"
	contractAllAliases = append(contractAllAliases, "address")
	contractAllAliases = append(contractAllAliases, "manager")
}

// configurable marshalling helper
type Contract struct {
	model.Contract
	verbose bool                              `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList                   `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params                     `csv:"-" pack:"-"` // blockchain amount conversion
	addrs   map[model.AccountID]chain.Address `csv:"-" pack:"-"` // address map
}

func (c *Contract) MarshalJSON() ([]byte, error) {
	if c.verbose {
		return c.MarshalJSONVerbose()
	} else {
		return c.MarshalJSONBrief()
	}
}

func (c *Contract) MarshalJSONVerbose() ([]byte, error) {
	contract := struct {
		RowId         uint64  `json:"row_id"`
		AccountId     uint64  `json:"account_id"`
		Account       string  `json:"address"`
		ManagerId     uint64  `json:"manager_id"`
		Manager       string  `json:"manager"`
		Height        int64   `json:"height"`
		Fee           float64 `json:"fee"`
		GasLimit      int64   `json:"gas_limit"`
		GasUsed       int64   `json:"gas_used"`
		GasPrice      float64 `json:"gas_price"`
		StorageLimit  int64   `json:"storage_limit"`
		StorageSize   int64   `json:"storage_size"`
		StoragePaid   int64   `json:"storage_paid"`
		Script        string  `json:"script"`
		IsSpendable   bool    `json:"is_spendable"`
		IsDelegatable bool    `json:"is_delegatable"`
	}{
		RowId:         c.RowId,
		AccountId:     c.AccountId.Value(),
		Account:       chain.NewAddress(chain.AddressTypeContract, c.Hash).String(),
		ManagerId:     c.ManagerId.Value(),
		Manager:       c.addrs[c.ManagerId].String(),
		Height:        c.Height,
		Fee:           c.params.ConvertValue(c.Fee),
		GasLimit:      c.GasLimit,
		GasUsed:       c.GasUsed,
		GasPrice:      c.GasPrice,
		StorageLimit:  c.StorageLimit,
		StorageSize:   c.StorageSize,
		StoragePaid:   c.StoragePaid,
		Script:        hex.EncodeToString(c.Script),
		IsSpendable:   c.IsSpendable,
		IsDelegatable: c.IsDelegatable,
	}

	return json.Marshal(contract)
}

func (c *Contract) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range c.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, c.RowId, 10)
		case "account_id":
			buf = strconv.AppendUint(buf, c.AccountId.Value(), 10)
		case "address":
			addr := chain.NewAddress(chain.AddressTypeContract, c.Hash)
			buf = strconv.AppendQuote(buf, addr.String())
		case "manager_id":
			buf = strconv.AppendUint(buf, c.ManagerId.Value(), 10)
		case "manager":
			buf = strconv.AppendQuote(buf, c.addrs[c.ManagerId].String())
		case "height":
			buf = strconv.AppendInt(buf, c.Height, 10)
		case "fee":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.Fee), 'f', c.params.Decimals, 64)
		case "gas_limit":
			buf = strconv.AppendInt(buf, c.GasLimit, 10)
		case "gas_used":
			buf = strconv.AppendInt(buf, c.GasUsed, 10)
		case "gas_price":
			buf = strconv.AppendFloat(buf, c.GasPrice, 'f', 3, 64)
		case "storage_limit":
			buf = strconv.AppendInt(buf, c.StorageLimit, 10)
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
		case "is_spendable":
			if c.IsSpendable {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_delegatable":
			if c.IsDelegatable {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
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
			res[i] = strconv.FormatUint(c.RowId, 10)
		case "account_id":
			res[i] = strconv.FormatUint(c.AccountId.Value(), 10)
		case "address":
			addr := chain.NewAddress(chain.AddressTypeContract, c.Hash)
			res[i] = strconv.Quote(addr.String())
		case "manager_id":
			res[i] = strconv.FormatUint(c.ManagerId.Value(), 10)
		case "manager":
			res[i] = strconv.Quote(c.addrs[c.ManagerId].String())
		case "height":
			res[i] = strconv.FormatInt(c.Height, 10)
		case "fee":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.Fee), 'f', c.params.Decimals, 64)
		case "gas_limit":
			res[i] = strconv.FormatInt(c.GasLimit, 10)
		case "gas_used":
			res[i] = strconv.FormatInt(c.GasUsed, 10)
		case "gas_price":
			res[i] = strconv.FormatFloat(c.GasPrice, 'f', 3, 64)
		case "storage_limit":
			res[i] = strconv.FormatInt(c.StorageLimit, 10)
		case "storage_size":
			res[i] = strconv.FormatInt(c.StorageSize, 10)
		case "storage_paid":
			res[i] = strconv.FormatInt(c.StoragePaid, 10)
		case "script":
			res[i] = strconv.Quote(hex.EncodeToString(c.Script))
		case "is_spendable":
			res[i] = strconv.FormatBool(c.IsSpendable)
		case "is_delegatable":
			res[i] = strconv.FormatBool(c.IsDelegatable)
		default:
			continue
		}
	}
	return res, nil
}

func StreamContractTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
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

	// translate long column names to short names used in pack tables
	var srcNames []string
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := contractSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n == "-" {
				continue
			}
			switch v {
			case "address":
				srcNames = append(srcNames, "H") // hash
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
				// single-address lookup and compile condition
				addr, err := chain.ParseAddress(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				if addr.Type != chain.AddressTypeContract {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid contract address '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  mode,
					Value: addr.Hash,
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup (Note: does not check for address type so may
				// return duplicates)
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := chain.ParseAddress(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					if addr.Type != chain.AddressTypeContract {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid contract address '%s'", v), err))
					}
					hashes = append(hashes, addr.Hash)
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  mode,
					Value: hashes,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "manager":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-account lookup and compile condition
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
						Field: table.Fields().Find("M"), // account id
						Mode:  mode,
						Value: uint64(math.MaxUint64),
						Raw:   "account not found", // debugging aid
					})
				} else {
					// add id as extra condition
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find("M"), // account id
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
					Field: table.Fields().Find("M"), // account id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := contractSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "fee":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
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

	// Step 2: resolve related accounts using lookup (when requested)
	accMap := make(map[model.AccountID]chain.Address)
	if res.Rows() > 0 {
		// get a unique copy of delegate and manager id columns (clip on request limit)
		mcol, _ := res.Uint64Column("M")
		find := vec.UniqueUint64Slice(mcol[:util.Min(len(mcol), int(args.Limit))])

		// lookup accounts from id
		q := pack.Query{
			Name:   ctx.RequestID + ".contract_lookup",
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

	// prepare return type marshalling
	contract := &Contract{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  params,
		addrs:   accMap,
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
			if err := r.Decode(contract); err != nil {
				return err
			}
			if err := enc.Encode(contract); err != nil {
				return err
			}
			count++
			lastId = contract.RowId
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
				if err := r.Decode(contract); err != nil {
					return err
				}
				if err := enc.EncodeRecord(contract); err != nil {
					return err
				}
				count++
				lastId = contract.RowId
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
