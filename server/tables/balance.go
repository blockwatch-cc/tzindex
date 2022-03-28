// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

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
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
    "blockwatch.cc/tzindex/server"
)

var (
    // long -> short form
    balanceSourceNames map[string]string
    // short -> long form
    balanceAliasNames map[string]string
    // all aliases as list
    balanceAllAliases []string
)

func init() {
    fields, err := pack.Fields(&model.Balance{})
    if err != nil {
        log.Fatalf("balance field type error: %v\n", err)
    }
    balanceSourceNames = fields.NameMapReverse()
    balanceAllAliases = fields.Aliases()

    // add extra translations
    balanceSourceNames["time"] = "-" // for balance series
    balanceSourceNames["address"] = "A"
    balanceSourceNames["valid_from_time"] = ">"
    balanceSourceNames["valid_until_time"] = "<"
    balanceAllAliases = append(balanceAllAliases,
        "address",
        "valid_from_time",
        "valid_until_time",
    )
}

// configurable marshalling helper
type Balance struct {
    model.Balance
    verbose bool            // cond. marshal
    columns util.StringList // cond. cols & order when brief
    params  *tezos.Params   // blockchain amount conversion
    ctx     *server.Context
}

func (b *Balance) MarshalJSON() ([]byte, error) {
    if b.verbose {
        return b.MarshalJSONVerbose()
    } else {
        return b.MarshalJSONBrief()
    }
}

func (b *Balance) MarshalJSONVerbose() ([]byte, error) {
    balance := struct {
        RowId          uint64    `json:"row_id"`
        AccountId      uint64    `json:"account_id"`
        Account        string    `json:"address"`
        Balance        float64   `json:"balance"`
        ValidFrom      int64     `json:"valid_from"`
        ValidFromTime  time.Time `json:"valid_from_time"`
        ValidUntil     int64     `json:"valid_until"`
        ValidUntilTime time.Time `json:"valid_until_time"`
    }{
        RowId:          b.RowId,
        AccountId:      b.AccountId.Value(),
        Account:        b.ctx.Indexer.LookupAddress(b.ctx, b.AccountId).String(),
        Balance:        b.params.ConvertValue(b.Balance.Balance),
        ValidFrom:      b.ValidFrom,
        ValidFromTime:  b.ctx.Indexer.LookupBlockTime(b.ctx, b.ValidFrom),
        ValidUntil:     b.ValidUntil,
        ValidUntilTime: b.ctx.Indexer.LookupBlockTime(b.ctx, b.ValidUntil),
    }
    return json.Marshal(balance)
}

func (b *Balance) MarshalJSONBrief() ([]byte, error) {
    dec := b.params.Decimals
    buf := make([]byte, 0, 2048)
    buf = append(buf, '[')
    for i, v := range b.columns {
        switch v {
        case "row_id":
            buf = strconv.AppendUint(buf, b.RowId, 10)
        case "account_id":
            buf = strconv.AppendUint(buf, b.AccountId.Value(), 10)
        case "address":
            buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.AccountId).String())
        case "balance":
            buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Balance.Balance), 'f', dec, 64)
        case "valid_from":
            buf = strconv.AppendInt(buf, b.ValidFrom, 10)
        case "valid_until":
            buf = strconv.AppendInt(buf, b.ValidUntil, 10)
        case "valid_from_time":
            buf = strconv.AppendInt(buf, b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.ValidFrom), 10)
        case "valid_until_time":
            buf = strconv.AppendInt(buf, b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.ValidUntil), 10)
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

func (b *Balance) MarshalCSV() ([]string, error) {
    dec := b.params.Decimals
    res := make([]string, len(b.columns))
    for i, v := range b.columns {
        switch v {
        case "row_id":
            res[i] = strconv.FormatUint(b.RowId, 10)
        case "account_id":
            res[i] = strconv.FormatUint(b.AccountId.Value(), 10)
        case "address":
            res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.AccountId).String())
        case "balance":
            res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Balance.Balance), 'f', dec, 64)
        case "valid_from":
            res[i] = strconv.FormatInt(b.ValidFrom, 10)
        case "valid_until":
            res[i] = strconv.FormatInt(b.ValidUntil, 10)
        case "valid_from_time":
            res[i] = strconv.FormatInt(b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.ValidFrom), 10)
        case "valid_until_time":
            res[i] = strconv.FormatInt(b.ctx.Indexer.LookupBlockTimeMs(b.ctx, b.ValidUntil), 10)
        default:
            continue
        }
    }
    return res, nil
}

func StreamBalanceTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
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
            n, ok := balanceSourceNames[v]
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
        args.Columns = balanceAllAliases
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
            field := "A" // account
            switch mode {
            case pack.FilterModeEqual, pack.FilterModeNotEqual:
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
        case "valid_from_time":
            from, err := util.ParseTime(val[0])
            if err != nil {
                panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time '%s'", val[0]), err))
            }
            q.Conditions.AddAndCondition(&pack.Condition{
                Field: table.Fields().Find(">"),
                Mode:  mode,
                Value: ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, from.Time()),
                Raw:   val[0], // debugging aid
            })
        case "valid_until_time":
            until, err := util.ParseTime(val[0])
            if err != nil {
                panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time '%s'", val[0]), err))
            }
            q.Conditions.AddAndCondition(&pack.Condition{
                Field: table.Fields().Find("<"),
                Mode:  mode,
                Value: ctx.Indexer.LookupBlockHeightFromTime(ctx.Context, until.Time()),
                Raw:   val[0], // debugging aid
            })
        default:
            // translate long column name used in query to short column name used in packs
            if short, ok := balanceSourceNames[prefix]; !ok {
                panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
            } else {
                key = strings.Replace(key, prefix, short, 1)
            }

            // the same field name may appear multiple times, in which case conditions
            // are combined like any other condition with logical AND
            for _, v := range val {
                // convert amounts from float to int64
                if prefix == "balance" {
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

    // prepare return type marshalling
    balance := &Balance{
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
        err = table.Stream(ctx.Context, q, func(r pack.Row) error {
            if needComma {
                io.WriteString(ctx.ResponseWriter, ",")
            } else {
                needComma = true
            }
            if err := r.Decode(balance); err != nil {
                return err
            }
            if err := enc.Encode(balance); err != nil {
                return err
            }
            count++
            lastId = balance.RowId
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
            err = table.Stream(ctx.Context, q, func(r pack.Row) error {
                if err := r.Decode(balance); err != nil {
                    return err
                }
                if err := enc.EncodeRecord(balance); err != nil {
                    return err
                }
                count++
                lastId = balance.RowId
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
