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
	ballotSourceNames map[string]string
	// short -> long form
	ballotAliasNames map[string]string
	// all aliases as list
	ballotAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Ballot{})
	if err != nil {
		log.Fatalf("ballot field type error: %v\n", err)
	}
	ballotSourceNames = fields.NameMapReverse()
	ballotAllAliases = fields.Aliases()

	// add extra translations
	ballotSourceNames["source"] = "S"
	ballotSourceNames["op"] = "O"
	ballotSourceNames["proposal"] = "P"

	ballotAllAliases = append(ballotAllAliases, "source")
	ballotAllAliases = append(ballotAllAliases, "op")
	ballotAllAliases = append(ballotAllAliases, "proposal")
}

// configurable marshalling helper
type Ballot struct {
	model.Ballot
	verbose  bool                               `csv:"-" pack:"-"` // cond. marshal
	columns  util.StringList                    `csv:"-" pack:"-"` // cond. cols & order when brief
	ctx      *ApiContext                        `csv:"-" pack:"-"` // blockchain amount conversion
	accounts map[model.AccountID]chain.Address  `csv:"-" pack:"-"` // address map
	ops      map[model.OpID]chain.OperationHash `csv:"-" pack:"-"` // op map
}

func (b *Ballot) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *Ballot) MarshalJSONVerbose() ([]byte, error) {
	ballot := struct {
		RowId            uint64 `json:"row_id"`
		ElectionId       uint64 `json:"election_id"`
		ProposalId       uint64 `json:"proposal_id"`
		Proposal         string `json:"proposal"`
		VotingPeriod     int64  `json:"voting_period"`
		VotingPeriodKind string `json:"voting_period_kind"`
		Height           int64  `json:"height"`
		Time             int64  `json:"time"`
		SourceId         uint64 `json:"source_id"`
		Source           string `json:"source"`
		OpId             uint64 `json:"op_id"`
		Op               string `json:"op"`
		Rolls            int64  `json:"rolls"`
		Ballot           string `json:"ballot"`
	}{
		RowId:            b.RowId,
		ElectionId:       b.ElectionId.Value(),
		ProposalId:       b.ProposalId.Value(),
		Proposal:         govLookupProposalHash(b.ctx, b.ProposalId).String(),
		VotingPeriod:     b.VotingPeriod,
		VotingPeriodKind: b.VotingPeriodKind.String(),
		Height:           b.Height,
		Time:             util.UnixMilliNonZero(b.Time),
		SourceId:         b.SourceId.Value(),
		Source:           b.accounts[b.SourceId].String(),
		OpId:             b.OpId.Value(),
		Op:               b.ops[b.OpId].String(),
		Rolls:            b.Rolls,
		Ballot:           b.Ballot.Ballot.String(),
	}
	return json.Marshal(ballot)
}

func (b *Ballot) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, b.RowId, 10)
		case "election_id":
			buf = strconv.AppendUint(buf, b.ElectionId.Value(), 10)
		case "proposal_id":
			buf = strconv.AppendUint(buf, b.ProposalId.Value(), 10)
		case "proposal":
			buf = strconv.AppendQuote(buf, govLookupProposalHash(b.ctx, b.ProposalId).String())
		case "voting_period":
			buf = strconv.AppendInt(buf, b.VotingPeriod, 10)
		case "voting_period_kind":
			buf = strconv.AppendQuote(buf, b.VotingPeriodKind.String())
		case "height":
			buf = strconv.AppendInt(buf, b.Height, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Time), 10)
		case "source_id":
			buf = strconv.AppendUint(buf, b.SourceId.Value(), 10)
		case "source":
			buf = strconv.AppendQuote(buf, b.accounts[b.SourceId].String())
		case "op_id":
			buf = strconv.AppendUint(buf, b.OpId.Value(), 10)
		case "op":
			buf = strconv.AppendQuote(buf, b.ops[b.OpId].String())
		case "rolls":
			buf = strconv.AppendInt(buf, b.Rolls, 10)
		case "ballot":
			buf = strconv.AppendQuote(buf, b.Ballot.Ballot.String())
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

func (b *Ballot) MarshalCSV() ([]string, error) {
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(b.RowId, 10)
		case "election_id":
			res[i] = strconv.FormatUint(b.ElectionId.Value(), 10)
		case "proposal_id":
			res[i] = strconv.FormatUint(b.ProposalId.Value(), 10)
		case "proposal":
			res[i] = strconv.Quote(govLookupProposalHash(b.ctx, b.ProposalId).String())
		case "voting_period":
			res[i] = strconv.FormatInt(b.VotingPeriod, 10)
		case "voting_period_kind":
			res[i] = strconv.Quote(b.VotingPeriodKind.String())
		case "height":
			res[i] = strconv.FormatInt(b.Height, 10)
		case "time":
			res[i] = strconv.Quote(b.Time.Format(time.RFC3339))
		case "source_id":
			res[i] = strconv.FormatUint(b.SourceId.Value(), 10)
		case "source":
			res[i] = strconv.Quote(b.accounts[b.SourceId].String())
		case "op_id":
			res[i] = strconv.FormatUint(b.OpId.Value(), 10)
		case "op":
			res[i] = strconv.Quote(b.ops[b.OpId].String())
		case "rolls":
			res[i] = strconv.FormatInt(b.Rolls, 10)
		case "ballot":
			res[i] = strconv.Quote(b.Ballot.Ballot.String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamBallotTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
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
			n, ok := ballotSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			switch v {
			case "source":
				needAccountT = true
			case "op":
				needOpT = true
			}
			if args.Verbose {
				needAccountT = true
				needOpT = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = ballotAllAliases
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
		case "source":
			// parse source/baker address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find("S"), // source id
						Mode:  mode,
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
							Field: table.Fields().Find("S"), // source id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "account not found", // debugging aid
						})
					} else {
						// keep for output
						accMap[acc.RowId] = chain.NewAddress(acc.Type, acc.Hash)
						// add addr id as extra fund_flow condition
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find("S"), // source id
							Mode:  mode,
							Value: acc.RowId.Value(),
							Raw:   val[0], // debugging aid
						})
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
					// keep for output
					accMap[acc.RowId] = chain.NewAddress(acc.Type, acc.Hash)
					// collect list of account ids
					ids = append(ids, acc.RowId.Value())
				}
				// Note: when list is empty (no accounts were found, the match will
				//       always be false and return no result as expected)
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("S"), // source id
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
						Field: table.Fields().Find("O"), // op id
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
							Field: table.Fields().Find("O"), // op id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "op not found", // debugging aid
						})
					} else {
						// add addr id as extra fund_flow condition
						opMap[op[0].RowId] = op[0].Hash.Clone()
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find("O"), // op id
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
					Field: table.Fields().Find("O"), // op id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := ballotSourceNames[prefix]; !ok {
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

	// Step 2: resolve accounts using lookup (when requested)
	if needAccountT && res.Rows() > 0 {
		// get a unique copy of source id column (clip on request limit)
		ucol, _ := res.Uint64Column("S")
		find := vec.UniqueUint64Slice(ucol[:util.Min(len(ucol), int(args.Limit))])

		// filter already known accounts
		var n int
		for _, v := range find {
			if _, ok := accMap[model.AccountID(v)]; !ok {
				find[n] = v
				n++
			}
		}
		find = find[:n]

		if len(find) > 0 {
			// lookup accounts from id
			q := pack.Query{
				Name:   ctx.RequestID + ".ballot_source_lookup",
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
	}

	// Step 3: resolve ops using lookup (when requested)
	if needOpT && res.Rows() > 0 {
		// get a unique copy of source id column (clip on request limit)
		ucol, _ := res.Uint64Column("O")
		find := vec.UniqueUint64Slice(ucol[:util.Min(len(ucol), int(args.Limit))])

		// filter already known accounts
		var n int
		for _, v := range find {
			if _, ok := opMap[model.OpID(v)]; !ok {
				find[n] = v
				n++
			}
		}
		find = find[:n]

		if len(find) > 0 {
			// lookup accounts from id
			q := pack.Query{
				Name:   ctx.RequestID + ".ballot_op_lookup",
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
	ballot := &Ballot{
		verbose:  args.Verbose,
		columns:  util.StringList(args.Columns),
		ctx:      ctx,
		accounts: accMap,
		ops:      opMap,
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
			if err := r.Decode(ballot); err != nil {
				return err
			}
			if err := enc.Encode(ballot); err != nil {
				return err
			}
			count++
			lastId = ballot.RowId
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
				if err := r.Decode(ballot); err != nil {
					return err
				}
				if err := enc.EncodeRecord(ballot); err != nil {
					return err
				}
				count++
				lastId = ballot.RowId
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
