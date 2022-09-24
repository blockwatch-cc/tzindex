// Copyright (c) 2020-2021 Blockwatch Data Inc.
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
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	ballotSourceNames map[string]string
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
	verbose bool                        // cond. marshal
	columns util.StringList             // cond. cols & order when brief
	ctx     *server.Context             // blockchain amount conversion
	ops     map[model.OpID]tezos.OpHash // op map
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
		RowId            uint64  `json:"row_id"`
		ElectionId       uint64  `json:"election_id"`
		ProposalId       uint64  `json:"proposal_id"`
		Proposal         string  `json:"proposal"`
		VotingPeriod     int64   `json:"voting_period"`
		VotingPeriodKind string  `json:"voting_period_kind"`
		Height           int64   `json:"height"`
		Time             int64   `json:"time"`
		SourceId         uint64  `json:"source_id"`
		Source           string  `json:"source"`
		OpId             uint64  `json:"op_id"`
		Op               string  `json:"op"`
		Rolls            int64   `json:"rolls"`
		Stake            float64 `json:"stake"`
		Ballot           string  `json:"ballot"`
	}{
		RowId:            b.RowId,
		ElectionId:       b.ElectionId.Value(),
		ProposalId:       b.ProposalId.Value(),
		Proposal:         b.ctx.Indexer.LookupProposalHash(b.ctx, b.ProposalId).String(),
		VotingPeriod:     b.VotingPeriod,
		VotingPeriodKind: b.VotingPeriodKind.String(),
		Height:           b.Height,
		Time:             util.UnixMilliNonZero(b.Time),
		SourceId:         b.SourceId.Value(),
		Source:           b.ctx.Indexer.LookupAddress(b.ctx, b.SourceId).String(),
		OpId:             b.OpId.Value(),
		Op:               b.ops[b.OpId].String(),
		Rolls:            b.Rolls,
		Stake:            b.ctx.Params.ConvertValue(b.Stake),
		Ballot:           b.Ballot.Ballot.String(),
	}
	return json.Marshal(ballot)
}

func (b *Ballot) MarshalJSONBrief() ([]byte, error) {
	dec := b.ctx.Params.Decimals
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
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupProposalHash(b.ctx, b.ProposalId).String())
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
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.SourceId).String())
		case "op_id":
			buf = strconv.AppendUint(buf, b.OpId.Value(), 10)
		case "op":
			buf = strconv.AppendQuote(buf, b.ops[b.OpId].String())
		case "rolls":
			buf = strconv.AppendInt(buf, b.Rolls, 10)
		case "stake":
			buf = strconv.AppendFloat(buf, b.ctx.Params.ConvertValue(b.Stake), 'f', dec, 64)
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
	dec := b.ctx.Params.Decimals
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
			res[i] = strconv.Quote(b.ctx.Indexer.LookupProposalHash(b.ctx, b.ProposalId).String())
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
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.SourceId).String())
		case "op_id":
			res[i] = strconv.FormatUint(b.OpId.Value(), 10)
		case "op":
			res[i] = strconv.Quote(b.ops[b.OpId].String())
		case "rolls":
			res[i] = strconv.FormatInt(b.Rolls, 10)
		case "stake":
			res[i] = strconv.FormatFloat(b.ctx.Params.ConvertValue(b.Stake), 'f', dec, 64)
		case "ballot":
			res[i] = strconv.Quote(b.Ballot.Ballot.String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamBallotTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}
	opT, err := ctx.Indexer.Table(index.OpTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", index.OpTableKey), err))
	}

	// translate long column names to short names used in pack tables
	var (
		needOpT  bool
		srcNames []string
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := ballotSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			if args.Verbose || v == "op" {
				needOpT = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = ballotAllAliases
		needOpT = true
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
		WithFields(srcNames...).
		WithLimit(int(args.Limit)).
		WithOrder(args.Order)
	accMap := make(map[model.AccountID]tezos.Address)
	opMap := make(map[model.OpID]tezos.OpHash)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		field := ballotSourceNames[prefix]
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
			q = q.And("I", cursorMode, id)
		case "ballot":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				ballot := tezos.ParseBallotVote(val[0])
				if !ballot.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid ballot vote '%s'", val[0]), nil))
				}
				q = q.And(field, mode, ballot)

			case pack.FilterModeIn, pack.FilterModeNotIn:
				ballots := make([]int64, 0)
				for _, v := range strings.Split(val[0], ",") {
					ballot := tezos.ParseBallotVote(v)
					if !ballot.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid ballot vote '%s'", v), nil))
					}
					ballots = append(ballots, int64(ballot))
				}
				q = q.And(field, mode, ballots)
			}
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
					q = q.And(field, mode, 0)
				} else {
					// single-address lookup and compile condition
					addr, err := tezos.ParseAddress(val[0])
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					// Note: when not found we insert an always false condition
					if acc == nil || acc.RowId == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						// keep for output
						accMap[acc.RowId] = acc.Address.Clone()
						q = q.And(field, mode, acc.RowId)
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
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					// skip not found account
					if acc == nil || acc.RowId == 0 {
						continue
					}
					// keep for output
					accMap[acc.RowId] = acc.Address.Clone()
					// collect list of account ids
					ids = append(ids, acc.RowId.Value())
				}
				// Note: when list is empty (no accounts were found, the match will
				//       always be false and return no result as expected)
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
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
					q = q.And(field, mode, 0)
				} else {
					// single-op lookup and compile condition
					op, err := ctx.Indexer.LookupOp(ctx, val[0], etl.ListRequest{})
					if err != nil {
						switch err {
						case index.ErrNoOpEntry:
							// expected
						case etl.ErrInvalidHash:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", val[0]), err))
						case index.ErrInvalidOpID:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op id '%s'", val[0]), err))
						default:
							panic(server.EInternal(server.EC_DATABASE, fmt.Sprintf("cannot lookup op id '%s'", val[0]), err))
						}
					}
					// Note: when not found we insert an always false condition
					if len(op) == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						opMap[op[0].RowId] = op[0].Hash.Clone()
						q = q.And(field, mode, op[0].RowId)
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-op lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					op, err := ctx.Indexer.LookupOp(ctx, v, etl.ListRequest{})
					if err != nil {
						switch err {
						case index.ErrNoOpEntry:
							// expected
						case etl.ErrInvalidHash:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op hash '%s'", v), err))
						case index.ErrInvalidOpID:
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid op id '%s'", val[0]), err))
						default:
							panic(server.EInternal(server.EC_DATABASE, fmt.Sprintf("cannot lookup op id '%s'", val[0]), err))
						}
					}
					// skip not found ops
					if len(op) == 0 {
						continue
					}
					// collect list of op ids (use first slice balue only since
					// we're looking for ballots which are always single-op)
					opMap[op[0].RowId] = op[0].Hash.Clone()
					ids = append(ids, op[0].RowId.Value())
				}
				// Note: when list is empty (no ops were found, the match will
				//       always be false and return no result as expected)
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := ballotSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				switch prefix {
				case "stake":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(ctx.Params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				case "voting_period_kind":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval := tezos.ParseVotingPeriod(vv)
						if !fval.IsValid() {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), nil))
						}
						fvals = append(fvals, strconv.Itoa(fval.Num()))
					}
					v = strings.Join(fvals, ",")
				}
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
	// 	ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

	// Step 1: query database
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "query failed", err))
	}
	// ctx.Log.Tracef("Processing result with %d rows %d cols", res.Rows(), res.Cols())
	defer res.Close()

	// Step 2: resolve ops using lookup (when requested)
	if needOpT && res.Rows() > 0 {
		// get a unique copy of op id column (clip on request limit)
		ucol, _ := res.Uint64Column("O")
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
			// ctx.Log.Tracef("Looking up %d ops", len(find))
			type XOp struct {
				Id   model.OpID   `pack:"I,pk"`
				Hash tezos.OpHash `pack:"H"`
			}
			op := &XOp{}
			err = pack.NewQuery(ctx.RequestID+".ballot_op_lookup").
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
				ctx.Log.Errorf("Op lookup failed: %w", err)
			}
		}
	}

	// prepare return type marshalling
	ballot := &Ballot{
		verbose: args.Verbose,
		columns: args.Columns,
		ctx:     ctx,
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
