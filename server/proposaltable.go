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
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// long -> short form
	proposalSourceNames map[string]string
	// short -> long form
	proposalAliasNames map[string]string
	// all aliases as list
	proposalAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Proposal{})
	if err != nil {
		log.Fatalf("proposal field type error: %v\n", err)
	}
	proposalSourceNames = fields.NameMapReverse()
	proposalAllAliases = fields.Aliases()

	// add extra translations
	proposalSourceNames["source"] = "S"
	proposalSourceNames["op"] = "O"

	proposalAllAliases = append(proposalAllAliases, "source")
	proposalAllAliases = append(proposalAllAliases, "op")
}

// configurable marshalling helper
type Proposal struct {
	model.Proposal
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	ctx     *ApiContext     `csv:"-" pack:"-"` // blockchain amount conversion
}

func (p *Proposal) MarshalJSON() ([]byte, error) {
	if p.verbose {
		return p.MarshalJSONVerbose()
	} else {
		return p.MarshalJSONBrief()
	}
}

func (p *Proposal) MarshalJSONVerbose() ([]byte, error) {
	proposal := struct {
		RowId        uint64 `json:"row_id"`
		Hash         string `json:"hash"`
		Height       int64  `json:"height"`
		Timestamp    int64  `json:"time"`
		SourceId     uint64 `json:"source_id"`
		Source       string `json:"source"`
		OpId         uint64 `json:"op_id"`
		Op           string `json:"op"`
		ElectionId   uint64 `json:"election_id"`
		VotingPeriod int64  `json:"voting_period"`
		Rolls        int64  `json:"rolls"`
		Voters       int64  `json:"voters"`
	}{
		RowId:        p.RowId.Value(),
		Hash:         p.Hash.String(),
		Height:       p.Height,
		Timestamp:    util.UnixMilliNonZero(p.Time),
		SourceId:     p.SourceId.Value(),
		Source:       lookupAddress(p.ctx, p.SourceId).String(),
		OpId:         p.OpId.Value(),
		Op:           govLookupOpHash(p.ctx, p.OpId).String(),
		ElectionId:   p.ElectionId.Value(),
		VotingPeriod: p.VotingPeriod,
		Rolls:        p.Rolls,
		Voters:       p.Voters,
	}
	return json.Marshal(proposal)
}

func (p *Proposal) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range p.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, p.RowId.Value(), 10)
		case "hash":
			buf = strconv.AppendQuote(buf, p.Hash.String())
		case "height":
			buf = strconv.AppendInt(buf, p.Height, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(p.Time), 10)
		case "source_id":
			buf = strconv.AppendUint(buf, p.SourceId.Value(), 10)
		case "source":
			buf = strconv.AppendQuote(buf, lookupAddress(p.ctx, p.SourceId).String())
		case "op_id":
			buf = strconv.AppendUint(buf, p.OpId.Value(), 10)
		case "op":
			buf = strconv.AppendQuote(buf, govLookupOpHash(p.ctx, p.OpId).String())
		case "election_id":
			buf = strconv.AppendUint(buf, p.ElectionId.Value(), 10)
		case "voting_period":
			buf = strconv.AppendInt(buf, p.VotingPeriod, 10)
		case "rolls":
			buf = strconv.AppendInt(buf, p.Rolls, 10)
		case "voters":
			buf = strconv.AppendInt(buf, p.Voters, 10)
		default:
			continue
		}
		if i < len(p.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (p *Proposal) MarshalCSV() ([]string, error) {
	res := make([]string, len(p.columns))
	for i, v := range p.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(p.RowId.Value(), 10)
		case "hash":
			res[i] = strconv.Quote(p.Hash.String())
		case "height":
			res[i] = strconv.FormatInt(p.Height, 10)
		case "time":
			res[i] = strconv.Quote(p.Time.Format(time.RFC3339))
		case "source_id":
			res[i] = strconv.FormatUint(p.SourceId.Value(), 10)
		case "source":
			res[i] = strconv.Quote(lookupAddress(p.ctx, p.SourceId).String())
		case "op_id":
			res[i] = strconv.FormatUint(p.OpId.Value(), 10)
		case "op":
			res[i] = strconv.Quote(govLookupOpHash(p.ctx, p.OpId).String())
		case "election_id":
			res[i] = strconv.FormatUint(p.ElectionId.Value(), 10)
		case "voting_period":
			res[i] = strconv.FormatInt(p.VotingPeriod, 10)
		case "rolls":
			res[i] = strconv.FormatInt(p.Rolls, 10)
		case "voters":
			res[i] = strconv.FormatInt(p.Voters, 10)
		default:
			continue
		}
	}
	return res, nil
}

func StreamProposalTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := proposalSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = proposalAllAliases
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
		case "hash":
			// special hash type to []byte conversion
			hashes := make([][]byte, len(val))
			for i, v := range val {
				h, err := chain.ParseProtocolHash(v)
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", val), err))
				}
				hashes[i] = h.Hash.Hash
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Find("H"),
				Mode:  pack.FilterModeIn,
				Value: hashes,
				Raw:   strings.Join(val, ","), // debugging aid
			})
		case "source":
			// parse source/baker address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
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
					// we're looking for proposals which are always single-op)
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
			if short, ok := proposalSourceNames[prefix]; !ok {
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

	// prepare return type marshalling
	proposal := &Proposal{
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
				ctx.Log.Errorf("%v", e)
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
			if err := r.Decode(proposal); err != nil {
				return err
			}
			if err := enc.Encode(proposal); err != nil {
				return err
			}
			count++
			lastId = proposal.RowId.Value()
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
			err = table.Stream(ctx, q, func(r pack.Row) error {
				if err := r.Decode(proposal); err != nil {
					return err
				}
				if err := enc.EncodeRecord(proposal); err != nil {
					return err
				}
				count++
				lastId = proposal.RowId.Value()
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
