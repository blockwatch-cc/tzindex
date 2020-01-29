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
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// long -> short form
	electionSourceNames map[string]string
	// short -> long form
	electionAliasNames map[string]string
	// all aliases as list
	electionAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Election{})
	if err != nil {
		log.Fatalf("election field type error: %v\n", err)
	}
	electionSourceNames = fields.NameMapReverse()
	electionAllAliases = fields.Aliases()
	// add extra translations
	electionSourceNames["proposal"] = "P"
	electionSourceNames["last_voting_period"] = "n"
	electionAllAliases = append(electionAllAliases, "proposal")
	electionAllAliases = append(electionAllAliases, "last_voting_period")
}

// configurable marshalling helper
type Election struct {
	model.Election
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	ctx     *ApiContext     `csv:"-" pack:"-"`
}

func (e *Election) MarshalJSON() ([]byte, error) {
	if e.verbose {
		return e.MarshalJSONVerbose()
	} else {
		return e.MarshalJSONBrief()
	}
}

func (e *Election) MarshalJSONVerbose() ([]byte, error) {
	election := struct {
		RowId            uint64 `json:"row_id"`
		ProposalId       uint64 `json:"proposal_id"`
		Proposal         string `json:"proposal"`
		NumPeriods       int    `json:"num_periods"`
		NumProposals     int    `json:"num_proposals"`
		VotingPeriod     int64  `json:"voting_perid"`
		StartTime        int64  `json:"start_time"`
		EndTime          int64  `json:"end_time"`
		StartHeight      int64  `json:"start_height"`
		EndHeight        int64  `json:"end_height"`
		IsEmpty          bool   `json:"is_empty"`
		IsOpen           bool   `json:"is_open"`
		IsFailed         bool   `json:"is_failed"`
		NoQuorum         bool   `json:"no_quorum"`
		NoMajority       bool   `json:"no_majority"`
		NoProposal       bool   `json:"no_proposal"`
		VotingPeriodKind string `json:"last_voting_period"`
	}{
		RowId:            e.RowId.Value(),
		ProposalId:       e.ProposalId.Value(),
		Proposal:         govLookupProposalHash(e.ctx, e.ProposalId).String(),
		NumPeriods:       e.NumPeriods,
		NumProposals:     e.NumProposals,
		VotingPeriod:     e.VotingPeriod,
		StartTime:        util.UnixMilliNonZero(e.StartTime),
		EndTime:          util.UnixMilliNonZero(e.EndTime),
		StartHeight:      e.StartHeight,
		EndHeight:        e.EndHeight,
		IsEmpty:          e.IsEmpty,
		IsOpen:           e.IsOpen,
		IsFailed:         e.IsFailed,
		NoQuorum:         e.NoQuorum,
		NoMajority:       e.NoMajority,
		NoProposal:       e.NumProposals == 0,
		VotingPeriodKind: chain.ToVotingPeriod(e.NumPeriods).String(),
	}
	return json.Marshal(election)
}

func (e *Election) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range e.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, e.RowId.Value(), 10)
		case "proposal_id":
			buf = strconv.AppendUint(buf, e.ProposalId.Value(), 10)
		case "proposal":
			buf = strconv.AppendQuote(buf, govLookupProposalHash(e.ctx, e.ProposalId).String())
		case "num_periods":
			buf = strconv.AppendInt(buf, int64(e.NumPeriods), 10)
		case "num_proposals":
			buf = strconv.AppendInt(buf, int64(e.NumProposals), 10)
		case "voting_perid":
			buf = strconv.AppendInt(buf, e.VotingPeriod, 10)
		case "start_time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(e.StartTime), 10)
		case "end_time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(e.EndTime), 10)
		case "start_height":
			buf = strconv.AppendInt(buf, e.StartHeight, 10)
		case "end_height":
			buf = strconv.AppendInt(buf, e.EndHeight, 10)
		case "is_empty":
			if e.IsEmpty {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_open":
			if e.IsOpen {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_failed":
			if e.IsFailed {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "no_quorum":
			if e.NoQuorum {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "no_majority":
			if e.NoMajority {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "no_proposal":
			if e.NumProposals == 0 {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "last_voting_period":
			buf = strconv.AppendQuote(buf, chain.ToVotingPeriod(e.NumPeriods).String())
		default:
			continue
		}
		if i < len(e.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (e *Election) MarshalCSV() ([]string, error) {
	res := make([]string, len(e.columns))
	for i, v := range e.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(e.RowId.Value(), 10)
		case "proposal_id":
			res[i] = strconv.FormatUint(e.ProposalId.Value(), 10)
		case "proposal":
			res[i] = strconv.Quote(govLookupProposalHash(e.ctx, e.ProposalId).String())
		case "num_periods":
			res[i] = strconv.FormatInt(int64(e.NumPeriods), 10)
		case "num_proposals":
			res[i] = strconv.FormatInt(int64(e.NumProposals), 10)
		case "voting_perid":
			res[i] = strconv.FormatInt(e.VotingPeriod, 10)
		case "start_time":
			res[i] = strconv.Quote(e.StartTime.Format(time.RFC3339))
		case "end_time":
			res[i] = strconv.Quote(e.EndTime.Format(time.RFC3339))
		case "start_height":
			res[i] = strconv.FormatInt(e.StartHeight, 10)
		case "end_height":
			res[i] = strconv.FormatInt(e.EndHeight, 10)
		case "is_empty":
			res[i] = strconv.FormatBool(e.IsEmpty)
		case "is_open":
			res[i] = strconv.FormatBool(e.IsOpen)
		case "is_failed":
			res[i] = strconv.FormatBool(e.IsFailed)
		case "no_quorum":
			res[i] = strconv.FormatBool(e.NoQuorum)
		case "no_majority":
			res[i] = strconv.FormatBool(e.NoMajority)
		case "no_proposal":
			res[i] = strconv.FormatBool(e.NumProposals == 0)
		case "last_voting_period":
			res[i] = strconv.Quote(chain.ToVotingPeriod(e.NumPeriods).String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamElectionTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
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
			n, ok := electionSourceNames[v]
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
		args.Columns = electionAllAliases
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
		case "proposal":
			// parse proposal hash and lookup id
			// valid filter modes: eq, in
			// 1 resolve proposal_id from account table
			// 2 add eq/in cond: account_id
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty hash matches id 0 (== missing proposal)
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find("P"), // proposal id
						Mode:  mode,
						Value: uint64(0),
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-proposal lookup and compile condition
					h, err := chain.ParseProtocolHash(val[0])
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", val[0]), err))
					}
					prop, err := ctx.Indexer.LookupProposal(ctx, h)
					if err != nil && err != index.ErrNoProposalEntry {
						panic(err)
					}
					// Note: when not found we insert an always false condition
					if prop == nil || prop.RowId == 0 {
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find("P"), // proposal id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "proposal not found", // debugging aid
						})
					} else {
						// add proto id as extra condition
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find("P"), // proposal id
							Mode:  mode,
							Value: prop.RowId.Value(),
							Raw:   val[0], // debugging aid
						})
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-proposal lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					h, err := chain.ParseProtocolHash(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", v), err))
					}
					prop, err := ctx.Indexer.LookupProposal(ctx, h)
					if err != nil && err != index.ErrNoProposalEntry {
						panic(err)
					}
					// skip not found proposal
					if prop == nil || prop.RowId == 0 {
						continue
					}
					// collect list of proposal ids
					ids = append(ids, prop.RowId.Value())
				}
				// Note: when list is empty (no proposal was found, the match will
				//       always be false and return no result as expected)
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("P"), // proposal id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "last_voting_period":
			// parse only the first value
			period := chain.ParseVotingPeriod(val[0])
			if !period.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid voting period '%s'", val[0]), nil))
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Find("n"), // num periods
				Mode:  mode,
				Value: int64(period.Num()),
				Raw:   val[0], // debugging aid
			})
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := electionSourceNames[prefix]; !ok {
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
	election := &Election{
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
			if err := r.Decode(election); err != nil {
				return err
			}
			if err := enc.Encode(election); err != nil {
				return err
			}
			count++
			lastId = election.RowId.Value()
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
				if err := r.Decode(election); err != nil {
					return err
				}
				if err := enc.EncodeRecord(election); err != nil {
					return err
				}
				count++
				lastId = election.RowId.Value()
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
