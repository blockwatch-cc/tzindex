// Copyright (c) 2020-2024 Blockwatch Data Inc.
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
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	voteSourceNames map[string]string
	// all aliases as list
	voteAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Vote{})
	if err != nil {
		log.Fatalf("vote field type error: %v\n", err)
	}
	voteSourceNames = fields.NameMapReverse()
	voteAllAliases = fields.Aliases()
	// add extra translations
	voteSourceNames["proposal"] = "P"
	voteAllAliases = append(voteAllAliases, "proposal")
}

// configurable marshalling helper
type Vote struct {
	model.Vote
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	ctx     *server.Context
}

func (v *Vote) MarshalJSON() ([]byte, error) {
	if v.verbose {
		return v.MarshalJSONVerbose()
	} else {
		return v.MarshalJSONBrief()
	}
}

func (v *Vote) MarshalJSONVerbose() ([]byte, error) {
	election := struct {
		RowId            uint64  `json:"row_id"`
		ElectionId       uint64  `json:"election_id"`
		ProposalId       uint64  `json:"proposal_id"`
		Proposal         string  `json:"proposal"`
		VotingPeriod     int64   `json:"voting_period"`
		VotingPeriodKind string  `json:"voting_period_kind"`
		StartTime        int64   `json:"period_start_time"`
		EndTime          int64   `json:"period_end_time"`
		StartHeight      int64   `json:"period_start_block"`
		EndHeight        int64   `json:"period_end_block"`
		EligibleStake    float64 `json:"eligible_stake"`
		EligibleVoters   int64   `json:"eligible_voters"`
		QuorumPct        int64   `json:"quorum_pct"`
		QuorumStake      float64 `json:"quorum_stake"`
		TurnoutStake     float64 `json:"turnout_stake"`
		TurnoutVoters    int64   `json:"turnout_voters"`
		TurnoutPct       int64   `json:"turnout_pct"`
		TurnoutEma       int64   `json:"turnout_ema"`
		YayStake         float64 `json:"yay_stake"`
		YayVoters        int64   `json:"yay_voters"`
		NayStake         float64 `json:"nay_stake"`
		NayVoters        int64   `json:"nay_voters"`
		PassStake        float64 `json:"pass_stake"`
		PassVoters       int64   `json:"pass_voters"`
		IsOpen           bool    `json:"is_open"`
		IsFailed         bool    `json:"is_failed"`
		IsDraw           bool    `json:"is_draw"`
		NoProposal       bool    `json:"no_proposal"`
		NoQuorum         bool    `json:"no_quorum"`
		NoMajority       bool    `json:"no_majority"`
	}{
		RowId:            v.RowId,
		ElectionId:       v.ElectionId.U64(),
		ProposalId:       v.ProposalId.U64(),
		Proposal:         v.ctx.Indexer.LookupProposalHash(v.ctx, v.ProposalId).String(),
		VotingPeriod:     v.VotingPeriod,
		VotingPeriodKind: v.VotingPeriodKind.String(),
		StartTime:        util.UnixMilliNonZero(v.StartTime),
		EndTime:          util.UnixMilliNonZero(v.EndTime),
		StartHeight:      v.StartHeight,
		EndHeight:        v.EndHeight,
		EligibleStake:    v.ctx.Params.ConvertValue(v.EligibleStake),
		EligibleVoters:   v.EligibleVoters,
		QuorumPct:        v.QuorumPct,
		QuorumStake:      v.ctx.Params.ConvertValue(v.QuorumStake),
		TurnoutStake:     v.ctx.Params.ConvertValue(v.TurnoutStake),
		TurnoutVoters:    v.TurnoutVoters,
		TurnoutPct:       v.TurnoutPct,
		TurnoutEma:       v.TurnoutEma,
		YayStake:         v.ctx.Params.ConvertValue(v.YayStake),
		YayVoters:        v.YayVoters,
		NayStake:         v.ctx.Params.ConvertValue(v.NayStake),
		NayVoters:        v.NayVoters,
		PassStake:        v.ctx.Params.ConvertValue(v.PassStake),
		PassVoters:       v.PassVoters,
		IsOpen:           v.IsOpen,
		IsFailed:         v.IsFailed,
		IsDraw:           v.IsDraw,
		NoProposal:       v.NoProposal,
		NoQuorum:         v.NoQuorum,
		NoMajority:       v.NoMajority,
	}
	return json.Marshal(election)
}

func (v *Vote) MarshalJSONBrief() ([]byte, error) {
	dec := v.ctx.Params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	p := v.ctx.Params
	for i, n := range v.columns {
		switch n {
		case "row_id":
			buf = strconv.AppendUint(buf, v.RowId, 10)
		case "election_id":
			buf = strconv.AppendUint(buf, v.ElectionId.U64(), 10)
		case "proposal_id":
			buf = strconv.AppendUint(buf, v.ProposalId.U64(), 10)
		case "proposal":
			buf = strconv.AppendQuote(buf, v.ctx.Indexer.LookupProposalHash(v.ctx, v.ProposalId).String())
		case "voting_period":
			buf = strconv.AppendInt(buf, v.VotingPeriod, 10)
		case "voting_period_kind":
			buf = strconv.AppendQuote(buf, v.VotingPeriodKind.String())
		case "period_start_time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(v.StartTime), 10)
		case "period_end_time":
			if v.IsOpen {
				diff := p.NumVotingPeriods*p.BlocksPerVotingPeriod - (v.ctx.Tip.BestHeight - v.StartHeight)
				endTime := v.StartTime.Add(time.Duration(diff) * p.BlockTime())
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(endTime), 10)
			} else {
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(v.EndTime), 10)
			}
		case "period_start_height":
			buf = strconv.AppendInt(buf, v.StartHeight, 10)
		case "period_end_height":
			if v.IsOpen {
				endHeight := v.StartHeight + p.NumVotingPeriods*p.BlocksPerVotingPeriod - 1
				buf = strconv.AppendInt(buf, endHeight, 10)
			} else {
				buf = strconv.AppendInt(buf, v.EndHeight, 10)
			}
		case "eligible_stake":
			buf = strconv.AppendFloat(buf, v.ctx.Params.ConvertValue(v.EligibleStake), 'f', dec, 64)
		case "eligible_voters":
			buf = strconv.AppendInt(buf, v.EligibleVoters, 10)
		case "quorum_pct":
			buf = strconv.AppendInt(buf, v.QuorumPct, 10)
		case "quorum_stake":
			buf = strconv.AppendFloat(buf, v.ctx.Params.ConvertValue(v.QuorumStake), 'f', dec, 64)
		case "turnout_stake":
			buf = strconv.AppendFloat(buf, v.ctx.Params.ConvertValue(v.TurnoutStake), 'f', dec, 64)
		case "turnout_voters":
			buf = strconv.AppendInt(buf, v.TurnoutVoters, 10)
		case "turnout_pct":
			buf = strconv.AppendInt(buf, v.TurnoutPct, 10)
		case "turnout_ema":
			buf = strconv.AppendInt(buf, v.TurnoutEma, 10)
		case "yay_stake":
			buf = strconv.AppendFloat(buf, v.ctx.Params.ConvertValue(v.YayStake), 'f', dec, 64)
		case "yay_voters":
			buf = strconv.AppendInt(buf, v.YayVoters, 10)
		case "nay_stake":
			buf = strconv.AppendFloat(buf, v.ctx.Params.ConvertValue(v.NayStake), 'f', dec, 64)
		case "nay_voters":
			buf = strconv.AppendInt(buf, v.NayVoters, 10)
		case "pass_stake":
			buf = strconv.AppendFloat(buf, v.ctx.Params.ConvertValue(v.PassStake), 'f', dec, 64)
		case "pass_voters":
			buf = strconv.AppendInt(buf, v.PassVoters, 10)
		case "is_open":
			if v.IsOpen {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_failed":
			if v.IsFailed {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_draw":
			if v.IsDraw {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "no_proposal":
			if v.NoProposal {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "no_quorum":
			if v.NoQuorum {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "no_majority":
			if v.NoMajority {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		default:
			continue
		}
		if i < len(v.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (v *Vote) MarshalCSV() ([]string, error) {
	dec := v.ctx.Params.Decimals
	res := make([]string, len(v.columns))
	p := v.ctx.Params
	for i, n := range v.columns {
		switch n {
		case "row_id":
			res[i] = strconv.FormatUint(v.RowId, 10)
		case "election_id":
			res[i] = strconv.FormatUint(v.ElectionId.U64(), 10)
		case "proposal_id":
			res[i] = strconv.FormatUint(v.ProposalId.U64(), 10)
		case "proposal":
			res[i] = strconv.Quote(v.ctx.Indexer.LookupProposalHash(v.ctx, v.ProposalId).String())
		case "voting_period":
			res[i] = strconv.FormatInt(v.VotingPeriod, 10)
		case "voting_period_kind":
			res[i] = strconv.Quote(v.VotingPeriodKind.String())
		case "period_start_time":
			res[i] = strconv.Quote(v.StartTime.Format(time.RFC3339))
		case "period_end_time":
			if v.IsOpen {
				diff := p.NumVotingPeriods*p.BlocksPerVotingPeriod - (v.ctx.Tip.BestHeight - v.StartHeight)
				endTime := v.StartTime.Add(time.Duration(diff) * p.BlockTime())
				res[i] = strconv.Quote(endTime.Format(time.RFC3339))
			} else {
				res[i] = strconv.Quote(v.EndTime.Format(time.RFC3339))
			}
		case "period_start_height":
			res[i] = strconv.FormatInt(v.StartHeight, 10)
		case "period_end_height":
			if v.IsOpen {
				endHeight := v.StartHeight + p.NumVotingPeriods*p.BlocksPerVotingPeriod - 1
				res[i] = strconv.FormatInt(endHeight, 10)
			} else {
				res[i] = strconv.FormatInt(v.EndHeight, 10)
			}
		case "eligible_stake":
			res[i] = strconv.FormatFloat(v.ctx.Params.ConvertValue(v.EligibleStake), 'f', dec, 64)
		case "eligible_voters":
			res[i] = strconv.FormatInt(v.EligibleVoters, 10)
		case "quorum_pct":
			res[i] = strconv.FormatInt(v.QuorumPct, 10)
		case "quorum_stake":
			res[i] = strconv.FormatFloat(v.ctx.Params.ConvertValue(v.QuorumStake), 'f', dec, 64)
		case "turnout_stake":
			res[i] = strconv.FormatFloat(v.ctx.Params.ConvertValue(v.TurnoutStake), 'f', dec, 64)
		case "turnout_voters":
			res[i] = strconv.FormatInt(v.TurnoutVoters, 10)
		case "turnout_pct":
			res[i] = strconv.FormatInt(v.TurnoutPct, 10)
		case "turnout_ema":
			res[i] = strconv.FormatInt(v.TurnoutEma, 10)
		case "yay_stake":
			res[i] = strconv.FormatFloat(v.ctx.Params.ConvertValue(v.YayStake), 'f', dec, 64)
		case "yay_voters":
			res[i] = strconv.FormatInt(v.YayVoters, 10)
		case "nay_stake":
			res[i] = strconv.FormatFloat(v.ctx.Params.ConvertValue(v.NayStake), 'f', dec, 64)
		case "nay_voters":
			res[i] = strconv.FormatInt(v.NayVoters, 10)
		case "pass_stake":
			res[i] = strconv.FormatFloat(v.ctx.Params.ConvertValue(v.PassStake), 'f', dec, 64)
		case "pass_voters":
			res[i] = strconv.FormatInt(v.PassVoters, 10)
		case "is_open":
			res[i] = strconv.FormatBool(v.IsOpen)
		case "is_failed":
			res[i] = strconv.FormatBool(v.IsFailed)
		case "is_draw":
			res[i] = strconv.FormatBool(v.IsDraw)
		case "no_proposal":
			res[i] = strconv.FormatBool(v.NoProposal)
		case "no_quorum":
			res[i] = strconv.FormatBool(v.NoQuorum)
		case "no_majority":
			res[i] = strconv.FormatBool(v.NoMajority)
		default:
			continue
		}
	}
	return res, nil
}

func StreamVoteTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
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
			n, ok := voteSourceNames[v]
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
		args.Columns = voteAllAliases
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
		WithFields(srcNames...).
		WithLimit(int(args.Limit)).
		WithOrder(args.Order)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		field := voteSourceNames[prefix]
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
		case "proposal":
			// parse proposal hash and lookup id
			// valid filter modes: eq, in
			// 1 resolve proposal_id from account table
			// 2 add eq/in cond: account_id
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty hash matches id 0 (== missing proposal)
					q = q.And(field, mode, 0)
				} else {
					// single-proposal lookup and compile condition
					h, err := tezos.ParseProtocolHash(val[0])
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", val[0]), err))
					}
					prop, err := ctx.Indexer.LookupProposal(ctx, h)
					if err != nil && err != model.ErrNoProposal {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", val[0]), err))
					}
					// Note: when not found we insert an always false condition
					if prop == nil || prop.RowId == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						// add proto id as extra condition
						q = q.And(field, mode, prop.RowId)
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-proposal lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					h, err := tezos.ParseProtocolHash(v)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", v), err))
					}
					prop, err := ctx.Indexer.LookupProposal(ctx, h)
					if err != nil && err != model.ErrNoProposal {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid protocol hash '%s'", v), err))
					}
					// skip not found proposal
					if prop == nil || prop.RowId == 0 {
						continue
					}
					// collect list of proposal ids
					ids = append(ids, prop.RowId.U64())
				}
				// Note: when list is empty (no proposal was found, the match will
				//       always be false and return no result as expected)
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := voteSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				switch prefix {
				case "eligible_stake", "quorum_stake", "turnout_stake",
					"yay_stake", "nay_stake", "pass_stake":
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

	// prepare return type marshalling
	vote := &Vote{
		verbose: args.Verbose,
		columns: args.Columns,
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
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(vote); err != nil {
				return err
			}
			if err := enc.Encode(vote); err != nil {
				return err
			}
			count++
			lastId = vote.RowId
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
			err = table.Stream(ctx, q, func(r pack.Row) error {
				if err := r.Decode(vote); err != nil {
					return err
				}
				if err := enc.EncodeRecord(vote); err != nil {
					return err
				}
				count++
				lastId = vote.RowId
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
