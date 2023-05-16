// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	chainSourceNames map[string]string
	// all aliases as list
	chainAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Chain{})
	if err != nil {
		log.Fatalf("chain field type error: %v\n", err)
	}
	chainSourceNames = fields.NameMapReverse()
	chainAllAliases = fields.Aliases()
}

// configurable marshalling helper
type Chain struct {
	model.Chain
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *rpc.Params     // blockchain amount conversion
}

func (c *Chain) MarshalJSON() ([]byte, error) {
	if c.verbose {
		return c.MarshalJSONVerbose()
	} else {
		return c.MarshalJSONBrief()
	}
}

func (c *Chain) MarshalJSONVerbose() ([]byte, error) {
	ch := struct {
		RowId                uint64 `json:"row_id"`
		Height               int64  `json:"height"`
		Cycle                int64  `json:"cycle"`
		Timestamp            int64  `json:"time"`
		TotalAccounts        int64  `json:"total_accounts"`
		TotalContracts       int64  `json:"total_contracts"`
		TotalRollups         int64  `json:"total_rollups"`
		TotalOps             int64  `json:"total_ops"`
		TotalOpsFailed       int64  `json:"total_ops_failed"`
		TotalContractOps     int64  `json:"total_contract_ops"`
		TotalContractCalls   int64  `json:"total_contract_calls"`
		TotalRollupCalls     int64  `json:"total_rollup_calls"`
		TotalActivations     int64  `json:"total_activations"`
		TotalSeedNonces      int64  `json:"total_nonce_revelations"`
		TotalEndorsements    int64  `json:"total_endorsements"`
		TotalPreendorsements int64  `json:"total_preendorsements"`
		TotalDoubleBake      int64  `json:"total_double_bakings"`
		TotalDoubleEndorse   int64  `json:"total_double_endorsements"`
		TotalDelegations     int64  `json:"total_delegations"`
		TotalReveals         int64  `json:"total_reveals"`
		TotalOriginations    int64  `json:"total_originations"`
		TotalTransactions    int64  `json:"total_transactions"`
		TotalProposals       int64  `json:"total_proposals"`
		TotalBallots         int64  `json:"total_ballots"`
		TotalConstants       int64  `json:"total_constants"`
		TotalSetLimits       int64  `json:"total_set_limits"`
		TotalStorageBytes    int64  `json:"total_storage_bytes"`
		TotalTicketTransfers int64  `json:"total_ticket_transfers"`
		FundedAccounts       int64  `json:"funded_accounts"`
		DustAccounts         int64  `json:"dust_accounts"`
		GhostAccounts        int64  `json:"ghost_accounts"`
		UnclaimedAccounts    int64  `json:"unclaimed_accounts"`
		TotalDelegators      int64  `json:"total_delegators"`
		ActiveDelegators     int64  `json:"active_delegators"`
		InactiveDelegators   int64  `json:"inactive_delegators"`
		DustDelegators       int64  `json:"dust_delegators"`
		TotalBakers          int64  `json:"total_bakers"`
		ActiveBakers         int64  `json:"active_bakers"`
		InactiveBakers       int64  `json:"inactive_bakers"`
		ZeroBakers           int64  `json:"zero_bakers"`
		SelfBakers           int64  `json:"self_bakers"`
		SingleBakers         int64  `json:"single_bakers"`
		MultiBakers          int64  `json:"multi_bakers"`
		Rolls                int64  `json:"rolls"`
		RollOwners           int64  `json:"roll_owners"`
	}{
		RowId:                c.RowId,
		Height:               c.Height,
		Cycle:                c.Cycle,
		Timestamp:            util.UnixMilliNonZero(c.Timestamp),
		TotalAccounts:        c.TotalAccounts,
		TotalContracts:       c.TotalContracts,
		TotalRollups:         c.TotalRollups,
		TotalOps:             c.TotalOps,
		TotalOpsFailed:       c.TotalOpsFailed,
		TotalContractOps:     c.TotalContractOps,
		TotalContractCalls:   c.TotalContractCalls,
		TotalRollupCalls:     c.TotalRollupCalls,
		TotalActivations:     c.TotalActivations,
		TotalSeedNonces:      c.TotalSeedNonces,
		TotalEndorsements:    c.TotalEndorsements,
		TotalPreendorsements: c.TotalPreendorsements,
		TotalDoubleBake:      c.TotalDoubleBake,
		TotalDoubleEndorse:   c.TotalDoubleEndorse,
		TotalDelegations:     c.TotalDelegations,
		TotalReveals:         c.TotalReveals,
		TotalOriginations:    c.TotalOriginations,
		TotalTransactions:    c.TotalTransactions,
		TotalProposals:       c.TotalProposals,
		TotalBallots:         c.TotalBallots,
		TotalConstants:       c.TotalConstants,
		TotalSetLimits:       c.TotalSetLimits,
		TotalTicketTransfers: c.TotalTicketTransfers,
		TotalStorageBytes:    c.TotalStorageBytes,
		FundedAccounts:       c.FundedAccounts,
		DustAccounts:         c.DustAccounts,
		GhostAccounts:        c.GhostAccounts,
		UnclaimedAccounts:    c.UnclaimedAccounts,
		TotalDelegators:      c.TotalDelegators,
		DustDelegators:       c.DustDelegators,
		ActiveDelegators:     c.ActiveDelegators,
		InactiveDelegators:   c.InactiveDelegators,
		TotalBakers:          c.TotalBakers,
		ActiveBakers:         c.ActiveBakers,
		InactiveBakers:       c.InactiveBakers,
		ZeroBakers:           c.ZeroBakers,
		SelfBakers:           c.SelfBakers,
		SingleBakers:         c.SingleBakers,
		MultiBakers:          c.MultiBakers,
		Rolls:                c.Rolls,
		RollOwners:           c.RollOwners,
	}
	return json.Marshal(ch)
}

func (c *Chain) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range c.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, c.RowId, 10)
		case "height":
			buf = strconv.AppendInt(buf, c.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, c.Cycle, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(c.Timestamp), 10)
		case "total_accounts":
			buf = strconv.AppendInt(buf, c.TotalAccounts, 10)
		case "total_contracts":
			buf = strconv.AppendInt(buf, c.TotalContracts, 10)
		case "total_rollups":
			buf = strconv.AppendInt(buf, c.TotalRollups, 10)
		case "total_ops":
			buf = strconv.AppendInt(buf, c.TotalOps, 10)
		case "total_ops_failed":
			buf = strconv.AppendInt(buf, c.TotalOpsFailed, 10)
		case "total_contract_ops":
			buf = strconv.AppendInt(buf, c.TotalContractOps, 10)
		case "total_contract_calls":
			buf = strconv.AppendInt(buf, c.TotalContractCalls, 10)
		case "total_rollup_calls":
			buf = strconv.AppendInt(buf, c.TotalRollupCalls, 10)
		case "total_activations":
			buf = strconv.AppendInt(buf, c.TotalActivations, 10)
		case "total_nonce_revelations":
			buf = strconv.AppendInt(buf, c.TotalSeedNonces, 10)
		case "total_endorsements":
			buf = strconv.AppendInt(buf, c.TotalEndorsements, 10)
		case "total_preendorsements":
			buf = strconv.AppendInt(buf, c.TotalPreendorsements, 10)
		case "total_double_bakings":
			buf = strconv.AppendInt(buf, c.TotalDoubleBake, 10)
		case "total_double_endorsements":
			buf = strconv.AppendInt(buf, c.TotalDoubleEndorse, 10)
		case "total_delegations":
			buf = strconv.AppendInt(buf, c.TotalDelegations, 10)
		case "total_reveals":
			buf = strconv.AppendInt(buf, c.TotalReveals, 10)
		case "total_originations":
			buf = strconv.AppendInt(buf, c.TotalOriginations, 10)
		case "total_transactions":
			buf = strconv.AppendInt(buf, c.TotalTransactions, 10)
		case "total_proposals":
			buf = strconv.AppendInt(buf, c.TotalProposals, 10)
		case "total_ballots":
			buf = strconv.AppendInt(buf, c.TotalBallots, 10)
		case "total_constants":
			buf = strconv.AppendInt(buf, c.TotalConstants, 10)
		case "total_set_limits":
			buf = strconv.AppendInt(buf, c.TotalSetLimits, 10)
		case "total_ticket_transfers":
			buf = strconv.AppendInt(buf, c.TotalTicketTransfers, 10)
		case "total_storage_bytes":
			buf = strconv.AppendInt(buf, c.TotalStorageBytes, 10)
		case "funded_accounts":
			buf = strconv.AppendInt(buf, c.FundedAccounts, 10)
		case "dust_accounts":
			buf = strconv.AppendInt(buf, c.DustAccounts, 10)
		case "ghost_accounts":
			buf = strconv.AppendInt(buf, c.GhostAccounts, 10)
		case "unclaimed_accounts":
			buf = strconv.AppendInt(buf, c.UnclaimedAccounts, 10)
		case "total_delegators":
			buf = strconv.AppendInt(buf, c.TotalDelegators, 10)
		case "active_delegators":
			buf = strconv.AppendInt(buf, c.ActiveDelegators, 10)
		case "inactive_delegators":
			buf = strconv.AppendInt(buf, c.InactiveDelegators, 10)
		case "dust_delegators":
			buf = strconv.AppendInt(buf, c.DustDelegators, 10)
		case "total_bakers":
			buf = strconv.AppendInt(buf, c.TotalBakers, 10)
		case "active_bakers":
			buf = strconv.AppendInt(buf, c.ActiveBakers, 10)
		case "inactive_bakers":
			buf = strconv.AppendInt(buf, c.InactiveBakers, 10)
		case "zero_bakers":
			buf = strconv.AppendInt(buf, c.ZeroBakers, 10)
		case "self_bakers":
			buf = strconv.AppendInt(buf, c.SelfBakers, 10)
		case "single_bakers":
			buf = strconv.AppendInt(buf, c.SingleBakers, 10)
		case "multi_bakers":
			buf = strconv.AppendInt(buf, c.MultiBakers, 10)
		case "rolls":
			buf = strconv.AppendInt(buf, c.Rolls, 10)
		case "roll_owners":
			buf = strconv.AppendInt(buf, c.RollOwners, 10)
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

func (c *Chain) MarshalCSV() ([]string, error) {
	res := make([]string, len(c.columns))
	for i, v := range c.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(c.RowId, 10)
		case "height":
			res[i] = strconv.FormatInt(c.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(c.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(c.Timestamp.Format(time.RFC3339))
		case "total_accounts":
			res[i] = strconv.FormatInt(c.TotalAccounts, 10)
		case "total_contracts":
			res[i] = strconv.FormatInt(c.TotalContracts, 10)
		case "total_rollups":
			res[i] = strconv.FormatInt(c.TotalRollups, 10)
		case "total_ops":
			res[i] = strconv.FormatInt(c.TotalOps, 10)
		case "total_ops_failed":
			res[i] = strconv.FormatInt(c.TotalOpsFailed, 10)
		case "total_contract_ops":
			res[i] = strconv.FormatInt(c.TotalContractOps, 10)
		case "total_contract_calls":
			res[i] = strconv.FormatInt(c.TotalContractCalls, 10)
		case "total_rollup_calls":
			res[i] = strconv.FormatInt(c.TotalRollupCalls, 10)
		case "total_activations":
			res[i] = strconv.FormatInt(c.TotalActivations, 10)
		case "total_nonce_revelations":
			res[i] = strconv.FormatInt(c.TotalSeedNonces, 10)
		case "total_endorsements":
			res[i] = strconv.FormatInt(c.TotalEndorsements, 10)
		case "total_preendorsements":
			res[i] = strconv.FormatInt(c.TotalPreendorsements, 10)
		case "total_double_bakings":
			res[i] = strconv.FormatInt(c.TotalDoubleBake, 10)
		case "total_double_endorsements":
			res[i] = strconv.FormatInt(c.TotalDoubleEndorse, 10)
		case "total_delegations":
			res[i] = strconv.FormatInt(c.TotalDelegations, 10)
		case "total_reveals":
			res[i] = strconv.FormatInt(c.TotalReveals, 10)
		case "total_originations":
			res[i] = strconv.FormatInt(c.TotalOriginations, 10)
		case "total_transactions":
			res[i] = strconv.FormatInt(c.TotalTransactions, 10)
		case "total_proposals":
			res[i] = strconv.FormatInt(c.TotalProposals, 10)
		case "total_ballots":
			res[i] = strconv.FormatInt(c.TotalBallots, 10)
		case "total_constants":
			res[i] = strconv.FormatInt(c.TotalConstants, 10)
		case "total_set_limits":
			res[i] = strconv.FormatInt(c.TotalSetLimits, 10)
		case "total_ticket_transfers":
			res[i] = strconv.FormatInt(c.TotalTicketTransfers, 10)
		case "total_storage_bytes":
			res[i] = strconv.FormatInt(c.TotalStorageBytes, 10)
		case "funded_accounts":
			res[i] = strconv.FormatInt(c.FundedAccounts, 10)
		case "dust_accounts":
			res[i] = strconv.FormatInt(c.DustAccounts, 10)
		case "ghost_accounts":
			res[i] = strconv.FormatInt(c.GhostAccounts, 10)
		case "unclaimed_accounts":
			res[i] = strconv.FormatInt(c.UnclaimedAccounts, 10)
		case "total_delegators":
			res[i] = strconv.FormatInt(c.TotalDelegators, 10)
		case "dust_delegators":
			res[i] = strconv.FormatInt(c.DustDelegators, 10)
		case "active_delegators":
			res[i] = strconv.FormatInt(c.ActiveDelegators, 10)
		case "inactive_delegators":
			res[i] = strconv.FormatInt(c.InactiveDelegators, 10)
		case "total_bakers":
			res[i] = strconv.FormatInt(c.TotalBakers, 10)
		case "active_bakers":
			res[i] = strconv.FormatInt(c.ActiveBakers, 10)
		case "inactive_bakers":
			res[i] = strconv.FormatInt(c.InactiveBakers, 10)
		case "zero_bakers":
			res[i] = strconv.FormatInt(c.ZeroBakers, 10)
		case "self_bakers":
			res[i] = strconv.FormatInt(c.SelfBakers, 10)
		case "single_bakers":
			res[i] = strconv.FormatInt(c.SingleBakers, 10)
		case "multi_bakers":
			res[i] = strconv.FormatInt(c.MultiBakers, 10)
		case "rolls":
			res[i] = strconv.FormatInt(c.Rolls, 10)
		case "roll_owners":
			res[i] = strconv.FormatInt(c.RollOwners, 10)
		default:
			continue
		}
	}
	return res, nil
}

func StreamChainTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
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
			n, ok := chainSourceNames[v]
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
		args.Columns = chainAllAliases
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
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := chainSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
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
	ch := &Chain{
		verbose: args.Verbose,
		columns: args.Columns,
		params:  params,
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
			if err := r.Decode(ch); err != nil {
				return err
			}
			if err := enc.Encode(ch); err != nil {
				return err
			}
			count++
			lastId = ch.RowId
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
				if err := r.Decode(ch); err != nil {
					return err
				}
				if err := enc.EncodeRecord(ch); err != nil {
					return err
				}
				count++
				lastId = ch.RowId
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
