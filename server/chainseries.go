// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// all series field names as list
	chainSeriesNames util.StringList
)

func init() {
	fields, err := pack.Fields(&model.Chain{})
	if err != nil {
		log.Fatalf("chain field type error: %v\n", err)
	}
	// strip row_id (first field)
	chainSeriesNames = util.StringList(fields.Aliases()[1:])
}

// configurable marshalling helper
type ChainSeries struct {
	model.Chain
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"` // blockchain amount conversion
}

func (c *ChainSeries) Reset() {
	c.Chain.Timestamp = time.Time{}
}

func (c *ChainSeries) MarshalJSON() ([]byte, error) {
	if c.verbose {
		return c.MarshalJSONVerbose()
	} else {
		return c.MarshalJSONBrief()
	}
}

func (c *ChainSeries) MarshalJSONVerbose() ([]byte, error) {
	ch := struct {
		Height             int64 `json:"height"`
		Cycle              int64 `json:"cycle"`
		Timestamp          int64 `json:"time"`
		TotalAccounts      int64 `json:"total_accounts"`
		TotalImplicit      int64 `json:"total_implicit"`
		TotalManaged       int64 `json:"total_managed"`
		TotalContracts     int64 `json:"total_contracts"`
		TotalOps           int64 `json:"total_ops"`
		TotalContractOps   int64 `json:"total_contract_ops"`
		TotalActivations   int64 `json:"total_activations"`
		TotalSeedNonces    int64 `json:"total_seed_nonce_revelations"`
		TotalEndorsements  int64 `json:"total_endorsements"`
		TotalDoubleBake    int64 `json:"total_double_baking_evidences"`
		TotalDoubleEndorse int64 `json:"total_double_endorsement_evidences"`
		TotalDelegations   int64 `json:"total_delegations"`
		TotalReveals       int64 `json:"total_reveals"`
		TotalOriginations  int64 `json:"total_originations"`
		TotalTransactions  int64 `json:"total_transactions"`
		TotalProposals     int64 `json:"total_proposals"`
		TotalBallots       int64 `json:"total_ballots"`
		TotalStorageBytes  int64 `json:"total_storage_bytes"`
		TotalPaidBytes     int64 `json:"total_paid_bytes"`
		TotalUsedBytes     int64 `json:"total_used_bytes"`
		TotalOrphans       int64 `json:"total_orphans"`
		FundedAccounts     int64 `json:"funded_accounts"`
		UnclaimedAccounts  int64 `json:"unclaimed_accounts"`
		TotalDelegators    int64 `json:"total_delegators"`
		ActiveDelegators   int64 `json:"active_delegators"`
		InactiveDelegators int64 `json:"inactive_delegators"`
		TotalDelegates     int64 `json:"total_delegates"`
		ActiveDelegates    int64 `json:"active_delegates"`
		InactiveDelegates  int64 `json:"inactive_delegates"`
		ZeroDelegates      int64 `json:"zero_delegates"`
		SelfDelegates      int64 `json:"self_delegates"`
		SingleDelegates    int64 `json:"single_delegates"`
		MultiDelegates     int64 `json:"multi_delegates"`
		Rolls              int64 `json:"rolls"`
		RollOwners         int64 `json:"roll_owners"`
	}{
		Height:             c.Height,
		Cycle:              c.Cycle,
		Timestamp:          util.UnixMilliNonZero(c.Timestamp),
		TotalAccounts:      c.TotalAccounts,
		TotalImplicit:      c.TotalImplicit,
		TotalManaged:       c.TotalManaged,
		TotalContracts:     c.TotalContracts,
		TotalOps:           c.TotalOps,
		TotalContractOps:   c.TotalContractOps,
		TotalActivations:   c.TotalActivations,
		TotalSeedNonces:    c.TotalSeedNonces,
		TotalEndorsements:  c.TotalEndorsements,
		TotalDoubleBake:    c.TotalDoubleBake,
		TotalDoubleEndorse: c.TotalDoubleEndorse,
		TotalDelegations:   c.TotalDelegations,
		TotalReveals:       c.TotalReveals,
		TotalOriginations:  c.TotalOriginations,
		TotalTransactions:  c.TotalTransactions,
		TotalProposals:     c.TotalProposals,
		TotalBallots:       c.TotalBallots,
		TotalStorageBytes:  c.TotalStorageBytes,
		TotalPaidBytes:     c.TotalPaidBytes,
		TotalUsedBytes:     c.TotalUsedBytes,
		TotalOrphans:       c.TotalOrphans,
		FundedAccounts:     c.FundedAccounts,
		UnclaimedAccounts:  c.UnclaimedAccounts,
		TotalDelegators:    c.TotalDelegators,
		ActiveDelegators:   c.ActiveDelegators,
		InactiveDelegators: c.InactiveDelegators,
		TotalDelegates:     c.TotalDelegates,
		ActiveDelegates:    c.ActiveDelegates,
		InactiveDelegates:  c.InactiveDelegates,
		ZeroDelegates:      c.ZeroDelegates,
		SelfDelegates:      c.SelfDelegates,
		SingleDelegates:    c.SingleDelegates,
		MultiDelegates:     c.MultiDelegates,
		Rolls:              c.Rolls,
		RollOwners:         c.RollOwners,
	}
	return json.Marshal(ch)
}

func (c *ChainSeries) MarshalJSONBrief() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range c.columns {
		switch v {
		case "height":
			buf = strconv.AppendInt(buf, c.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, c.Cycle, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(c.Timestamp), 10)
		case "total_accounts":
			buf = strconv.AppendInt(buf, c.TotalAccounts, 10)
		case "total_implicit":
			buf = strconv.AppendInt(buf, c.TotalImplicit, 10)
		case "total_managed":
			buf = strconv.AppendInt(buf, c.TotalManaged, 10)
		case "total_contracts":
			buf = strconv.AppendInt(buf, c.TotalContracts, 10)
		case "total_ops":
			buf = strconv.AppendInt(buf, c.TotalOps, 10)
		case "total_contract_ops":
			buf = strconv.AppendInt(buf, c.TotalContractOps, 10)
		case "total_activations":
			buf = strconv.AppendInt(buf, c.TotalActivations, 10)
		case "total_seed_nonce_revelations":
			buf = strconv.AppendInt(buf, c.TotalSeedNonces, 10)
		case "total_endorsements":
			buf = strconv.AppendInt(buf, c.TotalEndorsements, 10)
		case "total_double_baking_evidences":
			buf = strconv.AppendInt(buf, c.TotalDoubleBake, 10)
		case "total_double_endorsement_evidences":
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
		case "total_storage_bytes":
			buf = strconv.AppendInt(buf, c.TotalStorageBytes, 10)
		case "total_paid_bytes":
			buf = strconv.AppendInt(buf, c.TotalPaidBytes, 10)
		case "total_used_bytes":
			buf = strconv.AppendInt(buf, c.TotalUsedBytes, 10)
		case "total_orphans":
			buf = strconv.AppendInt(buf, c.TotalOrphans, 10)
		case "funded_accounts":
			buf = strconv.AppendInt(buf, c.FundedAccounts, 10)
		case "unclaimed_accounts":
			buf = strconv.AppendInt(buf, c.UnclaimedAccounts, 10)
		case "total_delegators":
			buf = strconv.AppendInt(buf, c.TotalDelegators, 10)
		case "active_delegators":
			buf = strconv.AppendInt(buf, c.ActiveDelegators, 10)
		case "inactive_delegators":
			buf = strconv.AppendInt(buf, c.InactiveDelegators, 10)
		case "total_delegates":
			buf = strconv.AppendInt(buf, c.TotalDelegates, 10)
		case "active_delegates":
			buf = strconv.AppendInt(buf, c.ActiveDelegates, 10)
		case "inactive_delegates":
			buf = strconv.AppendInt(buf, c.InactiveDelegates, 10)
		case "zero_delegates":
			buf = strconv.AppendInt(buf, c.ZeroDelegates, 10)
		case "self_delegates":
			buf = strconv.AppendInt(buf, c.SelfDelegates, 10)
		case "single_delegates":
			buf = strconv.AppendInt(buf, c.SingleDelegates, 10)
		case "multi_delegates":
			buf = strconv.AppendInt(buf, c.MultiDelegates, 10)
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

func (c *ChainSeries) MarshalCSV() ([]string, error) {
	res := make([]string, len(c.columns))
	for i, v := range c.columns {
		switch v {
		case "height":
			res[i] = strconv.FormatInt(c.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(c.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(c.Timestamp.Format(time.RFC3339))
		case "total_accounts":
			res[i] = strconv.FormatInt(c.TotalAccounts, 10)
		case "total_implicit":
			res[i] = strconv.FormatInt(c.TotalImplicit, 10)
		case "total_managed":
			res[i] = strconv.FormatInt(c.TotalManaged, 10)
		case "total_contracts":
			res[i] = strconv.FormatInt(c.TotalContracts, 10)
		case "total_ops":
			res[i] = strconv.FormatInt(c.TotalOps, 10)
		case "total_contract_ops":
			res[i] = strconv.FormatInt(c.TotalContractOps, 10)
		case "total_activations":
			res[i] = strconv.FormatInt(c.TotalActivations, 10)
		case "total_seed_nonce_revelations":
			res[i] = strconv.FormatInt(c.TotalSeedNonces, 10)
		case "total_endorsements":
			res[i] = strconv.FormatInt(c.TotalEndorsements, 10)
		case "total_double_baking_evidences":
			res[i] = strconv.FormatInt(c.TotalDoubleBake, 10)
		case "total_double_endorsement_evidences":
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
		case "total_storage_bytes":
			res[i] = strconv.FormatInt(c.TotalStorageBytes, 10)
		case "total_paid_bytes":
			res[i] = strconv.FormatInt(c.TotalPaidBytes, 10)
		case "total_used_bytes":
			res[i] = strconv.FormatInt(c.TotalUsedBytes, 10)
		case "total_orphans":
			res[i] = strconv.FormatInt(c.TotalOrphans, 10)
		case "funded_accounts":
			res[i] = strconv.FormatInt(c.FundedAccounts, 10)
		case "unclaimed_accounts":
			res[i] = strconv.FormatInt(c.UnclaimedAccounts, 10)
		case "total_delegators":
			res[i] = strconv.FormatInt(c.TotalDelegators, 10)
		case "active_delegators":
			res[i] = strconv.FormatInt(c.ActiveDelegators, 10)
		case "inactive_delegators":
			res[i] = strconv.FormatInt(c.InactiveDelegators, 10)
		case "total_delegates":
			res[i] = strconv.FormatInt(c.TotalDelegates, 10)
		case "active_delegates":
			res[i] = strconv.FormatInt(c.ActiveDelegates, 10)
		case "inactive_delegates":
			res[i] = strconv.FormatInt(c.InactiveDelegates, 10)
		case "zero_delegates":
			res[i] = strconv.FormatInt(c.ZeroDelegates, 10)
		case "self_delegates":
			res[i] = strconv.FormatInt(c.SelfDelegates, 10)
		case "single_delegates":
			res[i] = strconv.FormatInt(c.SingleDelegates, 10)
		case "multi_delegates":
			res[i] = strconv.FormatInt(c.MultiDelegates, 10)
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

func StreamChainSeries(ctx *ApiContext, args *SeriesRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access series source table '%s'", args.Series), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	// time is auto-added from parser
	if len(args.Columns) == 1 {
		// use all series columns
		args.Columns = chainSeriesNames
	}
	// resolve short column names
	srcNames = make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore non-series columns
		if !chainSeriesNames.Contains(v) {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		n, ok := chainSourceNames[v]
		if !ok {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
		}
		if n != "-" {
			srcNames = append(srcNames, n)
		}
	}

	// build table query, no dynamic filter conditions
	q := pack.Query{
		Name:   ctx.RequestID,
		Fields: table.Fields().Select(srcNames...),
		Order:  args.Order,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("T"), // time
				Mode:  pack.FilterModeRange,
				From:  args.From.Time(),
				To:    args.To.Time(),
				Raw:   args.From.String() + " - " + args.To.String(), // debugging aid
			},
		},
	}

	var count int
	start := time.Now()
	ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Series)
	defer func() {
		ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	}()

	// prepare for source and return type marshalling
	cs := &ChainSeries{params: params, verbose: args.Verbose, columns: args.Columns}
	cm := &model.Chain{}
	window := args.Collapse.Duration()
	nextBucketTime := args.From.Add(window).Time()
	mul := 1
	if args.Order == pack.OrderDesc {
		mul = 0
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

		// stream from database, result is assumed to be in timestamp order
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if err := r.Decode(cm); err != nil {
				return err
			}

			// output SupplySeries when valid and time has crossed next boundary
			if !cs.Timestamp.IsZero() && (cm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
				// output current data
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}
				if err := enc.Encode(cs); err != nil {
					return err
				}
				count++
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				cs.Reset()
			}

			// init next time window from data
			if cs.Timestamp.IsZero() {
				cs.Timestamp = cm.Timestamp.Truncate(window)
				nextBucketTime = cs.Timestamp.Add(window * time.Duration(mul))
			}

			// keep latest data
			cs.Chain = *cm
			return nil
		})
		// don't handle error here, will be picked up by trailer
		if err == nil {
			// output last series element
			if !cs.Timestamp.IsZero() {
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				}
				err = enc.Encode(cs)
				if err == nil {
					count++
				}
			}
		}

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
			// stream from database, result order is assumed to be in timestamp order
			err = table.Stream(ctx, q, func(r pack.Row) error {
				if err := r.Decode(cm); err != nil {
					return err
				}

				// output SupplySeries when valid and time has crossed next boundary
				if !cs.Timestamp.IsZero() && (cm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
					// output accumulated data
					if err := enc.EncodeRecord(cs); err != nil {
						return err
					}
					count++
					if args.Limit > 0 && count == int(args.Limit) {
						return io.EOF
					}
					cs.Reset()
				}

				// init next time window from data
				if cs.Timestamp.IsZero() {
					cs.Timestamp = cm.Timestamp.Truncate(window)
					nextBucketTime = cs.Timestamp.Add(window * time.Duration(mul))
				}

				// keep latest data
				cs.Chain = *cm
				return nil
			})
			if err == nil {
				// output last series element
				if !cs.Timestamp.IsZero() {
					err = enc.EncodeRecord(cs)
					if err == nil {
						count++
					}
				}
			}
		}
		ctx.Log.Tracef("CSV Encoded %d rows", count)
	}

	// write error (except EOF), cursor and count as http trailer
	ctx.StreamTrailer("", count, err)

	// streaming return
	return nil, -1
}
