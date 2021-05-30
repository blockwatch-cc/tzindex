// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
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
	chainSeriesNames.AddUnique("count")
}

// configurable marshalling helper
type ChainSeries struct {
	model.Chain

	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	verbose bool            // cond. marshal
	null    bool
}

var _ SeriesBucket = (*ChainSeries)(nil)

func (s *ChainSeries) Init(params *tezos.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *ChainSeries) IsEmpty() bool {
	return s.Chain.Height == 0 || s.Chain.Timestamp.IsZero()
}

func (s *ChainSeries) Add(m SeriesModel) {
	o := m.(*model.Chain)
	s.Chain = *o
}

func (c *ChainSeries) Reset() {
	c.Chain.Timestamp = time.Time{}
	c.null = false
}

func (c *ChainSeries) Null(ts time.Time) SeriesBucket {
	c.Reset()
	c.Timestamp = ts
	c.null = true
	return c
}

func (s *ChainSeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *ChainSeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *ChainSeries) Time() time.Time {
	return s.Timestamp
}

func (s *ChainSeries) Clone() SeriesBucket {
	c := &ChainSeries{
		Chain: s.Chain,
	}
	c.columns = s.columns
	c.params = s.params
	c.verbose = s.verbose
	c.null = s.null
	return c
}

func (s *ChainSeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	// unused, sematically there is one chain table entry per block
	return s
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
		Count              int   `json:"count"`
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
		Count:              1,
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
		if c.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(c.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "height":
				buf = strconv.AppendInt(buf, c.Height, 10)
			case "cycle":
				buf = strconv.AppendInt(buf, c.Cycle, 10)
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(c.Timestamp), 10)
			case "count":
				buf = strconv.AppendInt(buf, 1, 10)
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
		if c.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(c.Timestamp.Format(time.RFC3339))
			default:
				continue
			}

		}
		switch v {
		case "height":
			res[i] = strconv.FormatInt(c.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(c.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(c.Timestamp.Format(time.RFC3339))
		case "count":
			res[i] = strconv.FormatInt(1, 10)
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

func (s *ChainSeries) BuildQuery(ctx *ApiContext, args *SeriesRequest) pack.Query {
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
		// ignore count column
		if v == "count" {
			continue
		}
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
	return pack.NewQuery(ctx.RequestID, table).
		WithFields(srcNames...).
		WithOrder(args.Order).
		AndRange("time", args.From.Time(), args.To.Time())
}
