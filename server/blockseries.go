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
	blockSeriesNames = util.StringList([]string{
		"time",
		"n_slots_endorsed",
		"n_ops",
		"n_ops_failed",
		"n_ops_contract",
		"n_tx",
		"n_activation",
		"n_seed_nonce_revelation",
		"n_double_baking_evidence",
		"n_double_endorsement_evidence",
		"n_endorsement",
		"n_delegation",
		"n_reveal",
		"n_origination",
		"n_proposal",
		"n_ballot",
		"volume",
		"fees",
		"rewards",
		"deposits",
		"unfrozen_fees",
		"unfrozen_rewards",
		"unfrozen_deposits",
		"activated_supply",
		"burned_supply",
		"n_new_accounts",
		"n_new_implicit",
		"n_new_managed",
		"n_new_contracts",
		"n_cleared_accounts",
		"n_funded_accounts",
		"gas_used",
		"storage_size",
		"days_destroyed",
	})
)

// Only use fields that can be summed over time
// configurable marshalling helper
type BlockSeries struct {
	Timestamp           time.Time `json:"time"`
	NSlotsEndorsed      int64     `json:"n_slots_endorsed"`
	NOps                int64     `json:"n_ops"`
	NOpsFailed          int64     `json:"n_ops_failed"`
	NOpsContract        int64     `json:"n_ops_contract"`
	NTx                 int64     `json:"n_tx"`
	NActivation         int64     `json:"n_activation"`
	NSeedNonce          int64     `json:"n_seed_nonce_revelation"`
	N2Baking            int64     `json:"n_double_baking_evidence"`
	N2Endorsement       int64     `json:"n_double_endorsement_evidence"`
	NEndorsement        int64     `json:"n_endorsement"`
	NDelegation         int64     `json:"n_delegation"`
	NReveal             int64     `json:"n_reveal"`
	NOrigination        int64     `json:"n_origination"`
	NProposal           int64     `json:"n_proposal"`
	NBallot             int64     `json:"n_ballot"`
	Volume              int64     `json:"volume"`
	Fees                int64     `json:"fees"`
	Rewards             int64     `json:"rewards"`
	Deposits            int64     `json:"deposits"`
	UnfrozenFees        int64     `json:"unfrozen_fees"`
	UnfrozenRewards     int64     `json:"unfrozen_rewards"`
	UnfrozenDeposits    int64     `json:"unfrozen_deposits"`
	ActivatedSupply     int64     `json:"activated_supply"`
	BurnedSupply        int64     `json:"burned_supply"`
	NewAccounts         int64     `json:"n_new_accounts"`
	NewImplicitAccounts int64     `json:"n_new_implicit"`
	NewManagedAccounts  int64     `json:"n_new_managed"`
	NewContracts        int64     `json:"n_new_contracts"`
	ClearedAccounts     int64     `json:"n_cleared_accounts"`
	FundedAccounts      int64     `json:"n_funded_accounts"`
	GasUsed             int64     `json:"gas_used"`
	StorageSize         int64     `json:"storage_size"`
	TDD                 float64   `json:"days_destroyed"`

	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"`
	verbose bool            `csv:"-" pack:"-"`
}

func (s *BlockSeries) Add(b *model.Block) {
	s.NSlotsEndorsed += int64(b.NSlotsEndorsed)
	s.NOps += int64(b.NOps)
	s.NOpsFailed += int64(b.NOpsFailed)
	s.NOpsContract += int64(b.NOpsContract)
	s.NTx += int64(b.NTx)
	s.NActivation += int64(b.NActivation)
	s.NSeedNonce += int64(b.NSeedNonce)
	s.N2Baking += int64(b.N2Baking)
	s.N2Endorsement += int64(b.N2Endorsement)
	s.NEndorsement += int64(b.NEndorsement)
	s.NDelegation += int64(b.NDelegation)
	s.NReveal += int64(b.NReveal)
	s.NOrigination += int64(b.NOrigination)
	s.NProposal += int64(b.NProposal)
	s.NBallot += int64(b.NBallot)
	s.Volume += int64(b.Volume)
	s.Fees += int64(b.Fees)
	s.Rewards += int64(b.Rewards)
	s.Deposits += int64(b.Deposits)
	s.UnfrozenFees += int64(b.UnfrozenFees)
	s.UnfrozenRewards += int64(b.UnfrozenRewards)
	s.UnfrozenDeposits += int64(b.UnfrozenDeposits)
	s.ActivatedSupply += int64(b.ActivatedSupply)
	s.BurnedSupply += int64(b.BurnedSupply)
	s.NewAccounts += int64(b.NewAccounts)
	s.NewImplicitAccounts += int64(b.NewImplicitAccounts)
	s.NewManagedAccounts += int64(b.NewManagedAccounts)
	s.NewContracts += int64(b.NewContracts)
	s.ClearedAccounts += int64(b.ClearedAccounts)
	s.FundedAccounts += int64(b.FundedAccounts)
	s.GasUsed += int64(b.GasUsed)
	s.StorageSize += int64(b.StorageSize)
	s.TDD += b.TDD
}

func (s *BlockSeries) Reset() {
	s.Timestamp = time.Time{}
	s.NSlotsEndorsed = 0
	s.NOps = 0
	s.NOpsFailed = 0
	s.NOpsContract = 0
	s.NTx = 0
	s.NActivation = 0
	s.NSeedNonce = 0
	s.N2Baking = 0
	s.N2Endorsement = 0
	s.NEndorsement = 0
	s.NDelegation = 0
	s.NReveal = 0
	s.NOrigination = 0
	s.NProposal = 0
	s.NBallot = 0
	s.Volume = 0
	s.Fees = 0
	s.Rewards = 0
	s.Deposits = 0
	s.UnfrozenFees = 0
	s.UnfrozenRewards = 0
	s.UnfrozenDeposits = 0
	s.ActivatedSupply = 0
	s.BurnedSupply = 0
	s.NewAccounts = 0
	s.NewImplicitAccounts = 0
	s.NewManagedAccounts = 0
	s.NewContracts = 0
	s.ClearedAccounts = 0
	s.FundedAccounts = 0
	s.GasUsed = 0
	s.StorageSize = 0
	s.TDD = 0
}

func (s *BlockSeries) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (b *BlockSeries) MarshalJSONVerbose() ([]byte, error) {
	block := struct {
		Timestamp           time.Time `json:"time"`
		NSlotsEndorsed      int64     `json:"n_endorsed_slots"`
		NOps                int64     `json:"n_ops"`
		NOpsFailed          int64     `json:"n_ops_failed"`
		NOpsContract        int64     `json:"n_ops_contract"`
		NTx                 int64     `json:"n_tx"`
		NActivation         int64     `json:"n_activation"`
		NSeedNonce          int64     `json:"n_seed_nonce_revelation"`
		N2Baking            int64     `json:"n_double_baking_evidence"`
		N2Endorsement       int64     `json:"n_double_endorsement_evidence"`
		NEndorsement        int64     `json:"n_endorsement"`
		NDelegation         int64     `json:"n_delegation"`
		NReveal             int64     `json:"n_reveal"`
		NOrigination        int64     `json:"n_origination"`
		NProposal           int64     `json:"n_proposal"`
		NBallot             int64     `json:"n_ballot"`
		Volume              float64   `json:"volume"`
		Fees                float64   `json:"fees"`
		Rewards             float64   `json:"rewards"`
		Deposits            float64   `json:"deposits"`
		UnfrozenFees        float64   `json:"unfrozen_fees"`
		UnfrozenRewards     float64   `json:"unfrozen_rewards"`
		UnfrozenDeposits    float64   `json:"unfrozen_deposits"`
		ActivatedSupply     float64   `json:"activated_supply"`
		BurnedSupply        float64   `json:"burned_supply"`
		NewAccounts         int64     `json:"n_new_accounts"`
		NewImplicitAccounts int64     `json:"n_new_implicit"`
		NewManagedAccounts  int64     `json:"n_new_managed"`
		NewContracts        int64     `json:"n_new_contracts"`
		ClearedAccounts     int64     `json:"n_cleared_accounts"`
		FundedAccounts      int64     `json:"n_funded_accounts"`
		GasUsed             int64     `json:"gas_used"`
		StorageSize         int64     `json:"storage_size"`
		TDD                 float64   `json:"days_destroyed"`
	}{
		Timestamp:           b.Timestamp,
		NSlotsEndorsed:      b.NSlotsEndorsed,
		NOps:                b.NOps,
		NOpsFailed:          b.NOpsFailed,
		NOpsContract:        b.NOpsContract,
		NTx:                 b.NTx,
		NActivation:         b.NActivation,
		NSeedNonce:          b.NSeedNonce,
		N2Baking:            b.N2Baking,
		N2Endorsement:       b.N2Endorsement,
		NEndorsement:        b.NEndorsement,
		NDelegation:         b.NDelegation,
		NReveal:             b.NReveal,
		NOrigination:        b.NOrigination,
		NProposal:           b.NProposal,
		NBallot:             b.NBallot,
		Volume:              b.params.ConvertValue(b.Volume),
		Fees:                b.params.ConvertValue(b.Fees),
		Rewards:             b.params.ConvertValue(b.Rewards),
		Deposits:            b.params.ConvertValue(b.Deposits),
		UnfrozenFees:        b.params.ConvertValue(b.UnfrozenFees),
		UnfrozenRewards:     b.params.ConvertValue(b.UnfrozenRewards),
		UnfrozenDeposits:    b.params.ConvertValue(b.UnfrozenDeposits),
		ActivatedSupply:     b.params.ConvertValue(b.ActivatedSupply),
		BurnedSupply:        b.params.ConvertValue(b.BurnedSupply),
		NewAccounts:         b.NewAccounts,
		NewImplicitAccounts: b.NewImplicitAccounts,
		NewManagedAccounts:  b.NewManagedAccounts,
		NewContracts:        b.NewContracts,
		ClearedAccounts:     b.ClearedAccounts,
		FundedAccounts:      b.FundedAccounts,
		GasUsed:             b.GasUsed,
		StorageSize:         b.StorageSize,
		TDD:                 b.TDD,
	}
	return json.Marshal(block)
}

func (b *BlockSeries) MarshalJSONBrief() ([]byte, error) {
	dec := b.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
		case "n_endorsed_slots":
			buf = strconv.AppendInt(buf, int64(b.NSlotsEndorsed), 10)
		case "n_ops":
			buf = strconv.AppendInt(buf, int64(b.NOps), 10)
		case "n_ops_failed":
			buf = strconv.AppendInt(buf, int64(b.NOpsFailed), 10)
		case "n_ops_contract":
			buf = strconv.AppendInt(buf, int64(b.NOpsContract), 10)
		case "n_tx":
			buf = strconv.AppendInt(buf, int64(b.NTx), 10)
		case "n_activation":
			buf = strconv.AppendInt(buf, int64(b.NActivation), 10)
		case "n_seed_nonce_revelation":
			buf = strconv.AppendInt(buf, int64(b.NSeedNonce), 10)
		case "n_double_baking_evidence":
			buf = strconv.AppendInt(buf, int64(b.N2Baking), 10)
		case "n_double_endorsement_evidence":
			buf = strconv.AppendInt(buf, int64(b.N2Endorsement), 10)
		case "n_endorsement":
			buf = strconv.AppendInt(buf, int64(b.NEndorsement), 10)
		case "n_delegation":
			buf = strconv.AppendInt(buf, int64(b.NDelegation), 10)
		case "n_reveal":
			buf = strconv.AppendInt(buf, int64(b.NReveal), 10)
		case "n_origination":
			buf = strconv.AppendInt(buf, int64(b.NOrigination), 10)
		case "n_proposal":
			buf = strconv.AppendInt(buf, int64(b.NProposal), 10)
		case "n_ballot":
			buf = strconv.AppendInt(buf, int64(b.NBallot), 10)
		case "volume":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Volume), 'f', dec, 64)
		case "fees":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Fees), 'f', dec, 64)
		case "rewards":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Rewards), 'f', dec, 64)
		case "deposits":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Deposits), 'f', dec, 64)
		case "unfrozen_fees":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.UnfrozenFees), 'f', dec, 64)
		case "unfrozen_rewards":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.UnfrozenRewards), 'f', dec, 64)
		case "unfrozen_deposits":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.UnfrozenDeposits), 'f', dec, 64)
		case "activated_supply":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.ActivatedSupply), 'f', dec, 64)
		case "burned_supply":
			buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.BurnedSupply), 'f', dec, 64)
		case "n_new_accounts":
			buf = strconv.AppendInt(buf, int64(b.NewAccounts), 10)
		case "n_new_implicit":
			buf = strconv.AppendInt(buf, int64(b.NewImplicitAccounts), 10)
		case "n_new_managed":
			buf = strconv.AppendInt(buf, int64(b.NewManagedAccounts), 10)
		case "n_new_contracts":
			buf = strconv.AppendInt(buf, int64(b.NewContracts), 10)
		case "n_cleared_accounts":
			buf = strconv.AppendInt(buf, int64(b.ClearedAccounts), 10)
		case "n_funded_accounts":
			buf = strconv.AppendInt(buf, int64(b.FundedAccounts), 10)
		case "gas_used":
			buf = strconv.AppendInt(buf, b.GasUsed, 10)
		case "storage_size":
			buf = strconv.AppendInt(buf, b.StorageSize, 10)
		case "days_destroyed":
			buf = strconv.AppendFloat(buf, b.TDD, 'f', -1, 64)
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

func (b *BlockSeries) MarshalCSV() ([]string, error) {
	dec := b.params.Decimals
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "time":
			res[i] = strconv.FormatInt(util.UnixMilliNonZero(b.Timestamp), 10)
		case "n_endorsed_slots":
			res[i] = strconv.FormatInt(int64(b.NSlotsEndorsed), 10)
		case "n_ops":
			res[i] = strconv.FormatInt(int64(b.NOps), 10)
		case "n_ops_failed":
			res[i] = strconv.FormatInt(int64(b.NOpsFailed), 10)
		case "n_ops_contract":
			res[i] = strconv.FormatInt(int64(b.NOpsContract), 10)
		case "n_tx":
			res[i] = strconv.FormatInt(int64(b.NTx), 10)
		case "n_activation":
			res[i] = strconv.FormatInt(int64(b.NActivation), 10)
		case "n_seed_nonce_revelation":
			res[i] = strconv.FormatInt(int64(b.NSeedNonce), 10)
		case "n_double_baking_evidence":
			res[i] = strconv.FormatInt(int64(b.N2Baking), 10)
		case "n_double_endorsement_evidence":
			res[i] = strconv.FormatInt(int64(b.N2Endorsement), 10)
		case "n_endorsement":
			res[i] = strconv.FormatInt(int64(b.NEndorsement), 10)
		case "n_delegation":
			res[i] = strconv.FormatInt(int64(b.NDelegation), 10)
		case "n_reveal":
			res[i] = strconv.FormatInt(int64(b.NReveal), 10)
		case "n_origination":
			res[i] = strconv.FormatInt(int64(b.NOrigination), 10)
		case "n_proposal":
			res[i] = strconv.FormatInt(int64(b.NProposal), 10)
		case "n_ballot":
			res[i] = strconv.FormatInt(int64(b.NBallot), 10)
		case "volume":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Volume), 'f', dec, 64)
		case "fees":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Fees), 'f', dec, 64)
		case "rewards":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Rewards), 'f', dec, 64)
		case "deposits":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Deposits), 'f', dec, 64)
		case "unfrozen_fees":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.UnfrozenFees), 'f', dec, 64)
		case "unfrozen_rewards":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.UnfrozenRewards), 'f', dec, 64)
		case "unfrozen_deposits":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.UnfrozenDeposits), 'f', dec, 64)
		case "activated_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.ActivatedSupply), 'f', dec, 64)
		case "burned_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.BurnedSupply), 'f', dec, 64)
		case "n_new_accounts":
			res[i] = strconv.FormatInt(int64(b.NewAccounts), 10)
		case "n_new_implicit":
			res[i] = strconv.FormatInt(int64(b.NewImplicitAccounts), 10)
		case "n_new_managed":
			res[i] = strconv.FormatInt(int64(b.NewManagedAccounts), 10)
		case "n_new_contracts":
			res[i] = strconv.FormatInt(int64(b.NewContracts), 10)
		case "n_cleared_accounts":
			res[i] = strconv.FormatInt(int64(b.ClearedAccounts), 10)
		case "n_funded_accounts":
			res[i] = strconv.FormatInt(int64(b.FundedAccounts), 10)
		case "gas_used":
			res[i] = strconv.FormatInt(b.GasUsed, 10)
		case "storage_size":
			res[i] = strconv.FormatInt(b.StorageSize, 10)
		case "days_destroyed":
			res[i] = strconv.FormatFloat(b.TDD, 'f', -1, 64)
		default:
			continue
		}
	}
	return res, nil
}

func StreamBlockSeries(ctx *ApiContext, args *SeriesRequest) (interface{}, int) {
	// fetch chain params at current height
	params := ctx.Crawler.ParamsByHeight(-1)

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
		args.Columns = blockSeriesNames
	}
	// resolve short column names
	srcNames = make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore non-series columns
		if !blockSeriesNames.Contains(v) {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		n, ok := blockSourceNames[v]
		if !ok {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
		}
		if n != "-" {
			srcNames = append(srcNames, n)
		}
	}

	// build table query
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
		case "columns", "collapse", "start_date", "end_date", "limit", "order", "verbose":
			// skip these fields
			continue

		case "baker":
			// parse baker address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find("B"), // baker id
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
							Field: table.Fields().Find("B"), // baker id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "account not found", // debugging aid
						})
					} else {
						// add addr id as extra fund_flow condition
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find("B"), // baker id
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
					Field: table.Fields().Find("B"), // baker id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "voting_period_kind":
			// parse only the first value
			period := chain.ParseVotingPeriod(val[0])
			if !period.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid voting period '%s'", val[0]), nil))
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Find("p"),
				Mode:  mode,
				Value: int64(period),
				Raw:   val[0], // debugging aid
			})
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := blockSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "volume", "rewards", "fees", "deposits", "burned_supply",
					"unfrozen_fees", "unfrozen_rewards", "unfrozen_deposits",
					"activated_supply":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions = append(q.Conditions, cond)
				}
			}
		}
	}

	var count int
	start := time.Now()
	ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Series)
	defer func() {
		ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	}()

	// prepare for source and return type marshalling
	bs := &BlockSeries{params: params, verbose: args.Verbose, columns: args.Columns}
	bm := &model.Block{}
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
			if err := r.Decode(bm); err != nil {
				return err
			}

			// output BlockSeries when valid and time has crossed next boundary
			if !bs.Timestamp.IsZero() && (bm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
				// output accumulated data
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}
				if err := enc.Encode(bs); err != nil {
					return err
				}
				count++
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				bs.Reset()
			}

			// init next time window from data
			if bs.Timestamp.IsZero() {
				bs.Timestamp = bm.Timestamp.Truncate(window)
				nextBucketTime = bs.Timestamp.Add(window * time.Duration(mul))
			}

			// accumulate data in BlockSeries
			bs.Add(bm)
			return nil
		})
		// don't handle error here, will be picked up by trailer
		if err == nil {
			// output last series element
			if !bs.Timestamp.IsZero() {
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				}
				err = enc.Encode(bs)
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
				if err := r.Decode(bm); err != nil {
					return err
				}

				// output BlockSeries when valid and time has crossed next boundary
				if !bs.Timestamp.IsZero() && (bm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
					// output accumulated data
					if err := enc.EncodeRecord(bs); err != nil {
						return err
					}
					count++
					if args.Limit > 0 && count == int(args.Limit) {
						return io.EOF
					}
					bs.Reset()
				}

				// init next time window from data
				if bs.Timestamp.IsZero() {
					bs.Timestamp = bm.Timestamp.Truncate(window)
					nextBucketTime = bs.Timestamp.Add(window * time.Duration(mul))
				}

				// accumulate data in BlockSeries
				bs.Add(bm)
				return nil
			})
			if err == nil {
				// output last series element
				if !bs.Timestamp.IsZero() {
					err = enc.EncodeRecord(bs)
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
