// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

var (
	blockSeriesNames = util.StringList([]string{
		"time",
		"count",
		"n_endorsed_slots",
		"n_ops_applied",
		"n_ops_failed",
		"n_calls",
		"n_rollup_calls",
		"n_events",
		"n_tx",
		"n_tickets",
		"volume",
		"fee",
		"reward",
		"deposit",
		"activated_supply",
		"minted_supply",
		"burned_supply",
		"n_new_accounts",
		"n_new_contracts",
		"n_cleared_accounts",
		"n_funded_accounts",
		"gas_used",
		"storage_paid",
	})
)

// Only use fields that can be summed over time
// configurable marshalling helper
type BlockSeries struct {
	Timestamp       time.Time `json:"time"`
	Count           int       `json:"count"`
	NSlotsEndorsed  int64     `json:"n_endorsed_slots"`
	NOpsApplied     int64     `json:"n_ops_applied"`
	NOpsFailed      int64     `json:"n_ops_failed"`
	NContractCalls  int64     `json:"n_calls"`
	NRollupCalls    int64     `json:"n_rollup_calls"`
	NEvents         int64     `json:"n_events"`
	NTx             int64     `json:"n_tx"`
	NTickets        int64     `json:"n_tickets"`
	Volume          int64     `json:"volume"`
	Fee             int64     `json:"fee"`
	Reward          int64     `json:"reward"`
	Deposit         int64     `json:"deposit"`
	ActivatedSupply int64     `json:"activated_supply"`
	MintedSupply    int64     `json:"minted_supply"`
	BurnedSupply    int64     `json:"burned_supply"`
	NewAccounts     int64     `json:"n_new_accounts"`
	NewContracts    int64     `json:"n_new_contracts"`
	ClearedAccounts int64     `json:"n_cleared_accounts"`
	FundedAccounts  int64     `json:"n_funded_accounts"`
	GasUsed         int64     `json:"gas_used"`
	StoragePaid     int64     `json:"storage_paid"`

	columns util.StringList // cond. cols & order when brief
	params  *rpc.Params
	verbose bool
	null    bool
}

var _ SeriesBucket = (*BlockSeries)(nil)

func (s *BlockSeries) Init(params *rpc.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *BlockSeries) IsEmpty() bool {
	return s.Count == 0
}

func (s *BlockSeries) Add(m SeriesModel) {
	b := m.(*model.Block)
	s.NSlotsEndorsed += int64(b.NSlotsEndorsed)
	s.NOpsApplied += int64(model.Int16Correct(b.NOpsApplied))
	s.NOpsFailed += int64(model.Int16Correct(b.NOpsFailed))
	s.NContractCalls += int64(model.Int16Correct(b.NContractCalls))
	s.NRollupCalls += int64(model.Int16Correct(b.NRollupCalls))
	s.NEvents += int64(model.Int16Correct(b.NEvents))
	s.NTx += int64(model.Int16Correct(b.NTx))
	s.NTickets += int64(model.Int16Correct(b.NTickets))
	s.Volume += b.Volume
	s.Fee += b.Fee
	s.Reward += b.Reward
	s.Deposit += b.Deposit
	s.ActivatedSupply += b.ActivatedSupply
	s.MintedSupply += b.MintedSupply
	s.BurnedSupply += b.BurnedSupply
	s.NewAccounts += int64(model.Int16Correct(b.NewAccounts))
	s.NewContracts += int64(model.Int16Correct(b.NewContracts))
	s.ClearedAccounts += int64(model.Int16Correct(b.ClearedAccounts))
	s.FundedAccounts += int64(model.Int16Correct(b.FundedAccounts))
	s.GasUsed += b.GasUsed
	s.StoragePaid += b.StoragePaid
	s.Count++
}

func (s *BlockSeries) Reset() {
	s.Timestamp = time.Time{}
	s.NSlotsEndorsed = 0
	s.NOpsApplied = 0
	s.NOpsFailed = 0
	s.NContractCalls = 0
	s.NRollupCalls = 0
	s.NEvents = 0
	s.NTx = 0
	s.NTickets = 0
	s.Volume = 0
	s.Fee = 0
	s.Reward = 0
	s.Deposit = 0
	s.ActivatedSupply = 0
	s.MintedSupply = 0
	s.BurnedSupply = 0
	s.NewAccounts = 0
	s.NewContracts = 0
	s.ClearedAccounts = 0
	s.FundedAccounts = 0
	s.GasUsed = 0
	s.StoragePaid = 0
	s.Count = 0
	s.null = false
}

func (s *BlockSeries) Null(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	s.null = true
	return s
}

func (s *BlockSeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *BlockSeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *BlockSeries) Time() time.Time {
	return s.Timestamp
}

func (s *BlockSeries) Clone() SeriesBucket {
	return &BlockSeries{
		Timestamp:       s.Timestamp,
		NSlotsEndorsed:  s.NSlotsEndorsed,
		NOpsApplied:     s.NOpsApplied,
		NOpsFailed:      s.NOpsFailed,
		NContractCalls:  s.NContractCalls,
		NRollupCalls:    s.NRollupCalls,
		NEvents:         s.NEvents,
		NTx:             s.NTx,
		NTickets:        s.NTickets,
		Volume:          s.Volume,
		Fee:             s.Fee,
		Reward:          s.Reward,
		Deposit:         s.Deposit,
		ActivatedSupply: s.ActivatedSupply,
		MintedSupply:    s.MintedSupply,
		BurnedSupply:    s.BurnedSupply,
		NewAccounts:     s.NewAccounts,
		NewContracts:    s.NewContracts,
		ClearedAccounts: s.ClearedAccounts,
		FundedAccounts:  s.FundedAccounts,
		GasUsed:         s.GasUsed,
		StoragePaid:     s.StoragePaid,
		Count:           s.Count,
		columns:         s.columns,
		params:          s.params,
		verbose:         s.verbose,
		null:            s.null,
	}
}

func (s *BlockSeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	b := m.(*BlockSeries)
	weight := float64(ts.Sub(s.Timestamp)) / float64(b.Timestamp.Sub(s.Timestamp))
	if math.IsInf(weight, 1) {
		weight = 1
	}
	switch weight {
	case 0:
		return s
	default:
		return &BlockSeries{
			Timestamp:       ts,
			NSlotsEndorsed:  s.NSlotsEndorsed + int64(weight*float64(b.NSlotsEndorsed-s.NSlotsEndorsed)),
			NOpsApplied:     s.NOpsApplied + int64(weight*float64(b.NOpsApplied-s.NOpsApplied)),
			NOpsFailed:      s.NOpsFailed + int64(weight*float64(b.NOpsFailed-s.NOpsFailed)),
			NContractCalls:  s.NContractCalls + int64(weight*float64(b.NContractCalls-s.NContractCalls)),
			NRollupCalls:    s.NRollupCalls + int64(weight*float64(b.NRollupCalls-s.NRollupCalls)),
			NTx:             s.NTx + int64(weight*float64(b.NTx-s.NTx)),
			NTickets:        s.NTickets + int64(weight*float64(b.NTickets-s.NTickets)),
			Volume:          s.Volume + int64(weight*float64(b.Volume-s.Volume)),
			Fee:             s.Fee + int64(weight*float64(b.Fee-s.Fee)),
			Reward:          s.Reward + int64(weight*float64(b.Reward-s.Reward)),
			Deposit:         s.Deposit + int64(weight*float64(b.Deposit-s.Deposit)),
			ActivatedSupply: s.ActivatedSupply + int64(weight*float64(b.ActivatedSupply-s.ActivatedSupply)),
			MintedSupply:    s.MintedSupply + int64(weight*float64(b.MintedSupply-s.MintedSupply)),
			BurnedSupply:    s.BurnedSupply + int64(weight*float64(b.BurnedSupply-s.BurnedSupply)),
			NewAccounts:     s.NewAccounts + int64(weight*float64(b.NewAccounts-s.NewAccounts)),
			NewContracts:    s.NewContracts + int64(weight*float64(b.NewContracts-s.NewContracts)),
			ClearedAccounts: s.ClearedAccounts + int64(weight*float64(b.ClearedAccounts-s.ClearedAccounts)),
			FundedAccounts:  s.FundedAccounts + int64(weight*float64(b.FundedAccounts-s.FundedAccounts)),
			GasUsed:         s.GasUsed + int64(weight*float64(b.GasUsed-s.GasUsed)),
			StoragePaid:     s.StoragePaid + int64(weight*float64(b.StoragePaid-s.StoragePaid)),
			Count:           0,
			columns:         s.columns,
			params:          s.params,
			verbose:         s.verbose,
			null:            false,
		}
	}
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
		Timestamp       time.Time `json:"time"`
		Count           int       `json:"count"`
		NSlotsEndorsed  int64     `json:"n_endorsed_slots"`
		NOpsApplied     int64     `json:"n_ops_applied"`
		NOpsFailed      int64     `json:"n_ops_failed"`
		NContractCalls  int64     `json:"n_calls"`
		NRollupCalls    int64     `json:"n_rollup_calls"`
		NEvents         int64     `json:"n_events"`
		NTx             int64     `json:"n_tx"`
		NTickets        int64     `json:"n_tickets"`
		Volume          float64   `json:"volume"`
		Fee             float64   `json:"fee"`
		Reward          float64   `json:"reward"`
		Deposit         float64   `json:"deposit"`
		ActivatedSupply float64   `json:"activated_supply"`
		MintedSupply    float64   `json:"minted_supply"`
		BurnedSupply    float64   `json:"burned_supply"`
		NewAccounts     int64     `json:"n_new_accounts"`
		NewContracts    int64     `json:"n_new_contracts"`
		ClearedAccounts int64     `json:"n_cleared_accounts"`
		FundedAccounts  int64     `json:"n_funded_accounts"`
		GasUsed         int64     `json:"gas_used"`
		StoragePaid     int64     `json:"storage_paid"`
	}{
		Timestamp:       b.Timestamp,
		Count:           b.Count,
		NSlotsEndorsed:  b.NSlotsEndorsed,
		NOpsApplied:     b.NOpsApplied,
		NOpsFailed:      b.NOpsFailed,
		NContractCalls:  b.NContractCalls,
		NRollupCalls:    b.NRollupCalls,
		NEvents:         b.NEvents,
		NTx:             b.NTx,
		NTickets:        b.NTickets,
		Volume:          b.params.ConvertValue(b.Volume),
		Fee:             b.params.ConvertValue(b.Fee),
		Reward:          b.params.ConvertValue(b.Reward),
		Deposit:         b.params.ConvertValue(b.Deposit),
		ActivatedSupply: b.params.ConvertValue(b.ActivatedSupply),
		MintedSupply:    b.params.ConvertValue(b.MintedSupply),
		BurnedSupply:    b.params.ConvertValue(b.BurnedSupply),
		NewAccounts:     b.NewAccounts,
		NewContracts:    b.NewContracts,
		ClearedAccounts: b.ClearedAccounts,
		FundedAccounts:  b.FundedAccounts,
		GasUsed:         b.GasUsed,
		StoragePaid:     b.StoragePaid,
	}
	return json.Marshal(block)
}

func (b *BlockSeries) MarshalJSONBrief() ([]byte, error) {
	dec := b.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		if b.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
			case "count":
				buf = strconv.AppendInt(buf, int64(b.Count), 10)
			case "n_endorsed_slots":
				buf = strconv.AppendInt(buf, b.NSlotsEndorsed, 10)
			case "n_ops_applied":
				buf = strconv.AppendInt(buf, b.NOpsApplied, 10)
			case "n_ops_failed":
				buf = strconv.AppendInt(buf, b.NOpsFailed, 10)
			case "n_calls":
				buf = strconv.AppendInt(buf, b.NContractCalls, 10)
			case "n_rollup_calls":
				buf = strconv.AppendInt(buf, b.NRollupCalls, 10)
			case "n_events":
				buf = strconv.AppendInt(buf, b.NEvents, 10)
			case "n_tx":
				buf = strconv.AppendInt(buf, b.NTx, 10)
			case "n_tickets":
				buf = strconv.AppendInt(buf, b.NTickets, 10)
			case "volume":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Volume), 'f', dec, 64)
			case "fee":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Fee), 'f', dec, 64)
			case "reward":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Reward), 'f', dec, 64)
			case "deposit":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Deposit), 'f', dec, 64)
			case "activated_supply":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.ActivatedSupply), 'f', dec, 64)
			case "minted_supply":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.MintedSupply), 'f', dec, 64)
			case "burned_supply":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.BurnedSupply), 'f', dec, 64)
			case "n_new_accounts":
				buf = strconv.AppendInt(buf, b.NewAccounts, 10)
			case "n_new_contracts":
				buf = strconv.AppendInt(buf, b.NewContracts, 10)
			case "n_cleared_accounts":
				buf = strconv.AppendInt(buf, b.ClearedAccounts, 10)
			case "n_funded_accounts":
				buf = strconv.AppendInt(buf, b.FundedAccounts, 10)
			case "gas_used":
				buf = strconv.AppendInt(buf, b.GasUsed, 10)
			case "storage_paid":
				buf = strconv.AppendInt(buf, b.StoragePaid, 10)
			default:
				continue
			}
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
		if b.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
			default:
				continue
			}
		}
		switch v {
		case "time":
			res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
		case "count":
			res[i] = strconv.Itoa(b.Count)
		case "n_endorsed_slots":
			res[i] = strconv.FormatInt(b.NSlotsEndorsed, 10)
		case "n_ops_applied":
			res[i] = strconv.FormatInt(b.NOpsApplied, 10)
		case "n_ops_failed":
			res[i] = strconv.FormatInt(b.NOpsFailed, 10)
		case "n_calls":
			res[i] = strconv.FormatInt(b.NContractCalls, 10)
		case "n_rollup_calls":
			res[i] = strconv.FormatInt(b.NRollupCalls, 10)
		case "n_events":
			res[i] = strconv.FormatInt(b.NEvents, 10)
		case "n_tx":
			res[i] = strconv.FormatInt(b.NTx, 10)
		case "n_tickets":
			res[i] = strconv.FormatInt(b.NTickets, 10)
		case "volume":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Volume), 'f', dec, 64)
		case "fee":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Fee), 'f', dec, 64)
		case "reward":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Reward), 'f', dec, 64)
		case "deposit":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Deposit), 'f', dec, 64)
		case "activated_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.ActivatedSupply), 'f', dec, 64)
		case "minted_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.MintedSupply), 'f', dec, 64)
		case "burned_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.BurnedSupply), 'f', dec, 64)
		case "n_new_accounts":
			res[i] = strconv.FormatInt(b.NewAccounts, 10)
		case "n_new_contracts":
			res[i] = strconv.FormatInt(b.NewContracts, 10)
		case "n_cleared_accounts":
			res[i] = strconv.FormatInt(b.ClearedAccounts, 10)
		case "n_funded_accounts":
			res[i] = strconv.FormatInt(b.FundedAccounts, 10)
		case "gas_used":
			res[i] = strconv.FormatInt(b.GasUsed, 10)
		case "storage_paid":
			res[i] = strconv.FormatInt(b.StoragePaid, 10)
		default:
			continue
		}
	}
	return res, nil
}

func (s *BlockSeries) BuildQuery(ctx *server.Context, args *SeriesRequest) pack.Query {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Series), err))
	}

	// time is auto-added from parser
	if len(args.Columns) == 1 {
		// use all series columns
		args.Columns = blockSeriesNames
	}
	// resolve short column names
	srcNames := make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore count column
		if v == "count" {
			continue
		}
		// ignore non-series columns
		if !blockSeriesNames.Contains(v) {
			panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		srcNames = append(srcNames, v)
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
		WithFields(srcNames...).
		WithOrder(args.Order).
		AndRange("time", args.From.Time(), args.To.Time())

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
		case "columns", "collapse", "start_date", "end_date", "limit", "order", "verbose", "filename", "fill":
			// skip these fields
			continue

		case "baker", "proposer":
			field := "baker_id" // baker id
			if prefix == "proposer" {
				field = "proposer_id" // proposer id
			}
			// parse baker/proposer address and lookup id
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
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != model.ErrNoAccount {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					// Note: when not found we insert an always false condition
					if acc == nil || acc.RowId == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						// add addr id as extra fund_flow condition
						q = q.And(field, mode, acc.RowId)
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != model.ErrNoAccount {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					// skip not found account
					if acc == nil || acc.RowId == 0 {
						continue
					}
					// collect list of account ids
					ids = append(ids, acc.RowId.U64())
				}
				// Note: when list is empty (no accounts were found, the match will
				//       always be false and return no result as expected)
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "voting_period_kind":
			// parse only the first value
			period := tezos.ParseVotingPeriod(val[0])
			if !period.IsValid() {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid voting period '%s'", val[0]), nil))
			}
			q = q.And("p", mode, period)
		default:
			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "volume", "reward", "fee", "deposit",
					"minted_supply", "burned_supply", "activated_supply":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q = q.AndCondition(cond)
				}
			}
		}
	}

	return q
}
