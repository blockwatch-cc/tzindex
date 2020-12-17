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
	supplySeriesNames util.StringList
)

func init() {
	fields, err := pack.Fields(&model.Supply{})
	if err != nil {
		log.Fatalf("supply field type error: %v\n", err)
	}
	// strip row_id (first field)
	supplySeriesNames = util.StringList(fields.Aliases()[1:])
}

// configurable marshalling helper
type SupplySeries struct {
	model.Supply
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"` // blockchain amount conversion
}

func (s *SupplySeries) Reset() {
	s.Supply.Timestamp = time.Time{}
}

func (s *SupplySeries) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (s *SupplySeries) MarshalJSONVerbose() ([]byte, error) {
	supply := struct {
		Height              int64     `json:"height"`
		Cycle               int64     `json:"cycle"`
		Timestamp           time.Time `json:"time"`
		Total               float64   `json:"total"`
		Activated           float64   `json:"activated"`
		Unclaimed           float64   `json:"unclaimed"`
		Vested              float64   `json:"vested"`
		Unvested            float64   `json:"unvested"`
		Circulating         float64   `json:"circulating"`
		Delegated           float64   `json:"delegated"`
		Staking             float64   `json:"staking"`
		ActiveDelegated     float64   `json:"active_delegated"`
		ActiveStaking       float64   `json:"active_staking"`
		InactiveDelegated   float64   `json:"inactive_delegated"`
		InactiveStaking     float64   `json:"inactive_staking"`
		Minted              float64   `json:"minted"`
		MintedBaking        float64   `json:"minted_baking"`
		MintedEndorsing     float64   `json:"minted_endorsing"`
		MintedSeeding       float64   `json:"minted_seeding"`
		MintedAirdrop       float64   `json:"minted_airdrop"`
		Burned              float64   `json:"burned"`
		BurnedDoubleBaking  float64   `json:"burned_double_baking"`
		BurnedDoubleEndorse float64   `json:"burned_double_endorse"`
		BurnedOrigination   float64   `json:"burned_origination"`
		BurnedImplicit      float64   `json:"burned_implicit"`
		BurnedSeedMiss      float64   `json:"burned_seed_miss"`
		Frozen              float64   `json:"frozen"`
		FrozenDeposits      float64   `json:"frozen_deposits"`
		FrozenRewards       float64   `json:"frozen_rewards"`
		FrozenFees          float64   `json:"frozen_fees"`
	}{
		Height:              s.Height,
		Cycle:               s.Cycle,
		Timestamp:           s.Timestamp,
		Total:               s.params.ConvertValue(s.Total),
		Activated:           s.params.ConvertValue(s.Activated),
		Unclaimed:           s.params.ConvertValue(s.Unclaimed),
		Vested:              s.params.ConvertValue(s.Vested),
		Unvested:            s.params.ConvertValue(s.Unvested),
		Circulating:         s.params.ConvertValue(s.Circulating),
		Delegated:           s.params.ConvertValue(s.Delegated),
		Staking:             s.params.ConvertValue(s.Staking),
		ActiveDelegated:     s.params.ConvertValue(s.ActiveDelegated),
		ActiveStaking:       s.params.ConvertValue(s.ActiveStaking),
		InactiveDelegated:   s.params.ConvertValue(s.InactiveDelegated),
		InactiveStaking:     s.params.ConvertValue(s.InactiveStaking),
		Minted:              s.params.ConvertValue(s.Minted),
		MintedBaking:        s.params.ConvertValue(s.MintedBaking),
		MintedEndorsing:     s.params.ConvertValue(s.MintedEndorsing),
		MintedSeeding:       s.params.ConvertValue(s.MintedSeeding),
		MintedAirdrop:       s.params.ConvertValue(s.MintedAirdrop),
		Burned:              s.params.ConvertValue(s.Burned),
		BurnedDoubleBaking:  s.params.ConvertValue(s.BurnedDoubleBaking),
		BurnedDoubleEndorse: s.params.ConvertValue(s.BurnedDoubleEndorse),
		BurnedOrigination:   s.params.ConvertValue(s.BurnedOrigination),
		BurnedImplicit:      s.params.ConvertValue(s.BurnedImplicit),
		BurnedSeedMiss:      s.params.ConvertValue(s.BurnedSeedMiss),
		Frozen:              s.params.ConvertValue(s.Frozen),
		FrozenDeposits:      s.params.ConvertValue(s.FrozenDeposits),
		FrozenRewards:       s.params.ConvertValue(s.FrozenRewards),
		FrozenFees:          s.params.ConvertValue(s.FrozenFees),
	}
	return json.Marshal(supply)
}

func (s *SupplySeries) MarshalJSONBrief() ([]byte, error) {
	dec := s.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range s.columns {
		switch v {
		case "height":
			buf = strconv.AppendInt(buf, s.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, s.Cycle, 10)
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(s.Timestamp), 10)
		case "total":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Total), 'f', dec, 64)
		case "activated":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Activated), 'f', dec, 64)
		case "unclaimed":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Unclaimed), 'f', dec, 64)
		case "vested":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Vested), 'f', dec, 64)
		case "unvested":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Unvested), 'f', dec, 64)
		case "circulating":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Circulating), 'f', dec, 64)
		case "delegated":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Delegated), 'f', dec, 64)
		case "staking":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Staking), 'f', dec, 64)
		case "active_delegated":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.ActiveDelegated), 'f', dec, 64)
		case "active_staking":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.ActiveStaking), 'f', dec, 64)
		case "inactive_delegated":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.InactiveDelegated), 'f', dec, 64)
		case "inactive_staking":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.InactiveStaking), 'f', dec, 64)
		case "minted":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Minted), 'f', dec, 64)
		case "minted_baking":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.MintedBaking), 'f', dec, 64)
		case "minted_endorsing":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.MintedEndorsing), 'f', dec, 64)
		case "minted_seeding":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.MintedSeeding), 'f', dec, 64)
		case "minted_airdrop":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.MintedAirdrop), 'f', dec, 64)
		case "burned":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Burned), 'f', dec, 64)
		case "burned_double_baking":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.BurnedDoubleBaking), 'f', dec, 64)
		case "burned_double_endorse":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.BurnedDoubleEndorse), 'f', dec, 64)
		case "burned_origination":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.BurnedOrigination), 'f', dec, 64)
		case "burned_implicit":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.BurnedImplicit), 'f', dec, 64)
		case "burned_seed_miss":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.BurnedSeedMiss), 'f', dec, 64)
		case "frozen":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Frozen), 'f', dec, 64)
		case "frozen_deposits":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.FrozenDeposits), 'f', dec, 64)
		case "frozen_rewards":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.FrozenRewards), 'f', dec, 64)
		case "frozen_fees":
			buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.FrozenFees), 'f', dec, 64)
		default:
			continue
		}
		if i < len(s.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (s *SupplySeries) MarshalCSV() ([]string, error) {
	dec := s.params.Decimals
	res := make([]string, len(s.columns))
	for i, v := range s.columns {
		switch v {
		case "height":
			res[i] = strconv.FormatInt(s.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(s.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(s.Timestamp.Format(time.RFC3339))
		case "total":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Total), 'f', dec, 64)
		case "activated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Activated), 'f', dec, 64)
		case "unclaimed":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Unclaimed), 'f', dec, 64)
		case "vested":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Vested), 'f', dec, 64)
		case "unvested":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Unvested), 'f', dec, 64)
		case "circulating":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Circulating), 'f', dec, 64)
		case "delegated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Delegated), 'f', dec, 64)
		case "staking":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Staking), 'f', dec, 64)
		case "active_delegated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.ActiveDelegated), 'f', dec, 64)
		case "active_staking":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.ActiveStaking), 'f', dec, 64)
		case "inactive_delegated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.InactiveDelegated), 'f', dec, 64)
		case "inactive_staking":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.InactiveStaking), 'f', dec, 64)
		case "minted":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Minted), 'f', dec, 64)
		case "minted_baking":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.MintedBaking), 'f', dec, 64)
		case "minted_endorsing":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.MintedEndorsing), 'f', dec, 64)
		case "minted_seeding":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.MintedSeeding), 'f', dec, 64)
		case "minted_airdrop":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.MintedAirdrop), 'f', dec, 64)
		case "burned":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Burned), 'f', dec, 64)
		case "burned_double_baking":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.BurnedDoubleBaking), 'f', dec, 64)
		case "burned_double_endorse":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.BurnedDoubleEndorse), 'f', dec, 64)
		case "burned_origination":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.BurnedOrigination), 'f', dec, 64)
		case "burned_implicit":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.BurnedImplicit), 'f', dec, 64)
		case "burned_seed_miss":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.BurnedSeedMiss), 'f', dec, 64)
		case "frozen":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Frozen), 'f', dec, 64)
		case "frozen_deposits":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.FrozenDeposits), 'f', dec, 64)
		case "frozen_rewards":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.FrozenRewards), 'f', dec, 64)
		case "frozen_fees":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.FrozenFees), 'f', dec, 64)
		default:
			continue
		}
	}
	return res, nil
}

func StreamSupplySeries(ctx *ApiContext, args *SeriesRequest) (interface{}, int) {
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
		args.Columns = supplySeriesNames
	}
	// resolve short column names
	srcNames = make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore non-series columns
		if !supplySeriesNames.Contains(v) {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		n, ok := supplySourceNames[v]
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
	ss := &SupplySeries{params: params, verbose: args.Verbose, columns: args.Columns}
	sm := &model.Supply{}
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
			if err := r.Decode(sm); err != nil {
				return err
			}

			// output SupplySeries when valid and time has crossed next boundary
			if !ss.Timestamp.IsZero() && (sm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
				// output current data
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}
				if err := enc.Encode(ss); err != nil {
					return err
				}
				count++
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				ss.Reset()
			}

			// init next time window from data
			if ss.Timestamp.IsZero() {
				ss.Timestamp = sm.Timestamp.Truncate(window)
				nextBucketTime = ss.Timestamp.Add(window * time.Duration(mul))
			}

			// keep latest data
			ss.Supply = *sm
			return nil
		})
		// don't handle error here, will be picked up by trailer
		if err == nil {
			// output last series element
			if !ss.Timestamp.IsZero() {
				if needComma {
					io.WriteString(ctx.ResponseWriter, ",")
				}
				err = enc.Encode(ss)
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
				if err := r.Decode(sm); err != nil {
					return err
				}

				// output SupplySeries when valid and time has crossed next boundary
				if !ss.Timestamp.IsZero() && (sm.Timestamp.Before(nextBucketTime) != (mul == 1)) {
					// output accumulated data
					if err := enc.EncodeRecord(ss); err != nil {
						return err
					}
					count++
					if args.Limit > 0 && count == int(args.Limit) {
						return io.EOF
					}
					ss.Reset()
				}

				// init next time window from data
				if ss.Timestamp.IsZero() {
					ss.Timestamp = sm.Timestamp.Truncate(window)
					nextBucketTime = ss.Timestamp.Add(window * time.Duration(mul))
				}

				// keep latest data
				ss.Supply = *sm
				return nil
			})
			if err == nil {
				// output last series element
				if !ss.Timestamp.IsZero() {
					err = enc.EncodeRecord(ss)
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
