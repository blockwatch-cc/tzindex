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
	supplySeriesNames util.StringList
)

func init() {
	fields, err := pack.Fields(&model.Supply{})
	if err != nil {
		log.Fatalf("supply field type error: %v\n", err)
	}
	// strip row_id (first field)
	supplySeriesNames = util.StringList(fields.Aliases()[1:])
	supplySeriesNames.AddUnique("count")
}

// configurable marshalling helper
type SupplySeries struct {
	model.Supply

	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	verbose bool            // cond. marshal
	null    bool
}

var _ SeriesBucket = (*SupplySeries)(nil)

func (s *SupplySeries) Init(params *tezos.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *SupplySeries) IsEmpty() bool {
	return s.Supply.Height == 0 || s.Supply.Timestamp.IsZero()
}

func (s *SupplySeries) Add(m SeriesModel) {
	o := m.(*model.Supply)
	s.Supply = *o
}

func (s *SupplySeries) Reset() {
	s.Supply.Timestamp = time.Time{}
	s.null = false
}

func (s *SupplySeries) Null(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	s.null = true
	return s
}

func (s *SupplySeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *SupplySeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *SupplySeries) Time() time.Time {
	return s.Timestamp
}

func (s *SupplySeries) Clone() SeriesBucket {
	c := &SupplySeries{
		Supply: s.Supply,
	}
	c.columns = s.columns
	c.params = s.params
	c.verbose = s.verbose
	c.null = s.null
	return c
}

func (s *SupplySeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	// unused, sematically there is one supply table entry per block
	return s
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
		Count               int       `json:"count"`
		Total               float64   `json:"total"`
		Activated           float64   `json:"activated"`
		Unclaimed           float64   `json:"unclaimed"`
		Liquid              float64   `json:"liquid"`
		Delegated           float64   `json:"delegated"`
		Staking             float64   `json:"staking"`
		Shielded            float64   `json:"shielded"`
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
		Count:               1,
		Total:               s.params.ConvertValue(s.Total),
		Activated:           s.params.ConvertValue(s.Activated),
		Unclaimed:           s.params.ConvertValue(s.Unclaimed),
		Liquid:              s.params.ConvertValue(s.Liquid),
		Delegated:           s.params.ConvertValue(s.Delegated),
		Staking:             s.params.ConvertValue(s.Staking),
		Shielded:            s.params.ConvertValue(s.Shielded),
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
		if s.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(s.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "height":
				buf = strconv.AppendInt(buf, s.Height, 10)
			case "cycle":
				buf = strconv.AppendInt(buf, s.Cycle, 10)
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(s.Timestamp), 10)
			case "count":
				buf = strconv.AppendInt(buf, 1, 10)
			case "total":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Total), 'f', dec, 64)
			case "activated":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Activated), 'f', dec, 64)
			case "unclaimed":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Unclaimed), 'f', dec, 64)
			case "liquid":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Liquid), 'f', dec, 64)
			case "delegated":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Delegated), 'f', dec, 64)
			case "staking":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Staking), 'f', dec, 64)
			case "shielded":
				buf = strconv.AppendFloat(buf, s.params.ConvertValue(s.Shielded), 'f', dec, 64)
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
		if s.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(s.Timestamp.Format(time.RFC3339))
			default:
				continue
			}
		}
		switch v {
		case "height":
			res[i] = strconv.FormatInt(s.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(s.Cycle, 10)
		case "time":
			res[i] = strconv.Quote(s.Timestamp.Format(time.RFC3339))
		case "count":
			res[i] = strconv.FormatInt(1, 10)
		case "total":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Total), 'f', dec, 64)
		case "activated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Activated), 'f', dec, 64)
		case "unclaimed":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Unclaimed), 'f', dec, 64)
		case "liquid":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Liquid), 'f', dec, 64)
		case "delegated":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Delegated), 'f', dec, 64)
		case "staking":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Staking), 'f', dec, 64)
		case "shielded":
			res[i] = strconv.FormatFloat(s.params.ConvertValue(s.Shielded), 'f', dec, 64)
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

func (s *SupplySeries) BuildQuery(ctx *ApiContext, args *SeriesRequest) pack.Query {
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
		// ignore count column
		if v == "count" {
			continue
		}
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
	return pack.NewQuery(ctx.RequestID, table).
		WithFields(srcNames...).
		WithOrder(args.Order).
		AndRange("time", args.From.Time(), args.To.Time())
}
