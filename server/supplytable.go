// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

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
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// long -> short form
	supplySourceNames map[string]string
	// short -> long form
	supplyAliasNames map[string]string
	// all aliases as list
	supplyAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Supply{})
	if err != nil {
		log.Fatalf("chain field type error: %v\n", err)
	}
	supplySourceNames = fields.NameMapReverse()
	supplyAllAliases = fields.Aliases()
}

// configurable marshalling helper
type Supply struct {
	model.Supply
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"` // blockchain amount conversion
}

func (s *Supply) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (s *Supply) MarshalJSONVerbose() ([]byte, error) {
	supply := struct {
		RowId               uint64    `json:"row_id"`
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
		RowId:               s.RowId,
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

func (s *Supply) MarshalJSONBrief() ([]byte, error) {
	dec := s.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range s.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, s.RowId, 10)
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

func (s *Supply) MarshalCSV() ([]string, error) {
	dec := s.params.Decimals
	res := make([]string, len(s.columns))
	for i, v := range s.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(s.RowId, 10)
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

func StreamSupplyTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// fetch chain params at current height
	params := ctx.Crawler.ParamsByHeight(-1)

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
			n, ok := supplySourceNames[v]
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
		args.Columns = supplyAllAliases
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
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := supplySourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {

				switch prefix {
				case "cycle":
					if v == "head" {
						currentCycle := params.CycleFromHeight(ctx.Crawler.Height())
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				case "height", "time":
					// need no conversion
				default:
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				}

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
	supply := &Supply{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
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
		err = table.Stream(ctx, q, func(r pack.Row) error {
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(supply); err != nil {
				return err
			}
			if err := enc.Encode(supply); err != nil {
				return err
			}
			count++
			lastId = supply.RowId
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
				if err := r.Decode(supply); err != nil {
					return err
				}
				if err := enc.EncodeRecord(supply); err != nil {
					return err
				}
				count++
				lastId = supply.RowId
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
