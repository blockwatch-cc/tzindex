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
	incomeSourceNames map[string]string
	// short -> long form
	incomeAliasNames map[string]string
	// all aliases as list
	incomeAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Income{})
	if err != nil {
		log.Fatalf("imcome field type error: %v\n", err)
	}
	incomeSourceNames = fields.NameMapReverse()
	incomeAllAliases = fields.Aliases()

	// add extra translations
	incomeSourceNames["address"] = "A"
	incomeAllAliases = append(incomeAllAliases, "address")
}

// configurable marshalling helper
type Income struct {
	model.Income
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"` // blockchain amount conversion
	ctx     *ApiContext     `csv:"-" pack:"-"`
}

func (r *Income) MarshalJSON() ([]byte, error) {
	if r.verbose {
		return r.MarshalJSONVerbose()
	} else {
		return r.MarshalJSONBrief()
	}
}

func (c *Income) MarshalJSONVerbose() ([]byte, error) {
	inc := struct {
		RowId                  uint64  `json:"row_id"`
		Cycle                  int64   `json:"cycle"`
		AccountId              uint64  `json:"account_id"`
		Address                string  `json:"address"`
		Rolls                  int64   `json:"rolls"`
		Balance                float64 `json:"balance"`
		Delegated              float64 `json:"delegated"`
		NDelegations           int64   `json:"n_delegations"`
		NBakingRights          int64   `json:"n_baking_rights"`
		NEndorsingRights       int64   `json:"n_endorsing_rights"`
		Luck                   float64 `json:"luck"`
		LuckPct                float64 `json:"luck_percent"`
		ContributionPct        float64 `json:"contribution_percent"`
		PerformancePct         float64 `json:"performance_percent"`
		NBlocksBaked           int64   `json:"n_blocks_baked"`
		NBlocksLost            int64   `json:"n_blocks_lost"`
		NBlocksStolen          int64   `json:"n_blocks_stolen"`
		NSlotsEndorsed         int64   `json:"n_slots_endorsed"`
		NSlotsMissed           int64   `json:"n_slots_missed"`
		NSeedsRevealed         int64   `json:"n_seeds_revealed"`
		ExpectedIncome         float64 `json:"expected_income"`
		ExpectedBonds          float64 `json:"expected_bonds"`
		TotalIncome            float64 `json:"total_income"`
		TotalBonds             float64 `json:"total_bonds"`
		BakingIncome           float64 `json:"baking_income"`
		EndorsingIncome        float64 `json:"endorsing_income"`
		DoubleBakingIncome     float64 `json:"double_baking_income"`
		DoubleEndorsingIncome  float64 `json:"double_endorsing_income"`
		SeedIncome             float64 `json:"seed_income"`
		FeesIncome             float64 `json:"fees_income"`
		MissedBakingIncome     float64 `json:"missed_baking_income"`
		MissedEndorsingIncome  float64 `json:"missed_endorsing_income"`
		StolenBakingIncome     float64 `json:"stolen_baking_income"`
		TotalLost              float64 `json:"total_lost"`
		LostAccusationFees     float64 `json:"lost_accusation_fees"`
		LostAccusationRewards  float64 `json:"lost_accusation_rewards"`
		LostAccusationDeposits float64 `json:"lost_accusation_deposits"`
		LostRevelationFees     float64 `json:"lost_revelation_fees"`
		LostRevelationRewards  float64 `json:"lost_revelation_rewards"`
	}{
		RowId:                  c.RowId,
		Cycle:                  c.Cycle,
		AccountId:              c.AccountId.Value(),
		Address:                lookupAddress(c.ctx, c.AccountId).String(),
		Rolls:                  c.Rolls,
		Balance:                c.params.ConvertValue(c.Balance),
		Delegated:              c.params.ConvertValue(c.Delegated),
		NDelegations:           c.NDelegations,
		NBakingRights:          c.NBakingRights,
		NEndorsingRights:       c.NEndorsingRights,
		Luck:                   c.params.ConvertValue(c.Luck),
		LuckPct:                float64(c.LuckPct) / 100,
		ContributionPct:        float64(c.ContributionPct) / 100,
		PerformancePct:         float64(c.PerformancePct) / 100,
		NBlocksBaked:           c.NBlocksBaked,
		NBlocksLost:            c.NBlocksLost,
		NBlocksStolen:          c.NBlocksStolen,
		NSlotsEndorsed:         c.NSlotsEndorsed,
		NSlotsMissed:           c.NSlotsMissed,
		NSeedsRevealed:         c.NSeedsRevealed,
		ExpectedIncome:         c.params.ConvertValue(c.ExpectedIncome),
		ExpectedBonds:          c.params.ConvertValue(c.ExpectedBonds),
		TotalIncome:            c.params.ConvertValue(c.TotalIncome),
		TotalBonds:             c.params.ConvertValue(c.TotalBonds),
		BakingIncome:           c.params.ConvertValue(c.BakingIncome),
		EndorsingIncome:        c.params.ConvertValue(c.EndorsingIncome),
		DoubleBakingIncome:     c.params.ConvertValue(c.DoubleBakingIncome),
		DoubleEndorsingIncome:  c.params.ConvertValue(c.DoubleEndorsingIncome),
		SeedIncome:             c.params.ConvertValue(c.SeedIncome),
		FeesIncome:             c.params.ConvertValue(c.FeesIncome),
		MissedBakingIncome:     c.params.ConvertValue(c.MissedBakingIncome),
		MissedEndorsingIncome:  c.params.ConvertValue(c.MissedEndorsingIncome),
		StolenBakingIncome:     c.params.ConvertValue(c.StolenBakingIncome),
		TotalLost:              c.params.ConvertValue(c.TotalLost),
		LostAccusationFees:     c.params.ConvertValue(c.LostAccusationFees),
		LostAccusationRewards:  c.params.ConvertValue(c.LostAccusationRewards),
		LostAccusationDeposits: c.params.ConvertValue(c.LostAccusationDeposits),
		LostRevelationFees:     c.params.ConvertValue(c.LostRevelationFees),
		LostRevelationRewards:  c.params.ConvertValue(c.LostRevelationRewards),
	}
	return json.Marshal(inc)
}

func (c *Income) MarshalJSONBrief() ([]byte, error) {
	dec := c.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range c.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, c.RowId, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, c.Cycle, 10)
		case "account_id":
			buf = strconv.AppendUint(buf, c.AccountId.Value(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, lookupAddress(c.ctx, c.AccountId).String())
		case "rolls":
			buf = strconv.AppendInt(buf, int64(c.Rolls), 10)
		case "balance":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.Balance), 'f', dec, 64)
		case "delegated":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.Delegated), 'f', dec, 64)
		case "n_delegations":
			buf = strconv.AppendInt(buf, int64(c.NDelegations), 10)
		case "n_baking_rights":
			buf = strconv.AppendInt(buf, int64(c.NBakingRights), 10)
		case "n_endorsing_rights":
			buf = strconv.AppendInt(buf, int64(c.NEndorsingRights), 10)
		case "luck":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.Luck), 'f', dec, 64)
		case "luck_percent":
			buf = strconv.AppendFloat(buf, float64(c.LuckPct)/100, 'f', 2, 64)
		case "contribution_percent":
			buf = strconv.AppendFloat(buf, float64(c.ContributionPct)/100, 'f', 2, 64)
		case "performance_percent":
			buf = strconv.AppendFloat(buf, float64(c.PerformancePct)/100, 'f', 2, 64)
		case "n_blocks_baked":
			buf = strconv.AppendInt(buf, int64(c.NBlocksBaked), 10)
		case "n_blocks_lost":
			buf = strconv.AppendInt(buf, int64(c.NBlocksLost), 10)
		case "n_blocks_stolen":
			buf = strconv.AppendInt(buf, int64(c.NBlocksStolen), 10)
		case "n_slots_endorsed":
			buf = strconv.AppendInt(buf, int64(c.NSlotsEndorsed), 10)
		case "n_slots_missed":
			buf = strconv.AppendInt(buf, int64(c.NSlotsMissed), 10)
		case "n_seeds_revealed":
			buf = strconv.AppendInt(buf, int64(c.NSeedsRevealed), 10)
		case "expected_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.ExpectedIncome), 'f', dec, 64)
		case "expected_bonds":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.ExpectedBonds), 'f', dec, 64)
		case "total_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.TotalIncome), 'f', dec, 64)
		case "total_bonds":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.TotalBonds), 'f', dec, 64)
		case "baking_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.BakingIncome), 'f', dec, 64)
		case "endorsing_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.EndorsingIncome), 'f', dec, 64)
		case "double_baking_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.DoubleBakingIncome), 'f', dec, 64)
		case "double_endorsing_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.DoubleEndorsingIncome), 'f', dec, 64)
		case "seed_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.SeedIncome), 'f', dec, 64)
		case "fees_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.FeesIncome), 'f', dec, 64)
		case "missed_baking_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.MissedBakingIncome), 'f', dec, 64)
		case "missed_endorsing_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.MissedEndorsingIncome), 'f', dec, 64)
		case "stolen_baking_income":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.StolenBakingIncome), 'f', dec, 64)
		case "total_lost":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.TotalLost), 'f', dec, 64)
		case "lost_accusation_fees":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.LostAccusationFees), 'f', dec, 64)
		case "lost_accusation_rewards":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.LostAccusationRewards), 'f', dec, 64)
		case "lost_accusation_deposits":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.LostAccusationDeposits), 'f', dec, 64)
		case "lost_revelation_fees":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.LostRevelationFees), 'f', dec, 64)
		case "lost_revelation_rewards":
			buf = strconv.AppendFloat(buf, c.params.ConvertValue(c.LostRevelationRewards), 'f', dec, 64)
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

func (c *Income) MarshalCSV() ([]string, error) {
	dec := c.params.Decimals
	res := make([]string, len(c.columns))
	for i, v := range c.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(c.RowId, 10)
		case "cycle":
			res[i] = strconv.FormatInt(c.Cycle, 10)
		case "account_id":
			res[i] = strconv.FormatUint(c.AccountId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(lookupAddress(c.ctx, c.AccountId).String())
		case "rolls":
			res[i] = strconv.FormatInt(int64(c.Rolls), 10)
		case "balance":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.Balance), 'f', dec, 64)
		case "delegated":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.Delegated), 'f', dec, 64)
		case "n_delegations":
			res[i] = strconv.FormatInt(int64(c.NDelegations), 10)
		case "n_baking_rights":
			res[i] = strconv.FormatInt(int64(c.NBakingRights), 10)
		case "n_endorsing_rights":
			res[i] = strconv.FormatInt(int64(c.NEndorsingRights), 10)
		case "luck":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.Luck), 'f', dec, 64)
		case "luck_percent":
			res[i] = strconv.FormatFloat(float64(c.LuckPct)/100, 'f', 2, 64)
		case "contribution_percent":
			res[i] = strconv.FormatFloat(float64(c.ContributionPct)/100, 'f', 2, 64)
		case "performance_percent":
			res[i] = strconv.FormatFloat(float64(c.PerformancePct)/100, 'f', 2, 64)
		case "n_blocks_baked":
			res[i] = strconv.FormatInt(int64(c.NBlocksBaked), 10)
		case "n_blocks_lost":
			res[i] = strconv.FormatInt(int64(c.NBlocksLost), 10)
		case "n_blocks_stolen":
			res[i] = strconv.FormatInt(int64(c.NBlocksStolen), 10)
		case "n_slots_endorsed":
			res[i] = strconv.FormatInt(int64(c.NSlotsEndorsed), 10)
		case "n_slots_missed":
			res[i] = strconv.FormatInt(int64(c.NSlotsMissed), 10)
		case "n_seeds_revealed":
			res[i] = strconv.FormatInt(int64(c.NSeedsRevealed), 10)
		case "expected_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.ExpectedIncome), 'f', dec, 64)
		case "expected_bonds":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.ExpectedBonds), 'f', dec, 64)
		case "total_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.TotalIncome), 'f', dec, 64)
		case "total_bonds":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.TotalBonds), 'f', dec, 64)
		case "baking_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.BakingIncome), 'f', dec, 64)
		case "endorsing_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.EndorsingIncome), 'f', dec, 64)
		case "double_baking_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.DoubleBakingIncome), 'f', dec, 64)
		case "double_endorsing_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.DoubleEndorsingIncome), 'f', dec, 64)
		case "seed_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.SeedIncome), 'f', dec, 64)
		case "fees_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.FeesIncome), 'f', dec, 64)
		case "missed_baking_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.MissedBakingIncome), 'f', dec, 64)
		case "missed_endorsing_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.MissedEndorsingIncome), 'f', dec, 64)
		case "stolen_baking_income":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.StolenBakingIncome), 'f', dec, 64)
		case "total_lost":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.TotalLost), 'f', dec, 64)
		case "lost_accusation_fees":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.LostAccusationFees), 'f', dec, 64)
		case "lost_accusation_rewards":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.LostAccusationRewards), 'f', dec, 64)
		case "lost_accusation_deposits":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.LostAccusationDeposits), 'f', dec, 64)
		case "lost_revelation_fees":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.LostRevelationFees), 'f', dec, 64)
		case "lost_revelation_rewards":
			res[i] = strconv.FormatFloat(c.params.ConvertValue(c.LostRevelationRewards), 'f', dec, 64)
		default:
			continue
		}
	}
	return res, nil
}

func StreamIncomeTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
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
			n, ok := incomeSourceNames[v]
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
		args.Columns = incomeAllAliases
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
		case "address":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
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
						Field: table.Fields().Find("A"), // account id
						Mode:  mode,
						Value: uint64(math.MaxUint64),
						Raw:   "account not found", // debugging aid
					})
				} else {
					// add addr id as extra fund_flow condition
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find("A"), // account id
						Mode:  mode,
						Value: acc.RowId.Value(),
						Raw:   val[0], // debugging aid
					})
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
					Field: table.Fields().Find("A"), // account id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := incomeSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "cycle":
					if v == "head" {
						currentCycle := params.CycleFromHeight(ctx.Crawler.Height())
						v = strconv.FormatInt(int64(currentCycle), 10)
					}

				case "luck_percent", "contribution_percent", "performance_percent":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(int64(fval*10000), 10))
					}
					v = strings.Join(fvals, ",")

				case "luck", "balance", "delegated", "expected_income", "expected_bonds",
					"total_income", "total_bonds", "baking_income", "endorsing_income",
					"double_baking_income", "double_endorsing_income", "seed_income",
					"fees_income", "missed_baking_income", "missed_endorsing_income",
					"stolen_baking_income", "total_lost", "lost_accusation_fees",
					"lost_accusation_rewards", "lost_accusation_deposits",
					"lost_revelation_fees", "lost_revelation_rewards":
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
	inc := &Income{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  params,
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
			if err := r.Decode(inc); err != nil {
				return err
			}
			if err := enc.Encode(inc); err != nil {
				return err
			}
			count++
			lastId = inc.RowId
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
				if err := r.Decode(inc); err != nil {
					return err
				}
				if err := enc.EncodeRecord(inc); err != nil {
					return err
				}
				count++
				lastId = inc.RowId
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
