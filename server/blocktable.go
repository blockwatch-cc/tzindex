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
	blockSourceNames map[string]string
	// short -> long form
	blockAliasNames map[string]string
	// all aliases as list
	blockAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Block{})
	if err != nil {
		log.Fatalf("block field type error: %v\n", err)
	}
	blockSourceNames = fields.NameMapReverse()
	blockAllAliases = fields.Aliases()

	// add extra translations
	blockSourceNames["pct_account_reuse"] = "-"
	blockSourceNames["baker"] = "B"

	blockAllAliases = append(blockAllAliases, "pct_account_reuse")
	blockAllAliases = append(blockAllAliases, "baker")
}

// configurable marshalling helper
type Block struct {
	model.Block
	verbose bool            `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params   `csv:"-" pack:"-"` // blockchain amount conversion
	ctx     *ApiContext     `csv:"-" pack:"-"`
}

func (b *Block) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *Block) MarshalJSONVerbose() ([]byte, error) {
	block := struct {
		RowId               uint64                 `json:"row_id"`
		ParentId            uint64                 `json:"parent_id"`
		Hash                string                 `json:"hash"`
		IsOrphan            bool                   `json:"is_orphan,omitempty"`
		Timestamp           int64                  `json:"time"` // convert to UNIX milliseconds
		Height              int64                  `json:"height"`
		Cycle               int64                  `json:"cycle"`
		IsCycleSnapshot     bool                   `json:"is_cycle_snapshot"`
		Solvetime           int                    `json:"solvetime"`
		Version             int                    `json:"version"`
		Validation          int                    `json:"validation_pass"`
		Fitness             uint64                 `json:"fitness"`
		Priority            int                    `json:"priority"`
		Nonce               uint64                 `json:"nonce"`
		VotingPeriodKind    chain.VotingPeriodKind `json:"voting_period_kind"`
		BakerId             uint64                 `json:"baker_id"`
		Baker               string                 `json:"baker"`
		SlotsEndorsed       uint32                 `json:"endorsed_slots"`
		NSlotsEndorsed      int                    `json:"n_endorsed_slots"`
		NOps                int                    `json:"n_ops"`
		NOpsFailed          int                    `json:"n_ops_failed"`
		NOpsContract        int                    `json:"n_ops_contract"`
		NTx                 int                    `json:"n_tx"`
		NActivation         int                    `json:"n_activation"`
		NSeedNonce          int                    `json:"n_seed_nonce_revelation"`
		N2Baking            int                    `json:"n_double_baking_evidence"`
		N2Endorsement       int                    `json:"n_double_endorsement_evidence"`
		NEndorsement        int                    `json:"n_endorsement"`
		NDelegation         int                    `json:"n_delegation"`
		NReveal             int                    `json:"n_reveal"`
		NOrigination        int                    `json:"n_origination"`
		NProposal           int                    `json:"n_proposal"`
		NBallot             int                    `json:"n_ballot"`
		Volume              float64                `json:"volume"`
		Fees                float64                `json:"fees"`
		Rewards             float64                `json:"rewards"`
		Deposits            float64                `json:"deposits"`
		UnfrozenFees        float64                `json:"unfrozen_fees"`
		UnfrozenRewards     float64                `json:"unfrozen_rewards"`
		UnfrozenDeposits    float64                `json:"unfrozen_deposits"`
		ActivatedSupply     float64                `json:"activated_supply"`
		BurnedSupply        float64                `json:"burned_supply"`
		SeenAccounts        int                    `json:"n_accounts"`
		NewAccounts         int                    `json:"n_new_accounts"`
		NewImplicitAccounts int                    `json:"n_new_implicit"`
		NewManagedAccounts  int                    `json:"n_new_managed"`
		NewContracts        int                    `json:"n_new_contracts"`
		ClearedAccounts     int                    `json:"n_cleared_accounts"`
		FundedAccounts      int                    `json:"n_funded_accounts"`
		GasLimit            int64                  `json:"gas_limit"`
		GasUsed             int64                  `json:"gas_used"`
		GasPrice            float64                `json:"gas_price"`
		StorageSize         int64                  `json:"storage_size"`
		TDD                 float64                `json:"days_destroyed"`
		PctAccountsReused   float64                `json:"pct_account_reuse"`
	}{
		RowId:               b.RowId,
		ParentId:            b.ParentId,
		Hash:                b.Hash.String(),
		IsOrphan:            b.IsOrphan,
		Timestamp:           util.UnixMilliNonZero(b.Timestamp),
		Height:              b.Height,
		Cycle:               b.Cycle,
		IsCycleSnapshot:     b.IsCycleSnapshot,
		Solvetime:           b.Solvetime,
		Version:             b.Version,
		Validation:          b.Validation,
		Fitness:             b.Fitness,
		Priority:            b.Priority,
		Nonce:               b.Nonce,
		VotingPeriodKind:    b.VotingPeriodKind,
		BakerId:             b.BakerId.Value(),
		Baker:               lookupAddress(b.ctx, b.BakerId).String(),
		SlotsEndorsed:       b.SlotsEndorsed,
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
		SeenAccounts:        b.SeenAccounts,
		NewAccounts:         b.NewAccounts,
		NewImplicitAccounts: b.NewImplicitAccounts,
		NewManagedAccounts:  b.NewManagedAccounts,
		NewContracts:        b.NewContracts,
		ClearedAccounts:     b.ClearedAccounts,
		FundedAccounts:      b.FundedAccounts,
		GasLimit:            b.GasLimit,
		GasUsed:             b.GasUsed,
		GasPrice:            b.GasPrice,
		StorageSize:         b.StorageSize,
		TDD:                 b.TDD,
	}
	if b.SeenAccounts > 0 {
		block.PctAccountsReused = float64(b.SeenAccounts-b.NewAccounts) / float64(b.SeenAccounts) * 100
	}
	return json.Marshal(block)
}

func (b *Block) MarshalJSONBrief() ([]byte, error) {
	dec := b.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, b.RowId, 10)
		case "parent_id":
			buf = strconv.AppendUint(buf, b.ParentId, 10)
		case "hash":
			buf = strconv.AppendQuote(buf, b.Hash.String())
		case "is_orphan":
			if b.IsOrphan {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "time":
			buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
		case "height":
			buf = strconv.AppendInt(buf, b.Height, 10)
		case "cycle":
			buf = strconv.AppendInt(buf, b.Cycle, 10)
		case "is_cycle_snapshot":
			if b.IsCycleSnapshot {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "solvetime":
			buf = strconv.AppendInt(buf, int64(b.Solvetime), 10)
		case "version":
			buf = strconv.AppendInt(buf, int64(b.Version), 10)
		case "validation_pass":
			buf = strconv.AppendInt(buf, int64(b.Validation), 10)
		case "fitness":
			buf = strconv.AppendUint(buf, b.Fitness, 10)
		case "priority":
			buf = strconv.AppendInt(buf, int64(b.Priority), 10)
		case "nonce":
			buf = strconv.AppendUint(buf, b.Nonce, 10)
		case "voting_period_kind":
			buf = strconv.AppendQuote(buf, b.VotingPeriodKind.String())
		case "baker_id":
			buf = strconv.AppendUint(buf, b.BakerId.Value(), 10)
		case "baker":
			buf = strconv.AppendQuote(buf, lookupAddress(b.ctx, b.BakerId).String())
		case "endorsed_slots":
			buf = strconv.AppendInt(buf, int64(b.SlotsEndorsed), 10)
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
		case "n_accounts":
			buf = strconv.AppendInt(buf, int64(b.SeenAccounts), 10)
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
		case "gas_limit":
			buf = strconv.AppendInt(buf, b.GasLimit, 10)
		case "gas_used":
			buf = strconv.AppendInt(buf, b.GasUsed, 10)
		case "gas_price":
			buf = strconv.AppendFloat(buf, b.GasPrice, 'f', 3, 64)
		case "storage_size":
			buf = strconv.AppendInt(buf, b.StorageSize, 10)
		case "days_destroyed":
			buf = strconv.AppendFloat(buf, b.TDD, 'f', -1, 64)
		case "pct_account_reuse":
			var reuse float64
			if b.SeenAccounts > 0 {
				reuse = float64(b.SeenAccounts-b.NewAccounts) / float64(b.SeenAccounts) * 100
			}
			buf = strconv.AppendFloat(buf, reuse, 'f', -1, 64)
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

func (b *Block) MarshalCSV() ([]string, error) {
	dec := b.params.Decimals
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(b.RowId, 10)
		case "parent_id":
			res[i] = strconv.FormatUint(b.ParentId, 10)
		case "hash":
			res[i] = strconv.Quote(b.Hash.String())
		case "is_orphan":
			res[i] = strconv.FormatBool(b.IsOrphan)
		case "time":
			res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
		case "height":
			res[i] = strconv.FormatInt(b.Height, 10)
		case "cycle":
			res[i] = strconv.FormatInt(b.Cycle, 10)
		case "is_cycle_snapshot":
			res[i] = strconv.FormatBool(b.IsCycleSnapshot)
		case "solvetime":
			res[i] = strconv.FormatInt(int64(b.Solvetime), 10)
		case "version":
			res[i] = strconv.FormatInt(int64(b.Version), 10)
		case "validation_pass":
			res[i] = strconv.FormatInt(int64(b.Validation), 10)
		case "fitness":
			res[i] = strconv.FormatUint(b.Fitness, 10)
		case "priority":
			res[i] = strconv.FormatInt(int64(b.Priority), 10)
		case "nonce":
			res[i] = strconv.FormatUint(b.Nonce, 10)
		case "voting_period_kind":
			res[i] = strconv.Quote(b.VotingPeriodKind.String())
		case "baker_id":
			res[i] = strconv.FormatUint(b.BakerId.Value(), 10)
		case "baker":
			res[i] = strconv.Quote(lookupAddress(b.ctx, b.BakerId).String())
		case "endorsed_slots":
			res[i] = strconv.FormatInt(int64(b.SlotsEndorsed), 10)
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
		case "n_accounts":
			res[i] = strconv.FormatInt(int64(b.SeenAccounts), 10)
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
		case "gas_limit":
			res[i] = strconv.FormatInt(b.GasLimit, 10)
		case "gas_used":
			res[i] = strconv.FormatInt(b.GasUsed, 10)
		case "gas_price":
			res[i] = strconv.FormatFloat(b.GasPrice, 'f', 3, 64)
		case "storage_size":
			res[i] = strconv.FormatInt(b.StorageSize, 10)
		case "days_destroyed":
			res[i] = strconv.FormatFloat(b.TDD, 'f', -1, 64)
		case "pct_account_reuse":
			var reuse float64
			if b.SeenAccounts > 0 {
				reuse = float64(b.SeenAccounts-b.NewAccounts) / float64(b.SeenAccounts) * 100
			}
			res[i] = strconv.FormatFloat(reuse, 'f', -1, 64)
		default:
			continue
		}
	}
	return res, nil
}

func StreamBlockTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
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
			n, ok := blockSourceNames[v]
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
		args.Columns = blockAllAliases
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
		case "hash":
			// special hash type to []byte conversion
			hashes := make([][]byte, len(val))
			for i, v := range val {
				h, err := chain.ParseBlockHash(v)
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid block hash '%s'", val), err))
				}
				hashes[i] = h.Hash.Hash
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Find("H"),
				Mode:  pack.FilterModeIn,
				Value: hashes,
				Raw:   strings.Join(val, ","), // debugging aid
			})
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
		case "pct_account_reuse":
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("cannot filter by columns '%s'", prefix), nil))
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
				case "cycle":
					if v == "head" {
						currentCycle := params.CycleFromHeight(ctx.Crawler.Height())
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				case "volume", "rewards", "fees", "deposits", "burned_supply",
					"unfrozen_fees", "unfrozen_rewards", "unfrozen_deposits",
					"activated_supply":
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
	block := &Block{
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
			if err := r.Decode(block); err != nil {
				return err
			}
			if err := enc.Encode(block); err != nil {
				return err
			}
			count++
			lastId = block.RowId
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
				if err := r.Decode(block); err != nil {
					return err
				}
				if err := enc.EncodeRecord(block); err != nil {
					return err
				}
				count++
				lastId = block.RowId
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
