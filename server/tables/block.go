// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"encoding/binary"
	"encoding/hex"
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
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
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
	blockSourceNames["proposer"] = "X"
	blockSourceNames["predecessor"] = "P" // pred hash, requires parent_id
	blockSourceNames["protocol"] = "-"

	blockAllAliases = append(blockAllAliases,
		"pct_account_reuse",
		"baker",
		"proposer",
		"protocol",
	)
}

// configurable marshalling helper
type Block struct {
	model.Block
	Predecessor tezos.BlockHash `pack:"predecessor" json:"predecessor"`

	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	ctx     *server.Context
}

func (b *Block) MarshalJSON() ([]byte, error) {
	if b.verbose {
		return b.MarshalJSONVerbose()
	} else {
		return b.MarshalJSONBrief()
	}
}

func (b *Block) MarshalJSONVerbose() ([]byte, error) {
	if !b.params.ContainsHeight(b.Height) {
		b.params = b.ctx.Crawler.ParamsByHeight(b.Height)
	}
	block := struct {
		RowId             uint64                 `json:"row_id"`
		ParentId          uint64                 `json:"parent_id"`
		Hash              string                 `json:"hash"`
		Predecessor       string                 `json:"predecessor"`
		Timestamp         int64                  `json:"time"` // convert to UNIX milliseconds
		Height            int64                  `json:"height"`
		Cycle             int64                  `json:"cycle"`
		IsCycleSnapshot   bool                   `json:"is_cycle_snapshot"`
		Solvetime         int                    `json:"solvetime"`
		Version           int                    `json:"version"`
		Round             int                    `json:"round"`
		Nonce             string                 `json:"nonce"`
		VotingPeriodKind  tezos.VotingPeriodKind `json:"voting_period_kind"`
		BakerId           uint64                 `json:"baker_id"`
		Baker             string                 `json:"baker"`
		ProposerId        uint64                 `json:"proposer_id"`
		Proposer          string                 `json:"proposer"`
		NSlotsEndorsed    int                    `json:"n_endorsed_slots"`
		NOpsApplied       int                    `json:"n_ops_applied"`
		NOpsFailed        int                    `json:"n_ops_failed"`
		NEvents           int                    `json:"n_events"`
		NContractCalls    int                    `json:"n_calls"`
		Volume            float64                `json:"volume"`
		Fee               float64                `json:"fee"`
		Reward            float64                `json:"reward"`
		Deposit           float64                `json:"deposit"`
		ActivatedSupply   float64                `json:"activated_supply"`
		MintedSupply      float64                `json:"minted_supply"`
		BurnedSupply      float64                `json:"burned_supply"`
		SeenAccounts      int                    `json:"n_accounts"`
		NewAccounts       int                    `json:"n_new_accounts"`
		NewContracts      int                    `json:"n_new_contracts"`
		ClearedAccounts   int                    `json:"n_cleared_accounts"`
		FundedAccounts    int                    `json:"n_funded_accounts"`
		GasLimit          int64                  `json:"gas_limit"`
		GasUsed           int64                  `json:"gas_used"`
		StoragePaid       int64                  `json:"storage_paid"`
		PctAccountsReused float64                `json:"pct_account_reuse"`
		LbEscapeVote      bool                   `json:"lb_esc_vote"`
		LbEscapeEma       int64                  `json:"lb_esc_ema"`
		Protocol          tezos.ProtocolHash     `json:"protocol"`
	}{
		RowId:            b.RowId,
		ParentId:         b.ParentId,
		Hash:             b.Hash.String(),
		Predecessor:      b.Predecessor.String(),
		Timestamp:        util.UnixMilliNonZero(b.Timestamp),
		Height:           b.Height,
		Cycle:            b.Cycle,
		IsCycleSnapshot:  b.IsCycleSnapshot,
		Solvetime:        b.Solvetime,
		Version:          b.Version,
		Round:            b.Round,
		Nonce:            "",
		VotingPeriodKind: b.VotingPeriodKind,
		BakerId:          b.BakerId.Value(),
		Baker:            b.ctx.Indexer.LookupAddress(b.ctx, b.BakerId).String(),
		ProposerId:       b.ProposerId.Value(),
		Proposer:         b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerId).String(),
		NSlotsEndorsed:   b.NSlotsEndorsed,
		NOpsApplied:      model.Int16Correct(b.NOpsApplied),
		NOpsFailed:       model.Int16Correct(b.NOpsFailed),
		NEvents:          model.Int16Correct(b.NEvents),
		NContractCalls:   model.Int16Correct(b.NContractCalls),
		Volume:           b.params.ConvertValue(b.Volume),
		Fee:              b.params.ConvertValue(b.Fee),
		Reward:           b.params.ConvertValue(b.Reward),
		Deposit:          b.params.ConvertValue(b.Deposit),
		ActivatedSupply:  b.params.ConvertValue(b.ActivatedSupply),
		MintedSupply:     b.params.ConvertValue(b.MintedSupply),
		BurnedSupply:     b.params.ConvertValue(b.BurnedSupply),
		SeenAccounts:     model.Int16Correct(b.SeenAccounts),
		NewAccounts:      model.Int16Correct(b.NewAccounts),
		NewContracts:     model.Int16Correct(b.NewContracts),
		ClearedAccounts:  model.Int16Correct(b.ClearedAccounts),
		FundedAccounts:   model.Int16Correct(b.FundedAccounts),
		GasLimit:         b.GasLimit,
		GasUsed:          b.GasUsed,
		StoragePaid:      b.StoragePaid,
		LbEscapeVote:     b.LbEscapeVote,
		LbEscapeEma:      b.LbEscapeEma,
		Protocol:         b.params.Protocol,
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], b.Nonce)
	block.Nonce = hex.EncodeToString(buf[:])
	if b.SeenAccounts > 0 {
		block.PctAccountsReused = float64(b.SeenAccounts-b.NewAccounts) / float64(b.SeenAccounts) * 100
	}
	return json.Marshal(block)
}

func (b *Block) MarshalJSONBrief() ([]byte, error) {
	if !b.params.ContainsHeight(b.Height) {
		b.params = b.ctx.Crawler.ParamsByHeight(b.Height)
	}
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
		case "predecessor":
			buf = strconv.AppendQuote(buf, b.Predecessor.String())
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
		case "round":
			buf = strconv.AppendInt(buf, int64(b.Round), 10)
		case "nonce":
			var nonce [8]byte
			binary.BigEndian.PutUint64(nonce[:], b.Nonce)
			buf = strconv.AppendQuote(buf, hex.EncodeToString(nonce[:]))
		case "voting_period_kind":
			buf = strconv.AppendQuote(buf, b.VotingPeriodKind.String())
		case "baker_id":
			buf = strconv.AppendUint(buf, b.BakerId.Value(), 10)
		case "baker":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.BakerId).String())
		case "proposer_id":
			buf = strconv.AppendUint(buf, b.ProposerId.Value(), 10)
		case "proposer":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerId).String())
		case "n_endorsed_slots":
			buf = strconv.AppendInt(buf, int64(b.NSlotsEndorsed), 10)
		case "n_ops_applied":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NOpsApplied)), 10)
		case "n_ops_failed":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NOpsFailed)), 10)
		case "n_events":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NEvents)), 10)
		case "n_calls":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NContractCalls)), 10)
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
		case "n_accounts":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.SeenAccounts)), 10)
		case "n_new_accounts":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NewAccounts)), 10)
		case "n_new_contracts":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NewContracts)), 10)
		case "n_cleared_accounts":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.ClearedAccounts)), 10)
		case "n_funded_accounts":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.FundedAccounts)), 10)
		case "gas_limit":
			buf = strconv.AppendInt(buf, b.GasLimit, 10)
		case "gas_used":
			buf = strconv.AppendInt(buf, b.GasUsed, 10)
		case "storage_paid":
			buf = strconv.AppendInt(buf, b.StoragePaid, 10)
		case "pct_account_reuse":
			var reuse float64
			if b.SeenAccounts != 0 {
				reuse = float64(model.Int16Correct(b.SeenAccounts)-model.Int16Correct(b.NewAccounts)) / float64(model.Int16Correct(b.SeenAccounts)) * 100
			}
			buf = strconv.AppendFloat(buf, reuse, 'f', 6, 64)
		case "lb_esc_vote":
			if b.LbEscapeVote {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "lb_esc_ema":
			buf = strconv.AppendInt(buf, int64(b.LbEscapeEma), 10)
		case "protocol":
			buf = strconv.AppendQuote(buf, b.params.Protocol.String())
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
	if !b.params.ContainsHeight(b.Height) {
		b.params = b.ctx.Crawler.ParamsByHeight(b.Height)
	}
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
		case "predecessor":
			res[i] = strconv.Quote(b.Predecessor.String())
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
		case "round":
			res[i] = strconv.FormatInt(int64(b.Round), 10)
		case "nonce":
			var nonce [8]byte
			binary.BigEndian.PutUint64(nonce[:], b.Nonce)
			res[i] = strconv.Quote(hex.EncodeToString(nonce[:]))
		case "voting_period_kind":
			res[i] = strconv.Quote(b.VotingPeriodKind.String())
		case "baker_id":
			res[i] = strconv.FormatUint(b.BakerId.Value(), 10)
		case "baker":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.BakerId).String())
		case "proposer_id":
			res[i] = strconv.FormatUint(b.ProposerId.Value(), 10)
		case "proposer":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerId).String())
		case "n_endorsed_slots":
			res[i] = strconv.FormatInt(int64(b.NSlotsEndorsed), 10)
		case "n_ops_applied":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NOpsApplied)), 10)
		case "n_ops_failed":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NOpsFailed)), 10)
		case "n_events":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NEvents)), 10)
		case "n_calls":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NContractCalls)), 10)
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
		case "n_accounts":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.SeenAccounts)), 10)
		case "n_new_accounts":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NewAccounts)), 10)
		case "n_new_contracts":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NewContracts)), 10)
		case "n_cleared_accounts":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.ClearedAccounts)), 10)
		case "n_funded_accounts":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.FundedAccounts)), 10)
		case "gas_limit":
			res[i] = strconv.FormatInt(b.GasLimit, 10)
		case "gas_used":
			res[i] = strconv.FormatInt(b.GasUsed, 10)
		case "storage_paid":
			res[i] = strconv.FormatInt(b.StoragePaid, 10)
		case "pct_account_reuse":
			var reuse float64
			if b.SeenAccounts != 0 {
				reuse = float64(model.Int16Correct(b.SeenAccounts)-model.Int16Correct(b.NewAccounts)) / float64(model.Int16Correct(b.SeenAccounts)) * 100
			}
			res[i] = strconv.FormatFloat(reuse, 'f', -1, 64)
		case "lb_esc_vote":
			res[i] = strconv.FormatBool(b.LbEscapeVote)
		case "lb_esc_ema":
			res[i] = strconv.FormatInt(b.LbEscapeEma, 10)
		case "protocol":
			res[i] = strconv.Quote(b.params.Protocol.String())
		default:
			continue
		}
	}
	return res, nil
}

func StreamBlockTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames []string
		needJoin bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := blockSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
			if v == "predecessor" {
				needJoin = true
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = blockAllAliases
		// needJoin = true
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID, table).
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
			q.Conditions.AddAndCondition(&pack.Condition{
				Field: table.Fields().Pk(),
				Mode:  cursorMode,
				Value: id,
				Raw:   val[0], // debugging aid
			})
		case "hash":
			// special hash type to []byte conversion
			hashes := make([][]byte, len(val))
			for i, v := range val {
				h, err := tezos.ParseBlockHash(v)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid block hash '%s'", val), err))
				}
				hashes[i] = h.Hash.Hash
			}
			if len(hashes) == 1 {
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  pack.FilterModeEqual,
					Value: hashes[0],
					Raw:   strings.Join(val, ","), // debugging aid
				})
			} else {
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  pack.FilterModeIn,
					Value: hashes,
					Raw:   strings.Join(val, ","), // debugging aid
				})
			}
		case "baker", "proposer":
			field := table.Fields().Find("B") // baker id
			if prefix == "proposer" {
				field = table.Fields().Find("X") // proposer id
			}
			// parse baker address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: field,
						Mode:  mode,
						Value: 0,
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-address lookup and compile condition
					addr, err := tezos.ParseAddress(val[0])
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(err)
					}
					// Note: when not found we insert an always false condition
					if acc == nil || acc.RowId == 0 {
						q.Conditions.AddAndCondition(&pack.Condition{
							Field: field,
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "account not found", // debugging aid
						})
					} else {
						// add addr id as extra fund_flow condition
						q.Conditions.AddAndCondition(&pack.Condition{
							Field: table.Fields().Find("B"), // baker id
							Mode:  mode,
							Value: acc.RowId,
							Raw:   val[0], // debugging aid
						})
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
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
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: field,
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		case "voting_period_kind":
			// parse only the first value
			period := tezos.ParseVotingPeriod(val[0])
			if !period.IsValid() {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid voting period '%s'", val[0]), nil))
			}
			q.Conditions.AddAndCondition(&pack.Condition{
				Field: table.Fields().Find("k"),
				Mode:  mode,
				Value: period,
				Raw:   val[0], // debugging aid
			})
		case "pct_account_reuse":
			panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("cannot filter by columns '%s'", prefix), nil))
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := blockSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
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
						currentCycle := params.CycleFromHeight(ctx.Tip.BestHeight)
						v = strconv.FormatInt(int64(currentCycle), 10)
					}
				case "volume", "reward", "fee", "deposit",
					"minted_supply", "burned_supply", "activated_supply":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions.AddAndCondition(&cond)
				}
			}
		}
	}

	var (
		count  int
		lastId uint64
		res    *pack.Result
	)

	// when predecessor block hash is required, we join the result on parent_id - row_id
	if needJoin {
		// ensure primary key field is part of srcNames
		joinFieldNames := util.StringList(srcNames)
		joinFieldNames.AddUniqueFront("I")

		join := pack.Join{
			Type: pack.LeftJoin,
			Predicate: pack.BinaryCondition{
				Left:  table.Fields().Find("parent_id"),
				Right: table.Fields().Find("row_id"),
				Mode:  pack.FilterModeEqual,
			},
			Left: pack.JoinTable{
				Table:    table,                                    // block table
				Where:    q.Conditions,                             // use original query conds
				Fields:   table.Fields().Select(joinFieldNames...), // use user-defined fields (i.e. short names)
				FieldsAs: joinFieldNames,                           // keep field names (i.e. short names)
				Limit:    q.Limit,                                  // use original limit
			},
			Right: pack.JoinTable{
				Table:    table,                         // block table, no extra conditions
				Fields:   table.Fields().Select("hash"), // field
				FieldsAs: []string{"predecessor"},       // target name
			},
		}

		// clear query conditions
		q.Conditions = pack.ConditionTreeNode{}
		// log.Info(join.Dump())
		// run join query, order is not yet supported
		res, err = join.Query(ctx, pack.Query{Limit: q.Limit, Order: q.Order})
		if err != nil {
			panic(server.EInternal(server.EC_DATABASE, "block table join failed", err))
		}
		defer res.Close()
	}

	// start := time.Now()
	// ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Table)
	// defer func() {
	// 	ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

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
		process := func(r pack.Row) error {
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
		}

		if res != nil {
			err = res.Walk(process)
		} else {
			err = table.Stream(ctx, q, process)
		}

		// close JSON bracket
		io.WriteString(ctx.ResponseWriter, "]")
		// ctx.Log.Tracef("JSON encoded %d rows", count)

	case "csv":
		enc := csv.NewEncoder(ctx.ResponseWriter)
		// use custom header columns and order
		if len(args.Columns) > 0 {
			err = enc.EncodeHeader(args.Columns, nil)
		}
		if err == nil {
			process := func(r pack.Row) error {
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
			}

			// run query and stream results
			if res != nil {
				err = res.Walk(process)
			} else {
				err = table.Stream(ctx, q, process)
			}
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
