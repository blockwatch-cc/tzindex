// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

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
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	blockSourceNames map[string]string
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
	blockSourceNames["creator"] = "-" // baker or proposer
	blockSourceNames["proposer_consensus_key"] = "x"
	blockSourceNames["baker_consensus_key"] = "y"

	blockAllAliases = append(blockAllAliases,
		"pct_account_reuse",
		"baker",
		"proposer",
		"protocol",
		"proposer_consensus_key",
		"baker_consensus_key",
	)
}

// configurable marshalling helper
type Block struct {
	model.Block
	Predecessor tezos.BlockHash `pack:"predecessor" json:"predecessor"`

	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *rpc.Params     // blockchain amount conversion
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
		RowId                  uint64                 `json:"row_id"`
		ParentId               uint64                 `json:"parent_id"`
		Hash                   string                 `json:"hash"`
		Predecessor            string                 `json:"predecessor"`
		Timestamp              int64                  `json:"time"` // convert to UNIX milliseconds
		Height                 int64                  `json:"height"`
		Cycle                  int64                  `json:"cycle"`
		IsCycleSnapshot        bool                   `json:"is_cycle_snapshot"`
		Solvetime              int                    `json:"solvetime"`
		Version                int                    `json:"version"`
		Round                  int                    `json:"round"`
		Nonce                  string                 `json:"nonce"`
		VotingPeriodKind       tezos.VotingPeriodKind `json:"voting_period_kind"`
		BakerId                uint64                 `json:"baker_id"`
		Baker                  string                 `json:"baker"`
		ProposerId             uint64                 `json:"proposer_id"`
		Proposer               string                 `json:"proposer"`
		NSlotsEndorsed         int                    `json:"n_endorsed_slots"`
		NOpsApplied            int                    `json:"n_ops_applied"`
		NOpsFailed             int                    `json:"n_ops_failed"`
		NContractCalls         int                    `json:"n_calls"`
		NRollupCalls           int                    `json:"n_rollup_calls"`
		NTx                    int                    `json:"n_tx"`
		NEvents                int                    `json:"n_events"`
		NTickets               int                    `json:"n_tickets"`
		Volume                 float64                `json:"volume"`
		Fee                    float64                `json:"fee"`
		Reward                 float64                `json:"reward"`
		Deposit                float64                `json:"deposit"`
		ActivatedSupply        float64                `json:"activated_supply"`
		MintedSupply           float64                `json:"minted_supply"`
		BurnedSupply           float64                `json:"burned_supply"`
		SeenAccounts           int                    `json:"n_accounts"`
		NewAccounts            int                    `json:"n_new_accounts"`
		NewContracts           int                    `json:"n_new_contracts"`
		ClearedAccounts        int                    `json:"n_cleared_accounts"`
		FundedAccounts         int                    `json:"n_funded_accounts"`
		GasLimit               int64                  `json:"gas_limit"`
		GasUsed                int64                  `json:"gas_used"`
		StoragePaid            int64                  `json:"storage_paid"`
		PctAccountsReused      float64                `json:"pct_account_reuse"`
		LbVote                 tezos.FeatureVote      `json:"lb_vote"`
		LbEma                  int64                  `json:"lb_ema"`
		AiVote                 tezos.FeatureVote      `json:"ai_vote"`
		AiEma                  int64                  `json:"ai_ema"`
		Protocol               tezos.ProtocolHash     `json:"protocol"`
		ProposerConsensusKeyId uint64                 `json:"proposer_consensus_key_id"`
		BakerConsensusKeyId    uint64                 `json:"baker_consensus_key_id"`
		ProposerConsensusKey   string                 `json:"proposer_consensus_key"`
		BakerConsensusKey      string                 `json:"baker_consensus_key"`
	}{
		RowId:                  b.RowId,
		ParentId:               b.ParentId,
		Hash:                   b.Hash.String(),
		Predecessor:            b.Predecessor.String(),
		Timestamp:              util.UnixMilliNonZero(b.Timestamp),
		Height:                 b.Height,
		Cycle:                  b.Cycle,
		IsCycleSnapshot:        b.IsCycleSnapshot,
		Solvetime:              b.Solvetime,
		Version:                b.Version,
		Round:                  b.Round,
		Nonce:                  util.U64String(b.Nonce).Hex(),
		VotingPeriodKind:       b.VotingPeriodKind,
		BakerId:                b.BakerId.U64(),
		Baker:                  b.ctx.Indexer.LookupAddress(b.ctx, b.BakerId).String(),
		ProposerId:             b.ProposerId.U64(),
		Proposer:               b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerId).String(),
		NSlotsEndorsed:         b.NSlotsEndorsed,
		NOpsApplied:            model.Int16Correct(b.NOpsApplied),
		NOpsFailed:             model.Int16Correct(b.NOpsFailed),
		NContractCalls:         model.Int16Correct(b.NContractCalls),
		NRollupCalls:           model.Int16Correct(b.NRollupCalls),
		NTx:                    model.Int16Correct(b.NTx),
		NEvents:                model.Int16Correct(b.NEvents),
		NTickets:               model.Int16Correct(b.NTickets),
		Volume:                 b.params.ConvertValue(b.Volume),
		Fee:                    b.params.ConvertValue(b.Fee),
		Reward:                 b.params.ConvertValue(b.Reward),
		Deposit:                b.params.ConvertValue(b.Deposit),
		ActivatedSupply:        b.params.ConvertValue(b.ActivatedSupply),
		MintedSupply:           b.params.ConvertValue(b.MintedSupply),
		BurnedSupply:           b.params.ConvertValue(b.BurnedSupply),
		SeenAccounts:           model.Int16Correct(b.SeenAccounts),
		NewAccounts:            model.Int16Correct(b.NewAccounts),
		NewContracts:           model.Int16Correct(b.NewContracts),
		ClearedAccounts:        model.Int16Correct(b.ClearedAccounts),
		FundedAccounts:         model.Int16Correct(b.FundedAccounts),
		GasLimit:               b.GasLimit,
		GasUsed:                b.GasUsed,
		StoragePaid:            b.StoragePaid,
		LbVote:                 b.LbVote,
		LbEma:                  b.LbEma,
		AiVote:                 b.AiVote,
		AiEma:                  b.AiEma,
		Protocol:               b.params.Protocol,
		ProposerConsensusKeyId: b.ProposerConsensusKeyId.U64(),
		BakerConsensusKeyId:    b.BakerConsensusKeyId.U64(),
		ProposerConsensusKey:   b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerConsensusKeyId).String(),
		BakerConsensusKey:      b.ctx.Indexer.LookupAddress(b.ctx, b.BakerConsensusKeyId).String(),
	}
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
			if b.Predecessor.IsValid() {
				buf = strconv.AppendQuote(buf, b.Predecessor.String())
			} else {
				buf = append(buf, []byte(`""`)...)
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
		case "round":
			buf = strconv.AppendInt(buf, int64(b.Round), 10)
		case "nonce":
			buf = strconv.AppendQuote(buf, util.U64String(b.Nonce).Hex())
		case "voting_period_kind":
			buf = strconv.AppendQuote(buf, b.VotingPeriodKind.String())
		case "baker_id":
			buf = strconv.AppendUint(buf, b.BakerId.U64(), 10)
		case "baker":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.BakerId).String())
		case "proposer_id":
			buf = strconv.AppendUint(buf, b.ProposerId.U64(), 10)
		case "proposer":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerId).String())
		case "n_endorsed_slots":
			buf = strconv.AppendInt(buf, int64(b.NSlotsEndorsed), 10)
		case "n_ops_applied":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NOpsApplied)), 10)
		case "n_ops_failed":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NOpsFailed)), 10)
		case "n_calls":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NContractCalls)), 10)
		case "n_rollup_calls":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NRollupCalls)), 10)
		case "n_tx":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NTx)), 10)
		case "n_events":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NEvents)), 10)
		case "n_tickets":
			buf = strconv.AppendInt(buf, int64(model.Int16Correct(b.NTickets)), 10)
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
		case "lb_vote":
			buf = strconv.AppendQuote(buf, b.LbVote.String())
		case "lb_ema":
			buf = strconv.AppendInt(buf, b.LbEma, 10)
		case "ai_vote":
			buf = strconv.AppendQuote(buf, b.AiVote.String())
		case "ai_ema":
			buf = strconv.AppendInt(buf, b.AiEma, 10)
		case "protocol":
			buf = strconv.AppendQuote(buf, b.params.Protocol.String())
		case "proposer_consensus_key_id":
			buf = strconv.AppendUint(buf, b.ProposerConsensusKeyId.U64(), 10)
		case "proposer_consensus_key":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerConsensusKeyId).String())
		case "baker_consensus_key_id":
			buf = strconv.AppendUint(buf, b.BakerConsensusKeyId.U64(), 10)
		case "baker_consensus_key":
			buf = strconv.AppendQuote(buf, b.ctx.Indexer.LookupAddress(b.ctx, b.BakerConsensusKeyId).String())
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
			if b.Predecessor.IsValid() {
				res[i] = strconv.Quote(b.Predecessor.String())
			}
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
			res[i] = strconv.Quote(util.U64String(b.Nonce).Hex())
		case "voting_period_kind":
			res[i] = strconv.Quote(b.VotingPeriodKind.String())
		case "baker_id":
			res[i] = strconv.FormatUint(b.BakerId.U64(), 10)
		case "baker":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.BakerId).String())
		case "proposer_id":
			res[i] = strconv.FormatUint(b.ProposerId.U64(), 10)
		case "proposer":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerId).String())
		case "n_endorsed_slots":
			res[i] = strconv.FormatInt(int64(b.NSlotsEndorsed), 10)
		case "n_ops_applied":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NOpsApplied)), 10)
		case "n_ops_failed":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NOpsFailed)), 10)
		case "n_calls":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NContractCalls)), 10)
		case "n_rollup_calls":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NRollupCalls)), 10)
		case "n_tx":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NTx)), 10)
		case "n_events":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NEvents)), 10)
		case "n_tickets":
			res[i] = strconv.FormatInt(int64(model.Int16Correct(b.NTickets)), 10)
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
		case "lb_vote":
			res[i] = strconv.Quote(b.LbVote.String())
		case "lb_ema":
			res[i] = strconv.FormatInt(b.LbEma, 10)
		case "ai_vote":
			res[i] = strconv.Quote(b.AiVote.String())
		case "ai_ema":
			res[i] = strconv.FormatInt(b.AiEma, 10)
		case "protocol":
			res[i] = strconv.Quote(b.params.Protocol.String())
		case "proposer_consensus_key_id":
			res[i] = strconv.FormatUint(b.ProposerConsensusKeyId.U64(), 10)
		case "proposer_consensus_key":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.ProposerConsensusKeyId).String())
		case "baker_consensus_key_id":
			res[i] = strconv.FormatUint(b.BakerConsensusKeyId.U64(), 10)
		case "baker_consensus_key":
			res[i] = strconv.Quote(b.ctx.Indexer.LookupAddress(b.ctx, b.BakerConsensusKeyId).String())
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
			switch v {
			case "predecessor":
				needJoin = true
			case "creator":
				srcNames = append(srcNames, "baker_id", "proposer_id")
			case "baker":
				srcNames = append(srcNames, "baker_id")
			case "proposer":
				srcNames = append(srcNames, "proposer_id")
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = blockAllAliases
		// needJoin = true
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID).
		WithTable(table).
		WithFields(srcNames...).
		WithLimit(int(args.Limit)).
		WithOrder(args.Order)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		field := blockSourceNames[prefix]
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
			q = q.And("I", cursorMode, id)
		case "hash":
			// special hash type to []byte conversion
			hashes := make([][]byte, len(val))
			for i, v := range val {
				h, err := tezos.ParseBlockHash(v)
				if err != nil {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid block hash '%s'", val), err))
				}
				hashes[i] = h[:]
			}
			if len(hashes) == 1 {
				q = q.AndEqual(field, hashes[0])
			} else {
				q = q.AndIn(field, hashes)
			}
		case "creator":
			addrs := make([]model.AccountID, 0)
			for _, v := range strings.Split(val[0], ",") {
				addr, err := tezos.ParseAddress(v)
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != model.ErrNoAccount {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
				}
				if err == nil && acc.RowId > 0 {
					addrs = append(addrs, acc.RowId)
				}
			}

			switch mode {
			case pack.FilterModeEqual:
				if len(addrs) == 1 {
					q = q.OrCondition(
						pack.Equal("baker_id", addrs[0]),
						pack.Equal("proposer_id", addrs[0]),
					)
				}
				fallthrough
			case pack.FilterModeIn:
				if len(addrs) > 1 {
					q = q.OrCondition(
						pack.In("baker_id", addrs),
						pack.In("proposer_id", addrs),
					)
				}
			case pack.FilterModeNotEqual:
				if len(addrs) == 1 {
					q = q.OrCondition(
						pack.NotEqual("baker_id", addrs[0]),
						pack.NotEqual("proposer_id", addrs[0]),
					)
				}
				fallthrough
			case pack.FilterModeNotIn:
				if len(addrs) > 1 {
					q = q.OrCondition(
						pack.NotIn("baker_id", addrs),
						pack.NotIn("proposer_id", addrs),
					)
				}
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "baker", "proposer", "baker_consensus_key", "proposer_consensus_key":
			// parse baker address and lookup id
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
					if err != nil || !addr.IsValid() {
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
						q = q.And(field, mode, acc.RowId)
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
					if err != nil && err != model.ErrNoAccount {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
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
			q = q.And(field, mode, period)
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
					q = q.AndCondition(cond)
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
				Table:  table,          // block table
				Where:  q.Conditions,   // use original query conds
				Cols:   joinFieldNames, // use user-defined fields (i.e. short names)
				ColsAs: joinFieldNames, // keep field names (i.e. short names)
				Limit:  q.Limit,        // use original limit
			},
			Right: pack.JoinTable{
				Table:  table,                   // block table, no extra conditions
				Cols:   []string{"hash"},        // field
				ColsAs: []string{"predecessor"}, // target name
			},
		}

		// clear query conditions
		q.Conditions = pack.UnboundCondition{}
		// log.Info(join.Dump())
		// run join query, order is not yet supported
		res, err = join.Query(ctx, pack.NewQuery("join").WithTable(table).WithLimit(q.Limit).WithOrder(q.Order))
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
		columns: args.Columns,
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
		_, _ = io.WriteString(ctx.ResponseWriter, "[")
		// close JSON array on panic
		defer func() {
			if e := recover(); e != nil {
				_, _ = io.WriteString(ctx.ResponseWriter, "]")
				panic(e)
			}
		}()

		// run query and stream results
		var needComma bool
		process := func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
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
		_, _ = io.WriteString(ctx.ResponseWriter, "]")
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
