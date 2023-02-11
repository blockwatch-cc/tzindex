// Copyright (c) 2020-2021 Blockwatch Data Inc.
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
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// long -> short form
	accSourceNames map[string]string
	// all aliases as list
	accAllAliases []string
)

func init() {
	fields, err := pack.Fields(&model.Account{})
	if err != nil {
		log.Fatalf("account field type error: %v\n", err)
	}
	accSourceNames = fields.NameMapReverse()
	accAllAliases = fields.Aliases()

	// add extra translations for accounts
	accSourceNames["baker"] = "D"
	accSourceNames["creator"] = "C"
	accSourceNames["first_seen_time"] = "0"
	accSourceNames["first_seen_time"] = "0"
	accSourceNames["last_seen_time"] = "l"
	accSourceNames["first_in_time"] = "i"
	accSourceNames["last_in_time"] = "J"
	accSourceNames["first_out_time"] = "o"
	accSourceNames["last_out_time"] = "O"
	accSourceNames["delegated_since_time"] = "+"
	accAllAliases = append(accAllAliases, []string{
		"baker",
		"creator",
		"first_seen_time",
		"last_seen_time",
		"first_in_time",
		"last_in_time",
		"first_out_time",
		"last_out_time",
		"delegated_since_time",
	}...)
}

// configurable marshalling helper
type Account struct {
	model.Account
	verbose bool            // cond. marshal
	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params   // blockchain amount conversion
	ctx     *server.Context
}

func (a *Account) MarshalJSON() ([]byte, error) {
	if a.verbose {
		return a.MarshalJSONVerbose()
	} else {
		return a.MarshalJSONBrief()
	}
}

func (a *Account) MarshalJSONVerbose() ([]byte, error) {
	acc := struct {
		RowId              uint64    `json:"row_id"`
		Address            string    `json:"address"`
		AddressType        string    `json:"address_type"`
		Pubkey             tezos.Key `json:"pubkey"`
		Counter            int64     `json:"counter"`
		Baker              string    `json:"baker"`
		Creator            string    `json:"creator"`
		FirstIn            int64     `json:"first_in"`
		FirstOut           int64     `json:"first_out"`
		FirstSeen          int64     `json:"first_seen"`
		LastIn             int64     `json:"last_in"`
		LastOut            int64     `json:"last_out"`
		LastSeen           int64     `json:"last_seen"`
		DelegatedSince     int64     `json:"delegated_since"`
		TotalReceived      float64   `json:"total_received"`
		TotalSent          float64   `json:"total_sent"`
		TotalBurned        float64   `json:"total_burned"`
		TotalFeesPaid      float64   `json:"total_fees_paid"`
		TotalFeesUsed      float64   `json:"total_fees_used"`
		UnclaimedBalance   float64   `json:"unclaimed_balance"`
		SpendableBalance   float64   `json:"spendable_balance"`
		FrozenBond         float64   `json:"frozen_bond"`
		LostBond           float64   `json:"lost_bond"`
		IsFunded           bool      `json:"is_funded"`
		IsActivated        bool      `json:"is_activated"`
		IsDelegated        bool      `json:"is_delegated"`
		IsRevealed         bool      `json:"is_revealed"`
		IsBaker            bool      `json:"is_baker"`
		IsContract         bool      `json:"is_contract"`
		NTxSuccess         int       `json:"n_tx_success"`
		NTxFailed          int       `json:"n_tx_failed"`
		NTxOut             int       `json:"n_tx_out"`
		NTxIn              int       `json:"n_tx_in"`
		FirstSeenTime      int64     `json:"first_seen_time"`
		LastSeenTime       int64     `json:"last_seen_time"`
		FirstInTime        int64     `json:"first_in_time"`
		LastInTime         int64     `json:"last_in_time"`
		FirstOutTime       int64     `json:"first_out_time"`
		LastOutTime        int64     `json:"last_out_time"`
		DelegatedSinceTime int64     `json:"delegated_since_time"`
	}{
		RowId:              a.RowId.Value(),
		Address:            a.String(),
		AddressType:        a.Type.String(),
		Pubkey:             a.Pubkey,
		Counter:            a.Counter,
		Baker:              a.ctx.Indexer.LookupAddress(a.ctx, a.BakerId).String(),
		Creator:            a.ctx.Indexer.LookupAddress(a.ctx, a.CreatorId).String(),
		FirstIn:            a.FirstIn,
		FirstOut:           a.FirstOut,
		FirstSeen:          a.FirstSeen,
		LastIn:             a.LastIn,
		LastOut:            a.LastOut,
		LastSeen:           a.LastSeen,
		DelegatedSince:     a.DelegatedSince,
		TotalReceived:      a.params.ConvertValue(a.TotalReceived),
		TotalSent:          a.params.ConvertValue(a.TotalSent),
		TotalBurned:        a.params.ConvertValue(a.TotalBurned),
		TotalFeesPaid:      a.params.ConvertValue(a.TotalFeesPaid),
		TotalFeesUsed:      a.params.ConvertValue(a.TotalFeesUsed),
		UnclaimedBalance:   a.params.ConvertValue(a.UnclaimedBalance),
		SpendableBalance:   a.params.ConvertValue(a.SpendableBalance),
		FrozenBond:         a.params.ConvertValue(a.FrozenBond),
		LostBond:           a.params.ConvertValue(a.LostBond),
		IsFunded:           a.IsFunded,
		IsActivated:        a.IsActivated,
		IsDelegated:        a.IsDelegated,
		IsRevealed:         a.IsRevealed,
		IsBaker:            a.IsBaker,
		IsContract:         a.IsContract,
		NTxSuccess:         a.NTxSuccess,
		NTxFailed:          a.NTxFailed,
		NTxOut:             a.NTxOut,
		NTxIn:              a.NTxIn,
		FirstSeenTime:      a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.FirstSeen),
		LastSeenTime:       a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.LastSeen),
		FirstInTime:        a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.FirstIn),
		LastInTime:         a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.LastIn),
		FirstOutTime:       a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.FirstOut),
		LastOutTime:        a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.LastOut),
		DelegatedSinceTime: a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.DelegatedSince),
	}
	return json.Marshal(acc)
}

func (a *Account) MarshalJSONBrief() ([]byte, error) {
	dec := a.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range a.columns {
		switch v {
		case "row_id":
			buf = strconv.AppendUint(buf, a.RowId.Value(), 10)
		case "address":
			buf = strconv.AppendQuote(buf, a.String())
		case "address_type":
			buf = strconv.AppendQuote(buf, a.Type.String())
		case "pubkey":
			if a.Pubkey.IsValid() {
				buf = strconv.AppendQuote(buf, a.Pubkey.String())
			} else {
				buf = append(buf, null...)
			}
		case "counter":
			buf = strconv.AppendInt(buf, a.Counter, 10)
		case "baker":
			if a.BakerId > 0 {
				buf = strconv.AppendQuote(buf, a.ctx.Indexer.LookupAddress(a.ctx, a.BakerId).String())
			} else {
				buf = append(buf, null...)
			}
		case "creator":
			if a.CreatorId > 0 {
				buf = strconv.AppendQuote(buf, a.ctx.Indexer.LookupAddress(a.ctx, a.CreatorId).String())
			} else {
				buf = append(buf, null...)
			}
		case "first_in":
			buf = strconv.AppendInt(buf, a.FirstIn, 10)
		case "first_out":
			buf = strconv.AppendInt(buf, a.FirstOut, 10)
		case "first_seen":
			buf = strconv.AppendInt(buf, a.FirstSeen, 10)
		case "last_in":
			buf = strconv.AppendInt(buf, a.LastIn, 10)
		case "last_out":
			buf = strconv.AppendInt(buf, a.LastOut, 10)
		case "last_seen":
			buf = strconv.AppendInt(buf, a.LastSeen, 10)
		case "delegated_since":
			buf = strconv.AppendInt(buf, a.DelegatedSince, 10)
		case "total_received":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalReceived), 'f', dec, 64)
		case "total_sent":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalSent), 'f', dec, 64)
		case "total_burned":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalBurned), 'f', dec, 64)
		case "total_fees_paid":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalFeesPaid), 'f', dec, 64)
		case "total_fees_used":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalFeesUsed), 'f', dec, 64)
		case "unclaimed_balance":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.UnclaimedBalance), 'f', dec, 64)
		case "spendable_balance":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.SpendableBalance), 'f', dec, 64)
		case "frozen_bond":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.FrozenBond), 'f', dec, 64)
		case "lost_bond":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.LostBond), 'f', dec, 64)
		case "is_funded":
			if a.IsFunded {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_activated":
			if a.IsActivated {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_delegated":
			if a.IsDelegated {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_revealed":
			if a.IsRevealed {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_baker":
			if a.IsBaker {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_contract":
			if a.IsContract {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "n_tx_success":
			buf = strconv.AppendInt(buf, int64(a.NTxSuccess), 10)
		case "n_tx_failed":
			buf = strconv.AppendInt(buf, int64(a.NTxFailed), 10)
		case "n_tx_out":
			buf = strconv.AppendInt(buf, int64(a.NTxOut), 10)
		case "n_tx_in":
			buf = strconv.AppendInt(buf, int64(a.NTxIn), 10)
		case "first_seen_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.FirstSeen), 10)
		case "last_seen_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.LastSeen), 10)
		case "first_in_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.FirstIn), 10)
		case "last_in_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.LastIn), 10)
		case "first_out_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.FirstOut), 10)
		case "last_out_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.LastOut), 10)
		case "delegated_since_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.LookupBlockTimeMs(a.ctx.Context, a.DelegatedSince), 10)
		default:
			continue
		}
		if i < len(a.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (a *Account) MarshalCSV() ([]string, error) {
	dec := a.params.Decimals
	res := make([]string, len(a.columns))
	for i, v := range a.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(a.RowId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(a.String())
		case "address_type":
			res[i] = strconv.Quote(a.Type.String())
		case "pubkey":
			res[i] = strconv.Quote(a.Pubkey.String())
		case "counter":
			res[i] = strconv.FormatInt(a.Counter, 10)
		case "baker":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupAddress(a.ctx, a.BakerId).String())
		case "creator":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupAddress(a.ctx, a.CreatorId).String())
		case "first_in":
			res[i] = strconv.FormatInt(a.FirstIn, 10)
		case "first_out":
			res[i] = strconv.FormatInt(a.FirstOut, 10)
		case "first_seen":
			res[i] = strconv.FormatInt(a.FirstSeen, 10)
		case "last_in":
			res[i] = strconv.FormatInt(a.LastIn, 10)
		case "last_out":
			res[i] = strconv.FormatInt(a.LastOut, 10)
		case "last_seen":
			res[i] = strconv.FormatInt(a.LastSeen, 10)
		case "delegated_since":
			res[i] = strconv.FormatInt(a.DelegatedSince, 10)
		case "total_received":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalReceived), 'f', dec, 64)
		case "total_sent":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalSent), 'f', dec, 64)
		case "total_burned":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalBurned), 'f', dec, 64)
		case "total_fees_paid":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalFeesPaid), 'f', dec, 64)
		case "total_fees_used":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalFeesUsed), 'f', dec, 64)
		case "unclaimed_balance":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.UnclaimedBalance), 'f', dec, 64)
		case "spendable_balance":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.SpendableBalance), 'f', dec, 64)
		case "frozen_bond":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.FrozenBond), 'f', dec, 64)
		case "lost_bond":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.LostBond), 'f', dec, 64)
		case "is_funded":
			res[i] = strconv.FormatBool(a.IsFunded)
		case "is_activated":
			res[i] = strconv.FormatBool(a.IsActivated)
		case "is_delegated":
			res[i] = strconv.FormatBool(a.IsDelegated)
		case "is_revealed":
			res[i] = strconv.FormatBool(a.IsRevealed)
		case "is_baker":
			res[i] = strconv.FormatBool(a.IsBaker)
		case "is_contract":
			res[i] = strconv.FormatBool(a.IsContract)
		case "n_tx_success":
			res[i] = strconv.FormatInt(int64(a.NTxSuccess), 10)
		case "n_tx_failed":
			res[i] = strconv.FormatInt(int64(a.NTxFailed), 10)
		case "n_tx_out":
			res[i] = strconv.FormatInt(int64(a.NTxOut), 10)
		case "n_tx_in":
			res[i] = strconv.FormatInt(int64(a.NTxIn), 10)
		case "first_seen_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.FirstSeen).Format(time.RFC3339))
		case "last_seen_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.LastSeen).Format(time.RFC3339))
		case "first_in_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.FirstIn).Format(time.RFC3339))
		case "last_in_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.LastIn).Format(time.RFC3339))
		case "first_out_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.FirstOut).Format(time.RFC3339))
		case "last_out_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.LastOut).Format(time.RFC3339))
		case "delegated_since_time":
			res[i] = strconv.Quote(a.ctx.Indexer.LookupBlockTime(a.ctx.Context, a.DelegatedSince).Format(time.RFC3339))
		default:
			continue
		}
	}
	return res, nil
}

func StreamAccountTable(ctx *server.Context, args *TableRequest) (interface{}, int) {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := accSourceNames[v]
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			switch v {
			case "first_seen_time":
				srcNames = append(srcNames, "0") // first_seen
			case "last_seen_time":
				srcNames = append(srcNames, "l") // last_seen
			case "first_in_time":
				srcNames = append(srcNames, "i") // first_in
			case "last_in_time":
				srcNames = append(srcNames, "J") // last_in
			case "first_out_time":
				srcNames = append(srcNames, "o") // first_out
			case "last_out_time":
				srcNames = append(srcNames, "O") // last_out
			case "delegated_since_time":
				srcNames = append(srcNames, "+") // delegated_since
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = accAllAliases
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
		field := accSourceNames[prefix]
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
		case "address":
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				addr, err := tezos.ParseAddress(val[0])
				if err != nil || !addr.IsValid() {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				acc, err := ctx.Indexer.LookupAccount(ctx, addr)
				if err != nil && err != index.ErrNoAccountEntry {
					panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				// Note: when not found we insert an always false condition
				if acc == nil || acc.RowId == 0 {
					q = q.And("I", mode, uint64(math.MaxUint64))
				} else {
					// add id as extra condition
					q = q.And("I", mode, acc.RowId)
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup (Note: does not check for address type so may
				// return duplicates)
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					hashes = append(hashes, addr.Bytes22())
				}
				q = q.And("H", mode, hashes)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "pubkey":
			if mode != pack.FilterModeEqual {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
			k, err := tezos.ParseKey(val[0])
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid pubkey hash '%s'", val), err))
			}
			q = q.AndEqual(field, k.Data)

		case "baker", "creator":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== no delegate/manager set)
					q = q.And(field, mode, 0)
				} else {
					// single-account lookup and compile condition
					addr, err := tezos.ParseAddress(val[0])
					if err != nil || !addr.IsValid() {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					// Note: when not found we insert an always false condition
					if acc == nil || acc.RowId == 0 {
						q = q.And(field, mode, uint64(math.MaxUint64))
					} else {
						// add id as extra condition
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
					if err != nil && err != index.ErrNoAccountEntry {
						panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
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
				q = q.And(field, mode, ids)
			default:
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := accSourceNames[prefix]; !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64, handle multiple values for rg, in, nin
				switch prefix {
				case "total_received", "total_sent", "total_burned",
					"total_fees_paid", "total_fees_used",
					"unclaimed_balance", "spendable_balance",
					"frozen_bond", "lost_bond":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				case "address_type":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := tezos.ParseAddressType(t)
						if !typ.IsValid() {
							panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid address type '%s'", val[0]), nil))
						}
						typs = append(typs, int64(typ))
					}
					styps := make([]string, 0)
					for _, i := range vec.UniqueInt64Slice(typs) {
						styps = append(styps, strconv.FormatInt(i, 10))
					}
					v = strings.Join(styps, ",")
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
	)

	// start := time.Now()
	// ctx.Log.Tracef("Streaming max %d rows from %s", args.Limit, args.Table)
	// defer func() {
	// 	ctx.Log.Tracef("Streamed %d rows in %s", count, time.Since(start))
	// }()

	// query database (we cannot use Stream due to deadlock potential)
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "query failed", err))
	}
	// ctx.Log.Tracef("Processing result with %d rows %d cols", res.Rows(), res.Cols())
	defer res.Close()

	// prepare return type marshalling
	acc := &Account{
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
		err = res.Walk(func(r pack.Row) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			if err := r.Decode(acc); err != nil {
				return err
			}
			if err := enc.Encode(acc); err != nil {
				return err
			}
			count++
			lastId = acc.RowId.Value()
			if args.Limit > 0 && count == int(args.Limit) {
				return io.EOF
			}
			return nil
		})
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
			// run query and stream results
			err = res.Walk(func(r pack.Row) error {
				if err := r.Decode(acc); err != nil {
					return err
				}
				if err := enc.EncodeRecord(acc); err != nil {
					return err
				}
				count++
				lastId = acc.RowId.Value()
				if args.Limit > 0 && count == int(args.Limit) {
					return io.EOF
				}
				return nil
			})
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
