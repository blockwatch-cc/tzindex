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
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	// long -> short form
	accSourceNames map[string]string
	// short -> long form
	accAliasNames map[string]string
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
	accSourceNames["address"] = "-"
	accSourceNames["delegate"] = "D"
	accSourceNames["manager"] = "M"
	accSourceNames["pubkey"] = "-"
	accSourceNames["first_seen_time"] = "0"
	accSourceNames["last_seen_time"] = "l"
	accSourceNames["first_in_time"] = "i"
	accSourceNames["last_in_time"] = "J"
	accSourceNames["first_out_time"] = "o"
	accSourceNames["last_out_time"] = "O"
	accSourceNames["delegated_since_time"] = "+"
	accSourceNames["delegate_since_time"] = "*"
	accSourceNames["rich_rank"] = "-"
	accSourceNames["flow_rank"] = "-"
	accSourceNames["traffic_rank"] = "-"
	accAllAliases = append(accAllAliases, []string{
		"address",
		"delegate",
		"manager",
		"pubkey",
		"first_seen_time",
		"last_seen_time",
		"first_in_time",
		"last_in_time",
		"first_out_time",
		"last_out_time",
		"delegated_since_time",
		"delegate_since_time",
		"rich_rank",
		"flow_rank",
		"traffic_rank",
	}...)
	// hide some internal fields (don't let CSV encoder pick them up)
	for _, v := range []string{"hash", "pubkey_hash", "pubkey_type"} {
		for i, n := range accAllAliases {
			if n == v {
				accAllAliases = append(accAllAliases[:i], accAllAliases[i+1:]...)
				break
			}
		}
	}
}

// configurable marshalling helper
type Account struct {
	model.Account
	verbose bool                              `csv:"-" pack:"-"` // cond. marshal
	columns util.StringList                   `csv:"-" pack:"-"` // cond. cols & order when brief
	params  *chain.Params                     `csv:"-" pack:"-"` // blockchain amount conversion
	addrs   map[model.AccountID]chain.Address `csv:"-" pack:"-"` // address map
	ctx     *ApiContext                       `csv:"-" pack:"-"`
	ranking *etl.AccountRanking               `csv:"-" pack:"-"`
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
		RowId              uint64  `json:"row_id"`
		Address            string  `json:"address"`
		AddressType        string  `json:"address_type"`
		DelegateId         uint64  `json:"delegate_id"`
		Delegate           string  `json:"delegate"`
		ManagerId          uint64  `json:"manager_id"`
		Manager            string  `json:"manager"`
		Pubkey             string  `json:"pubkey"`
		FirstIn            int64   `json:"first_in"`
		FirstOut           int64   `json:"first_out"`
		FirstSeen          int64   `json:"first_seen"`
		LastIn             int64   `json:"last_in"`
		LastOut            int64   `json:"last_out"`
		LastSeen           int64   `json:"last_seen"`
		DelegatedSince     int64   `json:"delegated_since"`
		DelegateSince      int64   `json:"delegate_since"`
		TotalReceived      float64 `json:"total_received"`
		TotalSent          float64 `json:"total_sent"`
		TotalBurned        float64 `json:"total_burned"`
		TotalFeesPaid      float64 `json:"total_fees_paid"`
		TotalRewardsEarned float64 `json:"total_rewards_earned"`
		TotalFeesEarned    float64 `json:"total_fees_earned"`
		TotalLost          float64 `json:"total_lost"`
		FrozenDeposits     float64 `json:"frozen_deposits"`
		FrozenRewards      float64 `json:"frozen_rewards"`
		FrozenFees         float64 `json:"frozen_fees"`
		UnclaimedBalance   float64 `json:"unclaimed_balance"`
		SpendableBalance   float64 `json:"spendable_balance"`
		DelegatedBalance   float64 `json:"delegated_balance"`
		TotalDelegations   int64   `json:"total_delegations"`
		ActiveDelegations  int64   `json:"active_delegations"`
		IsFunded           bool    `json:"is_funded"`
		IsActivated        bool    `json:"is_activated"`
		IsVesting          bool    `json:"is_vesting"`
		IsSpendable        bool    `json:"is_spendable"`
		IsDelegatable      bool    `json:"is_delegatable"`
		IsDelegated        bool    `json:"is_delegated"`
		IsRevealed         bool    `json:"is_revealed"`
		IsDelegate         bool    `json:"is_delegate"`
		IsActiveDelegate   bool    `json:"is_active_delegate"`
		IsContract         bool    `json:"is_contract"`
		BlocksBaked        int     `json:"blocks_baked"`
		BlocksMissed       int     `json:"blocks_missed"`
		BlocksStolen       int     `json:"blocks_stolen"`
		BlocksEndorsed     int     `json:"blocks_endorsed"`
		SlotsEndorsed      int     `json:"slots_endorsed"`
		SlotsMissed        int     `json:"slots_missed"`
		NOps               int     `json:"n_ops"`
		NOpsFailed         int     `json:"n_ops_failed"`
		NTx                int     `json:"n_tx"`
		NDelegation        int     `json:"n_delegation"`
		NOrigination       int     `json:"n_origination"`
		NProposal          int     `json:"n_proposal"`
		NBallot            int     `json:"n_ballot"`
		TokenGenMin        int64   `json:"token_gen_min"`
		TokenGenMax        int64   `json:"token_gen_max"`
		GracePeriod        int64   `json:"grace_period"`
		FirstSeenTime      int64   `json:"first_seen_time"`
		LastSeenTime       int64   `json:"last_seen_time"`
		FirstInTime        int64   `json:"first_in_time"`
		LastInTime         int64   `json:"last_in_time"`
		FirstOutTime       int64   `json:"first_out_time"`
		LastOutTime        int64   `json:"last_out_time"`
		DelegatedSinceTime int64   `json:"delegated_since_time"`
		DelegateSinceTime  int64   `json:"delegate_since_time"`
		RichRank           int     `json:"rich_rank"`
		FlowRank           int     `json:"flow_rank"`
		TrafficRank        int     `json:"traffic_rank"`
	}{
		RowId:              a.RowId.Value(),
		Address:            a.String(),
		AddressType:        a.Type.String(),
		DelegateId:         a.DelegateId.Value(),
		Delegate:           a.addrs[a.DelegateId].String(),
		ManagerId:          a.ManagerId.Value(),
		Manager:            a.addrs[a.ManagerId].String(),
		Pubkey:             chain.NewHash(a.PubkeyType, a.PubkeyHash).String(),
		FirstIn:            a.FirstIn,
		FirstOut:           a.FirstOut,
		FirstSeen:          a.FirstSeen,
		LastIn:             a.LastIn,
		LastOut:            a.LastOut,
		LastSeen:           a.LastSeen,
		DelegatedSince:     a.DelegatedSince,
		DelegateSince:      a.DelegateSince,
		TotalReceived:      a.params.ConvertValue(a.TotalReceived),
		TotalSent:          a.params.ConvertValue(a.TotalSent),
		TotalBurned:        a.params.ConvertValue(a.TotalBurned),
		TotalFeesPaid:      a.params.ConvertValue(a.TotalFeesPaid),
		TotalRewardsEarned: a.params.ConvertValue(a.TotalRewardsEarned),
		TotalFeesEarned:    a.params.ConvertValue(a.TotalFeesEarned),
		TotalLost:          a.params.ConvertValue(a.TotalLost),
		FrozenDeposits:     a.params.ConvertValue(a.FrozenDeposits),
		FrozenRewards:      a.params.ConvertValue(a.FrozenRewards),
		FrozenFees:         a.params.ConvertValue(a.FrozenFees),
		UnclaimedBalance:   a.params.ConvertValue(a.UnclaimedBalance),
		SpendableBalance:   a.params.ConvertValue(a.SpendableBalance),
		DelegatedBalance:   a.params.ConvertValue(a.DelegatedBalance),
		TotalDelegations:   a.TotalDelegations,
		ActiveDelegations:  a.ActiveDelegations,
		IsFunded:           a.IsFunded,
		IsActivated:        a.IsActivated,
		IsVesting:          a.IsVesting,
		IsSpendable:        a.IsSpendable,
		IsDelegatable:      a.IsDelegatable,
		IsDelegated:        a.IsDelegated,
		IsRevealed:         a.IsRevealed,
		IsDelegate:         a.IsDelegate,
		IsActiveDelegate:   a.IsActiveDelegate,
		IsContract:         a.IsContract,
		BlocksBaked:        a.BlocksBaked,
		BlocksMissed:       a.BlocksMissed,
		BlocksStolen:       a.BlocksStolen,
		BlocksEndorsed:     a.BlocksEndorsed,
		SlotsEndorsed:      a.SlotsEndorsed,
		SlotsMissed:        a.SlotsMissed,
		NOps:               a.NOps,
		NOpsFailed:         a.NOpsFailed,
		NTx:                a.NTx,
		NDelegation:        a.NDelegation,
		NOrigination:       a.NOrigination,
		NProposal:          a.NProposal,
		NBallot:            a.NBallot,
		TokenGenMin:        a.TokenGenMin,
		TokenGenMax:        a.TokenGenMax,
		GracePeriod:        a.GracePeriod,
		FirstSeenTime:      a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.FirstSeen),
		LastSeenTime:       a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.LastSeen),
		FirstInTime:        a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.FirstIn),
		LastInTime:         a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.LastIn),
		FirstOutTime:       a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.FirstOut),
		LastOutTime:        a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.LastOut),
		DelegatedSinceTime: a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.DelegatedSince),
		DelegateSinceTime:  a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.DelegateSince),
	}
	if a.ranking != nil {
		if rank, ok := a.ranking.GetAccount(a.RowId); ok {
			acc.RichRank = rank.RichRank
			acc.TrafficRank = rank.TrafficRank
			acc.FlowRank = rank.FlowRank
		}
	}
	return json.Marshal(acc)
}

func (a *Account) MarshalJSONBrief() ([]byte, error) {
	dec := a.params.Decimals
	var rank *etl.AccountRankingEntry
	if a.ranking != nil {
		rank, _ = a.ranking.GetAccount(a.RowId)
	}
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
		case "delegate_id":
			buf = strconv.AppendUint(buf, a.DelegateId.Value(), 10)
		case "delegate":
			if a.DelegateId > 0 {
				buf = strconv.AppendQuote(buf, a.addrs[a.DelegateId].String())
			} else {
				buf = append(buf, "null"...)
			}
		case "manager_id":
			buf = strconv.AppendUint(buf, a.ManagerId.Value(), 10)
		case "manager":
			if a.ManagerId > 0 {
				buf = strconv.AppendQuote(buf, a.addrs[a.ManagerId].String())
			} else {
				buf = append(buf, "null"...)
			}
		case "pubkey":
			if a.PubkeyType.IsValid() {
				pk := chain.NewHash(a.PubkeyType, a.PubkeyHash)
				buf = strconv.AppendQuote(buf, pk.String())
			} else {
				buf = append(buf, "null"...)
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
		case "delegate_since":
			buf = strconv.AppendInt(buf, a.DelegateSince, 10)
		case "total_received":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalReceived), 'f', dec, 64)
		case "total_sent":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalSent), 'f', dec, 64)
		case "total_burned":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalBurned), 'f', dec, 64)
		case "total_fees_paid":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalFeesPaid), 'f', dec, 64)
		case "total_rewards_earned":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalRewardsEarned), 'f', dec, 64)
		case "total_fees_earned":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalFeesEarned), 'f', dec, 64)
		case "total_lost":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.TotalLost), 'f', dec, 64)
		case "frozen_deposits":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.FrozenDeposits), 'f', dec, 64)
		case "frozen_rewards":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.FrozenRewards), 'f', dec, 64)
		case "frozen_fees":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.FrozenFees), 'f', dec, 64)
		case "unclaimed_balance":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.UnclaimedBalance), 'f', dec, 64)
		case "spendable_balance":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.SpendableBalance), 'f', dec, 64)
		case "delegated_balance":
			buf = strconv.AppendFloat(buf, a.params.ConvertValue(a.DelegatedBalance), 'f', dec, 64)
		case "total_delegations":
			buf = strconv.AppendInt(buf, int64(a.TotalDelegations), 10)
		case "active_delegations":
			buf = strconv.AppendInt(buf, int64(a.ActiveDelegations), 10)
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
		case "is_vesting":
			if a.IsVesting {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_spendable":
			if a.IsSpendable {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_delegatable":
			if a.IsDelegatable {
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
		case "is_active_delegate":
			if a.IsActiveDelegate {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case "is_delegate":
			if a.IsDelegate {
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
		case "blocks_baked":
			buf = strconv.AppendInt(buf, int64(a.BlocksBaked), 10)
		case "blocks_missed":
			buf = strconv.AppendInt(buf, int64(a.BlocksMissed), 10)
		case "blocks_stolen":
			buf = strconv.AppendInt(buf, int64(a.BlocksStolen), 10)
		case "blocks_endorsed":
			buf = strconv.AppendInt(buf, int64(a.BlocksEndorsed), 10)
		case "slots_endorsed":
			buf = strconv.AppendInt(buf, int64(a.SlotsEndorsed), 10)
		case "slots_missed":
			buf = strconv.AppendInt(buf, int64(a.SlotsMissed), 10)
		case "n_ops":
			buf = strconv.AppendInt(buf, int64(a.NOps), 10)
		case "n_ops_failed":
			buf = strconv.AppendInt(buf, int64(a.NOpsFailed), 10)
		case "n_tx":
			buf = strconv.AppendInt(buf, int64(a.NTx), 10)
		case "n_delegation":
			buf = strconv.AppendInt(buf, int64(a.NDelegation), 10)
		case "n_origination":
			buf = strconv.AppendInt(buf, int64(a.NOrigination), 10)
		case "n_proposal":
			buf = strconv.AppendInt(buf, int64(a.NProposal), 10)
		case "n_ballot":
			buf = strconv.AppendInt(buf, int64(a.NBallot), 10)
		case "token_gen_min":
			buf = strconv.AppendInt(buf, a.TokenGenMin, 10)
		case "token_gen_max":
			buf = strconv.AppendInt(buf, a.TokenGenMax, 10)
		case "grace_period":
			buf = strconv.AppendInt(buf, a.GracePeriod, 10)
		case "first_seen_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.FirstSeen), 10)
		case "last_seen_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.LastSeen), 10)
		case "first_in_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.FirstIn), 10)
		case "last_in_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.LastIn), 10)
		case "first_out_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.FirstOut), 10)
		case "last_out_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.LastOut), 10)
		case "delegated_since_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.DelegatedSince), 10)
		case "delegate_since_time":
			buf = strconv.AppendInt(buf, a.ctx.Indexer.BlockTimeMs(a.ctx.Context, a.DelegateSince), 10)
		case "rich_rank":
			if rank != nil {
				buf = strconv.AppendInt(buf, int64(rank.RichRank), 10)
			} else {
				buf = append(buf, '0')
			}
		case "flow_rank":
			if rank != nil {
				buf = strconv.AppendInt(buf, int64(rank.FlowRank), 10)
			} else {
				buf = append(buf, '0')
			}
		case "traffic_rank":
			if rank != nil {
				buf = strconv.AppendInt(buf, int64(rank.TrafficRank), 10)
			} else {
				buf = append(buf, '0')
			}
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
	var rank *etl.AccountRankingEntry
	if a.ranking != nil {
		rank, _ = a.ranking.GetAccount(a.RowId)
	}
	res := make([]string, len(a.columns))
	for i, v := range a.columns {
		switch v {
		case "row_id":
			res[i] = strconv.FormatUint(a.RowId.Value(), 10)
		case "address":
			res[i] = strconv.Quote(a.String())
		case "address_type":
			res[i] = strconv.Quote(a.Type.String())
		case "delegate_id":
			res[i] = strconv.FormatUint(a.DelegateId.Value(), 10)
		case "delegate":
			res[i] = strconv.Quote(a.addrs[a.DelegateId].String())
		case "manager_id":
			res[i] = strconv.FormatUint(a.ManagerId.Value(), 10)
		case "manager":
			res[i] = strconv.Quote(a.addrs[a.ManagerId].String())
		case "pubkey":
			pk := chain.NewHash(a.PubkeyType, a.PubkeyHash)
			res[i] = strconv.Quote(pk.String())
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
		case "delegate_since":
			res[i] = strconv.FormatInt(a.DelegateSince, 10)
		case "total_received":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalReceived), 'f', dec, 64)
		case "total_sent":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalSent), 'f', dec, 64)
		case "total_burned":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalBurned), 'f', dec, 64)
		case "total_fees_paid":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalFeesPaid), 'f', dec, 64)
		case "total_rewards_earned":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalRewardsEarned), 'f', dec, 64)
		case "total_fees_earned":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalFeesEarned), 'f', dec, 64)
		case "total_lost":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.TotalLost), 'f', dec, 64)
		case "frozen_deposits":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.FrozenDeposits), 'f', dec, 64)
		case "frozen_rewards":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.FrozenRewards), 'f', dec, 64)
		case "frozen_fees":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.FrozenFees), 'f', dec, 64)
		case "unclaimed_balance":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.UnclaimedBalance), 'f', dec, 64)
		case "spendable_balance":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.SpendableBalance), 'f', dec, 64)
		case "delegated_balance":
			res[i] = strconv.FormatFloat(a.params.ConvertValue(a.DelegatedBalance), 'f', dec, 64)
		case "total_delegations":
			res[i] = strconv.FormatInt(a.TotalDelegations, 10)
		case "active_delegations":
			res[i] = strconv.FormatInt(a.ActiveDelegations, 10)
		case "is_funded":
			res[i] = strconv.FormatBool(a.IsFunded)
		case "is_activated":
			res[i] = strconv.FormatBool(a.IsActivated)
		case "is_vesting":
			res[i] = strconv.FormatBool(a.IsVesting)
		case "is_spendable":
			res[i] = strconv.FormatBool(a.IsSpendable)
		case "is_delegatable":
			res[i] = strconv.FormatBool(a.IsDelegatable)
		case "is_delegated":
			res[i] = strconv.FormatBool(a.IsDelegated)
		case "is_revealed":
			res[i] = strconv.FormatBool(a.IsRevealed)
		case "is_delegate":
			res[i] = strconv.FormatBool(a.IsDelegate)
		case "is_active_delegate":
			res[i] = strconv.FormatBool(a.IsActiveDelegate)
		case "is_contract":
			res[i] = strconv.FormatBool(a.IsContract)
		case "blocks_baked":
			res[i] = strconv.FormatInt(int64(a.BlocksBaked), 10)
		case "blocks_missed":
			res[i] = strconv.FormatInt(int64(a.BlocksMissed), 10)
		case "blocks_stolen":
			res[i] = strconv.FormatInt(int64(a.BlocksStolen), 10)
		case "blocks_endorsed":
			res[i] = strconv.FormatInt(int64(a.BlocksEndorsed), 10)
		case "slots_endorsed":
			res[i] = strconv.FormatInt(int64(a.SlotsEndorsed), 10)
		case "slots_missed":
			res[i] = strconv.FormatInt(int64(a.SlotsMissed), 10)
		case "n_ops":
			res[i] = strconv.FormatInt(int64(a.NOps), 10)
		case "n_ops_failed":
			res[i] = strconv.FormatInt(int64(a.NOpsFailed), 10)
		case "n_tx":
			res[i] = strconv.FormatInt(int64(a.NTx), 10)
		case "n_delegation":
			res[i] = strconv.FormatInt(int64(a.NDelegation), 10)
		case "n_origination":
			res[i] = strconv.FormatInt(int64(a.NOrigination), 10)
		case "n_proposal":
			res[i] = strconv.FormatInt(int64(a.NProposal), 10)
		case "n_ballot":
			res[i] = strconv.FormatInt(int64(a.NBallot), 10)
		case "token_gen_min":
			res[i] = strconv.FormatInt(a.TokenGenMin, 10)
		case "token_gen_max":
			res[i] = strconv.FormatInt(a.TokenGenMax, 10)
		case "grace_period":
			res[i] = strconv.FormatInt(a.GracePeriod, 10)
		case "first_seen_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.FirstSeen).Format(time.RFC3339))
		case "last_seen_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.LastSeen).Format(time.RFC3339))
		case "first_in_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.FirstIn).Format(time.RFC3339))
		case "last_in_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.LastIn).Format(time.RFC3339))
		case "first_out_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.FirstOut).Format(time.RFC3339))
		case "last_out_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.LastOut).Format(time.RFC3339))
		case "delegated_since_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.DelegatedSince).Format(time.RFC3339))
		case "delegate_since_time":
			res[i] = strconv.Quote(a.ctx.Indexer.BlockTime(a.ctx.Context, a.DelegateSince).Format(time.RFC3339))
		case "rich_rank":
			if rank != nil {
				res[i] = strconv.FormatInt(int64(rank.RichRank), 10)
			} else {
				res[i] = "0"
			}
		case "flow_rank":
			if rank != nil {
				res[i] = strconv.FormatInt(int64(rank.FlowRank), 10)
			} else {
				res[i] = "0"
			}
		case "traffic_rank":
			if rank != nil {
				res[i] = strconv.FormatInt(int64(rank.TrafficRank), 10)
			} else {
				res[i] = "0"
			}
		default:
			continue
		}
	}
	return res, nil
}

func StreamAccountTable(ctx *ApiContext, args *TableRequest) (interface{}, int) {
	// fetch chain params at current height
	params := ctx.Crawler.ParamsByHeight(-1)

	// access table
	table, err := ctx.Indexer.Table(args.Table)
	if err != nil {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("cannot access table '%s'", args.Table), err))
	}

	// translate long column names to short names used in pack tables
	var (
		srcNames     []string
		needAccountT bool
		needRanking  bool
	)
	if len(args.Columns) > 0 {
		// resolve short column names
		srcNames = make([]string, 0, len(args.Columns))
		for _, v := range args.Columns {
			n, ok := accSourceNames[v]
			if !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
			}
			switch v {
			case "manager", "delegate":
				needAccountT = true
			case "rich_rank", "flow_rank", "traffic_rank":
				needRanking = true
			case "pubkey":
				srcNames = append(srcNames, "k") // pubkey hash
				srcNames = append(srcNames, "K") // pubkey type
			case "address":
				srcNames = append(srcNames, "H") // hash
				srcNames = append(srcNames, "t") // type
			}
			if args.Verbose {
				needAccountT = true
				needRanking = true
			}
			if n != "-" {
				srcNames = append(srcNames, n)
			}
		}
	} else {
		// use all table columns in order and reverse lookup their long names
		srcNames = table.Fields().Names()
		args.Columns = accAllAliases
		needAccountT = true
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
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				// single-address lookup and compile condition
				addr, err := chain.ParseAddress(val[0])
				if err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  mode,
					Value: addr.Hash,
					Raw:   val[0], // debugging aid
				}, pack.Condition{
					Field: table.Fields().Find("t"),
					Mode:  mode,
					Value: int64(addr.Type),
					Raw:   val[0], // debugging aid
				})
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup (Note: does not check for address type so may
				// return duplicates)
				hashes := make([][]byte, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := chain.ParseAddress(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					hashes = append(hashes, addr.Hash)
				}
				q.Conditions = append(q.Conditions, pack.Condition{
					Field: table.Fields().Find("H"),
					Mode:  mode,
					Value: hashes,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "pubkey":
			if mode != pack.FilterModeEqual {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
			h, err := chain.ParseHash(val[0])
			if err != nil {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid pubkey hash '%s'", val), err))
			}
			q.Conditions = append(q.Conditions, pack.Condition{
				Field: table.Fields().Find("k"),
				Mode:  pack.FilterModeEqual,
				Value: h.Hash,
				Raw:   val[0], // debugging aid
			}, pack.Condition{
				Field: table.Fields().Find("K"),
				Mode:  pack.FilterModeEqual,
				Value: int64(h.Type),
				Raw:   val[0], // debugging aid
			})

		case "delegate", "manager":
			// parse address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			field := "D" // delegate
			if prefix == "manager" {
				field = "M"
			}
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== no delegate/manager set)
					q.Conditions = append(q.Conditions, pack.Condition{
						Field: table.Fields().Find(field), // account id
						Mode:  mode,
						Value: uint64(0),
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-account lookup and compile condition
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
							Field: table.Fields().Find(field), // account id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "account not found", // debugging aid
						})
					} else {
						// add id as extra condition
						q.Conditions = append(q.Conditions, pack.Condition{
							Field: table.Fields().Find(field), // account id
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
					Field: table.Fields().Find(field), // account id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := accSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64, handle multiple values for rg, in, nin
				switch prefix {
				case "total_received", "total_sent", "total_burned",
					"total_fees_paid", "total_rewards_earned", "total_fees_earned",
					"total_lost", "frozen_deposits", "frozen_rewards", "frozen_fees",
					"unclaimed_balance", "spendable_balance", "delegated_balance":
					fvals := make([]string, 0)
					for _, vv := range strings.Split(v, ",") {
						fval, err := strconv.ParseFloat(vv, 64)
						if err != nil {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, vv), err))
						}
						fvals = append(fvals, strconv.FormatInt(params.ConvertAmount(fval), 10))
					}
					v = strings.Join(fvals, ",")
				case "address_type":
					// consider comma separated lists, convert type to int and back to string list
					typs := make([]int64, 0)
					for _, t := range strings.Split(v, ",") {
						typ := chain.ParseAddressType(t)
						if !typ.IsValid() {
							panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address type '%s'", val[0]), nil))
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

	// Step 1: query database
	res, err := table.Query(ctx, q)
	if err != nil {
		panic(EInternal(EC_DATABASE, "query failed", err))
	}
	ctx.Log.Tracef("Processing result with %d rows %d cols", res.Rows(), res.Cols())
	defer res.Close()

	// Step 2: resolve related accounts using lookup (when requested)
	accMap := make(map[model.AccountID]chain.Address)
	if needAccountT && res.Rows() > 0 {
		// get a unique copy of delegate and manager id columns (clip on request limit)
		dcol, _ := res.Uint64Column("D")
		mcol, _ := res.Uint64Column("M")
		find := vec.UniqueUint64Slice(dcol[:util.Min(len(dcol), int(args.Limit))])
		find = vec.UniqueUint64Slice(append(find, mcol[:util.Min(len(mcol), int(args.Limit))]...))

		// lookup accounts from id
		q := pack.Query{
			Name:   ctx.RequestID + ".account_lookup",
			Fields: table.Fields().Select("I", "H", "t"),
			Conditions: pack.ConditionList{pack.Condition{
				Field: table.Fields().Find("I"),
				Mode:  pack.FilterModeIn,
				Value: find,
			}},
		}
		ctx.Log.Tracef("Looking up %d accounts", len(find))
		type XAcc struct {
			Id   model.AccountID   `pack:"I,pk"`
			Hash []byte            `pack:"H"`
			Type chain.AddressType `pack:"t"`
		}
		acc := &XAcc{}
		err := table.Stream(ctx, q, func(r pack.Row) error {
			if err := r.Decode(acc); err != nil {
				return err
			}
			accMap[acc.Id] = chain.NewAddress(acc.Type, acc.Hash)
			return nil
		})
		if err != nil {
			// non-fatal error
			ctx.Log.Errorf("Account lookup failed: %v", err)
		}
	}

	var ranking *etl.AccountRanking
	if needRanking {
		ranking, _ = ctx.Indexer.GetRanking(ctx.Context, ctx.Now)
	}

	// prepare return type marshalling
	acc := &Account{
		verbose: args.Verbose,
		columns: util.StringList(args.Columns),
		params:  params,
		addrs:   accMap,
		ctx:     ctx,
		ranking: ranking,
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
		err = res.Walk(func(r pack.Row) error {
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
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
