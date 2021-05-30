// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"time"
)

func init() {
	register(Ranks{})
}

var _ RESTful = (*Ranks)(nil)

type Ranks struct{}

func (rx Ranks) RESTPrefix() string {
	return "/explorer/rank"
}

func (rx Ranks) RESTPath(r *mux.Router) string {
	return rx.RESTPrefix()
}

func (rx Ranks) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (rx Ranks) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/traffic", C(GetTrafficList)).Methods("GET")
	r.HandleFunc("/volume", C(GetVolumeList)).Methods("GET")
	r.HandleFunc("/balances", C(GetRichList)).Methods("GET")
	return nil
}

type RankListItem struct {
	Rank    int     `json:"rank"`
	Address string  `json:"address"`
	Balance float64 `json:"balance,omitempty"`
	Traffic int64   `json:"traffic,omitempty"`
	Volume  float64 `json:"volume,omitempty"`
}

type RankList struct {
	list     []RankListItem
	expires  time.Time
	modified time.Time
}

func (l RankList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l RankList) LastModified() time.Time      { return l.modified }
func (l RankList) Expires() time.Time           { return l.expires }

func GetTrafficList(ctx *ApiContext) (interface{}, int) {
	args := &ListRequest{}
	ctx.ParseRequestArgs(args)
	tip := ctx.Tip
	params := ctx.Params
	args.Limit = ctx.Cfg.ClampExplore(args.Limit)

	list, err := ctx.Indexer.TopTraffic(ctx.Context, int(args.Limit), int(args.Offset))
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot construct rank list", err))
	}
	resp := &RankList{
		list:     make([]RankListItem, len(list)),
		expires:  tip.BestTime.Add(params.TimeBetweenBlocks[0]),
		modified: tip.BestTime,
	}
	for i, v := range list {
		if v.TrafficRank == 0 {
			resp.list = resp.list[:i]
			break
		}
		resp.list[i].Rank = v.TrafficRank
		resp.list[i].Address = ctx.Indexer.LookupAddress(ctx, v.AccountId).String()
		resp.list[i].Traffic = v.TxTraffic24h
	}
	return resp, http.StatusOK
}

func GetVolumeList(ctx *ApiContext) (interface{}, int) {
	args := &ListRequest{}
	ctx.ParseRequestArgs(args)
	tip := ctx.Tip
	params := ctx.Params
	args.Limit = ctx.Cfg.ClampExplore(args.Limit)

	list, err := ctx.Indexer.TopVolume(ctx.Context, int(args.Limit), int(args.Offset))
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot construct rank list", err))
	}
	resp := &RankList{
		list:     make([]RankListItem, len(list)),
		expires:  tip.BestTime.Add(params.TimeBetweenBlocks[0]),
		modified: tip.BestTime,
	}
	for i, v := range list {
		if v.VolumeRank == 0 {
			resp.list = resp.list[:i]
			break
		}
		resp.list[i].Rank = v.VolumeRank
		resp.list[i].Address = ctx.Indexer.LookupAddress(ctx, v.AccountId).String()
		resp.list[i].Volume = params.ConvertValue(v.TxVolume24h)
	}
	return resp, http.StatusOK
}

func GetRichList(ctx *ApiContext) (interface{}, int) {
	args := &ListRequest{}
	ctx.ParseRequestArgs(args)
	tip := ctx.Tip
	params := ctx.Params
	args.Limit = ctx.Cfg.ClampExplore(args.Limit)

	list, err := ctx.Indexer.TopRich(ctx.Context, int(args.Limit), int(args.Offset))
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot construct rank list", err))
	}
	resp := &RankList{
		list:     make([]RankListItem, len(list)),
		expires:  tip.BestTime.Add(params.TimeBetweenBlocks[0]),
		modified: tip.BestTime,
	}
	for i, v := range list {
		if v.RichRank == 0 {
			resp.list = resp.list[:i]
			break
		}
		resp.list[i].Rank = v.RichRank
		resp.list[i].Address = ctx.Indexer.LookupAddress(ctx, v.AccountId).String()
		resp.list[i].Balance = params.ConvertValue(v.Balance)
	}
	return resp, http.StatusOK
}
