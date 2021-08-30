// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"net/http"
	"sort"
	"time"

	"github.com/echa/code/iso"
	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(BakerList{})
}

type ExplorerBaker struct {
	Id                model.AccountID   `json:"id"`
	Address           tezos.Address     `json:"address"`
	BakerSince        time.Time         `json:"baker_since_time"`
	BakerVersion      string            `json:"baker_version"`
	TotalBalance      float64           `json:"total_balance"`
	SpendableBalance  float64           `json:"spendable_balance"`
	FrozenDeposits    float64           `json:"frozen_deposits"`
	FrozenRewards     float64           `json:"frozen_rewards"`
	FrozenFees        float64           `json:"frozen_fees"`
	StakingBalance    float64           `json:"staking_balance"`
	StakingCapacity   float64           `json:"staking_capacity"`
	ActiveDelegations int64             `json:"active_delegations"`
	IsFull            bool              `json:"is_full"`
	Rolls             int64             `json:"rolls"`
	Share             float64           `json:"share"`
	AvgLuck64         int64             `json:"avg_luck_64"`
	AvgPerformance64  int64             `json:"avg_performance_64"`
	AvgContribution64 int64             `json:"avg_contribution_64"`
	Metadata          *ExplorerMetadata `json:"metadata,omitempty"`
}

var _ RESTful = (*BakerList)(nil)

func (_ BakerList) RESTPrefix() string {
	return "/explorer/bakers"
}

func (l BakerList) RESTPath(r *mux.Router) string {
	return l.RESTPrefix()
}

func (a BakerList) RegisterDirectRoutes(r *mux.Router) error {
	r.HandleFunc(a.RESTPrefix(), C(ListBakers)).Methods("GET")
	return nil
}

func (b BakerList) RegisterRoutes(r *mux.Router) error {
	return nil
}

type BakerListRequest struct {
	ListRequest
	Status  *string        `schema:"status"`
	Country *iso.Country   `schema:"country"`
	Suggest *tezos.Address `schema:"suggest"`
}

type BakerList struct {
	list     []ExplorerBaker
	expires  time.Time
	modified time.Time
}

func (l BakerList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l BakerList) LastModified() time.Time      { return l.modified }
func (l BakerList) Expires() time.Time           { return l.expires }

func ListBakers(ctx *ApiContext) (interface{}, int) {
	args := &BakerListRequest{}
	ctx.ParseRequestArgs(args)
	args.Limit = ctx.Cfg.ClampExplore(args.Limit)

	// load suggest account
	var suggest *model.Account
	if args.Suggest != nil {
		var err error
		if suggest, err = ctx.Indexer.LookupAccount(ctx.Context, *args.Suggest); err != nil {
			switch err {
			case index.ErrNoAccountEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such account", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}

		// ensure account is not empty
		if !suggest.IsFunded {
			panic(ENotFound(EC_RESOURCE_CONFLICT, "account is not funded", nil))
		}
		if suggest.IsDelegate {
			panic(ENotFound(EC_RESOURCE_CONFLICT, "account is not delegatable", nil))
		}

		// suggestions are limited to public bakers
		status := "public"
		args.Status = &status
	}

	// load list of all current bakers (no limit to keep loading logic simple
	// when used in combination with suggest feature)
	bakers, err := ctx.Indexer.ListActiveDelegates(ctx.Context)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot list bakers", err))
	}
	// log.Infof("Found %d active bakers", len(bakers))

	// get chain data from cache
	tip := getTip(ctx)
	netRolls := tip.Rolls

	// get current alias data
	meta := allMetadataById(ctx)

	// prepare response lists
	bkr := make([]ExplorerBaker, 0)

	// filter bakers
	for _, v := range bakers {
		// filter by alias attributes
		alias, hasAlias := meta[v.RowId.Value()]
		if hasAlias {
			if args.Status != nil && *args.Status != alias.Status {
				// log.Infof("Skip %s status %s", v, alias.Status)
				continue
			}
			if args.Country != nil && *args.Country != alias.Country {
				// log.Infof("Skip %s country %s", v, alias.Country)
				continue
			}
			// filter by suggestion attributes
			if suggest != nil {
				if alias.MinDelegation > 0 && ctx.Params.ConvertValue(suggest.Balance()) < alias.MinDelegation {
					// log.Infof("Skip %s balance < min %d", v, suggest.Balance())
					continue
				}
			}
		} else {
			// if no alias is known
			if args.Status != nil {
				// log.Infof("Skip %s non status", v)
				continue
			}
			if args.Country != nil {
				// log.Infof("Skip %s non country", v)
				continue
			}
		}

		if suggest != nil {
			// skip non-delegatable bakers
			if alias.NonDelegatable {
				continue
			}
			// filter by capacity
			if suggest.Balance() > v.StakingCapacity(ctx.Params, netRolls)-v.StakingBalance() {
				// log.Infof("Skip %s capacity %d < %d",
				// 	v,
				// 	v.StakingCapacity(ctx.Params, netRolls)-v.StakingBalance(),
				// 	suggest.Balance(),
				// )
				continue
			}
			// remove the current baker, if any
			if suggest.DelegateId == v.RowId {
				continue
			}
		}

		// apply offset and cursor (only in non-suggest mode)
		if suggest == nil {
			if args.Offset > 0 {
				// log.Infof("Skip %s offset %d", v, args.Offset)
				args.Offset--
				continue
			}
			if args.Cursor > 0 && v.RowId.Value() <= args.Cursor {
				// log.Infof("Skip %s cursor %d", v, v.RowId)
				continue
			}
		}

		// build result
		capacity := v.StakingCapacity(ctx.Params, netRolls)
		baker := ExplorerBaker{
			Id:                v.RowId,
			Address:           v.Address,
			BakerSince:        ctx.Indexer.LookupBlockTime(ctx.Context, v.DelegateSince),
			BakerVersion:      hex.EncodeToString(v.GetVersionBytes()),
			TotalBalance:      ctx.Params.ConvertValue(v.TotalBalance()),
			SpendableBalance:  ctx.Params.ConvertValue(v.SpendableBalance),
			FrozenDeposits:    ctx.Params.ConvertValue(v.FrozenDeposits),
			FrozenRewards:     ctx.Params.ConvertValue(v.FrozenRewards),
			FrozenFees:        ctx.Params.ConvertValue(v.FrozenFees),
			StakingBalance:    ctx.Params.ConvertValue(v.StakingBalance()),
			StakingCapacity:   ctx.Params.ConvertValue(capacity),
			ActiveDelegations: v.ActiveDelegations,
			IsFull:            v.StakingBalance() >= capacity,
			Rolls:             v.Rolls(ctx.Params),
			Share:             float64(v.Rolls(ctx.Params)) / float64(netRolls),
		}

		// attach alias and append to lists
		if hasAlias {
			baker.Metadata = alias
		}
		bkr = append(bkr, baker)

		// apply limit only when not in suggest mode (need all results for randomization)
		if suggest == nil && args.Limit > 0 && len(bkr) == int(args.Limit) {
			break
		}
	}

	// log.Infof("Filtered %d active bakers", len(bkr))

	// build result
	resp := &BakerList{
		list:     make([]ExplorerBaker, 0),
		modified: tip.Timestamp,
		expires:  ctx.Now,
	}

	// only cache non-randomized results
	if suggest == nil {
		resp.expires = tip.Timestamp.Add(ctx.Params.TimeBetweenBlocks[0])
	}

	// randomize suggestion: <=50% sponsored
	if args.Limit > 0 && suggest != nil {
		for args.Limit > 0 && len(bkr) > 0 {
			if len(resp.list) < int(args.Limit) {
				idx := rand.Intn(len(bkr))
				resp.list = append(resp.list, bkr[idx])
				bkr = append(bkr[:idx], bkr[idx+1:]...)
			}
			args.Limit--
		}
	} else {
		resp.list = bkr
		if args.Limit > 0 {
			resp.list = resp.list[:util.Min(int(args.Limit), len(resp.list))]
		}
		if args.Order == pack.OrderAsc {
			sort.Slice(resp.list, func(i, j int) bool { return resp.list[i].Id < resp.list[j].Id })
		} else {
			sort.Slice(resp.list, func(i, j int) bool { return resp.list[i].Id > resp.list[j].Id })
		}
	}

	// log.Infof("Final %d active bakers cycle %d", len(resp.list), tip.Cycle)

	// add expensive performance data
	for i, v := range resp.list {
		if p, err := ctx.Indexer.BakerPerformance(ctx, v.Id, util.Max64(0, tip.Cycle-64), tip.Cycle); err == nil {
			resp.list[i].AvgLuck64 = p[0]
			resp.list[i].AvgPerformance64 = p[1]
			resp.list[i].AvgContribution64 = p[2]
		}
	}

	return resp, http.StatusOK
}
