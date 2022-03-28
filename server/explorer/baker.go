// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"time"

	"github.com/echa/code/iso"
	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(BakerList{})
}

type BakerStatistics struct {
	TotalRewardsEarned float64 `json:"total_rewards_earned"`
	TotalFeesEarned    float64 `json:"total_fees_earned"`
	TotalLost          float64 `json:"total_lost"`
	BlocksBaked        int64   `json:"blocks_baked"`
	BlocksProposed     int64   `json:"blocks_proposed"`
	SlotsEndorsed      int64   `json:"slots_endorsed"`
	AvgLuck64          *int64  `json:"avg_luck_64,omitempty"`
	AvgPerformance64   *int64  `json:"avg_performance_64,omitempty"`
	AvgContribution64  *int64  `json:"avg_contribution_64,omitempty"`
	NBakerOps          int64   `json:"n_baker_ops"`
	NProposal          int64   `json:"n_proposals"`
	NBallot            int64   `json:"n_ballots"`
	NEndorsement       int64   `json:"n_endorsements"`
	NPreendorsement    int64   `json:"n_preendorsements"`
	NSeedNonce         int64   `json:"n_nonce_revelations"`
	N2Baking           int64   `json:"n_double_bakings"`
	N2Endorsement      int64   `json:"n_double_endorsements"`
	NAccusations       int64   `json:"n_accusations"`
	NSetDepositsLimit  int64   `json:"n_set_limits"`
}

type Baker struct {
	Id                model.AccountID `json:"-"`
	Address           tezos.Address   `json:"address"`
	BakerSince        time.Time       `json:"baker_since_time"`
	BakerUntil        *time.Time      `json:"baker_until,omitempty"`
	GracePeriod       int64           `json:"grace_period"`
	BakerVersion      string          `json:"baker_version"`
	TotalBalance      float64         `json:"total_balance"`
	SpendableBalance  float64         `json:"spendable_balance"`
	FrozenBalance     float64         `json:"frozen_balance"`
	DelegatedBalance  float64         `json:"delegated_balance"`
	StakingBalance    float64         `json:"staking_balance"`
	StakingCapacity   float64         `json:"staking_capacity"`
	DepositsLimit     *float64        `json:"deposits_limit"`
	StakingShare      float64         `json:"staking_share"`
	ActiveDelegations int64           `json:"active_delegations"`
	IsFull            bool            `json:"is_full"`
	IsActive          bool            `json:"is_active"`

	Stats    *BakerStatistics `json:"stats,omitempty"`
	Metadata *Metadata        `json:"metadata,omitempty"`

	// caching
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func NewBaker(ctx *server.Context, b *model.Baker, args server.Options) *Baker {
	tip := getTip(ctx)
	capacity := b.StakingCapacity(ctx.Params, tip.Rolls)
	baker := &Baker{
		Id:                b.AccountId,
		Address:           b.Address,
		BakerSince:        ctx.Indexer.LookupBlockTime(ctx.Context, b.BakerSince),
		GracePeriod:       b.GracePeriod,
		BakerVersion:      hex.EncodeToString(b.GetVersionBytes()),
		TotalBalance:      ctx.Params.ConvertValue(b.TotalBalance()),
		SpendableBalance:  ctx.Params.ConvertValue(b.Account.SpendableBalance),
		FrozenBalance:     ctx.Params.ConvertValue(b.FrozenBalance()),
		DelegatedBalance:  ctx.Params.ConvertValue(b.DelegatedBalance),
		StakingBalance:    ctx.Params.ConvertValue(b.StakingBalance()),
		StakingCapacity:   ctx.Params.ConvertValue(capacity),
		ActiveDelegations: b.ActiveDelegations,
		IsFull:            b.StakingBalance() >= capacity,
		StakingShare:      math.Ceil(float64(b.Rolls(ctx.Params))/float64(tip.Rolls)*100) / 100,
		expires:           tip.Timestamp.Add(ctx.Params.BlockTime()),
		lastmod:           ctx.Indexer.LookupBlockTime(ctx.Context, b.Account.LastSeen),
	}

	if !b.IsActive {
		baker.BakerUntil = ctx.Indexer.LookupBlockTimePtr(ctx.Context, b.BakerUntil)
		baker.StakingShare = 0
	}

	if b.DepositsLimit >= 0 {
		baker.DepositsLimit = Float64Ptr(ctx.Params.ConvertValue(b.DepositsLimit))
	}

	if args.WithMeta() {
		// add statistics
		baker.Stats = &BakerStatistics{
			TotalRewardsEarned: ctx.Params.ConvertValue(b.TotalRewardsEarned),
			TotalFeesEarned:    ctx.Params.ConvertValue(b.TotalFeesEarned),
			TotalLost:          ctx.Params.ConvertValue(b.TotalLost),
			BlocksBaked:        b.BlocksBaked,
			BlocksProposed:     b.BlocksProposed,
			SlotsEndorsed:      b.SlotsEndorsed,
			NBakerOps:          b.NBakerOps,
			NProposal:          b.NProposal,
			NBallot:            b.NBallot,
			NEndorsement:       b.NEndorsement,
			NPreendorsement:    b.NPreendorsement,
			NSeedNonce:         b.NSeedNonce,
			N2Baking:           b.N2Baking,
			N2Endorsement:      b.N2Endorsement,
			NSetDepositsLimit:  b.NSetDepositsLimit,
			NAccusations:       b.NAccusations,
		}

		// add metadata
		if md, ok := lookupMetadataById(ctx, b.AccountId, 0, false); ok {
			baker.Metadata = md
		}
	}

	return baker
}

var _ server.RESTful = (*BakerList)(nil)
var _ server.Resource = (*BakerList)(nil)

func (_ BakerList) RESTPrefix() string {
	return "/explorer/bakers"
}

func (l BakerList) RESTPath(r *mux.Router) string {
	return l.RESTPrefix()
}

func (a BakerList) RegisterDirectRoutes(r *mux.Router) error {
	r.HandleFunc(a.RESTPrefix(), server.C(ListBakers)).Methods("GET")
	return nil
}

func (b BakerList) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadBaker)).Methods("GET").Name("baker")
	r.HandleFunc("/{ident}/votes", server.C(ListBakerVotes)).Methods("GET")
	r.HandleFunc("/{ident}/delegations", server.C(ListBakerDelegations)).Methods("GET")
	r.HandleFunc("/{ident}/metadata", server.C(ReadMetadata)).Methods("GET")
	return nil
}

type BakerListRequest struct {
	ListRequest
	Status        *string        `schema:"status"`
	Country       *iso.Country   `schema:"country"`
	Suggest       *tezos.Address `schema:"suggest"`
	WithSponsored bool           `schema:"ads"`
}

type BakerList struct {
	list     []Baker
	expires  time.Time
	modified time.Time
}

func (l BakerList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l BakerList) LastModified() time.Time      { return l.modified }
func (l BakerList) Expires() time.Time           { return l.expires }

func ListBakers(ctx *server.Context) (interface{}, int) {
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
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such account", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}

		// ensure account is not empty
		if !suggest.IsFunded {
			panic(server.ENotFound(server.EC_RESOURCE_CONFLICT, "account is not funded", nil))
		}
		if suggest.IsBaker {
			panic(server.ENotFound(server.EC_RESOURCE_CONFLICT, "account is not delegatable", nil))
		}

		// suggestions are limited to public bakers
		status := "public"
		args.Status = &status
	}

	// load list of all current bakers (no limit to keep loading logic simple
	// when used in combination with suggest feature)
	bakers, err := ctx.Indexer.ListBakers(ctx.Context, true)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list bakers", err))
	}
	// log.Infof("Found %d active bakers", len(bakers))

	// get chain data from cache
	tip := getTip(ctx)
	netRolls := tip.Rolls

	// get current alias data
	meta := allMetadataById(ctx)

	// prepare response lists
	ads := make([]Baker, 0)
	bkr := make([]Baker, 0)

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
			if suggest.BakerId == v.AccountId {
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
		baker := Baker{
			Id:                v.AccountId,
			GracePeriod:       v.GracePeriod,
			Address:           v.Address,
			BakerSince:        ctx.Indexer.LookupBlockTime(ctx.Context, v.BakerSince),
			BakerVersion:      hex.EncodeToString(v.GetVersionBytes()),
			TotalBalance:      ctx.Params.ConvertValue(v.TotalBalance()),
			SpendableBalance:  ctx.Params.ConvertValue(v.Account.SpendableBalance),
			FrozenBalance:     ctx.Params.ConvertValue(v.FrozenBalance()),
			DelegatedBalance:  ctx.Params.ConvertValue(v.DelegatedBalance),
			StakingBalance:    ctx.Params.ConvertValue(v.StakingBalance()),
			StakingCapacity:   ctx.Params.ConvertValue(capacity),
			ActiveDelegations: v.ActiveDelegations,
			IsFull:            v.StakingBalance() >= capacity,
			IsActive:          v.IsActive,
			StakingShare:      math.Ceil(float64(v.Rolls(ctx.Params))/float64(netRolls)*100) / 100,
			Stats: &BakerStatistics{
				TotalRewardsEarned: ctx.Params.ConvertValue(v.TotalRewardsEarned),
				TotalFeesEarned:    ctx.Params.ConvertValue(v.TotalFeesEarned),
				TotalLost:          ctx.Params.ConvertValue(v.TotalLost),
				BlocksBaked:        v.BlocksBaked,
				BlocksProposed:     v.BlocksProposed,
				SlotsEndorsed:      v.SlotsEndorsed,
				NBakerOps:          v.NBakerOps,
				NProposal:          v.NProposal,
				NBallot:            v.NBallot,
				NEndorsement:       v.NEndorsement,
				NPreendorsement:    v.NPreendorsement,
				NSeedNonce:         v.NSeedNonce,
				N2Baking:           v.N2Baking,
				N2Endorsement:      v.N2Endorsement,
				NSetDepositsLimit:  v.NSetDepositsLimit,
				NAccusations:       v.NAccusations,
			},
		}

		// attach alias and append to lists
		if hasAlias {
			baker.Metadata = alias
			if alias.IsSponsored && args.WithSponsored {
				ads = append(ads, baker)
			} else {
				bkr = append(bkr, baker)
			}
		} else {
			bkr = append(bkr, baker)
		}

		// apply limit only when not in suggest mode (need all results for randomization)
		if suggest == nil && args.Limit > 0 && len(ads)+len(bkr) == int(args.Limit) {
			break
		}
	}

	// log.Infof("Filtered %d + %d active bakers", len(ads), len(bkr))

	// build result
	resp := &BakerList{
		list:     make([]Baker, 0),
		modified: tip.Timestamp,
		expires:  ctx.Now,
	}

	// only cache non-randomized results
	if suggest == nil {
		resp.expires = tip.Timestamp.Add(ctx.Params.BlockTime())
	}

	// randomize suggestion: <=50% sponsored
	if args.Limit > 0 && suggest != nil {
		for args.Limit > 0 && len(ads)+len(bkr) > 0 {
			if len(resp.list) < int(args.Limit) && len(ads) > 0 {
				// draw random from sponsored
				idx := rand.Intn(len(ads))
				resp.list = append(resp.list, ads[idx])
				ads = append(ads[:idx], ads[idx+1:]...)
			} else {
				// draw random from other
				idx := rand.Intn(len(bkr))
				resp.list = append(resp.list, bkr[idx])
				bkr = append(bkr[:idx], bkr[idx+1:]...)
			}
			args.Limit--
		}
	} else {
		resp.list = append(ads, bkr...)
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
	return resp, http.StatusOK
}

func loadBaker(ctx *server.Context) *model.Baker {
	if accIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || accIdent == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing baker address", nil))
	} else {
		addr, err := tezos.ParseAddress(accIdent)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		bkr, err := ctx.Indexer.LookupBaker(ctx, addr)
		if err != nil {
			switch err {
			case index.ErrNoBakerEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such account", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return bkr
	}
}

func ReadBaker(ctx *server.Context) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	return NewBaker(ctx, loadBaker(ctx), args), http.StatusOK
}

func ListBakerVotes(ctx *server.Context) (interface{}, int) {
	args := &OpsRequest{
		ListRequest: ListRequest{
			Order: pack.OrderDesc,
		},
	}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	r := etl.ListRequest{
		Account: acc,
		Typs:    model.OpTypeList{model.OpTypeProposal, model.OpTypeBallot},
		Since:   args.SinceHeight,
		Until:   args.BlockHeight,
		Offset:  args.Offset,
		Limit:   ctx.Cfg.ClampExplore(args.Limit),
		Cursor:  args.Cursor,
		Order:   args.Order,
	}

	ops, err := ctx.Indexer.ListAccountOps(ctx, r)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read account operations", err))
	}

	resp := make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewOp(ctx, v, nil, nil, args, cache), args.WithMerge())
	}
	return resp, http.StatusOK
}

func ListBakerDelegations(ctx *server.Context) (interface{}, int) {
	args := &OpsRequest{
		ListRequest: ListRequest{
			Order: pack.OrderDesc,
		},
	}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	r := etl.ListRequest{
		Account: acc,
		// ReceiverId: acc.RowId,
		Mode:   pack.FilterModeEqual,
		Typs:   []model.OpType{model.OpTypeDelegation},
		Since:  args.SinceHeight,
		Until:  args.BlockHeight,
		Offset: args.Offset,
		Limit:  ctx.Cfg.ClampExplore(args.Limit),
		Cursor: args.Cursor,
		Order:  args.Order,
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}

	ops, err := ctx.Indexer.ListAccountOps(ctx, r)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read account operations", err))
	}

	resp := make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewOp(ctx, v, nil, nil, args, cache), args.WithMerge())
	}
	return resp, http.StatusOK
}
