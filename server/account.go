// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/hex"
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerAccount{})
}

var _ RESTful = (*ExplorerAccount)(nil)

type ExplorerAccount struct {
	Address            string                       `json:"address"`
	Type               string                       `json:"address_type"`
	Delegate           string                       `json:"delegate,omitempty"`
	Creator            string                       `json:"creator,omitempty"`
	Pubkey             tezos.Key                    `json:"pubkey,omitempty"`
	FirstIn            int64                        `json:"first_in"`
	FirstOut           int64                        `json:"first_out"`
	LastIn             int64                        `json:"last_in"`
	LastOut            int64                        `json:"last_out"`
	FirstSeen          int64                        `json:"first_seen"`
	LastSeen           int64                        `json:"last_seen"`
	DelegatedSince     int64                        `json:"delegated_since,omitempty"`
	DelegateSince      int64                        `json:"delegate_since,omitempty"`
	DelegateUntil      int64                        `json:"delegate_until,omitempty"`
	FirstInTime        *time.Time                   `json:"first_in_time,omitempty"`
	FirstOutTime       *time.Time                   `json:"first_out_time,omitempty"`
	LastInTime         *time.Time                   `json:"last_in_time,omitempty"`
	LastOutTime        *time.Time                   `json:"last_out_time,omitempty"`
	FirstSeenTime      time.Time                    `json:"first_seen_time,omitempty"`
	LastSeenTime       time.Time                    `json:"last_seen_time,omitempty"`
	DelegatedSinceTime *time.Time                   `json:"delegated_since_time,omitempty"`
	DelegateSinceTime  *time.Time                   `json:"delegate_since_time,omitempty"`
	DelegateUntilTime  *time.Time                   `json:"delegate_until_time,omitempty"`
	TotalReceived      float64                      `json:"total_received"`
	TotalSent          float64                      `json:"total_sent"`
	TotalBurned        float64                      `json:"total_burned"`
	TotalFeesPaid      float64                      `json:"total_fees_paid"`
	TotalRewardsEarned *float64                     `json:"total_rewards_earned,omitempty"`
	TotalFeesEarned    *float64                     `json:"total_fees_earned,omitempty"`
	TotalLost          *float64                     `json:"total_lost,omitempty"`
	FrozenDeposits     *float64                     `json:"frozen_deposits,omitempty"`
	FrozenRewards      *float64                     `json:"frozen_rewards,omitempty"`
	FrozenFees         *float64                     `json:"frozen_fees,omitempty"`
	UnclaimedBalance   float64                      `json:"unclaimed_balance,omitempty"`
	SpendableBalance   float64                      `json:"spendable_balance"`
	TotalBalance       float64                      `json:"total_balance"`
	DelegatedBalance   *float64                     `json:"delegated_balance,omitempty"`
	TotalDelegations   *int64                       `json:"total_delegations,omitempty"`
	ActiveDelegations  *int64                       `json:"active_delegations,omitempty"`
	IsFunded           bool                         `json:"is_funded"`
	IsActivated        bool                         `json:"is_activated"`
	IsDelegated        bool                         `json:"is_delegated"`
	IsRevealed         bool                         `json:"is_revealed"`
	IsDelegate         bool                         `json:"is_delegate"`
	IsActiveDelegate   bool                         `json:"is_active_delegate"`
	IsContract         bool                         `json:"is_contract"`
	BlocksBaked        *int                         `json:"blocks_baked,omitempty"`
	BlocksMissed       *int                         `json:"blocks_missed,omitempty"`
	BlocksStolen       *int                         `json:"blocks_stolen,omitempty"`
	BlocksEndorsed     *int                         `json:"blocks_endorsed,omitempty"`
	SlotsEndorsed      *int                         `json:"slots_endorsed,omitempty"`
	SlotsMissed        *int                         `json:"slots_missed,omitempty"`
	NOps               int                          `json:"n_ops"`
	NOpsFailed         int                          `json:"n_ops_failed"`
	NTx                int                          `json:"n_tx"`
	NDelegation        int                          `json:"n_delegation"`
	NOrigination       int                          `json:"n_origination"`
	NProposal          *int                         `json:"n_proposal,omitempty"`
	NBallot            *int                         `json:"n_ballot,omitempty"`
	TokenGenMin        int64                        `json:"token_gen_min"`
	TokenGenMax        int64                        `json:"token_gen_max"`
	GracePeriod        *int64                       `json:"grace_period,omitempty"`
	StakingBalance     *float64                     `json:"staking_balance,omitempty"`
	StakingCapacity    *float64                     `json:"staking_capacity,omitempty"`
	Rolls              *int64                       `json:"rolls,omitempty"`
	LastBakeHeight     *int64                       `json:"last_bake_height,omitempty"`
	LastBakeBlock      *string                      `json:"last_bake_block,omitempty"`
	LastBakeTime       *time.Time                   `json:"last_bake_time,omitempty"`
	LastEndorseHeight  *int64                       `json:"last_endorse_height,omitempty"`
	LastEndorseBlock   *string                      `json:"last_endorse_block,omitempty"`
	LastEndorseTime    *time.Time                   `json:"last_endorse_time,omitempty"`
	NextBakeHeight     *int64                       `json:"next_bake_height,omitempty"`
	NextBakePriority   *int                         `json:"next_bake_priority,omitempty"`
	NextBakeTime       *time.Time                   `json:"next_bake_time,omitempty"`
	NextEndorseHeight  *int64                       `json:"next_endorse_height,omitempty"`
	NextEndorseTime    *time.Time                   `json:"next_endorse_time,omitempty"`
	AvgLuck64          *int64                       `json:"avg_luck_64,omitempty"`
	AvgPerformance64   *int64                       `json:"avg_performance_64,omitempty"`
	AvgContribution64  *int64                       `json:"avg_contribution_64,omitempty"`
	BakerVersion       *string                      `json:"baker_version,omitempty"`
	Metadata           map[string]*ExplorerMetadata `json:"metadata,omitempty"`

	// LEGACY
	Ops     *ExplorerOpList   `json:"ops,omitempty"`
	Ballots []*ExplorerBallot `json:"ballots,omitempty"`

	// caching
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func NewExplorerAccount(ctx *ApiContext, a *model.Account, args Options) *ExplorerAccount {
	tip := ctx.Tip
	p := ctx.Params
	acc := &ExplorerAccount{
		Address:          a.String(),
		Type:             a.Type.String(),
		Creator:          ctx.Indexer.LookupAddress(ctx, a.CreatorId).String(),
		Delegate:         ctx.Indexer.LookupAddress(ctx, a.DelegateId).String(),
		Pubkey:           a.Key(),
		FirstIn:          a.FirstIn,
		FirstOut:         a.FirstOut,
		LastIn:           a.LastIn,
		LastOut:          a.LastOut,
		FirstSeen:        a.FirstSeen,
		LastSeen:         a.LastSeen,
		DelegatedSince:   a.DelegatedSince,
		DelegateSince:    a.DelegateSince,
		DelegateUntil:    a.DelegateUntil,
		TotalReceived:    p.ConvertValue(a.TotalReceived),
		TotalSent:        p.ConvertValue(a.TotalSent),
		TotalBurned:      p.ConvertValue(a.TotalBurned),
		TotalFeesPaid:    p.ConvertValue(a.TotalFeesPaid),
		UnclaimedBalance: p.ConvertValue(a.UnclaimedBalance),
		SpendableBalance: p.ConvertValue(a.SpendableBalance),
		TotalBalance:     p.ConvertValue(a.TotalBalance()),
		IsFunded:         a.IsFunded,
		IsActivated:      a.IsActivated,
		IsDelegated:      a.IsDelegated,
		IsRevealed:       a.IsRevealed,
		IsDelegate:       a.IsDelegate,
		IsActiveDelegate: a.IsActiveDelegate,
		IsContract:       a.IsContract,
		NOps:             a.NOps,
		NOpsFailed:       a.NOpsFailed,
		NTx:              a.NTx,
		NDelegation:      a.NDelegation,
		NOrigination:     a.NOrigination,
		TokenGenMin:      a.TokenGenMin,
		TokenGenMax:      a.TokenGenMax,
		expires:          tip.BestTime.Add(p.TimeBetweenBlocks[0]),
	}

	// resolve block times
	acc.FirstSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.FirstSeen)
	acc.LastSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.LastSeen)
	acc.FirstInTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.FirstIn)
	acc.FirstOutTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.FirstOut)
	acc.LastInTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.LastIn)
	acc.LastOutTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.LastOut)
	acc.DelegatedSinceTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.DelegatedSince)

	// fill in baker info, skip for non-delegates
	if a.IsDelegate {
		acc.DelegateSinceTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.DelegateSince)
		acc.DelegateUntilTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.DelegateUntil)
		acc.TotalDelegations = &a.TotalDelegations
		acc.ActiveDelegations = &a.ActiveDelegations
		acc.NProposal = &a.NProposal
		acc.NBallot = &a.NBallot
		acc.BlocksBaked = &a.BlocksBaked
		acc.BlocksMissed = &a.BlocksMissed
		acc.BlocksStolen = &a.BlocksStolen
		acc.BlocksEndorsed = &a.BlocksEndorsed
		acc.SlotsEndorsed = &a.SlotsEndorsed
		acc.SlotsMissed = &a.SlotsMissed
		acc.GracePeriod = &a.GracePeriod
		acc.TotalRewardsEarned = Float64Ptr(p.ConvertValue(a.TotalRewardsEarned))
		acc.TotalFeesEarned = Float64Ptr(p.ConvertValue(a.TotalFeesEarned))
		acc.TotalLost = Float64Ptr(p.ConvertValue(a.TotalLost))
		acc.FrozenDeposits = Float64Ptr(p.ConvertValue(a.FrozenDeposits))
		acc.FrozenRewards = Float64Ptr(p.ConvertValue(a.FrozenRewards))
		acc.FrozenFees = Float64Ptr(p.ConvertValue(a.FrozenFees))
		acc.StakingBalance = Float64Ptr(p.ConvertValue(a.StakingBalance()))
		acc.StakingCapacity = Float64Ptr(p.ConvertValue(a.StakingCapacity(ctx.Params, getTip(ctx).Rolls)))
		acc.DelegatedBalance = Float64Ptr(p.ConvertValue(a.DelegatedBalance))

		// calculate current rolls
		rolls := a.StakingBalance() / p.TokensPerRoll
		acc.Rolls = &rolls

		var (
			zeroInt    int
			zeroInt64  int64
			zeroString string
			zeroTime   time.Time
		)

		// get performance data
		recentCycle := p.CycleFromHeight(a.LastSeen) - 1
		if p, err := ctx.Indexer.BakerPerformance(ctx, a.RowId, util.Max64(0, recentCycle-64), recentCycle); err == nil {
			acc.AvgLuck64 = &p[0]
			acc.AvgPerformance64 = &p[1]
			acc.AvgContribution64 = &p[2]
		} else {
			acc.AvgLuck64 = &zeroInt64
			acc.AvgPerformance64 = &zeroInt64
			acc.AvgContribution64 = &zeroInt64
		}

		if a.BlocksBaked > 0 {
			if b, err := ctx.Indexer.LookupLastBakedBlock(ctx, a); err == nil {
				acc.LastBakeHeight = &b.Height
				h := b.Hash.String()
				acc.LastBakeBlock = &h
				acc.LastBakeTime = &b.Timestamp
			}
		}
		if acc.LastBakeHeight == nil {
			acc.LastBakeHeight = &zeroInt64
			acc.LastBakeBlock = &zeroString
			acc.LastBakeTime = &zeroTime
		}

		if a.BlocksEndorsed > 0 {
			if b, err := ctx.Indexer.LookupLastEndorsedBlock(ctx, a); err == nil {
				// from op table
				acc.LastEndorseHeight = &b.Height
				h := b.Hash.String()
				acc.LastEndorseBlock = &h
				acc.LastEndorseTime = &b.Timestamp
			}
		}
		if acc.LastEndorseHeight == nil {
			acc.LastEndorseHeight = &zeroInt64
			acc.LastEndorseBlock = &zeroString
			acc.LastEndorseTime = &zeroTime
		}

		if a.BakerVersion > 0 {
			str := hex.EncodeToString(a.GetVersionBytes())
			acc.BakerVersion = &str
		} else {
			acc.BakerVersion = &zeroString
		}

		if a.IsActiveDelegate {
			// from rights cache
			bh, eh := ctx.Indexer.NextRights(ctx, a.RowId, tip.BestHeight)
			if bh > 0 {
				acc.NextBakeHeight = &bh
				acc.NextBakePriority = &zeroInt
				tm := tip.BestTime.Add(p.TimeBetweenBlocks[0] * time.Duration(bh-tip.BestHeight))
				acc.NextBakeTime = &tm
			} else {
				acc.NextBakeHeight = &zeroInt64
				acc.NextBakePriority = &zeroInt
				acc.NextBakeTime = &zeroTime
			}
			if eh > 0 {
				acc.NextEndorseHeight = &eh
				tm := tip.BestTime.Add(p.TimeBetweenBlocks[0] * time.Duration(eh-tip.BestHeight))
				acc.NextEndorseTime = &tm
			} else {
				acc.NextEndorseHeight = &zeroInt64
				acc.NextEndorseTime = &zeroTime
			}
		}
	}

	if args.WithMeta() {
		// add metadata
		acc.Metadata = make(map[string]*ExplorerMetadata)
		if md, ok := lookupMetadataById(ctx, a.RowId, 0, false); ok {
			acc.Metadata[acc.Address] = md
		}

		// baker metadata for delegators
		if a.IsDelegated {
			if md, ok := lookupMetadataById(ctx, a.DelegateId, 0, false); ok {
				acc.Metadata[acc.Delegate] = md
			}
		}

		// manager/creator metadata for contracts
		if md, ok := lookupMetadataById(ctx, a.CreatorId, 0, false); ok {
			acc.Metadata[acc.Creator] = md
		}
	}

	// update last-modified header at least once per cycle to reflect closed baker decay
	acc.lastmod = acc.LastSeenTime
	if cc := p.CycleFromHeight(tip.BestHeight); p.CycleFromHeight(acc.LastSeen) < cc {
		acc.lastmod = ctx.Indexer.LookupBlockTime(ctx.Context, p.CycleStartHeight(cc))
	}

	return acc
}

func (a ExplorerAccount) LastModified() time.Time {
	return a.lastmod
}

func (a ExplorerAccount) Expires() time.Time {
	return a.expires
}

func (a ExplorerAccount) RESTPrefix() string {
	return "/explorer/account"
}

func (a ExplorerAccount) RESTPath(r *mux.Router) string {
	path, _ := r.Get("account").URLPath("ident", a.Address)
	return path.String()
}

func (b ExplorerAccount) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b ExplorerAccount) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadAccount)).Methods("GET").Name("account")
	r.HandleFunc("/{ident}/contracts", C(ReadManagedAccounts)).Methods("GET")
	r.HandleFunc("/{ident}/operations", C(ListAccountOperations)).Methods("GET")
	r.HandleFunc("/{ident}/ballots", C(ListAccountBallots)).Methods("GET")
	r.HandleFunc("/{ident}/metadata", C(ReadMetadata)).Methods("GET")

	// LEGACY: keep here for dapp and wallet compatibility
	r.HandleFunc("/{ident}/op", C(ReadAccountOps)).Methods("GET")
	r.HandleFunc("/{ident}/managed", C(ReadManagedAccounts)).Methods("GET")

	return nil
}

type AccountRequest struct {
	ListRequest      // offset, limit, cursor, order
	Meta        bool `schema:"meta"` // include account metadata
}

func (r *AccountRequest) WithPrim() bool     { return false }
func (r *AccountRequest) WithUnpack() bool   { return false }
func (r *AccountRequest) WithHeight() int64  { return 0 }
func (r *AccountRequest) WithMeta() bool     { return r != nil && r.Meta }
func (r *AccountRequest) WithRights() bool   { return false }
func (r *AccountRequest) WithCollapse() bool { return false }

func loadAccount(ctx *ApiContext) *model.Account {
	if accIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || accIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing account address", nil))
	} else {
		addr, err := tezos.ParseAddress(accIdent)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		acc, err := ctx.Indexer.LookupAccount(ctx, addr)
		if err != nil {
			switch err {
			case index.ErrNoAccountEntry:
				// cross-lookup activated account from blinded address
				if addr.Type != tezos.AddressTypeBlinded {
					panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such account", err))
				}
				acc, err = ctx.Indexer.FindActivatedAccount(ctx, addr)
				if err != nil {
					switch err {
					case index.ErrNoAccountEntry:
						panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such account", err))
					default:
						panic(EInternal(EC_DATABASE, err.Error(), nil))
					}
				}
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return acc
	}
}

func ReadAccount(ctx *ApiContext) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	return NewExplorerAccount(ctx, loadAccount(ctx), args), http.StatusOK
}

func ReadManagedAccounts(ctx *ApiContext) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	m, err := ctx.Indexer.ListManaged(ctx, acc.RowId, args.Limit, args.Offset, args.Cursor, args.Order)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read managed accounts", err))
	}
	resp := make([]*ExplorerAccount, 0, len(m))
	for _, v := range m {
		resp = append(resp, NewExplorerAccount(ctx, v, args))
	}
	return resp, http.StatusOK
}

// LEGACY
func ReadAccountOps(ctx *ApiContext) (interface{}, int) {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	r := etl.ListRequest{
		Account: acc,
		Mode:    args.TypeMode,
		Typs:    args.TypeList,
		Since:   args.SinceHeight,
		Until:   args.BlockHeight,
		Offset:  args.Offset,
		Limit:   ctx.Cfg.ClampExplore(args.Limit),
		Cursor:  args.Cursor,
		Order:   args.Order,
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
		}
	}

	ops, err := ctx.Indexer.ListAccountOps(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read account operations", err))
	}
	a := NewExplorerAccount(ctx, acc, args)
	a.Ops = &ExplorerOpList{
		list: make([]*ExplorerOp, 0),
	}
	cache := make(map[int64]interface{})
	for _, v := range ops {
		a.Ops.Append(NewExplorerOp(ctx, v, nil, nil, args, cache), args.WithCollapse())
	}
	return a, http.StatusOK
}

func ListAccountOperations(ctx *ApiContext) (interface{}, int) {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	r := etl.ListRequest{
		Account: acc,
		Mode:    args.TypeMode,
		Typs:    args.TypeList,
		Since:   args.SinceHeight,
		Until:   args.BlockHeight,
		Offset:  args.Offset,
		Limit:   ctx.Cfg.ClampExplore(args.Limit),
		Cursor:  args.Cursor,
		Order:   args.Order,
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
		}
	}

	ops, err := ctx.Indexer.ListAccountOps(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read account operations", err))
	}

	resp := &ExplorerOpList{
		list:    make([]*ExplorerOp, 0),
		expires: ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewExplorerOp(ctx, v, nil, nil, args, cache), args.WithCollapse())
	}
	return resp, http.StatusOK
}

func ListAccountBallots(ctx *ApiContext) (interface{}, int) {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	r := etl.ListRequest{
		Account: acc,
		Since:   args.SinceHeight,
		Until:   args.BlockHeight,
		Offset:  args.Offset,
		Limit:   ctx.Cfg.ClampExplore(args.Limit),
		Cursor:  args.Cursor,
		Order:   args.Order,
	}

	// fetch ballots
	ballots, err := ctx.Indexer.ListAccountBallots(ctx, r)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, err.Error(), nil))
		default:
			panic(EInternal(EC_DATABASE, "cannot read account ballots", err))
		}
	}

	// fetch op hashes for each ballot
	oids := make([]uint64, 0)
	for _, v := range ballots {
		oids = append(oids, v.OpId.Value())
	}

	// lookup
	ops, err := ctx.Indexer.LookupOpIds(ctx, vec.UniqueUint64Slice(oids))
	if err != nil && err != index.ErrNoOpEntry {
		panic(EInternal(EC_DATABASE, "cannot read ops for ballots", err))
	}

	// prepare for lookup
	opMap := make(map[model.OpID]tezos.OpHash)
	for _, v := range ops {
		opMap[v.RowId] = v.Hash
	}
	ops = nil
	ebs := make([]*ExplorerBallot, len(ballots))
	for i, v := range ballots {
		o, _ := opMap[v.OpId]
		ebs[i] = NewExplorerBallot(ctx, v, govLookupProposalHash(ctx, v.ProposalId), o)
	}
	return ebs, http.StatusOK
}
