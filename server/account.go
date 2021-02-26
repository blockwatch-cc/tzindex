// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

// keep a cache of addresses to avoid expensive database lookups id->hash
type (
	addrMap map[model.AccountID]chain.Address
)

var (
	addrMapStore atomic.Value
	addrMutex    sync.Mutex
)

func init() {
	addrMapStore.Store(make(addrMap))
}

func purgeAddrStore() {
	addrMutex.Lock()
	defer addrMutex.Unlock()
	addrMapStore.Store(make(addrMap))
}

func lookupAddress(ctx *ApiContext, id model.AccountID) chain.Address {
	if id == 0 {
		return chain.Address{}
	}
	m := addrMapStore.Load().(addrMap)
	a, ok := m[id]
	if !ok {
		addrMutex.Lock()
		defer addrMutex.Unlock()
		addr, err := ctx.Indexer.LookupAccountId(ctx.Context, id)
		if err != nil {
			log.Errorf("explorer: cannot resolve account id %d: %v", id, err)
		} else {
			a = chain.Address{
				Type: addr.Type,
				Hash: addr.Hash,
			}
			m2 := make(addrMap) // create a new map
			for k, v := range m {
				m2[k] = v // copy all data
			}
			m2[id] = a // add new address
			addrMapStore.Store(m2)
		}
	}
	return a
}

func init() {
	register(ExplorerAccount{})
}

var _ RESTful = (*ExplorerAccount)(nil)

type ExplorerAccount struct {
	Address            string           `json:"address"`
	Type               string           `json:"address_type"`
	Delegate           string           `json:"delegate"`
	Manager            string           `json:"manager"`
	Pubkey             string           `json:"pubkey"`
	FirstIn            int64            `json:"first_in"`
	FirstOut           int64            `json:"first_out"`
	LastIn             int64            `json:"last_in"`
	LastOut            int64            `json:"last_out"`
	FirstSeen          int64            `json:"first_seen"`
	LastSeen           int64            `json:"last_seen"`
	DelegatedSince     int64            `json:"delegated_since"`
	DelegateSince      int64            `json:"delegate_since"`
	FirstInTime        time.Time        `json:"first_in_time"`
	FirstOutTime       time.Time        `json:"first_out_time"`
	LastInTime         time.Time        `json:"last_in_time"`
	LastOutTime        time.Time        `json:"last_out_time"`
	FirstSeenTime      time.Time        `json:"first_seen_time"`
	LastSeenTime       time.Time        `json:"last_seen_time"`
	DelegatedSinceTime time.Time        `json:"delegated_since_time"`
	DelegateSinceTime  time.Time        `json:"delegate_since_time"`
	TotalReceived      float64          `json:"total_received"`
	TotalSent          float64          `json:"total_sent"`
	TotalBurned        float64          `json:"total_burned"`
	TotalFeesPaid      float64          `json:"total_fees_paid"`
	TotalRewardsEarned float64          `json:"total_rewards_earned"`
	TotalFeesEarned    float64          `json:"total_fees_earned"`
	TotalLost          float64          `json:"total_lost"`
	FrozenDeposits     float64          `json:"frozen_deposits"`
	FrozenRewards      float64          `json:"frozen_rewards"`
	FrozenFees         float64          `json:"frozen_fees"`
	UnclaimedBalance   float64          `json:"unclaimed_balance"`
	SpendableBalance   float64          `json:"spendable_balance"`
	TotalBalance       float64          `json:"total_balance"`
	DelegatedBalance   float64          `json:"delegated_balance"`
	TotalDelegations   int64            `json:"total_delegations"`
	ActiveDelegations  int64            `json:"active_delegations"`
	IsFunded           bool             `json:"is_funded"`
	IsActivated        bool             `json:"is_activated"`
	IsVesting          bool             `json:"is_vesting"`
	IsSpendable        bool             `json:"is_spendable"`
	IsDelegatable      bool             `json:"is_delegatable"`
	IsDelegated        bool             `json:"is_delegated"`
	IsRevealed         bool             `json:"is_revealed"`
	IsDelegate         bool             `json:"is_delegate"`
	IsActiveDelegate   bool             `json:"is_active_delegate"`
	IsContract         bool             `json:"is_contract"`
	BlocksBaked        int              `json:"blocks_baked"`
	BlocksMissed       int              `json:"blocks_missed"`
	BlocksStolen       int              `json:"blocks_stolen"`
	BlocksEndorsed     int              `json:"blocks_endorsed"`
	SlotsEndorsed      int              `json:"slots_endorsed"`
	SlotsMissed        int              `json:"slots_missed"`
	NOps               int              `json:"n_ops"`
	NOpsFailed         int              `json:"n_ops_failed"`
	NTx                int              `json:"n_tx"`
	NDelegation        int              `json:"n_delegation"`
	NOrigination       int              `json:"n_origination"`
	NProposal          int              `json:"n_proposal"`
	NBallot            int              `json:"n_ballot"`
	TokenGenMin        int64            `json:"token_gen_min"`
	TokenGenMax        int64            `json:"token_gen_max"`
	GracePeriod        int64            `json:"grace_period"`
	StakingBalance     float64          `json:"staking_balance"`
	Rolls              int64            `json:"rolls"`
	RichRank           int              `json:"rich_rank"`
	TrafficRank        int              `json:"traffic_rank"`
	FlowRank           int              `json:"flow_rank"`
	LastBakeHeight     int64            `json:"last_bake_height"`
	LastBakeBlock      string           `json:"last_bake_block"`
	LastBakeTime       time.Time        `json:"last_bake_time"`
	LastEndorseHeight  int64            `json:"last_endorse_height"`
	LastEndorseBlock   string           `json:"last_endorse_block"`
	LastEndorseTime    time.Time        `json:"last_endorse_time"`
	NextBakeHeight     int64            `json:"next_bake_height"`
	NextBakePriority   int              `json:"next_bake_priority"`
	NextBakeTime       time.Time        `json:"next_bake_time"`
	NextEndorseHeight  int64            `json:"next_endorse_height"`
	NextEndorseTime    time.Time        `json:"next_endorse_time"`
	DelegateAcc        *ExplorerAccount `json:"delegate_account,omitempty"`
	ManagerAcc         *ExplorerAccount `json:"manager_account,omitempty"`

	// LEGACY
	Ops     []*ExplorerOp     `json:"ops,omitempty"`
	Ballots []*ExplorerBallot `json:"ballots,omitempty"`

	// caching
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func NewExplorerAccount(ctx *ApiContext, a *model.Account, details bool) *ExplorerAccount {
	p := ctx.Params
	acc := &ExplorerAccount{
		Address: a.String(),
		Type:    a.Type.String(),
		// FIXME: update once fixed
		Pubkey:             chain.NewHash(a.PubkeyType.KeyType().HashType(), a.PubkeyHash).String(),
		FirstIn:            a.FirstIn,
		FirstOut:           a.FirstOut,
		LastIn:             a.LastIn,
		LastOut:            a.LastOut,
		FirstSeen:          a.FirstSeen,
		LastSeen:           a.LastSeen,
		DelegatedSince:     a.DelegatedSince,
		DelegateSince:      a.DelegateSince,
		TotalReceived:      p.ConvertValue(a.TotalReceived),
		TotalSent:          p.ConvertValue(a.TotalSent),
		TotalBurned:        p.ConvertValue(a.TotalBurned),
		TotalFeesPaid:      p.ConvertValue(a.TotalFeesPaid),
		TotalRewardsEarned: p.ConvertValue(a.TotalRewardsEarned),
		TotalFeesEarned:    p.ConvertValue(a.TotalFeesEarned),
		TotalLost:          p.ConvertValue(a.TotalLost),
		FrozenDeposits:     p.ConvertValue(a.FrozenDeposits),
		FrozenRewards:      p.ConvertValue(a.FrozenRewards),
		FrozenFees:         p.ConvertValue(a.FrozenFees),
		UnclaimedBalance:   p.ConvertValue(a.UnclaimedBalance),
		SpendableBalance:   p.ConvertValue(a.SpendableBalance),
		TotalBalance:       p.ConvertValue(a.SpendableBalance) + p.ConvertValue(a.FrozenDeposits) + p.ConvertValue(a.FrozenFees),
		DelegatedBalance:   p.ConvertValue(a.DelegatedBalance),
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
		expires:            ctx.Tip.BestTime.Add(p.TimeBetweenBlocks[0]),
	}

	// resolve block times
	acc.FirstInTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstIn)
	acc.FirstOutTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstOut)
	acc.LastInTime = ctx.Indexer.BlockTime(ctx.Context, a.LastIn)
	acc.LastOutTime = ctx.Indexer.BlockTime(ctx.Context, a.LastOut)
	acc.FirstSeenTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstSeen)
	acc.LastSeenTime = ctx.Indexer.BlockTime(ctx.Context, a.LastSeen)
	acc.DelegatedSinceTime = ctx.Indexer.BlockTime(ctx.Context, a.DelegatedSince)
	acc.DelegateSinceTime = ctx.Indexer.BlockTime(ctx.Context, a.DelegateSince)

	// fetch ranking data (may lazy-load on first call)
	if rank, ok := ctx.Indexer.LookupRanking(ctx.Context, a.RowId); ok {
		acc.RichRank = rank.RichRank
		acc.TrafficRank = rank.TrafficRank
		acc.FlowRank = rank.FlowRank
	}

	// fill in bake/endorse info, skip for non-delegates
	if a.IsDelegate {
		// set staking balance
		acc.StakingBalance = p.ConvertValue(a.StakingBalance())

		if details && a.BlocksBaked > 0 {
			if b, err := ctx.Indexer.LookupLastBakedBlock(ctx, a); err == nil {
				// from block table
				acc.LastBakeHeight = b.Height
				acc.LastBakeBlock = b.Hash.String()
				acc.LastBakeTime = b.Timestamp
			}
		}

		if details && a.BlocksEndorsed > 0 {
			if b, err := ctx.Indexer.LookupLastEndorsedBlock(ctx, a); err == nil {
				// from op table
				acc.LastEndorseHeight = b.Height
				acc.LastEndorseBlock = b.Hash.String()
				acc.LastEndorseTime = b.Timestamp
			}
		}

		if a.IsActiveDelegate {
			// calculate currelt rolls
			acc.Rolls = a.StakingBalance() / p.TokensPerRoll
		}

		if details && a.IsActiveDelegate {
			// from rights table
			// tip := ctx.Tip
			// if r, err := ctx.Indexer.LookupNextRight(ctx, a, tip.BestHeight, chain.RightTypeBaking, 0); err == nil {
			// 	acc.NextBakeHeight = r.Height
			// 	acc.NextBakePriority = 0
			// 	acc.NextBakeTime = tip.BestTime.Add(time.Duration(p.TimeBetweenBlocks[0] * time.Duration(r.Height-tip.BestHeight)))
			// }

			// if r, err := ctx.Indexer.LookupNextRight(ctx, a, tip.BestHeight, chain.RightTypeEndorsing, -1); err == nil {
			// 	acc.NextEndorseHeight = r.Height
			// 	acc.NextEndorseTime = tip.BestTime.Add(time.Duration(p.TimeBetweenBlocks[0] * time.Duration(r.Height-tip.BestHeight)))
			// }
			// from rights cache
			bh, eh := ctx.Indexer.NextRights(ctx, a.RowId, ctx.Tip.BestHeight)
			if bh > 0 {
				acc.NextBakeHeight = bh
				acc.NextBakePriority = 0
				tm := ctx.Tip.BestTime.Add(p.TimeBetweenBlocks[0] * time.Duration(bh-ctx.Tip.BestHeight))
				acc.NextBakeTime = tm
			} else {
				acc.NextBakeHeight = 0
				acc.NextBakePriority = 0
				acc.NextBakeTime = time.Time{}
			}
			if eh > 0 {
				acc.NextEndorseHeight = eh
				tm := ctx.Tip.BestTime.Add(p.TimeBetweenBlocks[0] * time.Duration(eh-ctx.Tip.BestHeight))
				acc.NextEndorseTime = tm
			} else {
				acc.NextEndorseHeight = 0
				acc.NextEndorseTime = time.Time{}
			}
		}
	}

	// skip for self-delegates
	if details && a.RowId != a.DelegateId && (a.ManagerId+a.DelegateId > 0) {
		// load related accounts from id
		xc, err := ctx.Indexer.LookupAccountIds(ctx.Context,
			vec.UniqueUint64Slice([]uint64{
				a.ManagerId.Value(),
				a.DelegateId.Value(),
			}))
		if err != nil {
			log.Errorf("explorer account: cannot resolve related accounts: %v", err)
		}
		for _, xcc := range xc {
			if xcc.RowId == a.ManagerId {
				acc.Manager = xcc.String()
				acc.ManagerAcc = NewExplorerAccount(ctx, xcc, false)
			}
			if xcc.RowId == a.DelegateId {
				acc.Delegate = xcc.String()
				acc.DelegateAcc = NewExplorerAccount(ctx, xcc, false)
			}
		}
	} else {
		acc.Manager = lookupAddress(ctx, a.ManagerId).String()
		acc.Delegate = lookupAddress(ctx, a.DelegateId).String()
	}

	// update last-modified header at least once per cycle to reflect closed baker decay
	acc.lastmod = acc.LastSeenTime
	if cc := p.CycleFromHeight(ctx.Tip.BestHeight); p.CycleFromHeight(acc.LastSeen) < cc {
		acc.lastmod = ctx.Indexer.BlockTime(ctx.Context, p.CycleStartHeight(cc))
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
	r.HandleFunc("/{ident}/managed", C(ReadManagedAccounts)).Methods("GET")
	r.HandleFunc("/{ident}/operations", C(ListAccountOperations)).Methods("GET")
	r.HandleFunc("/{ident}/ballots", C(ListAccountBallots)).Methods("GET")

	// LEGACY
	r.HandleFunc("/{ident}/op", C(ReadAccountOps)).Methods("GET")
	r.HandleFunc("/{ident}/ballots", C(ReadAccountBallots)).Methods("GET")
	return nil

}

func loadAccount(ctx *ApiContext) *model.Account {
	if accIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || accIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing account address", nil))
	} else {
		addr, err := chain.ParseAddress(accIdent)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		acc, err := ctx.Indexer.LookupAccount(ctx, addr)
		if err != nil {
			switch err {
			case index.ErrNoAccountEntry:
				// cross-lookup activated account from blinded address
				if addr.Type != chain.AddressTypeBlinded {
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
	return NewExplorerAccount(ctx, loadAccount(ctx), true), http.StatusOK
}

func ReadManagedAccounts(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerListRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	m, err := ctx.Indexer.ListManaged(ctx, acc.RowId, args.Limit, args.Offset)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read managed accounts", err))
	}
	resp := make([]*ExplorerAccount, 0, len(m))
	for _, v := range m {
		resp = append(resp, NewExplorerAccount(ctx, v, false))
	}
	return resp, http.StatusOK
}

// LEGACY
func ReadAccountOps(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerOpsRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)
	ops, err := ctx.Indexer.ListAccountOps(
		ctx,
		acc,
		args.TypeMode,
		args.TypeList,
		args.SinceHeight,
		args.BlockHeight,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
		args.Cursor,
		args.Order,
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read account operations", err))
	}

	a := NewExplorerAccount(ctx, acc, false)
	a.Ops = make([]*ExplorerOp, len(ops))
	for i, v := range ops {
		a.Ops[i] = NewExplorerOp(ctx, v, nil, nil, args)
	}
	return a, http.StatusOK
}

// LEGACY
func ReadAccountBallots(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerListRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	// fetch ballots
	ballots, err := ctx.Indexer.ListAccountBallots(
		ctx,
		acc,
		acc.FirstSeen-1,
		acc.LastSeen,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
		args.Cursor,
		args.Order,
	)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, err.Error(), nil))
		default:
			panic(EInternal(EC_DATABASE, "cannot read account ballots", err))
		}
	}

	// fetch proposal and op hashes for each ballot
	oids := make([]uint64, 0)
	for _, v := range ballots {
		oids = append(oids, v.OpId.Value())
	}
	oids = vec.UniqueUint64Slice(oids)

	// lookup
	ops, err := ctx.Indexer.LookupOpIds(ctx, oids)
	if err != nil && err != index.ErrNoOpEntry {
		panic(EInternal(EC_DATABASE, "cannot read ops for ballots", err))
	}

	// prepare for lookup
	opMap := make(map[model.OpID]chain.OperationHash)
	for _, v := range ops {
		opMap[v.RowId] = v.Hash
	}

	a := NewExplorerAccount(ctx, acc, false)
	a.Ballots = make([]*ExplorerBallot, len(ballots))
	for i, v := range ballots {
		o, _ := opMap[v.OpId]
		a.Ballots[i] = NewExplorerBallot(ctx, v, govLookupProposalHash(ctx, v.ProposalId), o)
	}
	return a, http.StatusOK
}

func ListAccountOperations(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerOpsRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)
	ops, err := ctx.Indexer.ListAccountOps(
		ctx,
		acc,
		args.TypeMode,
		args.TypeList,
		args.SinceHeight,
		args.BlockHeight,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
		args.Cursor,
		args.Order,
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read account operations", err))
	}

	resp := make([]*ExplorerOp, len(ops))
	for i, v := range ops {
		resp[i] = NewExplorerOp(ctx, v, nil, nil, args)
	}
	return resp, http.StatusOK
}

func ListAccountBallots(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerOpsRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	// fetch ballots
	ballots, err := ctx.Indexer.ListAccountBallots(
		ctx,
		acc,
		args.SinceHeight,
		args.BlockHeight,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
		args.Cursor,
		args.Order,
	)
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
	opMap := make(map[model.OpID]chain.OperationHash)
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
