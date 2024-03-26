// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Account{})
}

var _ server.RESTful = (*Account)(nil)
var _ server.Resource = (*Account)(nil)

type Account struct {
	RowId              model.AccountID      `json:"row_id"`
	Address            string               `json:"address"`
	Type               string               `json:"address_type"`
	Pubkey             string               `json:"pubkey,omitempty"`
	Counter            int64                `json:"counter"`
	Baker              string               `json:"baker,omitempty"`
	Creator            string               `json:"creator,omitempty"`
	FirstIn            int64                `json:"first_in"`
	FirstOut           int64                `json:"first_out"`
	LastIn             int64                `json:"last_in"`
	LastOut            int64                `json:"last_out"`
	FirstSeen          int64                `json:"first_seen"`
	LastSeen           int64                `json:"last_seen"`
	DelegatedSince     int64                `json:"delegated_since,omitempty"`
	FirstInTime        *time.Time           `json:"first_in_time,omitempty"`
	FirstOutTime       *time.Time           `json:"first_out_time,omitempty"`
	LastInTime         *time.Time           `json:"last_in_time,omitempty"`
	LastOutTime        *time.Time           `json:"last_out_time,omitempty"`
	FirstSeenTime      time.Time            `json:"first_seen_time,omitempty"`
	LastSeenTime       time.Time            `json:"last_seen_time,omitempty"`
	DelegatedSinceTime *time.Time           `json:"delegated_since_time,omitempty"`
	TotalReceived      float64              `json:"total_received"`
	TotalSent          float64              `json:"total_sent"`
	TotalBurned        float64              `json:"total_burned"`
	TotalFeesPaid      float64              `json:"total_fees_paid"`
	TotalFeesUsed      float64              `json:"total_fees_used"`
	UnclaimedBalance   float64              `json:"unclaimed_balance,omitempty"`
	SpendableBalance   float64              `json:"spendable_balance"`
	FrozenRollupBond   float64              `json:"frozen_rollup_bond,omitempty"`
	LostRollupBond     float64              `json:"lost_rollup_bond,omitempty"`
	StakedBalance      float64              `json:"staked_balance"`
	UnstakedBalance    float64              `json:"unstaked_balance"`
	FrozenRewards      float64              `json:"frozen_rewards"` // calculated
	LostStake          float64              `json:"lost_stake"`
	IsFunded           bool                 `json:"is_funded"`
	IsActivated        bool                 `json:"is_activated,omitempty"`
	IsDelegated        bool                 `json:"is_delegated"`
	IsStaked           bool                 `json:"is_staked"`
	IsRevealed         bool                 `json:"is_revealed"`
	IsBaker            bool                 `json:"is_baker,omitempty"`
	IsContract         bool                 `json:"is_contract,omitempty"`
	NTxSuccess         int                  `json:"n_tx_success"`
	NTxFailed          int                  `json:"n_tx_failed"`
	NTxOut             int                  `json:"n_tx_out"`
	NTxIn              int                  `json:"n_tx_in"`
	Metadata           map[string]*Metadata `json:"metadata,omitempty"`

	// LEGACY
	Ops OpList `json:"ops,omitempty"`

	// caching
	expires time.Time
	lastmod time.Time
}

func NewAccount(ctx *server.Context, a *model.Account, args server.Options) *Account {
	p := ctx.Params
	acc := &Account{
		RowId:            a.RowId,
		Address:          a.String(),
		Type:             a.Type.String(),
		Pubkey:           a.Pubkey.String(),
		Counter:          a.Counter,
		FirstIn:          a.FirstIn,
		FirstOut:         a.FirstOut,
		LastIn:           a.LastIn,
		LastOut:          a.LastOut,
		FirstSeen:        a.FirstSeen,
		LastSeen:         a.LastSeen,
		DelegatedSince:   a.DelegatedSince,
		TotalReceived:    p.ConvertValue(a.TotalReceived),
		TotalSent:        p.ConvertValue(a.TotalSent),
		TotalBurned:      p.ConvertValue(a.TotalBurned),
		TotalFeesPaid:    p.ConvertValue(a.TotalFeesPaid),
		TotalFeesUsed:    p.ConvertValue(a.TotalFeesUsed),
		SpendableBalance: p.ConvertValue(a.SpendableBalance),
		FrozenRollupBond: p.ConvertValue(a.FrozenRollupBond),
		LostRollupBond:   p.ConvertValue(a.LostRollupBond),
		StakedBalance:    p.ConvertValue(a.StakedBalance),
		UnstakedBalance:  p.ConvertValue(a.UnstakedBalance),
		LostStake:        p.ConvertValue(a.LostStake),
		IsFunded:         a.IsFunded,
		IsActivated:      a.IsActivated,
		IsDelegated:      a.IsDelegated,
		IsStaked:         a.IsStaked,
		IsRevealed:       a.IsRevealed,
		IsBaker:          a.IsBaker,
		IsContract:       a.IsContract,
		NTxSuccess:       a.NTxSuccess,
		NTxFailed:        a.NTxFailed,
		NTxOut:           a.NTxOut,
		NTxIn:            a.NTxIn,
		expires:          ctx.Expires,
	}

	if a.Type == tezos.AddressTypeBlinded {
		acc.UnclaimedBalance = p.ConvertValue(a.UnclaimedBalance)
	}

	if a.IsDelegated {
		acc.Baker = ctx.Indexer.LookupAddress(ctx, a.BakerId).String()
	}

	if a.CreatorId > 0 {
		acc.Creator = ctx.Indexer.LookupAddress(ctx, a.CreatorId).String()
	}

	// resolve block times
	acc.FirstSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.FirstSeen)
	acc.LastSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.LastSeen)
	acc.FirstInTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.FirstIn)
	acc.FirstOutTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.FirstOut)
	acc.LastInTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.LastIn)
	acc.LastOutTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.LastOut)
	acc.DelegatedSinceTime = ctx.Indexer.LookupBlockTimePtr(ctx.Context, a.DelegatedSince)

	if args.WithMeta() {
		// add metadata
		acc.Metadata = make(map[string]*Metadata)
		if md, ok := lookupAddressIdMetadata(ctx, a.RowId); ok {
			acc.Metadata[acc.Address] = md
		}

		// baker metadata for delegators
		if a.IsDelegated {
			if md, ok := lookupAddressIdMetadata(ctx, a.BakerId); ok {
				acc.Metadata[acc.Baker] = md
			}
		}

		// manager/creator metadata for contracts
		if a.CreatorId > 0 {
			if md, ok := lookupAddressIdMetadata(ctx, a.CreatorId); ok {
				acc.Metadata[acc.Creator] = md
			}
		}
	}

	// resolve baking rewards
	if a.IsStaked {
		if bkr, err := ctx.Indexer.LookupBakerId(ctx, a.BakerId); err == nil {
			ownStake := bkr.StakeAmount(a.StakeShares)
			acc.FrozenRewards = p.ConvertValue(ownStake - a.StakedBalance)
		}
	}

	acc.lastmod = acc.LastSeenTime
	return acc
}

func (a Account) LastModified() time.Time {
	return a.lastmod
}

func (a Account) Expires() time.Time {
	return a.expires
}

func (a Account) RESTPrefix() string {
	return "/explorer/account"
}

func (a Account) RESTPath(r *mux.Router) string {
	path, _ := r.Get("account").URLPath("ident", a.Address)
	return path.String()
}

func (b Account) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Account) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadAccount)).Methods("GET").Name("account")
	r.HandleFunc("/{ident}/contracts", server.C(ReadDeployedContracts)).Methods("GET")
	r.HandleFunc("/{ident}/operations", server.C(ListAccountOperations)).Methods("GET")
	r.HandleFunc("/{ident}/metadata", server.C(ReadMetadata)).Methods("GET")
	r.HandleFunc("/{ident}/token_balances", server.C(ListAccountTokenBalances)).Methods("GET")
	r.HandleFunc("/{ident}/token_events", server.C(ListAccountTokenEvents)).Methods("GET")
	r.HandleFunc("/{ident}/ticket_balances", server.C(ListAccountTicketBalances)).Methods("GET")
	r.HandleFunc("/{ident}/ticket_events", server.C(ListAccountTicketEvents)).Methods("GET")

	// LEGACY: keep here for dapp and wallet compatibility
	r.HandleFunc("/{ident}/op", server.C(ReadAccountOps)).Methods("GET")
	return nil
}

type AccountRequest struct {
	ListRequest      // offset, limit, cursor, order
	Meta        bool `schema:"meta"` // include account metadata
}

func (r *AccountRequest) WithPrim() bool    { return false }
func (r *AccountRequest) WithUnpack() bool  { return false }
func (r *AccountRequest) WithHeight() int64 { return 0 }
func (r *AccountRequest) WithMeta() bool    { return r != nil && r.Meta }
func (r *AccountRequest) WithRights() bool  { return false }
func (r *AccountRequest) WithMerge() bool   { return false }
func (r *AccountRequest) WithStorage() bool { return false }

func loadAccount(ctx *server.Context) *model.Account {
	if accIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || accIdent == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing account address", nil))
	} else {
		addr, err := tezos.ParseAddress(accIdent)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		acc, err := ctx.Indexer.LookupAccount(ctx, addr)
		if err != nil {
			if errors.Is(err, model.ErrNoAccount) {
				// cross-lookup activated account from blinded address
				if addr.Type() != tezos.AddressTypeBlinded {
					panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such account", err))
				}
				acc, err = ctx.Indexer.FindActivatedAccount(ctx, addr)
				if err != nil {
					if errors.Is(err, model.ErrNoAccount) {
						panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such account", err))
					} else {
						panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
					}
				}
			} else {
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return acc
	}
}

func ReadAccount(ctx *server.Context) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	return NewAccount(ctx, loadAccount(ctx), args), http.StatusOK
}

type Payout struct {
	LifetimeRewards  float64 `json:"lifetime_rewards"`
	EstimatedRewards float64 `json:"estimated_rewards"`

	// caching
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func (p Payout) LastModified() time.Time {
	return p.lastmod
}

func (p Payout) Expires() time.Time {
	return p.expires
}

func ReadDeployedContracts(ctx *server.Context) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	ccs, err := ctx.Indexer.ListContracts(ctx, etl.ListRequest{
		Account: acc,
		Offset:  args.Offset,
		Limit:   ctx.Cfg.ClampExplore(args.Limit),
		Cursor:  args.Cursor,
		Order:   args.Order,
	})
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read deployed contracts", err))
	}

	// we also need the account data for contracts
	ids := make([]uint64, 0, len(ccs))
	for _, v := range ccs {
		ids = append(ids, v.AccountId.U64())
	}
	ids = vec.UniqueUint64Slice(ids)
	accs, err := ctx.Indexer.LookupAccountsById(ctx.Context, ids)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read contract accounts", err))
	}
	accMap := make(map[model.AccountID]*model.Account)
	for _, v := range accs {
		accMap[v.RowId] = v
	}
	resp := make([]*Contract, 0, len(ccs))
	for _, v := range ccs {
		resp = append(resp, NewContract(ctx, v, accMap[v.AccountId], args))
	}
	return resp, http.StatusOK
}

// LEGACY
func ReadAccountOps(ctx *server.Context) (interface{}, int) {
	args := &OpsRequest{
		ListRequest: ListRequest{
			Order: pack.OrderDesc,
		},
	}
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
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
		}
	}

	ops, err := ctx.Indexer.ListAccountOps(ctx, r)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read account operations", err))
	}
	a := NewAccount(ctx, acc, args)
	a.Ops = make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		a.Ops.Append(NewOp(ctx, v, nil, nil, args, cache), args.WithMerge())
	}
	return a, http.StatusOK
}

func ListAccountOperations(ctx *server.Context) (interface{}, int) {
	args := &OpsRequest{
		ListRequest: ListRequest{
			Order: pack.OrderDesc,
		},
	}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	r := etl.ListRequest{
		Account:     acc,
		Mode:        args.TypeMode,
		Typs:        args.TypeList,
		Since:       args.SinceHeight,
		Until:       args.BlockHeight,
		Offset:      args.Offset,
		Limit:       ctx.Cfg.ClampExplore(args.Limit),
		Cursor:      args.Cursor,
		Order:       args.Order,
		WithStorage: args.WithStorage(),
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
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
