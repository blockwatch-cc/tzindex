// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

func init() {
	register(ExplorerContract{})
}

var _ RESTful = (*ExplorerContract)(nil)

type ExplorerContract struct {
	Address            string            `json:"address"`
	Manager            string            `json:"manager"`
	Delegate           string            `json:"delegate"`
	Height             int64             `json:"height"`
	Fee                float64           `json:"fee"`
	GasLimit           int64             `json:"gas_limit"`
	GasUsed            int64             `json:"gas_used"`
	GasPrice           float64           `json:"gas_price"`
	StorageLimit       int64             `json:"storage_limit"`
	StorageSize        int64             `json:"storage_size"`
	StoragePaid        int64             `json:"storage_paid"`
	Script             *micheline.Script `json:"script,omitempty"`
	IsFunded           bool              `json:"is_funded"`
	IsVesting          bool              `json:"is_vesting"`
	IsSpendable        bool              `json:"is_spendable"`
	IsDelegatable      bool              `json:"is_delegatable"`
	IsDelegated        bool              `json:"is_delegated"`
	FirstIn            int64             `json:"first_in"`
	FirstOut           int64             `json:"first_out"`
	LastIn             int64             `json:"last_in"`
	LastOut            int64             `json:"last_out"`
	FirstSeen          int64             `json:"first_seen"`
	LastSeen           int64             `json:"last_seen"`
	DelegatedSince     int64             `json:"delegated_since"`
	FirstInTime        time.Time         `json:"first_in_time"`
	FirstOutTime       time.Time         `json:"first_out_time"`
	LastInTime         time.Time         `json:"last_in_time"`
	LastOutTime        time.Time         `json:"last_out_time"`
	FirstSeenTime      time.Time         `json:"first_seen_time"`
	LastSeenTime       time.Time         `json:"last_seen_time"`
	DelegatedSinceTime time.Time         `json:"delegated_since_time"`
	NOps               int               `json:"n_ops"`
	NOpsFailed         int               `json:"n_ops_failed"`
	NTx                int               `json:"n_tx"`
	NDelegation        int               `json:"n_delegation"`
	NOrigination       int               `json:"n_origination"`
	TokenGenMin        int64             `json:"token_gen_min"`
	TokenGenMax        int64             `json:"token_gen_max"`
	DelegateAcc        *ExplorerAccount  `json:"delegate_account,omitempty"`
	ManagerAcc         *ExplorerAccount  `json:"manager_account,omitempty"`
	Ops                *[]*ExplorerOp    `json:"ops,omitempty"`
	expires            time.Time         `json:"-"`
}

func NewExplorerContract(ctx *ApiContext, c *model.Contract, a *model.Account, p *chain.Params, details bool) *ExplorerContract {
	cc := &ExplorerContract{
		Address:        a.String(),
		Height:         c.Height,
		Fee:            p.ConvertValue(c.Fee),
		GasLimit:       c.GasLimit,
		GasUsed:        c.GasUsed,
		GasPrice:       c.GasPrice,
		StorageLimit:   c.StorageLimit,
		StorageSize:    c.StorageSize,
		StoragePaid:    c.StoragePaid,
		IsFunded:       a.IsFunded,
		IsVesting:      a.IsVesting,
		IsSpendable:    a.IsSpendable,
		IsDelegatable:  a.IsDelegatable,
		IsDelegated:    a.IsDelegated,
		FirstIn:        a.FirstIn,
		FirstOut:       a.FirstOut,
		LastIn:         a.LastIn,
		LastOut:        a.LastOut,
		FirstSeen:      a.FirstSeen,
		LastSeen:       a.LastSeen,
		DelegatedSince: a.DelegatedSince,
		NOps:           a.NOps,
		NOpsFailed:     a.NOpsFailed,
		NTx:            a.NTx,
		NDelegation:    a.NDelegation,
		NOrigination:   a.NOrigination,
		TokenGenMin:    a.TokenGenMin,
		TokenGenMax:    a.TokenGenMax,
		expires:        ctx.Now.Add(p.TimeBetweenBlocks[0]),
	}

	if c.Script != nil {
		cc.Script = micheline.NewScript()
		if err := cc.Script.UnmarshalBinary(c.Script); err != nil {
			log.Errorf("explorer contract: unmarshal script: %v", err)
		}
	}

	// resolve block times
	cc.FirstInTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstIn)
	cc.FirstOutTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstOut)
	cc.LastInTime = ctx.Indexer.BlockTime(ctx.Context, a.LastIn)
	cc.LastOutTime = ctx.Indexer.BlockTime(ctx.Context, a.LastOut)
	cc.FirstSeenTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstSeen)
	cc.LastSeenTime = ctx.Indexer.BlockTime(ctx.Context, a.LastSeen)
	cc.DelegatedSinceTime = ctx.Indexer.BlockTime(ctx.Context, a.DelegatedSince)

	if details {
		// load related accounts from id
		xc, err := ctx.Indexer.LookupAccountIds(ctx.Context,
			vec.UniqueUint64Slice([]uint64{
				a.ManagerId.Value(),
				a.DelegateId.Value(),
			}))
		if err != nil {
			log.Errorf("explorer contract: cannot resolve related accounts: %v", err)
		}
		for _, xcc := range xc {
			if xcc.RowId == a.ManagerId {
				cc.Manager = xcc.String()
				cc.ManagerAcc = NewExplorerAccount(ctx, xcc, p, false)
			}
			if xcc.RowId == a.DelegateId {
				cc.Delegate = xcc.String()
				cc.DelegateAcc = NewExplorerAccount(ctx, xcc, p, false)
			}
		}
	} else {
		cc.Manager = lookupAddress(ctx, a.ManagerId).String()
		cc.Delegate = lookupAddress(ctx, a.DelegateId).String()
	}

	return cc
}

func (a ExplorerContract) LastModified() time.Time {
	return a.LastSeenTime
}

func (a ExplorerContract) Expires() time.Time {
	return a.expires
}

func (a ExplorerContract) RESTPrefix() string {
	return "/explorer/contract"
}

func (a ExplorerContract) RESTPath(r *mux.Router) string {
	path, _ := r.Get("contract").URLPath("ident", a.Address)
	return path.String()
}

func (b ExplorerContract) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b ExplorerContract) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadContract)).Methods("GET").Name("contract")
	r.HandleFunc("/{ident}/op", C(ReadContractOps)).Methods("GET")
	return nil

}

func loadContract(ctx *ApiContext) *model.Contract {
	if ccIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || ccIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing contract address", nil))
	} else {
		addr, err := chain.ParseAddress(ccIdent)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		cc, err := ctx.Indexer.LookupContract(ctx, addr)
		if err != nil {
			switch err {
			case index.ErrNoContractEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such contract", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return cc
	}
}

func ReadContract(ctx *ApiContext) (interface{}, int) {
	cc := loadContract(ctx)
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.AccountId)
	if err != nil {
		switch err {
		case index.ErrNoContractEntry:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such contract", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	return NewExplorerContract(ctx, cc, acc, ctx.Crawler.ParamsByHeight(-1), true), http.StatusOK
}

func ReadContractOps(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerOpsRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.AccountId)
	if err != nil {
		switch err {
		case index.ErrNoContractEntry:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such contract", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	params := ctx.Crawler.ParamsByHeight(-1)
	c := NewExplorerContract(ctx, cc, acc, params, false)
	ops, err := ctx.Indexer.ListAccountOps(ctx, acc.RowId, args.Type, args.Offset, ctx.Cfg.ClampExplore(args.Limit))
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read contract operations", err))
	}

	// FIXME: collect account and op lookup into only two queries
	eops := make([]*ExplorerOp, len(ops))
	for i, v := range ops {
		eops[i] = NewExplorerOp(ctx, v, nil, params)
	}
	c.Ops = &eops
	return c, http.StatusOK
}
