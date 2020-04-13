// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/packdb/pack"
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
	Address            string    `json:"address"`
	Manager            string    `json:"manager"`
	Delegate           string    `json:"delegate"`
	Height             int64     `json:"height"`
	Fee                float64   `json:"fee"`
	GasLimit           int64     `json:"gas_limit"`
	GasUsed            int64     `json:"gas_used"`
	GasPrice           float64   `json:"gas_price"`
	StorageLimit       int64     `json:"storage_limit"`
	StorageSize        int64     `json:"storage_size"`
	StoragePaid        int64     `json:"storage_paid"`
	IsFunded           bool      `json:"is_funded"`
	IsVesting          bool      `json:"is_vesting"`
	IsSpendable        bool      `json:"is_spendable"`
	IsDelegatable      bool      `json:"is_delegatable"`
	IsDelegated        bool      `json:"is_delegated"`
	FirstIn            int64     `json:"first_in"`
	FirstOut           int64     `json:"first_out"`
	LastIn             int64     `json:"last_in"`
	LastOut            int64     `json:"last_out"`
	FirstSeen          int64     `json:"first_seen"`
	LastSeen           int64     `json:"last_seen"`
	DelegatedSince     int64     `json:"delegated_since"`
	FirstInTime        time.Time `json:"first_in_time"`
	FirstOutTime       time.Time `json:"first_out_time"`
	LastInTime         time.Time `json:"last_in_time"`
	LastOutTime        time.Time `json:"last_out_time"`
	FirstSeenTime      time.Time `json:"first_seen_time"`
	LastSeenTime       time.Time `json:"last_seen_time"`
	DelegatedSinceTime time.Time `json:"delegated_since_time"`
	NOps               int       `json:"n_ops"`
	NOpsFailed         int       `json:"n_ops_failed"`
	NTx                int       `json:"n_tx"`
	NDelegation        int       `json:"n_delegation"`
	NOrigination       int       `json:"n_origination"`
	TokenGenMin        int64     `json:"token_gen_min"`
	TokenGenMax        int64     `json:"token_gen_max"`
	BigMapIds          []int64   `json:"bigmap_ids"`
	expires            time.Time `json:"-"`
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

	// resolve block times
	cc.FirstInTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstIn)
	cc.FirstOutTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstOut)
	cc.LastInTime = ctx.Indexer.BlockTime(ctx.Context, a.LastIn)
	cc.LastOutTime = ctx.Indexer.BlockTime(ctx.Context, a.LastOut)
	cc.FirstSeenTime = ctx.Indexer.BlockTime(ctx.Context, a.FirstSeen)
	cc.LastSeenTime = ctx.Indexer.BlockTime(ctx.Context, a.LastSeen)
	cc.DelegatedSinceTime = ctx.Indexer.BlockTime(ctx.Context, a.DelegatedSince)

	var err error
	cc.BigMapIds, err = ctx.Indexer.ListContractBigMapIds(ctx.Context, a.RowId)
	if err != nil {
		log.Errorf("explorer contract: cannot load bigmap ids: %v", err)
	}

	cc.Manager = lookupAddress(ctx, a.ManagerId).String()
	cc.Delegate = lookupAddress(ctx, a.DelegateId).String()

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
	r.HandleFunc("/{ident}/calls", C(ReadContractCalls)).Methods("GET")
	r.HandleFunc("/{ident}/manager", C(ReadContractManager)).Methods("GET")
	r.HandleFunc("/{ident}/script", C(ReadContractScript)).Methods("GET")
	r.HandleFunc("/{ident}/storage", C(ReadContractStorage)).Methods("GET")
	return nil

}

type ContractRequest struct {
	ExplorerListRequest // offset, limit

	Order  pack.OrderType `schema:"order"`
	Block  string         `schema:"block"`  // height or hash for time-lock
	Since  string         `schema:"since"`  // block hash or height for updates
	Unpack bool           `schema:"unpack"` // unpack packed key/values
	Prim   bool           `schema:"prim"`   // for prim/value rendering

	// decoded values
	BlockHeight int64           `schema:"-"`
	BlockHash   chain.BlockHash `schema:"-"`
	SinceHeight int64           `schema:"-"`
	SinceHash   chain.BlockHash `schema:"-"`
}

func (r *ContractRequest) WithPrim() bool {
	return r != nil && r.Prim
}

func (r *ContractRequest) WithUnpack() bool {
	return r != nil && r.Unpack
}

func (r *ContractRequest) ParseBlockIdent(ctx *ApiContext) {
	if len(r.Block) > 0 {
		b, err := ctx.Indexer.LookupBlock(ctx.Context, r.Block)
		if err != nil {
			switch err {
			case index.ErrNoBlockEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such block", err))
			case index.ErrInvalidBlockHeight:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case index.ErrInvalidBlockHash:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid block hash", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		// make sure block is not orphaned
		if b.IsOrphan {
			panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, "block is orphaned", nil))
		}
		r.BlockHeight = b.Height
		r.BlockHash = b.Hash.Clone()
	}
	if len(r.Since) > 0 {
		b, err := ctx.Indexer.LookupBlock(ctx.Context, r.Since)
		if err != nil {
			switch err {
			case index.ErrNoBlockEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such block", err))
			case index.ErrInvalidBlockHeight:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case index.ErrInvalidBlockHash:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid block hash", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		// make sure block is not orphaned
		if b.IsOrphan {
			panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, "block is orphaned", nil))
		}
		r.SinceHeight = b.Height
		r.SinceHash = b.Hash.Clone()
	}
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

// list incoming transaction with data
func ReadContractCalls(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.AccountId)
	if err != nil {
		switch err {
		case index.ErrNoAccountEntry:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such contract", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	args.ParseBlockIdent(ctx)
	params := ctx.Crawler.ParamsByHeight(-1)
	ops, err := ctx.Indexer.ListContractCalls(
		ctx,
		acc.RowId,
		args.SinceHeight,
		args.BlockHeight, // since, until (optional)
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit), // offset, limit (optional)
		args.Order,
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read contract calls", err))
	}

	// we reuse explorer ops here
	// FIXME: collect account and op lookup into only two queries
	eops := make([]*ExplorerOp, len(ops))
	for i, v := range ops {
		eops[i] = NewExplorerOp(ctx, v, nil, cc, params, args)
	}
	return eops, http.StatusOK
}

func ReadContractManager(ctx *ApiContext) (interface{}, int) {
	cc := loadContract(ctx)
	if cc.ManagerId == 0 {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "no manager for this contract", nil))
	}
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.ManagerId)
	if err != nil {
		switch err {
		case index.ErrNoAccountEntry:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such account", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	params := ctx.Crawler.ParamsByHeight(-1)
	return NewExplorerAccount(ctx, acc, params, false), http.StatusOK
}

type ExplorerScript struct {
	Script      *micheline.Script     `json:"script,omitempty"`
	Type        micheline.BigMapType  `json:"storage_type"`
	Entrypoints micheline.Entrypoints `json:"entrypoints"`
	modified    time.Time             `json:"-"`
}

func (s ExplorerScript) LastModified() time.Time { return s.modified }
func (s ExplorerScript) Expires() time.Time      { return time.Time{} }

var _ Resource = (*ExplorerScript)(nil)

func ReadContractScript(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	args.ParseBlockIdent(ctx)

	// unmarshal and optionally migrate script
	if args.BlockHeight == 0 {
		args.BlockHeight = ctx.Crawler.Height()
	}

	// need manager hash
	var mgrHash []byte
	if mgr, err := ctx.Indexer.LookupAccountId(ctx, cc.ManagerId); err == nil {
		mgrHash = mgr.Address().Bytes()
	}
	script, err := cc.LoadScript(ctx.Crawler.Tip(), args.BlockHeight, mgrHash)
	if err != nil {
		panic(EInternal(EC_SERVER, "script unmarshal failed", err))
	}

	ep, err := script.Entrypoints(args.WithPrim())
	if err != nil {
		panic(EInternal(EC_SERVER, "script entrypoint parsing failed", err))
	}

	resp := &ExplorerScript{
		Script:      script,
		Type:        script.StorageType(),
		Entrypoints: ep,
		modified:    ctx.Indexer.BlockTime(ctx.Context, cc.Height),
	}
	if !args.WithPrim() {
		resp.Script = nil
	}

	return resp, http.StatusOK
}

func ReadContractStorage(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	args.ParseBlockIdent(ctx)

	if args.BlockHeight > 0 && args.BlockHeight < cc.Height {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "empty storage before origination", nil))
	}

	var mgrHash []byte
	if mgr, err := ctx.Indexer.LookupAccountId(ctx, cc.ManagerId); err == nil {
		mgrHash = mgr.Address().Bytes()
	}

	var (
		prim   *micheline.Prim
		typ    *micheline.Prim
		ts     time.Time
		height int64
	)

	// find most recent incoming call
	op, err := ctx.Indexer.FindLastCall(ctx.Context, cc.AccountId, args.BlockHeight)
	if err != nil {
		if err != index.ErrNoOpEntry {
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		} else {
			// unmarshal and optionally migrate script at origination height
			script, err := cc.LoadScript(ctx.Crawler.Tip(), cc.Height, mgrHash)
			if err != nil {
				panic(EInternal(EC_SERVER, "script unmarshal failed", err))
			}
			// use storage init from origination, may contain full bigmap definition
			prim = script.Storage
			typ = script.Code.Storage.Args[0]
			ts = ctx.Indexer.BlockTime(ctx.Context, cc.Height)
			height = cc.Height
		}
	} else {
		// unmarshal latest storage update from op, contains bigmap reference
		ts = op.Timestamp
		height = op.Height
		prim = &micheline.Prim{}
		if err := prim.UnmarshalBinary(op.Storage); err != nil {
			log.Errorf("explorer: storage unmarshal in op %s: %v", op.Hash, err)
		}
		// unmarshal and optionally migrate script at operation height
		script, err := cc.LoadScript(ctx.Crawler.Tip(), op.Height, mgrHash)
		if err != nil {
			panic(EInternal(EC_SERVER, "script unmarshal failed", err))
		}
		typ = script.Code.Storage.Args[0]
	}

	hash, _ := ctx.Indexer.BlockHashByHeight(ctx.Context, height)

	resp := &ExplorerStorageValue{
		Meta: ExplorerStorageMeta{
			Contract: cc.String(),
			Time:     ts,
			Height:   height,
			Block:    hash,
		},
		Value: &micheline.BigMapValue{
			Type:  typ,
			Value: prim,
		},
		modified: ts,
		expires:  ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}

	if args.WithPrim() {
		resp.Prim = prim
	}

	if args.WithUnpack() && prim.IsPackedAny() {
		if p, err := prim.UnpackAny(); err == nil {
			resp.ValueUnpacked = &micheline.BigMapValue{
				Type:  p.BuildType(),
				Value: p,
			}
		}
	}

	return resp, http.StatusOK
}
