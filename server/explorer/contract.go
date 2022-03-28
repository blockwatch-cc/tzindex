// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Contract{})
}

var _ server.RESTful = (*Contract)(nil)
var _ server.Resource = (*Contract)(nil)

type Contract struct {
	AccountId     model.AccountID      `json:"account_id"`
	Address       string               `json:"address"`
	Creator       string               `json:"creator"`
	Baker         string               `json:"baker"`
	StorageSize   int64                `json:"storage_size"`
	StoragePaid   int64                `json:"storage_paid"`
	FirstSeen     int64                `json:"first_seen"`
	LastSeen      int64                `json:"last_seen"`
	FirstSeenTime time.Time            `json:"first_seen_time"`
	LastSeenTime  time.Time            `json:"last_seen_time"`
	NCallsSuccess int                  `json:"n_calls_success"`
	NCallsFailed  int                  `json:"n_calls_failed"`
	Bigmaps       map[string]int64     `json:"bigmaps,omitempty"`
	InterfaceHash string               `json:"iface_hash"`
	CodeHash      string               `json:"code_hash"`
	StorageHash   string               `json:"storage_hash"`
	CallStats     map[string]int       `json:"call_stats"`
	Features      micheline.Features   `json:"features"`
	Interfaces    micheline.Interfaces `json:"interfaces"`
	Metadata      map[string]*Metadata `json:"metadata,omitempty"`

	expires time.Time `json:"-"`
}

func NewContract(ctx *server.Context, c *model.Contract, a *model.Account, args server.Options) *Contract {
	p := ctx.Params
	cc := &Contract{
		AccountId:     a.RowId,
		Address:       a.String(),
		StorageSize:   c.StorageSize,
		StoragePaid:   c.StoragePaid,
		FirstSeen:     a.FirstSeen,
		LastSeen:      a.LastSeen,
		NCallsSuccess: a.NOps - a.NOpsFailed,
		NCallsFailed:  a.NOpsFailed,
		CallStats:     c.ListCallStats(),
		Features:      c.Features,
		Interfaces:    c.Interfaces,
		expires:       ctx.Tip.BestTime.Add(p.BlockTime()),
	}

	// hashes
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], c.InterfaceHash)
	cc.InterfaceHash = hex.EncodeToString(tmp[:])
	binary.BigEndian.PutUint64(tmp[:], c.CodeHash)
	cc.CodeHash = hex.EncodeToString(tmp[:])
	binary.BigEndian.PutUint64(tmp[:], c.StorageHash)
	cc.StorageHash = hex.EncodeToString(tmp[:])

	// resolve block times
	cc.FirstSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.FirstSeen)
	cc.LastSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.LastSeen)

	// map bigmap ids to storage annotation names
	if ids, err := ctx.Indexer.ListContractBigmapIds(ctx.Context, a.RowId); err == nil {
		cc.Bigmaps = c.NamedBigmaps(ids)
	} else {
		log.Errorf("explorer contract: cannot load bigmap ids: %v", err)
	}
	cc.Creator = ctx.Indexer.LookupAddress(ctx, a.CreatorId).String()
	cc.Baker = ctx.Indexer.LookupAddress(ctx, a.BakerId).String()

	// add metadata
	if args.WithMeta() {
		cc.Metadata = make(map[string]*Metadata)
		if md, ok := lookupMetadataById(ctx, a.RowId, 0, false); ok {
			cc.Metadata[cc.Address] = md
		}
		// fetch baker metadata for delegators
		if a.IsDelegated {
			if md, ok := lookupMetadataById(ctx, a.BakerId, 0, false); ok {
				cc.Metadata[cc.Baker] = md
			}
		}
		if md, ok := lookupMetadataById(ctx, c.CreatorId, 0, false); ok {
			cc.Metadata[cc.Creator] = md
		}
	}

	return cc
}

func (a Contract) LastModified() time.Time {
	return a.LastSeenTime
}

func (a Contract) Expires() time.Time {
	return a.expires
}

func (a Contract) RESTPrefix() string {
	return "/explorer/contract"
}

func (a Contract) RESTPath(r *mux.Router) string {
	path, _ := r.Get("contract").URLPath("ident", a.Address)
	return path.String()
}

func (b Contract) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Contract) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadContract)).Methods("GET").Name("contract")
	r.HandleFunc("/{ident}/calls", server.C(ReadContractCalls)).Methods("GET")
	r.HandleFunc("/{ident}/creator", server.C(ReadContractCreator)).Methods("GET")
	r.HandleFunc("/{ident}/script", server.C(ReadContractScript)).Methods("GET")
	r.HandleFunc("/{ident}/storage", server.C(ReadContractStorage)).Methods("GET")
	return nil

}

type ContractRequest struct {
	ListRequest // offset, limit, cursor, order

	Block   string        `schema:"block"`   // height or hash for time-lock
	Since   string        `schema:"since"`   // block hash or height for updates
	Unpack  bool          `schema:"unpack"`  // unpack packed key/values
	Prim    bool          `schema:"prim"`    // for prim/value rendering
	Meta    bool          `schema:"meta"`    // include account metadata
	Merge   bool          `schema:"merge"`   // collapse internal calls
	Storage bool          `schema:"storage"` // embed storage updates
	Sender  tezos.Address `schema:"sender"`  // sender address

	// decoded entrypoint condition (list of name, num or branch)
	EntrypointMode pack.FilterMode `schema:"-"`
	EntrypointCond string          `schema:"-"`

	// decoded values
	BlockHeight int64           `schema:"-"`
	BlockHash   tezos.BlockHash `schema:"-"`
	SinceHeight int64           `schema:"-"`
	SinceHash   tezos.BlockHash `schema:"-"`
}

func (r *ContractRequest) WithPrim() bool   { return r != nil && r.Prim }
func (r *ContractRequest) WithUnpack() bool { return r != nil && r.Unpack }
func (r *ContractRequest) WithHeight() int64 {
	if r != nil {
		return r.BlockHeight
	}
	return 0
}

func (r *ContractRequest) WithMeta() bool    { return r != nil && r.Meta }
func (r *ContractRequest) WithRights() bool  { return false }
func (r *ContractRequest) WithMerge() bool   { return r != nil && r.Merge }
func (r *ContractRequest) WithStorage() bool { return r != nil && r.Storage }

func (r *ContractRequest) Parse(ctx *server.Context) {
	if len(r.Block) > 0 {
		hash, height, err := ctx.Indexer.LookupBlockId(ctx.Context, r.Block)
		if err != nil {
			switch err {
			case index.ErrNoBlockEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such block", err))
			case index.ErrInvalidBlockHeight:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case index.ErrInvalidBlockHash:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block hash", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		r.BlockHeight = height
		r.BlockHash = hash
	}
	if len(r.Since) > 0 {
		hash, height, err := ctx.Indexer.LookupBlockId(ctx.Context, r.Since)
		if err != nil {
			switch err {
			case index.ErrNoBlockEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such block", err))
			case index.ErrInvalidBlockHeight:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case index.ErrInvalidBlockHash:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block hash", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		r.SinceHeight = height
		r.SinceHash = hash
	}
	// filter by entrypoint condition
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		if keys[0] != "entrypoint" {
			continue
		}
		// parse mode
		r.EntrypointMode = pack.FilterModeEqual
		if len(keys) > 1 {
			r.EntrypointMode = pack.ParseFilterMode(keys[1])
			if !r.EntrypointMode.IsValid() {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid entrypoint filter mode '%s'", keys[1]), nil))
			}
		}
		// use entrypoint condition list as is (will be parsed later)
		r.EntrypointCond = val[0]
		// allow constructs of form `entrypoint=a,b`
		if strings.Contains(r.EntrypointCond, ",") {
			if r.EntrypointMode == pack.FilterModeEqual {
				r.EntrypointMode = pack.FilterModeIn
			}
		} else {
			// check for single value mode  `entrypoint.in=a`
			switch r.EntrypointMode {
			case pack.FilterModeIn:
				r.EntrypointMode = pack.FilterModeEqual
			case pack.FilterModeNotIn:
				r.EntrypointMode = pack.FilterModeNotEqual
			}
		}
	}
}

func loadContract(ctx *server.Context) *model.Contract {
	if ccIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || ccIdent == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing contract address", nil))
	} else {
		addr, err := tezos.ParseAddress(ccIdent)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		cc, err := ctx.Indexer.LookupContract(ctx, addr)
		if err != nil {
			switch err {
			case index.ErrNoContractEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such contract", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return cc
	}
}

func ReadContract(ctx *server.Context) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.AccountId)
	if err != nil {
		switch err {
		case index.ErrNoContractEntry, index.ErrNoAccountEntry:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such contract", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}
	return NewContract(ctx, cc, acc, args), http.StatusOK
}

// list incoming transaction with data
func ReadContractCalls(ctx *server.Context) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.AccountId)
	if err != nil {
		switch err {
		case index.ErrNoAccountEntry:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such contract", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}

	r := etl.ListRequest{
		Account: acc,
		Mode:    args.EntrypointMode,
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

	// parse entrypoint filter
	// - name (eg. "default")
	// - branch (eg. "/R/R/L")
	// - id (eg. 5)
	r.Entrypoints = make([]int64, 0)
	if len(args.EntrypointCond) > 0 {
		pTyp, _, err := cc.LoadType()
		if err != nil {
			panic(server.EInternal(server.EC_SERVER, "script type unmarshal failed", err))
		}
		scriptEntrypoints, err := pTyp.Entrypoints(false)
		if err != nil {
			panic(server.EInternal(server.EC_SERVER, "script entrypoint parsing failed", err))
		}

		// parse entrypoint list
		for _, v := range strings.Split(args.EntrypointCond, ",") {
			// ignore matching errors
			isBranch, _ := regexp.MatchString(`^[RL/]+$`, v)
			isNum, _ := regexp.MatchString(`^[\d]+$`, v)
			switch true {
			case isNum:
				ep, err := strconv.Atoi(v)
				if err != nil {
					panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, fmt.Sprintf("invalid entrypoint id %s", v), err))
				}
				r.Entrypoints = append(r.Entrypoints, int64(ep))
			case isBranch:
				fallthrough
			default:
				if isBranch {
					e, ok := scriptEntrypoints.FindBranch(v)
					if !ok {
						panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, fmt.Sprintf("missing entrypoint branch %s", v), err))
					}
					r.Entrypoints = append(r.Entrypoints, int64(e.Id))
				} else {
					e, ok := scriptEntrypoints[v]
					if !ok {
						panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, fmt.Sprintf("missing entrypoint %s", v), err))
					}
					r.Entrypoints = append(r.Entrypoints, int64(e.Id))
				}
			}
		}
	}

	ops, err := ctx.Indexer.ListContractCalls(ctx, r)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read contract calls", err))
	}

	// we reuse explorer ops here
	resp := make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewOp(ctx, v, nil, cc, args, cache), args.WithMerge())
	}

	return resp, http.StatusOK
}

func ReadContractCreator(ctx *server.Context) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	if cc.CreatorId == 0 {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no creator for this contract", nil))
	}
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.CreatorId)
	if err != nil {
		switch err {
		case index.ErrNoAccountEntry:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such account", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}
	return NewAccount(ctx, acc, args), http.StatusOK
}

type Script struct {
	Script      *micheline.Script     `json:"script,omitempty"`
	Type        micheline.Typedef     `json:"storage_type"`
	Entrypoints micheline.Entrypoints `json:"entrypoints"`
	Views       micheline.Views       `json:"views,omitempty"`
	Bigmaps     map[string]int64      `json:"bigmaps,omitempty"`
	modified    time.Time             `json:"-"`
}

func (s Script) LastModified() time.Time { return s.modified }
func (s Script) Expires() time.Time      { return time.Time{} }

var _ server.Resource = (*Script)(nil)

func ReadContractScript(ctx *server.Context) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)

	// unmarshal and optionally migrate full script
	if args.BlockHeight == 0 {
		args.BlockHeight = ctx.Tip.BestHeight
	}

	script, err := cc.LoadScript()
	if err != nil {
		panic(server.EInternal(server.EC_SERVER, "script unmarshal failed", err))
	}

	// empty script before babylon is OK
	if script == nil {
		return nil, http.StatusNoContent
	}

	ep, err := script.Entrypoints(args.WithPrim())
	if err != nil {
		ctx.Log.Errorf("script entrypoint parsing failed", err)
	}
	views, err := script.Views(args.WithPrim(), args.WithPrim())
	if err != nil {
		ctx.Log.Errorf("script view parsing failed", err)
	}
	ids, err := ctx.Indexer.ListContractBigmapIds(ctx.Context, cc.AccountId)
	if err != nil {
		ctx.Log.Errorf("script bigmap parsing failed", err)
	}

	resp := &Script{
		Script:      script,
		Type:        script.StorageType().Typedef("storage"),
		Entrypoints: ep,
		Views:       views,
		Bigmaps:     cc.NamedBigmaps(ids),
		modified:    ctx.Indexer.LookupBlockTime(ctx.Context, cc.FirstSeen),
	}
	if !args.WithPrim() {
		resp.Script = nil
	}

	return resp, http.StatusOK
}

func ReadContractStorage(ctx *server.Context) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)

	if args.BlockHeight > 0 && args.BlockHeight < cc.FirstSeen {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "empty storage before origination", nil))
	}

	// unmarshal full script, post-babylon migration has been applied
	script, err := cc.LoadScript()
	if err != nil {
		panic(server.EInternal(server.EC_SERVER, "script unmarshal failed", err))
	}

	// type is always the most recently upgraded type stored in contract table
	var (
		prim   micheline.Prim = micheline.Prim{}
		typ    micheline.Type = script.StorageType()
		ts     time.Time
		height int64
		// patchBigmaps bool
	)

	if args.BlockHeight == 0 || args.BlockHeight >= cc.LastSeen {
		// most recent storage is now stored in contract table!
		height = cc.LastSeen
		if err := prim.UnmarshalBinary(cc.Storage); err != nil {
			log.Errorf("explorer: storage unmarshal in contract %s: %v", cc, err)
		}
		// when data is loaded from origination, we must patch bigmap pointers
		// patchBigmaps = cc.FirstSeen == cc.LastSeen && bytes.Count(cc.CallStats, []byte{0}) == len(cc.CallStats)
		ts = ctx.Indexer.LookupBlockTime(ctx.Context, height)
	} else {
		// find earlier incoming call before height
		op, err := ctx.Indexer.FindLastCall(
			ctx.Context,
			cc.AccountId,
			cc.FirstSeen,
			args.BlockHeight,
		)
		if err != nil && err != index.ErrNoOpEntry {
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}

		// when no most recent call exists, load from origination
		if op == nil {
			op, err = ctx.Indexer.FindOrigination(ctx, cc.AccountId, cc.FirstSeen)
			if err != nil {
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
			// patchBigmaps = true
		}

		// unmarshal from op
		height = op.Height
		ts = op.Timestamp
		if err := prim.UnmarshalBinary(op.Storage); err != nil {
			log.Errorf("explorer: storage unmarshal in op %s: %v", op.Hash, err)
		}
	}

	// patch bigmap pointers when storage is loaded from origination
	// if patchBigmaps && len(ids) > 0 {
	// 	// Note: This is a heuristic only, and should work in the majority of cases.
	// 	// Reason is that in value trees we cannot distinguish between bigmaps
	// 	// and any other container type using PrimSequence as encoding (list, map, set).
	// 	var i int
	// 	prim.Visit(func(p *micheline.Prim) error {
	// 		if p.LooksLikeContainer() && p.LooksLikeMap() {
	// 			*p = micheline.NewBigmapRef(ids[i])
	// 			i++
	// 			if len(ids) <= i {
	// 				return io.EOF
	// 			}
	// 			return micheline.PrimSkip
	// 		}
	// 		return nil
	// 	})
	// }
	resp := &StorageValue{
		modified: ts,
		expires:  ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
	}

	if args.WithUnpack() && prim.IsPackedAny() {
		if up, err := prim.UnpackAll(); err == nil {
			prim = up
		}
	}

	val := micheline.NewValue(typ, prim)
	if m, err := val.Map(); err == nil {
		resp.Value = m
	} else {
		resp.Prim = &prim
	}

	if args.WithPrim() {
		resp.Prim = &prim
	}

	return resp, http.StatusOK
}
