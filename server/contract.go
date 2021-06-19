// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/gorilla/mux"
	"io"
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
)

func init() {
	register(ExplorerContract{})
}

var _ RESTful = (*ExplorerContract)(nil)

type ExplorerContract struct {
	Address       string                       `json:"address"`
	Creator       string                       `json:"creator"`
	Delegate      string                       `json:"delegate"`
	StorageSize   int64                        `json:"storage_size"`
	StoragePaid   int64                        `json:"storage_paid"`
	FirstSeen     int64                        `json:"first_seen"`
	LastSeen      int64                        `json:"last_seen"`
	FirstSeenTime time.Time                    `json:"first_seen_time"`
	LastSeenTime  time.Time                    `json:"last_seen_time"`
	NOps          int                          `json:"n_ops"`
	NOpsFailed    int                          `json:"n_ops_failed"`
	Bigmaps       map[string]int64             `json:"bigmaps,omitempty"`
	InterfaceHash string                       `json:"iface_hash"`
	CodeHash      string                       `json:"code_hash"`
	CallStats     map[string]int               `json:"call_stats"`
	Features      micheline.Features           `json:"features"`
	Interfaces    micheline.Interfaces         `json:"interfaces"`
	Metadata      map[string]*ExplorerMetadata `json:"metadata,omitempty"`

	expires time.Time `json:"-"`
}

func NewExplorerContract(ctx *ApiContext, c *model.Contract, a *model.Account, args Options) *ExplorerContract {
	p := ctx.Params
	cc := &ExplorerContract{
		Address:       a.String(),
		StorageSize:   c.StorageSize,
		StoragePaid:   c.StoragePaid,
		FirstSeen:     a.FirstSeen,
		LastSeen:      a.LastSeen,
		NOps:          a.NOps,
		NOpsFailed:    a.NOpsFailed,
		InterfaceHash: hex.EncodeToString(c.InterfaceHash),
		CodeHash:      hex.EncodeToString(c.CodeHash),
		CallStats:     c.ListCallStats(),
		Features:      c.ListFeatures(),
		Interfaces:    c.ListInterfaces(),
		expires:       ctx.Tip.BestTime.Add(p.TimeBetweenBlocks[0]),
	}

	// resolve block times
	cc.FirstSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.FirstSeen)
	cc.LastSeenTime = ctx.Indexer.LookupBlockTime(ctx.Context, a.LastSeen)

	// map bigmap ids to storage annotation names
	if ids, err := ctx.Indexer.ListContractBigMapIds(ctx.Context, a.RowId); err == nil {
		cc.Bigmaps = c.NamedBigmaps(ids)
	} else {
		log.Errorf("explorer contract: cannot load bigmap ids: %v", err)
	}
	cc.Creator = ctx.Indexer.LookupAddress(ctx, a.CreatorId).String()
	cc.Delegate = ctx.Indexer.LookupAddress(ctx, a.DelegateId).String()

	// add metadata
	if args.WithMeta() {
		cc.Metadata = make(map[string]*ExplorerMetadata)
		if md, ok := lookupMetadataById(ctx, a.RowId, 0, false); ok {
			cc.Metadata[cc.Address] = md
		}
		// fetch baker metadata for delegators
		if a.IsDelegated {
			if md, ok := lookupMetadataById(ctx, a.DelegateId, 0, false); ok {
				cc.Metadata[cc.Delegate] = md
			}
		}
		if md, ok := lookupMetadataById(ctx, c.CreatorId, 0, false); ok {
			cc.Metadata[cc.Creator] = md
		}
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
	r.HandleFunc("/{ident}/calls", C(ReadContractCalls)).Methods("GET")
	r.HandleFunc("/{ident}/creator", C(ReadContractCreator)).Methods("GET")
	r.HandleFunc("/{ident}/script", C(ReadContractScript)).Methods("GET")
	r.HandleFunc("/{ident}/storage", C(ReadContractStorage)).Methods("GET")
	return nil

}

type ContractRequest struct {
	ListRequest // offset, limit, cursor, order

	Block    string        `schema:"block"`    // height or hash for time-lock
	Since    string        `schema:"since"`    // block hash or height for updates
	Unpack   bool          `schema:"unpack"`   // unpack packed key/values
	Prim     bool          `schema:"prim"`     // for prim/value rendering
	Meta     bool          `schema:"meta"`     // include account metadata
	Collapse bool          `schema:"collapse"` // collapse internal calls
	Sender   tezos.Address `schema:"sender"`   // sender address

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

func (r *ContractRequest) WithMeta() bool     { return r != nil && r.Meta }
func (r *ContractRequest) WithRights() bool   { return false }
func (r *ContractRequest) WithCollapse() bool { return r != nil && r.Collapse }

func (r *ContractRequest) Parse(ctx *ApiContext) {
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
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid entrypoint filter mode '%s'", keys[1]), nil))
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

func loadContract(ctx *ApiContext) *model.Contract {
	if ccIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || ccIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing contract address", nil))
	} else {
		addr, err := tezos.ParseAddress(ccIdent)
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
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.AccountId)
	if err != nil {
		switch err {
		case index.ErrNoContractEntry, index.ErrNoAccountEntry:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such contract", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	return NewExplorerContract(ctx, cc, acc, args), http.StatusOK
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
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}

	// parse entrypoint filter
	// - name (eg. "default")
	// - branch (eg. "RRL")
	// - id (eg. 5)
	r.Entrypoints = make([]int64, 0)
	if len(args.EntrypointCond) > 0 {
		script, err := cc.LoadScript()
		if err != nil {
			panic(EInternal(EC_SERVER, "script unmarshal failed", err))
		}
		scriptEntrypoints, err := script.Entrypoints(false)
		if err != nil {
			panic(EInternal(EC_SERVER, "script entrypoint parsing failed", err))
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
					panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, fmt.Sprintf("invalid entrypoint id %s", v), err))
				}
				r.Entrypoints = append(r.Entrypoints, int64(ep))
			case isBranch:
				fallthrough
			default:
				if isBranch {
					e, ok := scriptEntrypoints.FindBranch(v)
					if !ok {
						panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, fmt.Sprintf("missing entrypoint branch %s", v), err))
					}
					r.Entrypoints = append(r.Entrypoints, int64(e.Id))
				} else {
					e, ok := scriptEntrypoints[v]
					if !ok {
						panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, fmt.Sprintf("missing entrypoint %s", v), err))
					}
					r.Entrypoints = append(r.Entrypoints, int64(e.Id))
				}
			}
		}
	}

	ops, err := ctx.Indexer.ListContractCalls(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read contract calls", err))
	}

	// we reuse explorer ops here
	resp := &ExplorerOpList{
		list:    make([]*ExplorerOp, 0),
		expires: ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewExplorerOp(ctx, v, nil, cc, args, cache), args.WithCollapse())
	}

	return resp, http.StatusOK
}

func ReadContractCreator(ctx *ApiContext) (interface{}, int) {
	args := &AccountRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)
	if cc.CreatorId == 0 {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "no creator for this contract", nil))
	}
	acc, err := ctx.Indexer.LookupAccountId(ctx, cc.CreatorId)
	if err != nil {
		switch err {
		case index.ErrNoAccountEntry:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such account", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	return NewExplorerAccount(ctx, acc, args), http.StatusOK
}

type ExplorerScript struct {
	Script      *micheline.Script     `json:"script,omitempty"`
	Type        micheline.Typedef     `json:"storage_type"`
	Entrypoints micheline.Entrypoints `json:"entrypoints"`
	Bigmaps     map[string]int64      `json:"bigmaps,omitempty"`
	modified    time.Time             `json:"-"`
}

func (s ExplorerScript) LastModified() time.Time { return s.modified }
func (s ExplorerScript) Expires() time.Time      { return time.Time{} }

var _ Resource = (*ExplorerScript)(nil)

func ReadContractScript(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)

	// unmarshal and optionally migrate script
	if args.BlockHeight == 0 {
		args.BlockHeight = ctx.Tip.BestHeight
	}

	script, err := cc.LoadScript()
	if err != nil {
		panic(EInternal(EC_SERVER, "script unmarshal failed", err))
	}

	// empty script before babylon is OK
	if script == nil {
		return nil, http.StatusNoContent
	}

	ep, err := script.Entrypoints(args.WithPrim())
	if err != nil {
		panic(EInternal(EC_SERVER, "script entrypoint parsing failed", err))
	}
	ids, _ := ctx.Indexer.ListContractBigMapIds(ctx.Context, cc.AccountId)

	resp := &ExplorerScript{
		Script:      script,
		Type:        script.StorageType().Typedef("storage"),
		Entrypoints: ep,
		Bigmaps:     cc.NamedBigmaps(ids),
		modified:    ctx.Indexer.LookupBlockTime(ctx.Context, cc.FirstSeen),
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

	if args.BlockHeight > 0 && args.BlockHeight < cc.FirstSeen {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "empty storage before origination", nil))
	}

	// unmarshal script, post-babylon migration has been applied
	script, err := cc.LoadScript()
	if err != nil {
		panic(EInternal(EC_SERVER, "script unmarshal failed", err))
	}
	ids, _ := ctx.Indexer.ListContractBigMapIds(ctx.Context, cc.AccountId)

	// type is always the most recently upgraded type stored in contract table
	var (
		prim         micheline.Prim = micheline.Prim{}
		typ          micheline.Type = script.StorageType()
		ts           time.Time
		height       int64
		patchBigmaps bool
	)

	if args.BlockHeight == 0 || args.BlockHeight >= cc.LastSeen {
		// most recent storage is now stored in contract table!
		height = cc.LastSeen
		if err := prim.UnmarshalBinary(cc.Storage); err != nil {
			log.Errorf("explorer: storage unmarshal in contract %s: %v", cc, err)
		}
		// when data is loaded from origination, we must patch bigmap pointers
		patchBigmaps = cc.FirstSeen == cc.LastSeen && bytes.Count(cc.CallStats, []byte{0}) == len(cc.CallStats)
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
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}

		// when no most recent call exists, load from origination
		if op == nil {
			op, err = ctx.Indexer.FindOrigination(ctx, cc.AccountId, cc.FirstSeen)
			if err != nil {
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
			patchBigmaps = true
		}

		// unmarshal from op
		height = op.Height
		ts = op.Timestamp
		if err := prim.UnmarshalBinary(op.Storage); err != nil {
			log.Errorf("explorer: storage unmarshal in op %s: %v", op.Hash, err)
		}
	}

	// patch bigmap pointers when storage is loaded from origination
	if patchBigmaps && len(ids) > 0 {

		// Note: This is a heuristic only, and should work in the majority of cases.
		// Reason is that in value trees we cannot distinguish between bigmaps
		// and any other container type using PrimSequence as encoding (list, map, set).
		//
		var i int
		prim.Visit(func(p *micheline.Prim) error {
			if p.LooksLikeContainer() {
				*p = micheline.NewBigmapRef(ids[i])
				i++
				if len(ids) <= i {
					return io.EOF
				}
			}
			return nil
		})
	}

	resp := &ExplorerStorageValue{
		Value:    micheline.NewValue(typ, prim),
		Bigmaps:  cc.NamedBigmaps(ids),
		modified: ts,
		expires:  ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}
	if args.WithMeta() {
		resp.Meta = &ExplorerStorageMeta{
			Contract: cc.String(),
			Time:     ts,
			Height:   height,
			Block:    ctx.Indexer.LookupBlockHash(ctx.Context, height),
		}
	}

	if args.WithPrim() {
		resp.Prim = &prim
	}

	if args.WithUnpack() && resp.Value.IsPackedAny() {
		if up, err := resp.Value.UnpackAll(); err == nil {
			resp.Value = up
		}
	}

	return resp, http.StatusOK
}
