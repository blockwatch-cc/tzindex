// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

func init() {
	register(ExplorerOp{})
}

var (
	_ RESTful = (*ExplorerOp)(nil)
	_ RESTful = (*ExplorerOpList)(nil)
)

type ExplorerOpList []*ExplorerOp

func (t ExplorerOpList) LastModified() time.Time {
	return t[0].Timestamp
}

func (t ExplorerOpList) Expires() time.Time {
	return time.Time{}
}

func (t ExplorerOpList) RESTPrefix() string {
	return ""
}

func (t ExplorerOpList) RESTPath(r *mux.Router) string {
	return ""
}

func (t ExplorerOpList) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t ExplorerOpList) RegisterRoutes(r *mux.Router) error {
	return nil
}

type ExplorerOp struct {
	Hash         chain.OperationHash       `json:"hash"`
	Type         chain.OpType              `json:"type"`
	BlockHash    chain.BlockHash           `json:"block"`
	Timestamp    time.Time                 `json:"time"`
	Height       int64                     `json:"height"`
	Cycle        int64                     `json:"cycle"`
	Counter      int64                     `json:"counter"`
	OpN          int                       `json:"op_n"`
	OpC          int                       `json:"op_c"`
	OpI          int                       `json:"op_i"`
	Status       string                    `json:"status"`
	IsSuccess    bool                      `json:"is_success"`
	IsContract   bool                      `json:"is_contract"`
	GasLimit     int64                     `json:"gas_limit"`
	GasUsed      int64                     `json:"gas_used"`
	GasPrice     float64                   `json:"gas_price"`
	StorageLimit int64                     `json:"storage_limit"`
	StorageSize  int64                     `json:"storage_size"`
	StoragePaid  int64                     `json:"storage_paid"`
	Volume       float64                   `json:"volume"`
	Fee          float64                   `json:"fee"`
	Reward       float64                   `json:"reward"`
	Deposit      float64                   `json:"deposit"`
	Burned       float64                   `json:"burned"`
	IsInternal   bool                      `json:"is_internal"`
	HasData      bool                      `json:"has_data"`
	TDD          float64                   `json:"days_destroyed"`
	Data         json.RawMessage           `json:"data,omitempty"`
	Errors       json.RawMessage           `json:"errors,omitempty"`
	Parameters   *ExplorerParameters       `json:"parameters,omitempty"`
	Storage      *ExplorerStorageValue     `json:"storage,omitempty"`
	BigMapDiff   *ExplorerBigMapUpdateList `json:"big_map_diff,omitempty"`
	Sender       *chain.Address            `json:"sender,omitempty"`
	Receiver     *chain.Address            `json:"receiver,omitempty"`
	Manager      *chain.Address            `json:"manager,omitempty"`
	Delegate     *chain.Address            `json:"delegate,omitempty"`
	BranchId     uint64                    `json:"branch_id"`
	BranchHeight int64                     `json:"branch_height"`
	BranchDepth  int64                     `json:"branch_depth"`
	BranchHash   chain.BlockHash           `json:"branch"`

	expires time.Time `json:"-"`
}

func NewExplorerOp(ctx *ApiContext, op *model.Op, block *model.Block, cc *model.Contract, p *chain.Params, args *ContractRequest) *ExplorerOp {
	t := &ExplorerOp{
		Hash:         op.Hash,
		Type:         op.Type,
		Timestamp:    op.Timestamp,
		Height:       op.Height,
		Cycle:        op.Cycle,
		Counter:      op.Counter,
		OpN:          op.OpN,
		OpC:          op.OpC,
		OpI:          op.OpI,
		Status:       op.Status.String(),
		IsSuccess:    op.IsSuccess,
		IsContract:   op.IsContract,
		GasLimit:     op.GasLimit,
		GasUsed:      op.GasUsed,
		GasPrice:     op.GasPrice,
		StorageLimit: op.StorageLimit,
		StorageSize:  op.StorageSize,
		StoragePaid:  op.StoragePaid,
		Volume:       p.ConvertValue(op.Volume),
		Fee:          p.ConvertValue(op.Fee),
		Reward:       p.ConvertValue(op.Reward),
		Deposit:      p.ConvertValue(op.Deposit),
		Burned:       p.ConvertValue(op.Burned),
		IsInternal:   op.IsInternal,
		HasData:      op.HasData,
		TDD:          op.TDD,
		BranchId:     op.BranchId,
		BranchHeight: op.BranchHeight,
		BranchDepth:  op.BranchDepth,
	}

	// lookup accounts
	accs, err := ctx.Indexer.LookupAccountIds(ctx.Context,
		vec.UniqueUint64Slice([]uint64{
			op.SenderId.Value(),
			op.ReceiverId.Value(),
			op.ManagerId.Value(),
			op.DelegateId.Value(),
		}))
	if err != nil {
		log.Errorf("explorer op: cannot resolve accounts: %v", err)
	}
	for _, acc := range accs {
		if acc.RowId == op.SenderId {
			t.Sender = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
		if acc.RowId == op.ReceiverId {
			t.Receiver = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
		if acc.RowId == op.ManagerId {
			t.Manager = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
		if acc.RowId == op.DelegateId {
			t.Delegate = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
	}

	if op.HasData {
		switch op.Type {
		case chain.OpTypeDoubleBakingEvidence, chain.OpTypeDoubleEndorsementEvidence:
			t.Data = json.RawMessage{}
			if err := json.Unmarshal([]byte(op.Data), &t.Data); err != nil {
				t.Data = nil
				log.Errorf("explorer op: unmarshal %s data: %v", op.Type, err)
			}
		default:
			if op.Data != "" {
				t.Data = json.RawMessage(strconv.Quote(op.Data))
			}
		}
	}

	if op.Errors != "" {
		t.Errors = json.RawMessage(op.Errors)
	}

	// set block hash
	if block != nil {
		t.BlockHash = block.Hash
	} else {
		if h, err := ctx.Indexer.BlockHashByHeight(ctx.Context, op.Height); err == nil {
			t.BlockHash = h
		}
	}

	// set branch hash
	if op.BranchId != 0 {
		if h, err := ctx.Indexer.BlockHashById(ctx.Context, op.BranchId); err == nil {
			t.BranchHash = h
		}
	}

	if len(op.Parameters) > 0 || len(op.Storage) > 0 {
		var (
			err    error
			script *micheline.Script
		)
		if cc == nil || cc.AccountId != op.ReceiverId {
			cc, err = ctx.Indexer.LookupContractId(ctx.Context, op.ReceiverId)
			if err != nil {
				log.Errorf("explorer: lookup contract for account %d: %v", op.ReceiverId, err)
			}
		}
		// need parameter and contract types from script, unmarshal and optionally migrate
		if cc != nil {
			var mgrHash []byte
			if mgr, err := ctx.Indexer.LookupAccountId(ctx, cc.ManagerId); err == nil {
				mgrHash = mgr.Address().Bytes()
			}
			script, err = cc.LoadScript(ctx.Crawler.Tip(), op.Height, mgrHash)
			if err != nil {
				log.Errorf("explorer: script unmarshal: %v", err)
			}
		}

		// set params
		if len(op.Parameters) > 0 && script != nil {
			params := &micheline.Parameters{}
			if err := params.UnmarshalBinary(op.Parameters); err != nil {
				log.Errorf("explorer op: unmarshal %s params: %v", op.Type, err)
			}

			eps, _ := script.Entrypoints(true)
			branch := params.Branch(eps) // can be [LR]+ or empty when entrypoint is used
			ep, ok := eps[params.Entrypoint]
			if !ok && branch != "" {
				ep, ok = eps.FindBranch(branch)
			}
			if !ok {
				switch params.Entrypoint {
				case "default":
					// rebase entrypoint by prepending the default branch
					branch = script.SearchEntrypointName("default") + branch
					// log.Debugf("rebasing call %s to %s default entrypoint %s", t.Hash, t.Receiver, branch)
					ep, ok = eps.FindBranch(branch)
					if !ok {
						// log.Debugf("using fallback default entrypoint 0")
						ep, ok = eps.FindId(0)
					}

				case "root":
					log.Errorf("explorer op: %s missing entrypoint from root %s", t.Hash, params.Entrypoint)
				default:
					log.Errorf("explorer op: %s missing entrypoint %s", t.Hash, params.Entrypoint)
				}
			}
			prim := params.Unwrap(eps) // strip L/R T_OR data
			t.Parameters = &ExplorerParameters{
				Entrypoint: params.Entrypoint, // from params, e.g. "default"
				Id:         ep.Id,
				Branch:     ep.Branch,
			}
			// only render params when type check did not fail
			if op.Status != chain.OpStatusFailed {
				t.Parameters.Value = &micheline.BigMapValue{
					Type:  ep.Prim,
					Value: prim,
				}
				// HOTFIX: try marshalling the entrypoint and handle failure
				if _, err := t.Parameters.Value.MarshalJSON(); err != nil {
					log.Warnf("Invalid entrypoint detected name='%s' branch=%s", params.Entrypoint, params.Branch(eps))
					t.Parameters.Value = nil
				}
			}
			if args.WithPrim() {
				t.Parameters.Prim = prim
			}
			if args.WithUnpack() && prim.IsPackedAny() {
				if p, err := prim.UnpackAny(); err == nil {
					t.Parameters.ValueUnpacked = &micheline.BigMapValue{
						Type:  p.BuildType(),
						Value: p,
					}
				}
			}
		}

		if len(op.Storage) > 0 && script != nil && cc != nil {
			prim := &micheline.Prim{}
			if err := prim.UnmarshalBinary(op.Storage); err != nil {
				log.Errorf("explorer op: unmarshal %s storage: %v", op.Type, err)
			}
			typ := script.Code.Storage.Args[0]
			t.Storage = &ExplorerStorageValue{
				Meta: ExplorerStorageMeta{
					Contract: cc.String(),
					Time:     op.Timestamp,
					Height:   op.Height,
					Block:    t.BlockHash,
				},
				Value: &micheline.BigMapValue{
					Type:  typ,
					Value: prim,
				},
			}
			if args.WithPrim() {
				t.Storage.Prim = prim
			}
			if args.WithUnpack() && prim.IsPackedAny() {
				if p, err := prim.UnpackAny(); err == nil {
					t.Storage.ValueUnpacked = &micheline.BigMapValue{
						Type:  p.BuildType(),
						Value: p,
					}
				}
			}
		}
	}

	if len(op.BigMapDiff) > 0 {
		bmd := make(micheline.BigMapDiff, 0)
		if err := bmd.UnmarshalBinary(op.BigMapDiff); err != nil {
			log.Errorf("explorer op: unmarshal %s bigmap: %v", op.Type, err)
		}
		var alloc *model.BigMapItem
		typ := &micheline.Prim{}

		t.BigMapDiff = &ExplorerBigMapUpdateList{
			diff: make([]ExplorerBigMapUpdate, 0, len(bmd)),
		}
		for _, v := range bmd {
			// need bigmap type to unbox and convert keys
			if alloc == nil || alloc.BigMapId != v.Id {
				var err error
				alloc, _, err = ctx.Indexer.LookupBigmap(ctx.Context, v.Id, false)
				if err != nil {
					log.Errorf("explorer op: unmarshal bigmap %d alloc: %v", v.Id, err)
					continue
				}
				typ = &micheline.Prim{}
				if err := typ.UnmarshalBinary(alloc.Value); err != nil {
					log.Errorf("explorer op: bigmap type unmarshal: %v", err)
					continue
				}
			}

			upd := ExplorerBigMapUpdate{
				Action: v.Action,
				ExplorerBigmapValue: ExplorerBigmapValue{
					Meta: ExplorerBigmapMeta{
						Contract:     t.Receiver.String(),
						BigMapId:     alloc.BigMapId,
						UpdateTime:   op.Timestamp,
						UpdateHeight: op.Height,
						UpdateBlock:  t.BlockHash,
					},
				},
			}
			switch v.Action {
			case micheline.BigMapDiffActionUpdate:
				k := v.KeyAs(alloc.KeyType)
				upd.Key = k
				upd.KeyHash = v.KeyHash
				upd.KeyBinary = k.Encode()
				upd.Value = &micheline.BigMapValue{
					Type:  typ,
					Value: v.Value,
				}
				if args.WithPrim() {
					upd.Prim = &ExplorerBigmapKeyValue{
						Key:   k.Prim(),
						Value: v.Value,
					}
				}
				if args.WithUnpack() {
					if v.Value.IsPackedAny() {
						if p, err := v.Value.UnpackAny(); err == nil {
							upd.ValueUnpacked = &micheline.BigMapValue{
								Type:  p.BuildType(),
								Value: p,
							}
						}
					}
					if k.IsPacked() {
						if upd.KeyUnpacked, err = k.UnpackKey(); err == nil {
							upd.KeyPretty = upd.KeyUnpacked.String()
						}
					}
				}

			case micheline.BigMapDiffActionRemove:
				k := v.KeyAs(alloc.KeyType)
				upd.Key = k
				upd.KeyHash = v.KeyHash
				upd.KeyBinary = k.Encode()
				if args.WithPrim() {
					upd.Prim = &ExplorerBigmapKeyValue{
						Key: k.Prim(),
					}
				}

			case micheline.BigMapDiffActionAlloc:
				// no unboxed value, just types
				bmt := micheline.BigMapType(*v.ValueType)
				upd.KeyType = &alloc.KeyType
				upd.KeyEncoding = &alloc.KeyEncoding
				upd.ValueType = &bmt
				if args.WithPrim() {
					upd.Prim = &ExplorerBigmapKeyValue{
						KeyType: &micheline.Prim{
							Type:   micheline.PrimNullary,
							OpCode: v.KeyType,
						},
						ValueType: v.ValueType,
					}
				}

			case micheline.BigMapDiffActionCopy:
				// no unboxed value, just types
				bmt := micheline.BigMapType(*v.ValueType)
				upd.KeyType = &alloc.KeyType
				upd.KeyEncoding = &alloc.KeyEncoding
				upd.ValueType = &bmt
				upd.SourceId = v.SourceId
				upd.DestId = v.DestId
				if args.WithPrim() {
					upd.Prim = &ExplorerBigmapKeyValue{
						KeyType: &micheline.Prim{
							Type:   micheline.PrimNullary,
							OpCode: v.KeyType,
						},
						ValueType: v.ValueType,
					}
				}
			}
			t.BigMapDiff.diff = append(t.BigMapDiff.diff, upd)
		}
	}

	// cache blocks in the reorg safety zone only until next block is expected
	if op.Height+chain.MaxBranchDepth >= ctx.Crawler.Height() {
		t.expires = ctx.Crawler.Time().Add(p.TimeBetweenBlocks[0])
	}

	return t
}

func (t ExplorerOp) LastModified() time.Time {
	return t.Timestamp
}

func (t ExplorerOp) Expires() time.Time {
	return t.expires
}

func (t ExplorerOp) RESTPrefix() string {
	return "/explorer/op"
}

func (t ExplorerOp) RESTPath(r *mux.Router) string {
	path, _ := r.Get("op").URLPath("ident", t.Hash.String())
	return path.String()
}

func (t ExplorerOp) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t ExplorerOp) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadOp)).Methods("GET").Name("op")
	return nil

}

// used when listing ops in block/account/contract context
type ExplorerOpsRequest struct {
	ExplorerListRequest
	Type  chain.OpType   `schema:"type"`
	Order pack.OrderType `schema:"order"`
	Block string         `schema:"block"` // height or hash for time-lock
	Since string         `schema:"since"` // block hash or height for updates

	// decoded values
	BlockHeight int64           `schema:"-"`
	BlockHash   chain.BlockHash `schema:"-"`
	SinceHeight int64           `schema:"-"`
	SinceHash   chain.BlockHash `schema:"-"`
}

func (r *ExplorerOpsRequest) ParseBlockIdent(ctx *ApiContext) {
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

func loadOps(ctx *ApiContext) []*model.Op {
	if opIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || opIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing operation hash", nil))
	} else {
		ops, err := ctx.Indexer.LookupOp(ctx, opIdent)
		if err != nil {
			switch err {
			case index.ErrNoOpEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such operation", err))
			case etl.ErrInvalidHash:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid operation hash", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return ops
	}
}

func ReadOp(ctx *ApiContext) (interface{}, int) {
	args := ExplorerListRequest{}
	ctx.ParseRequestArgs(&args)
	ops := loadOps(ctx)
	resp := make(ExplorerOpList, 0, len(ops))
	var params *chain.Params
	for _, v := range ops {
		if params == nil {
			params = ctx.Crawler.ParamsByHeight(v.Height)
		}
		resp = append(resp, NewExplorerOp(ctx, v, nil, nil, params, nil))
	}
	return resp, http.StatusOK
}
