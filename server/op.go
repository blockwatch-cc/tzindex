// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
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
	register(ExplorerOp{})
}

var (
	_ RESTful = (*ExplorerOp)(nil)
	_ RESTful = (*ExplorerOpList)(nil)
)

type ExplorerOpList struct {
	list    []*ExplorerOp
	expires time.Time
}

func (l *ExplorerOpList) Append(op *ExplorerOp, collapsed bool) {
	if !collapsed {
		l.list = append(l.list, op)
		return
	}

	// build op tree based on list and internal op positions

	// find op for insert
	var idx int = -1
	for i, v := range l.list {
		if v.Height != op.Height {
			break
		}
		if v.OpL == op.OpL && v.OpP == op.OpP {
			idx = i
			break
		}
	}

	// append if no match was found
	if idx < 0 {
		l.list = append(l.list, op)
		return
	}

	// there's 2 types of related operations (they can appear mixed)
	// 1 batch: list of operations, e.g. reveal+tx, approve+transfer, multi-transfer
	// 2 group: internal operation sequences (can appear alone or inside a batch)

	// if the found op is not a batch, but the to be appended op belongs to a batch
	// wrap the existing op as batch
	ins := l.list[idx]
	if !ins.IsBatch && op.OpC != ins.OpC {
		ins = WrapAsBatchOp(l.list[idx])
		l.list[idx] = ins
	}

	// count embedded ops
	ins.NOps++

	// append to batch if op is not an internal operation, update batch op summary
	if ins.IsBatch && !op.IsInternal {
		ins.Batch = append(ins.Batch, op)
		ins.GasLimit += op.GasLimit
		ins.GasUsed += op.GasUsed
		ins.GasPrice += op.GasPrice
		ins.StorageLimit += op.StorageLimit
		ins.StorageSize += op.StorageSize
		ins.StoragePaid += op.StoragePaid
		ins.Volume += op.Volume
		ins.Fee += op.Fee
		return
	}

	// append internal ops to the last batch member (or the current group)
	if ins.IsBatch {
		ins = ins.Batch[len(ins.Batch)-1]
	}

	// init group if not done yet
	if len(ins.Internal) == 0 {
		ins.Internal = make([]*ExplorerOp, 0)
	}
	ins.Internal = append(ins.Internal, op)

	// TODO: should we summarize gas/fee across internal ops into a batch header?
}

func (t ExplorerOpList) LastModified() time.Time {
	l := len(t.list)
	if l == 0 {
		return time.Time{}
	}
	a, b := t.list[0].Timestamp, t.list[l-1].Timestamp
	if a.After(b) {
		return a
	}
	return b
}

func (l ExplorerOpList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l ExplorerOpList) Expires() time.Time           { return l.expires }

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
	RowId         model.OpID                   `json:"row_id,omitempty"`
	Hash          tezos.OpHash                 `json:"hash,omitempty"`
	Type          tezos.OpType                 `json:"type"`
	BlockHash     tezos.BlockHash              `json:"block"`
	Timestamp     time.Time                    `json:"time"`
	Height        int64                        `json:"height"`
	Cycle         int64                        `json:"cycle"`
	Counter       int64                        `json:"counter"`
	OpL           int                          `json:"op_l"`
	OpP           int                          `json:"op_p"`
	OpC           int                          `json:"op_c"`
	OpI           int                          `json:"op_i"`
	Status        string                       `json:"status"`
	IsSuccess     bool                         `json:"is_success"`
	IsContract    bool                         `json:"is_contract"`
	GasLimit      int64                        `json:"gas_limit"`
	GasUsed       int64                        `json:"gas_used"`
	GasPrice      float64                      `json:"gas_price"`
	StorageLimit  int64                        `json:"storage_limit"`
	StorageSize   int64                        `json:"storage_size"`
	StoragePaid   int64                        `json:"storage_paid"`
	Volume        float64                      `json:"volume"`
	Fee           float64                      `json:"fee"`
	Reward        float64                      `json:"reward,omitempty"`
	Deposit       float64                      `json:"deposit,omitempty"`
	Burned        float64                      `json:"burned,omitempty"`
	IsInternal    bool                         `json:"is_internal,omitempty"`
	HasData       bool                         `json:"has_data,omitempty"`
	TDD           float64                      `json:"days_destroyed"`
	Data          json.RawMessage              `json:"data,omitempty"`
	Errors        json.RawMessage              `json:"errors,omitempty"`
	Parameters    *ExplorerParameters          `json:"parameters,omitempty"`
	Storage       *ExplorerStorageValue        `json:"storage,omitempty"`
	BigmapDiff    *ExplorerBigmapUpdateList    `json:"big_map_diff,omitempty"`
	Sender        *tezos.Address               `json:"sender,omitempty"`
	Receiver      *tezos.Address               `json:"receiver,omitempty"`
	Creator       *tezos.Address               `json:"creator,omitempty"`
	Delegate      *tezos.Address               `json:"delegate,omitempty"`
	BranchHeight  int64                        `json:"branch_height,omitempty"`
	BranchDepth   int64                        `json:"branch_depth,omitempty"`
	BranchHash    *tezos.BlockHash             `json:"branch_hash,omitempty"`
	IsImplicit    bool                         `json:"is_implicit,omitempty"`
	EntrypointId  *int                         `json:"entrypoint_id,omitempty"`
	Entrypoint    string                       `json:"entrypoint,omitempty"`
	IsOrphan      bool                         `json:"is_orphan,omitempty"`
	IsBatch       bool                         `json:"is_batch,omitempty"`
	IsSapling     bool                         `json:"is_sapling,omitempty"`
	BatchVolume   float64                      `json:"batch_volume,omitempty"`
	Metadata      map[string]*ExplorerMetadata `json:"metadata,omitempty"`
	Batch         []*ExplorerOp                `json:"batch,omitempty"`
	Internal      []*ExplorerOp                `json:"internal,omitempty"`
	NOps          int                          `json:"n_ops,omitempty"`
	Confirmations int64                        `json:"confirmations"`
	Value         *micheline.Prim              `json:"value,omitempty"`

	expires time.Time `json:"-"`
}

func WrapAsBatchOp(op *ExplorerOp) *ExplorerOp {
	return &ExplorerOp{
		Hash:          op.Hash,
		BlockHash:     op.BlockHash,
		Type:          tezos.OpTypeBatch,
		Timestamp:     op.Timestamp,
		Height:        op.Height,
		Cycle:         op.Cycle,
		Counter:       op.Counter,
		OpL:           op.OpL,
		OpP:           op.OpP,
		Status:        op.Status,
		IsSuccess:     op.IsSuccess,
		IsContract:    op.IsContract,
		GasLimit:      op.GasLimit,
		GasUsed:       op.GasUsed,
		GasPrice:      op.GasPrice,
		StorageLimit:  op.StorageLimit,
		StorageSize:   op.StorageSize,
		StoragePaid:   op.StoragePaid,
		Volume:        op.Volume,
		Fee:           op.Fee,
		IsImplicit:    true,
		IsBatch:       true,
		Batch:         []*ExplorerOp{op},
		NOps:          1,
		Confirmations: op.Confirmations,
	}
}

func NewExplorerOp(ctx *ApiContext, op *model.Op, block *model.Block, cc *model.Contract, args Options, cache map[int64]interface{}) *ExplorerOp {
	p := ctx.Params
	o := &ExplorerOp{
		RowId:         op.RowId,
		Type:          op.Type,
		Timestamp:     op.Timestamp,
		Height:        op.Height,
		Cycle:         op.Cycle,
		Counter:       op.Counter,
		OpL:           op.OpL,
		OpP:           op.OpP,
		OpC:           op.OpC,
		OpI:           op.OpI,
		Status:        op.Status.String(),
		IsSuccess:     op.IsSuccess,
		IsContract:    op.IsContract,
		GasLimit:      op.GasLimit,
		GasUsed:       op.GasUsed,
		GasPrice:      op.GasPrice,
		StorageLimit:  op.StorageLimit,
		StorageSize:   op.StorageSize,
		StoragePaid:   op.StoragePaid,
		Volume:        p.ConvertValue(op.Volume),
		Fee:           p.ConvertValue(op.Fee),
		Reward:        p.ConvertValue(op.Reward),
		Deposit:       p.ConvertValue(op.Deposit),
		Burned:        p.ConvertValue(op.Burned),
		IsInternal:    op.IsInternal,
		HasData:       op.HasData,
		TDD:           op.TDD,
		IsImplicit:    op.IsImplicit,
		IsOrphan:      op.IsOrphan,
		IsBatch:       op.IsBatch && !args.WithCollapse(),
		IsSapling:     op.IsSapling,
		Confirmations: ctx.Tip.BestHeight - op.Height,
	}

	if !op.Hash.Equal(tezos.ZeroOpHash) {
		o.Hash = op.Hash
	}

	// lookup accounts
	if op.SenderId > 0 {
		a := ctx.Indexer.LookupAddress(ctx, op.SenderId)
		o.Sender = &a
	}
	if op.ReceiverId > 0 {
		a := ctx.Indexer.LookupAddress(ctx, op.ReceiverId)
		o.Receiver = &a
	}
	if op.CreatorId > 0 {
		a := ctx.Indexer.LookupAddress(ctx, op.CreatorId)
		o.Creator = &a
	}
	if op.DelegateId > 0 {
		a := ctx.Indexer.LookupAddress(ctx, op.DelegateId)
		o.Delegate = &a
	}

	// add metadata, if any account is supported
	if args.WithMeta() {
		meta := make(map[string]*ExplorerMetadata)
		if op.SenderId > 0 {
			if md, ok := lookupMetadataById(ctx, op.SenderId, 0, false); ok {
				meta[o.Sender.String()] = md
			}
		}
		if op.ReceiverId > 0 {
			if md, ok := lookupMetadataById(ctx, op.ReceiverId, 0, false); ok {
				meta[o.Receiver.String()] = md
			}
		}
		if op.CreatorId > 0 {
			if md, ok := lookupMetadataById(ctx, op.CreatorId, 0, false); ok {
				meta[o.Creator.String()] = md
			}
		}
		if op.DelegateId > 0 {
			if md, ok := lookupMetadataById(ctx, op.DelegateId, 0, false); ok {
				meta[o.Delegate.String()] = md
			}
		}
		if len(meta) > 0 {
			o.Metadata = meta
		}

		// set branch hash
		o.BranchHeight = op.BranchHeight
		o.BranchDepth = op.BranchDepth
		if op.BranchId != 0 {
			found := false
			if v, ok := cache[int64(op.BranchId)]; ok {
				if h, ok := v.(tezos.BlockHash); ok {
					o.BranchHash = &h
					found = true
				}
			}
			if !found {
				if h := ctx.Indexer.LookupBlockHash(ctx.Context, op.BranchHeight); h.IsValid() {
					o.BranchHash = &h
					cache[int64(op.BranchId)] = h
				}
			}
		}
	}

	if op.HasData {
		switch op.Type {
		case tezos.OpTypeDoubleBakingEvidence, tezos.OpTypeDoubleEndorsementEvidence:
			o.Data = json.RawMessage{}
			if err := json.Unmarshal([]byte(op.Data), &o.Data); err != nil {
				o.Data = nil
				log.Errorf("explorer op: unmarshal %s data: %v", op.Type, err)
			}
		case tezos.OpTypeRegisterConstant:
			o.Value = &micheline.Prim{}
			if err := o.Value.UnmarshalBinary(op.Storage); err != nil {
				o.Value = nil
				log.Errorf("explorer op: unmarshal %s value: %v", op.Type, err)
			}
		default:
			if op.Data != "" {
				o.Data = json.RawMessage(strconv.Quote(op.Data))
			}
		}
	}

	if op.Errors != "" {
		o.Errors = json.RawMessage(op.Errors)
	}

	var blockHash tezos.BlockHash
	if block != nil {
		blockHash = block.Hash
	} else {
		blockHash = ctx.Indexer.LookupBlockHash(ctx.Context, op.Height)
	}
	o.BlockHash = blockHash

	if len(op.Parameters) > 0 || len(op.Storage) > 0 {
		var (
			err    error
			script *micheline.Script
		)
		if cc == nil || cc.AccountId != op.ReceiverId {
			cc, err = ctx.Indexer.LookupContractId(ctx.Context, op.ReceiverId)
			if err != nil && op.IsContract {
				log.Errorf("explorer: lookup contract for account %d: %v", op.ReceiverId, err)
			}
		}
		// need parameter and contract types from script
		// Note: post babylon scripts (params, storage types) are migrated!!
		if cc != nil {
			script, err = cc.LoadScript()
			if err != nil {
				log.Errorf("explorer: script unmarshal: %v", err)
			}
		}

		o.EntrypointId = IntPtr(op.Entrypoint)
		o.Entrypoint = op.Data
		o.Data = nil

		// set params
		if len(op.Parameters) > 0 && script != nil {
			callParams := &micheline.Parameters{}
			if err := callParams.UnmarshalBinary(op.Parameters); err != nil {
				log.Errorf("explorer op: unmarshal %s params: %v", op.Type, err)
			}

			// find entrypoint
			ep, prim, err := callParams.MapEntrypoint(script.ParamType())
			if err == nil {
				o.Parameters = &ExplorerParameters{
					Entrypoint: callParams.Entrypoint, // from params, e.g. "default"
					Id:         ep.Id,
					Branch:     ep.Branch,
					Call:       ep.Call,
					Value:      micheline.NewValue(ep.Type(), prim),
				}
				// only render params when type check did not fail / fix type
				if op.Status == tezos.OpStatusFailed {
					if _, err := json.Marshal(o.Parameters.Value); err != nil {
						// log.Infof("Ignoring param render error on failed call %s: %v", op.Hash, err)
						o.Parameters.Value.FixType()
					}
				}
				if args.WithPrim() {
					o.Parameters.Prim = &prim
				}
				if args.WithUnpack() && o.Parameters.Value.IsPackedAny() {
					if up, err := o.Parameters.Value.UnpackAll(); err == nil {
						o.Parameters.Value = up
					}
				}
			}
		}

		if len(op.Storage) > 0 && script != nil && cc != nil {
			prim := micheline.Prim{}
			if err := prim.UnmarshalBinary(op.Storage); err != nil {
				log.Errorf("explorer op: unmarshal %s storage: %v", op.Type, err)
			}
			// storage type is patched post-Babylon, but pre-Babylon storage
			// updates are unpatched
			typ := script.StorageType()

			// always output post-babylon storage
			// upgrade pre-babylon storage value to post-babylon spec
			if etl.NeedsBabylonUpgradeContract(cc, ctx.Params) && ctx.Params.IsPreBabylonHeight(op.Height) {
				if acc, err := ctx.Indexer.LookupAccountId(ctx, cc.CreatorId); err == nil {
					prim = prim.MigrateToBabylonStorage(acc.Address.Bytes())
				}
			}

			o.Storage = &ExplorerStorageValue{
				Value: micheline.NewValue(typ, prim),
			}
			if args.WithMeta() {
				o.Storage.Meta = &ExplorerStorageMeta{
					Contract: cc.Address,
					Time:     op.Timestamp,
					Height:   op.Height,
					Block:    blockHash,
				}
			}
			if args.WithPrim() {
				o.Storage.Prim = &prim
			}
			if args.WithUnpack() && o.Storage.Value.IsPackedAny() {
				if up, err := o.Storage.Value.UnpackAll(); err == nil {
					o.Storage.Value = up
				}
			}
		}
	}

	if len(op.BigmapDiff) > 0 {
		var (
			alloc            *model.BigmapAlloc
			keyType, valType micheline.Type
			err              error
		)
		bmd := make(micheline.BigmapDiff, 0)
		if err := bmd.UnmarshalBinary(op.BigmapDiff); err != nil {
			log.Errorf("%s: unmarshal %s bigmap: %v", op.Hash, op.Type, err)
		}
		o.BigmapDiff = &ExplorerBigmapUpdateList{
			diff: make([]ExplorerBigmapUpdate, 0, len(bmd)),
		}
		for _, v := range bmd {
			// need bigmap type to unbox and convert keys
			if alloc == nil || alloc.BigmapId != v.Id {
				lookupId := v.Id
				switch v.Action {
				case micheline.DiffActionAlloc:
					if v.Id < 0 {
						cache[lookupId] = model.NewBigmapAlloc(op, v)
					}
				case micheline.DiffActionCopy:
					lookupId = v.SourceId
				}
				a, ok := cache[lookupId]
				if ok {
					alloc, ok = a.(*model.BigmapAlloc)
				}
				if !ok {
					alloc, err = ctx.Indexer.LookupBigmapAlloc(ctx.Context, lookupId)
					if err != nil {
						// skip (happens only when listing internal contract calls)
						log.Debugf("%s: unmarshal bigmap %d alloc: %v", op.Hash, lookupId, err)
						continue
					}
					cache[lookupId] = alloc
				}
				if v.Action == micheline.DiffActionCopy {
					cache[v.DestId] = alloc
				}
				keyType, valType = alloc.GetKeyType(), alloc.GetValueType()
			}

			upd := ExplorerBigmapUpdate{
				Action:   v.Action,
				BigmapId: v.Id,
			}
			if args.WithMeta() {
				upd.ExplorerBigmapValue.Meta = &ExplorerBigmapMeta{
					Contract:     *o.Receiver,
					BigmapId:     v.Id,
					UpdateTime:   &op.Timestamp,
					UpdateHeight: op.Height,
					UpdateBlock:  &blockHash,
				}
			}
			switch v.Action {
			case micheline.DiffActionUpdate:
				// temporary bigmap updates may lack type info
				if !keyType.IsValid() {
					keyType = v.Key.BuildType()
				}
				if !valType.IsValid() {
					valType = v.Value.BuildType()
				}
				// regular bigmap updates
				upd.Key = v.GetKeyPtr(keyType)
				kh := v.KeyHash.Clone()
				upd.KeyHash = &kh
				upd.Value = micheline.NewValuePtr(valType, v.Value)
				if args.WithPrim() {
					upd.KeyPrim = upd.Key.PrimPtr()
					upd.ValuePrim = &upd.Value.Value
				}
				if args.WithUnpack() {
					if upd.Value.IsPackedAny() {
						if up, err := upd.Value.UnpackAll(); err == nil {
							upd.Value = &up
						}
					}
					if upd.Key.IsPacked() {
						if up, err := upd.Key.Unpack(); err == nil {
							upd.Key = &up
						}
					}
				}

			case micheline.DiffActionRemove:
				// remove may be a bigmap removal without key
				if v.Key.OpCode != micheline.I_EMPTY_BIG_MAP {
					// temporary bigmap updates lack type info
					if !keyType.IsValid() {
						keyType = v.Key.BuildType()
					}
					upd.Key = v.GetKeyPtr(keyType)
					kh := v.KeyHash.Clone()
					upd.KeyHash = &kh
					if args.WithPrim() {
						upd.KeyPrim = upd.Key.PrimPtr()
					}
					if args.WithUnpack() {
						if upd.Key.IsPacked() {
							if up, err := upd.Key.Unpack(); err == nil {
								upd.Key = &up
							}
						}
					}
				}

			case micheline.DiffActionAlloc:
				upd.KeyType = micheline.NewType(v.KeyType).TypedefPtr(micheline.CONST_KEY)
				upd.ValueType = micheline.NewType(v.ValueType).TypedefPtr(micheline.CONST_VALUE)
				if args.WithPrim() {
					kt := v.KeyType.Clone()
					upd.KeyTypePrim = &kt
					vt := v.ValueType.Clone()
					upd.ValueTypePrim = &vt
				}

			case micheline.DiffActionCopy:
				upd.BigmapId = v.DestId
				upd.KeyType = keyType.TypedefPtr(micheline.CONST_KEY)
				upd.ValueType = valType.TypedefPtr(micheline.CONST_VALUE)
				upd.SourceId = v.SourceId
				upd.DestId = v.DestId
				if v.DestId < 0 {
					cache[v.DestId] = alloc
				}
				if args.WithMeta() {
					upd.ExplorerBigmapValue.Meta.BigmapId = v.DestId
				}
				if args.WithPrim() {
					upd.KeyTypePrim = &keyType.Prim
					upd.ValueTypePrim = &valType.Prim
				}
			}
			o.BigmapDiff.diff = append(o.BigmapDiff.diff, upd)
		}
	}

	// cache until next block is expected
	o.expires = ctx.Tip.BestTime.Add(p.BlockTime())

	return o
}

func (o ExplorerOp) LastModified() time.Time {
	return o.Timestamp
}

func (o ExplorerOp) Expires() time.Time {
	return o.expires
}

func (o ExplorerOp) RESTPrefix() string {
	return "/explorer/op"
}

func (o ExplorerOp) RESTPath(r *mux.Router) string {
	path, _ := r.Get("op").URLPath("ident", o.Hash.String())
	return path.String()
}

func (o ExplorerOp) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t ExplorerOp) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadOp)).Methods("GET").Name("op")
	return nil

}

// used when listing ops in block/account/contract context
type OpsRequest struct {
	ListRequest // offset, limit, cursor, order

	Block    string        `schema:"block"`    // height or hash for time-lock
	Since    string        `schema:"since"`    // block hash or height for updates
	Unpack   bool          `schema:"unpack"`   // unpack packed key/values
	Prim     bool          `schema:"prim"`     // for prim/value rendering
	Meta     bool          `schema:"meta"`     // include account metadata
	Rights   bool          `schema:"rights"`   // include block rights
	Collapse bool          `schema:"collapse"` // collapse batch lists and internal ops
	Sender   tezos.Address `schema:"sender"`
	Receiver tezos.Address `schema:"receiver"`

	// decoded type condition
	TypeMode pack.FilterMode `schema:"-"`
	TypeList []tezos.OpType  `schema:"-"`

	// decoded values
	BlockHeight int64           `schema:"-"`
	BlockHash   tezos.BlockHash `schema:"-"`
	SinceHeight int64           `schema:"-"`
	SinceHash   tezos.BlockHash `schema:"-"`
}

func (r *OpsRequest) WithPrim() bool   { return r != nil && r.Prim }
func (r *OpsRequest) WithUnpack() bool { return r != nil && r.Unpack }
func (r *OpsRequest) WithHeight() int64 {
	if r != nil {
		return r.BlockHeight
	}
	return 0
}
func (r *OpsRequest) WithMeta() bool     { return r != nil && r.Meta }
func (r *OpsRequest) WithRights() bool   { return r != nil && r.Rights }
func (r *OpsRequest) WithCollapse() bool { return r != nil && r.Collapse }

// implement ParsableRequest interface
func (r *OpsRequest) Parse(ctx *ApiContext) {
	// lock to specific block hash or height
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
	// filter by specific block hash or height
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
	// filter by type condition
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		if keys[0] != "type" {
			continue
		}
		// parse mode
		r.TypeMode = pack.FilterModeEqual
		if len(keys) > 1 {
			r.TypeMode = pack.ParseFilterMode(keys[1])
			if !r.TypeMode.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid type filter mode '%s'", keys[1]), nil))
			}
		}
		// check op types and convert to []int64 for use in condition
		for _, t := range strings.Split(val[0], ",") {
			typ := tezos.ParseOpType(t)
			if !typ.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", t), nil))
			}
			r.TypeList = append(r.TypeList, typ)
		}
		// allow constructs of form `type=a,b`
		if len(r.TypeList) > 1 {
			if r.TypeMode == pack.FilterModeEqual {
				r.TypeMode = pack.FilterModeIn
			}
		} else {
			// check for single value mode `type.in=a`
			switch r.TypeMode {
			case pack.FilterModeIn:
				r.TypeMode = pack.FilterModeEqual
			case pack.FilterModeNotIn:
				r.TypeMode = pack.FilterModeNotEqual
			}
		}
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
			case index.ErrInvalidOpID:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid event id", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return ops
	}
}

func ReadOp(ctx *ApiContext) (interface{}, int) {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	ops := loadOps(ctx)
	resp := &ExplorerOpList{
		list:    make([]*ExplorerOp, 0),
		expires: ctx.Now.Add(maxCacheExpires),
	}
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewExplorerOp(ctx, v, nil, nil, args, cache), args.WithCollapse())
	}
	return resp, http.StatusOK
}
