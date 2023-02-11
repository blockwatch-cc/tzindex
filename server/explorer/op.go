// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Op{})
}

var (
	_ server.RESTful  = (*Op)(nil)
	_ server.Resource = (*Op)(nil)
	_ server.Resource = (*OpList)(nil)
)

type OpList []*Op

func (l *OpList) Append(op *Op, collapsed bool) {
	if !collapsed {
		(*l) = append(*l, op)
		return
	}

	// build op tree based on list and internal op positions;
	// only works for ascending order

	// find op for insert
	var (
		idx  int = -1
		last int = len(*l) - 1
	)
	if !op.IsEvent && last >= 0 {
		v := (*l)[last]
		if !v.IsEvent && v.Height == op.Height && *v.OpP == *op.OpP {
			idx = last
		}
	}

	// append if no match was found
	if idx < 0 {
		(*l) = append((*l), op)
		return
	}

	// there's 2 types of related operations (they can appear mixed)
	// 1 batch: list of operations, e.g. reveal+tx, approve+transfer, multi-transfer
	// 2 group: internal operation sequences (can appear alone or inside a batch)

	// if the found op is not a batch, but the to be appended op belongs to a batch
	// wrap the existing op as batch
	ins := (*l)[idx]

	// append to batch if op is not an internal operation
	if !op.IsInternal {
		// upgrade last op to batch if not done already
		if ins.Type != model.OpTypeBatch {
			ins = WrapAsBatchOp(ins)
			(*l)[idx] = ins
		}
		ins.GasLimit += op.GasLimit
		ins.GasUsed += op.GasUsed
		ins.StorageLimit += op.StorageLimit
		ins.StoragePaid += op.StoragePaid
		ins.Volume += op.Volume
		ins.Fee += op.Fee
		ins.NOps++
		ins.Batch = append(ins.Batch, op)
		return
	}

	// append internal ops to the last batch member (or the current group)
	if ins.Type == model.OpTypeBatch {
		ins = ins.Batch[len(ins.Batch)-1]
	}

	// init group if not done yet
	if len(ins.Internal) == 0 {
		ins.Internal = make([]*Op, 0)
	}
	ins.Internal = append(ins.Internal, op)
}

func (l OpList) LastModified() time.Time {
	if len(l) == 0 {
		return time.Time{}
	}
	a, b := l[0].Timestamp, l[len(l)-1].Timestamp
	if a.After(b) {
		return a
	}
	return b
}

func (l OpList) Expires() time.Time {
	if len(l) == 0 {
		return time.Time{}
	} else {
		a, b := l[0].expires, l[len(l)-1].expires
		if a.After(b) {
			return a
		}
		return b
	}
}

func (l OpList) RESTPrefix() string {
	return ""
}

func (l OpList) RESTPath(r *mux.Router) string {
	return ""
}

func (l OpList) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (l OpList) RegisterRoutes(r *mux.Router) error {
	return nil
}

type Op struct {
	Id            uint64                    `json:"id"`
	Hash          string                    `json:"hash,omitempty"`
	Type          model.OpType              `json:"type"`
	BlockHash     tezos.BlockHash           `json:"block"`
	Timestamp     time.Time                 `json:"time"`
	Height        int64                     `json:"height"`
	Cycle         int64                     `json:"cycle"`
	Counter       int64                     `json:"counter,omitempty"`
	OpN           int                       `json:"op_n"`
	OpP           *int                      `json:"op_p,omitempty"`
	Status        string                    `json:"status,omitempty"`
	IsSuccess     bool                      `json:"is_success"`
	IsContract    bool                      `json:"is_contract,omitempty"`
	IsEvent       bool                      `json:"is_event,omitempty"`
	IsInternal    bool                      `json:"is_internal,omitempty"`
	IsRollup      bool                      `json:"is_rollup,omitempty"`
	GasLimit      int64                     `json:"gas_limit,omitempty"`
	GasUsed       int64                     `json:"gas_used,omitempty"`
	StorageLimit  int64                     `json:"storage_limit,omitempty"`
	StoragePaid   int64                     `json:"storage_paid,omitempty"`
	Volume        float64                   `json:"volume,omitempty"`
	Fee           float64                   `json:"fee,omitempty"`
	Reward        float64                   `json:"reward,omitempty"`
	Deposit       float64                   `json:"deposit,omitempty"`
	Burned        float64                   `json:"burned,omitempty"`
	Data          json.RawMessage           `json:"data,omitempty"`
	Errors        json.RawMessage           `json:"errors,omitempty"`
	Parameters    *Parameters               `json:"parameters,omitempty"`
	Storage       *Storage                  `json:"storage,omitempty"`
	BigmapDiff    *BigmapUpdateList         `json:"big_map_diff,omitempty"`
	Value         *micheline.Prim           `json:"value,omitempty"`
	CodeHash      string                    `json:"code_hash,omitempty"`
	Sender        *tezos.Address            `json:"sender,omitempty"`
	Receiver      *tezos.Address            `json:"receiver,omitempty"`
	Creator       *tezos.Address            `json:"creator,omitempty"`
	Baker         *tezos.Address            `json:"baker,omitempty"`
	OldBaker      *tezos.Address            `json:"previous_baker,omitempty"`
	Source        *tezos.Address            `json:"source,omitempty"`
	Accuser       *tezos.Address            `json:"accuser,omitempty"`
	Offender      *tezos.Address            `json:"offender,omitempty"`
	Power         int64                     `json:"power,omitempty"`
	Level         *int64                    `json:"level,omitempty"`
	Limit         *NullMoney                `json:"limit,omitempty"`
	Solution      tezos.HexBytes            `json:"solution,omitempty"`
	Proof         tezos.HexBytes            `json:"proof,omitempty"`
	Confirmations int64                     `json:"confirmations"`
	NOps          int                       `json:"n_ops,omitempty"`
	Batch         []*Op                     `json:"batch,omitempty"`
	Internal      []*Op                     `json:"internal,omitempty"`
	Metadata      map[string]*ShortMetadata `json:"metadata,omitempty"`
	Events        []*Event                  `json:"events,omitempty"`
	TicketUpdates []*TicketUpdate           `json:"ticket_updates,omitempty"`

	expires time.Time `json:"-"`
}

func WrapAsBatchOp(op *Op) *Op {
	return &Op{
		Id:            op.Id,
		Hash:          op.Hash,
		BlockHash:     op.BlockHash,
		Type:          model.OpTypeBatch,
		Timestamp:     op.Timestamp,
		Height:        op.Height,
		Cycle:         op.Cycle,
		OpN:           op.OpN,
		OpP:           op.OpP,
		Status:        op.Status,
		IsSuccess:     op.IsSuccess,
		GasLimit:      op.GasLimit,
		GasUsed:       op.GasUsed,
		StorageLimit:  op.StorageLimit,
		StoragePaid:   op.StoragePaid,
		Volume:        op.Volume,
		Fee:           op.Fee,
		Batch:         []*Op{op},
		NOps:          1,
		Confirmations: op.Confirmations,
	}
}

func NewOp(ctx *server.Context, op *model.Op, block *model.Block, cc *model.Contract, args server.Options, cache map[int64]interface{}) *Op {
	p := ctx.Params
	o := &Op{
		Id:            op.Id(),
		Type:          op.Type,
		Timestamp:     op.Timestamp,
		Height:        op.Height,
		Cycle:         op.Cycle,
		Counter:       op.Counter,
		OpN:           op.OpN,
		OpP:           IntPtr(op.OpP),
		Status:        op.Status.String(),
		IsSuccess:     op.IsSuccess,
		IsContract:    op.IsContract,
		IsInternal:    op.IsInternal,
		IsEvent:       op.IsEvent,
		IsRollup:      op.IsRollup,
		GasLimit:      op.GasLimit,
		GasUsed:       op.GasUsed,
		StorageLimit:  op.StorageLimit,
		StoragePaid:   op.StoragePaid,
		Volume:        p.ConvertValue(op.Volume),
		Fee:           p.ConvertValue(op.Fee),
		Reward:        p.ConvertValue(op.Reward),
		Deposit:       p.ConvertValue(op.Deposit),
		Burned:        p.ConvertValue(op.Burned),
		Confirmations: util.Max64(0, ctx.Tip.BestHeight-op.Height),
	}

	if op.Hash.IsValid() && !op.Hash.IsZero() {
		o.Hash = op.Hash.String()
	}

	// events have no within-list position
	if op.IsEvent {
		o.OpP = nil
	}

	// lookup accounts
	switch op.Type {
	case model.OpTypeBake, model.OpTypeBonus:
		if op.SenderId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.SenderId)
			o.Sender = &a
		}

	case model.OpTypeUnfreeze, model.OpTypeInvoice, model.OpTypeAirdrop,
		model.OpTypeSeedSlash, model.OpTypeMigration, model.OpTypeSubsidy,
		model.OpTypeDeposit, model.OpTypeReward:
		if op.ReceiverId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.ReceiverId)
			o.Receiver = &a
		}

	case model.OpTypeEndorsement, model.OpTypePreendorsement:
		if op.SenderId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.SenderId)
			o.Sender = &a
		}
		o.Timestamp = ctx.Indexer.LookupBlockTime(ctx, op.Height)
		o.Cycle = ctx.Params.CycleFromHeight(op.Height)

	case model.OpTypeDoubleBaking, model.OpTypeDoubleEndorsement, model.OpTypeDoublePreendorsement:
		if op.SenderId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.SenderId)
			o.Accuser = &a
		}
		if op.ReceiverId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.ReceiverId)
			o.Offender = &a
		}

	case model.OpTypeDelegation:
		if op.SenderId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.SenderId)
			o.Sender = &a
		}
		if op.ReceiverId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.ReceiverId)
			o.OldBaker = &a
		}
		if op.BakerId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.BakerId)
			o.Baker = &a
		}
		if op.CreatorId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.CreatorId)
			o.Creator = &a
		}

	default:
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
		if op.BakerId > 0 {
			a := ctx.Indexer.LookupAddress(ctx, op.BakerId)
			o.Baker = &a
		}
	}
	if op.IsInternal {
		// flip source for internal transactions/delegations/originations
		// source: always tzX account
		// sender: KT1
		o.Source, o.Sender = o.Sender, o.Creator
		o.Creator = nil
	}

	// add metadata, if any account is supported
	if args.WithMeta() {
		meta := make(map[string]*ShortMetadata)
		for _, v := range []model.AccountID{
			op.SenderId,
			op.ReceiverId,
			op.CreatorId,
			op.BakerId,
		} {
			if v == 0 {
				continue
			}
			if md, ok := lookupMetadataById(ctx, v, 0, false); ok {
				// a := ctx.Indexer.LookupAddress(ctx, v)
				meta[md.Address.String()] = md.Short()
			}
		}
		if len(meta) > 0 {
			o.Metadata = meta
		}
	}

	// unpack data
	switch op.Type {
	case model.OpTypeDoubleBaking, model.OpTypeDoubleEndorsement, model.OpTypeDoublePreendorsement:
		o.Data = json.RawMessage{}
		if err := json.Unmarshal([]byte(op.Data), &o.Data); err != nil {
			o.Data = nil
			log.Errorf("explorer op: unmarshal %s data: %v", op.Type, err)
		}
	case model.OpTypeRegisterConstant:
		expr, _ := tezos.ParseExprHash(op.Data)
		if con, err := ctx.Indexer.LookupConstant(ctx, expr); err != nil {
			log.Errorf("explorer op: loading constant %s value: %v", expr, err)
		} else {
			o.Value = &micheline.Prim{}
			if err := o.Value.UnmarshalBinary(con.Value); err != nil {
				o.Value = nil
				log.Errorf("explorer op: unmarshal constant %s value: %v", expr, err)
			}
		}
	case model.OpTypeEndorsement, model.OpTypePreendorsement:
		o.Power, _ = strconv.ParseInt(op.Data, 10, 64)
		o.Data = nil
	case model.OpTypeDepositsLimit:
		var limit int64 = -1
		if op.Data != "" {
			limit, _ = strconv.ParseInt(op.Data, 10, 64)
		}
		nm := NullMoney(limit)
		o.Limit = &nm
		o.Data = nil
	case model.OpTypeVdfRevelation:
		if l := len(op.Parameters); l > 0 {
			o.Solution = tezos.HexBytes(op.Parameters[:l/2])
			o.Proof = tezos.HexBytes(op.Parameters[l/2:])
		}
	default:
		if op.Data != "" && !(op.IsContract || op.IsRollup) {
			o.Data = json.RawMessage(strconv.Quote(op.Data))
		}
	}

	if len(op.Errors) > 0 {
		o.Errors = json.RawMessage(op.Errors)
	}

	var blockHash tezos.BlockHash
	if block != nil {
		blockHash = block.Hash
	} else {
		blockHash = ctx.Indexer.LookupBlockHash(ctx.Context, op.Height)
	}
	o.BlockHash = blockHash

	// special treatment for rollup ops (params behave differently)
	if op.IsRollup && op.Type != model.OpTypeRollupOrigination {
		o.Parameters = &Parameters{
			Method: op.Data,
		}

		switch op.Data {
		case "deposit":
			// fake deposit entrypoint call with ticket
			o.Parameters.Method = ""
			o.Parameters.Entrypoint = op.Data
			var call micheline.Parameters
			_ = call.UnmarshalBinary(op.Parameters)
			if call.Value.IsValid() {
				o.Parameters.Value = micheline.NewValuePtr(
					micheline.TicketType(call.Value.Args[1]),
					call.Value.Args[0].Args[0],
				)
				o.Parameters.L2Address = &tezos.Address{
					Type: tezos.AddressTypeBls12_381,
					Hash: call.Value.Args[0].Args[1].Bytes,
				}
			}
		case "transfer_ticket":
			if ticket, err := rpc.DecodeTransferTicket(op.Parameters); err == nil {
				o.Parameters.Arguments, _ = ticket.MarshalJSON()
			}
		case "tx_rollup_submit_batch":
			if batch, err := rpc.DecodeRollupBatch(op.Parameters); err == nil {
				o.Parameters.Arguments, _ = batch.MarshalJSON()
			}
		case "tx_rollup_commit":
			if commit, err := rpc.DecodeRollupCommit(op.Parameters); err == nil {
				o.Parameters.Arguments, _ = commit.MarshalJSON()
			}
		case "tx_rollup_return_bond":
			// empty, no args
		case "tx_rollup_finalize_commitment", "tx_rollup_remove_commitment":
			var p micheline.Prim
			if err := p.UnmarshalBinary(op.Parameters); err == nil {
				lvl := p.Int.Int64()
				o.Level = &lvl
			}
		case "tx_rollup_rejection":
			if reject, err := rpc.DecodeRollupRejection(op.Parameters); err == nil {
				o.Parameters.Arguments, _ = reject.MarshalJSON()
			}
			if o.Creator != nil {
				o.Offender, o.Creator = o.Creator, nil
			}
		case "tx_rollup_dispatch_tickets":
			if dispatch, err := rpc.DecodeRollupDispatch(op.Parameters); err == nil {
				o.Parameters.Arguments, _ = dispatch.MarshalJSON()
			}
		case "sc_rollup_originate", "sc_rollup_add_messages", "sc_rollup_cement", "sc_rollup_publish":
			// unsupported right niw
		}

		o.Data = nil
	}

	if o.IsContract {
		pTyp, sTyp, codeHash, err := ctx.Indexer.LookupContractType(ctx.Context, op.ReceiverId)
		if err != nil {
			log.Errorf("explorer: op %d: loading contract type for %s (%d): %v", o.Id, o.Receiver, op.ReceiverId, err)
		}
		o.CodeHash = util.U64String(codeHash).Hex()

		// set params
		if len(op.Parameters) > 0 && pTyp.IsValid() {
			o.Parameters = NewParameters(ctx, op.Parameters, pTyp, op.Hash, args)
		}

		// handle storage
		if len(op.Storage) > 0 && sTyp.IsValid() {
			data := op.Storage
			if cc != nil {
				// storage type is patched post-Babylon, but pre-Babylon ops are unpatched,
				// we always output post-babylon storage
				if etl.NeedsBabylonUpgradeContract(cc, ctx.Params) && ctx.Params.IsPreBabylonHeight(op.Height) {
					if acc, err := ctx.Indexer.LookupAccountId(ctx, cc.CreatorId); err == nil {
						prim := micheline.Prim{}
						if err := prim.UnmarshalBinary(op.Storage); err == nil {
							prim = prim.MigrateToBabylonStorage(acc.Address.Bytes())
						}
						data, _ = prim.MarshalBinary()
					}
				}
			}
			o.Storage = NewStorage(ctx, data, sTyp, op.Timestamp, args)
		}

		// handle bigmap diffs
		if len(op.BigmapUpdates) > 0 {
			var (
				alloc            *model.BigmapAlloc
				keyType, valType micheline.Type
				err              error
			)
			o.BigmapDiff = &BigmapUpdateList{
				diff: make([]BigmapUpdate, 0),
			}

			for _, v := range op.BigmapUpdates {
				// need bigmap type to unbox and convert keys
				if alloc == nil || alloc.BigmapId != v.BigmapId {
					lookupId := v.BigmapId
					// cache temporary bigmap types
					switch v.Action {
					case micheline.DiffActionAlloc:
						if v.BigmapId < 0 {
							cache[lookupId] = v.ToAlloc()
						}
					case micheline.DiffActionCopy:
						if sourceId := int64(v.KeyId); sourceId < 0 {
							cache[sourceId] = v.ToAlloc()
						} else {
							lookupId = sourceId
						}
					}
					a, ok := cache[lookupId]
					if ok {
						alloc, ok = a.(*model.BigmapAlloc)
					}
					if !ok {
						alloc, err = ctx.Indexer.LookupBigmapType(ctx.Context, lookupId)
						if err != nil {
							// skip (happens only when listing internal contract calls)
							log.Debugf("%s: unmarshal bigmap %d alloc: %v", op.Hash, lookupId, err)
							continue
						}
						cache[lookupId] = alloc
					}
					if v.Action == micheline.DiffActionCopy {
						cache[v.BigmapId] = alloc
					}
					keyType, valType = alloc.GetKeyType(), alloc.GetValueType()
				}

				upd := BigmapUpdate{
					Action:   v.Action,
					BigmapId: v.BigmapId,
				}
				if args.WithMeta() {
					upd.BigmapValue.Meta = &BigmapMeta{
						Contract:     *o.Receiver,
						BigmapId:     v.BigmapId,
						UpdateTime:   &op.Timestamp,
						UpdateHeight: op.Height,
					}
				}
				switch v.Action {
				case micheline.DiffActionUpdate:
					// temporary bigmap updates may lack type info
					// FIXME: IS THIS NECESSARY?
					if !keyType.IsValid() {
						var prim micheline.Prim
						_ = prim.UnmarshalBinary(v.Key)
						keyType = prim.BuildType()
					}
					if !valType.IsValid() {
						var prim micheline.Prim
						_ = prim.UnmarshalBinary(v.Value)
						valType = prim.BuildType()
					}
					// regular bigmap updates
					k, _ := v.GetKey(keyType)
					upd.Key = &k
					kh := v.GetKeyHash()
					upd.KeyHash = &kh
					val := v.GetValue(valType)
					upd.Value = &val
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
					var keyprim micheline.Prim
					_ = keyprim.UnmarshalBinary(v.Key)
					if keyprim.IsValid() {
						// temporary bigmap updates lack type info
						if !keyType.IsValid() {
							keyType = keyprim.BuildType()
						}
						k, _ := v.GetKey(keyType)
						upd.Key = &k
						kh := v.GetKeyHash()
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
					kt, vt := v.GetKeyType(), v.GetValueType()
					upd.KeyType = kt.TypedefPtr(micheline.CONST_KEY)
					upd.ValueType = vt.TypedefPtr(micheline.CONST_VALUE)
					if args.WithPrim() {
						upd.KeyTypePrim = &kt.Prim
						upd.ValueTypePrim = &vt.Prim
					}

				case micheline.DiffActionCopy:
					upd.BigmapId = v.BigmapId
					upd.KeyType = keyType.TypedefPtr(micheline.CONST_KEY)
					upd.ValueType = valType.TypedefPtr(micheline.CONST_VALUE)
					upd.SourceId = int64(v.KeyId)
					upd.DestId = v.BigmapId
					if v.BigmapId < 0 {
						cache[v.BigmapId] = alloc
					}
					if args.WithMeta() {
						upd.BigmapValue.Meta.BigmapId = v.BigmapId
					}
					if args.WithPrim() {
						upd.KeyTypePrim = &keyType.Prim
						upd.ValueTypePrim = &valType.Prim
					}
				}
				o.BigmapDiff.diff = append(o.BigmapDiff.diff, upd)
			}
		}

		// handle events (must use external op id as filter)
		for _, ev := range op.Events {
			o.Events = append(o.Events, NewEvent(ctx, ev, args))
		}

	}

	// handle ticket updates
	for _, up := range op.TicketUpdates {
		o.TicketUpdates = append(o.TicketUpdates, NewTicketUpdate(ctx, up, args))
	}

	// cache until next block is expected
	o.expires = ctx.Tip.BestTime.Add(p.BlockTime())

	return o
}

func (o Op) LastModified() time.Time {
	return o.Timestamp
}

func (o Op) Expires() time.Time {
	return o.expires
}

func (o Op) RESTPrefix() string {
	return "/explorer/op"
}

func (o Op) RESTPath(r *mux.Router) string {
	path, _ := r.Get("op").URLPath("ident", o.Hash)
	return path.String()
}

func (o Op) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t Op) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadOp)).Methods("GET").Name("op")
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
	Merge    bool          `schema:"merge"`    // merge batch lists and internal ops
	Storage  bool          `schema:"storage"`  // embed storage update
	Address  tezos.Address `schema:"address"`  // filter by any address
	Sender   tezos.Address `schema:"sender"`   // filter by sender
	Receiver tezos.Address `schema:"receiver"` // filter by receiver

	// decoded type condition
	TypeMode pack.FilterMode  `schema:"-"`
	TypeList model.OpTypeList `schema:"-"`

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
func (r *OpsRequest) WithMeta() bool    { return r != nil && r.Meta }
func (r *OpsRequest) WithRights() bool  { return r != nil && r.Rights }
func (r *OpsRequest) WithMerge() bool   { return r != nil && r.Merge }
func (r *OpsRequest) WithStorage() bool { return r != nil && r.Storage }

// implement ParsableRequest interface
func (r *OpsRequest) Parse(ctx *server.Context) {
	// lock to specific block hash or height
	if len(r.Block) > 0 {
		b, err := ctx.Indexer.LookupBlock(ctx.Context, r.Block)
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
		r.BlockHeight = b.Height
		r.BlockHash = b.Hash.Clone()
	}
	// filter by specific block hash or height
	if len(r.Since) > 0 {
		b, err := ctx.Indexer.LookupBlock(ctx.Context, r.Since)
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
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid type filter mode '%s'", keys[1]), nil))
			}
		}
		// check op types and convert to []int64 for use in condition
		for _, t := range strings.Split(val[0], ",") {
			typ := model.ParseOpType(t)
			if !typ.IsValid() {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid operation type '%s'", t), nil))
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

func loadOps(ctx *server.Context, args server.Options) []*model.Op {
	if opIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || opIdent == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing operation hash", nil))
	} else {
		r := etl.ListRequest{
			WithStorage: args.WithStorage(),
		}
		var err2 error
		ops, err := ctx.Indexer.LookupOp(ctx, opIdent, r)
		if err == index.ErrNoOpEntry {
			// also try loading an endorsement
			ops, err2 = ctx.Indexer.LookupEndorsement(ctx, opIdent)
		}
		if err != nil && err2 != nil {
			switch err {
			case index.ErrNoOpEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such operation", err))
			case etl.ErrInvalidHash:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid operation hash", err))
			case index.ErrInvalidOpID:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid event id", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return ops
	}
}

func ReadOp(ctx *server.Context) (interface{}, int) {
	args := &OpsRequest{
		Storage: true,
	}
	ctx.ParseRequestArgs(args)
	ops := loadOps(ctx, args)
	resp := make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewOp(ctx, v, nil, nil, args, cache), args.WithMerge())
	}
	return resp, http.StatusOK
}
