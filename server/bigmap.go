// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerBigmap{})
}

var _ RESTful = (*ExplorerBigmap)(nil)

type ExplorerBigmap struct {
	Contract      tezos.Address     `json:"contract"`
	BigmapId      int64             `json:"bigmap_id"`
	NUpdates      int64             `json:"n_updates"`
	NKeys         int64             `json:"n_keys"`
	AllocHeight   int64             `json:"alloc_height"`
	AllocBlock    tezos.BlockHash   `json:"alloc_block"`
	AllocTime     time.Time         `json:"alloc_time"`
	UpdatedHeight int64             `json:"update_height"`
	UpdatedBlock  tezos.BlockHash   `json:"update_block"`
	UpdatedTime   time.Time         `json:"update_time"`
	KeyType       micheline.Typedef `json:"key_type"`
	ValueType     micheline.Typedef `json:"value_type"`
	KeyTypePrim   *micheline.Prim   `json:"key_type_prim,omitempty"`
	ValueTypePrim *micheline.Prim   `json:"value_type_prim,omitempty"`

	expires time.Time `json:"-"`
}

func NewExplorerBigmap(ctx *ApiContext, alloc *model.BigmapAlloc, args Options) *ExplorerBigmap {
	kt, vt := alloc.GetKeyType(), alloc.GetValueType()
	m := &ExplorerBigmap{
		Contract:      ctx.Indexer.LookupAddress(ctx, alloc.AccountId),
		BigmapId:      alloc.BigmapId,
		NUpdates:      alloc.NUpdates,
		NKeys:         alloc.NKeys,
		AllocHeight:   alloc.Height,
		AllocTime:     ctx.Indexer.LookupBlockTime(ctx, alloc.Height),
		AllocBlock:    ctx.Indexer.LookupBlockHash(ctx.Context, alloc.Height),
		UpdatedHeight: alloc.Updated,
		UpdatedTime:   ctx.Indexer.LookupBlockTime(ctx, alloc.Updated),
		UpdatedBlock:  ctx.Indexer.LookupBlockHash(ctx.Context, alloc.Updated),
		KeyType:       kt.Typedef(micheline.CONST_KEY),
		ValueType:     vt.Typedef(micheline.CONST_VALUE),
		expires:       ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
	}
	if args.WithPrim() {
		m.KeyTypePrim = &kt.Prim
		m.ValueTypePrim = &vt.Prim
	}
	return m
}

func (b ExplorerBigmap) LastModified() time.Time { return b.UpdatedTime }
func (b ExplorerBigmap) Expires() time.Time      { return b.expires }
func (b ExplorerBigmap) RESTPrefix() string      { return "/explorer/bigmap" }

func (b ExplorerBigmap) RESTPath(r *mux.Router) string {
	path, _ := r.Get("bigmap").URLPath("id", strconv.FormatInt(b.BigmapId, 10))
	return path.String()
}

func (b ExplorerBigmap) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b ExplorerBigmap) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{id}", C(ReadBigmap)).Methods("GET").Name("bigmap")
	r.HandleFunc("/{id}/keys", C(ListBigmapKeys)).Methods("GET")
	r.HandleFunc("/{id}/values", C(ListBigmapValues)).Methods("GET")
	r.HandleFunc("/{id}/updates", C(ListBigmapUpdates)).Methods("GET")
	r.HandleFunc("/{id}/{key}/updates", C(ListBigmapKeyUpdates)).Methods("GET")
	r.HandleFunc("/{id}/{key}", C(ReadBigmapValue)).Methods("GET")
	return nil
}

// bigmap metadata
type ExplorerBigmapMeta struct {
	Contract     tezos.Address    `json:"contract"`
	BigmapId     int64            `json:"bigmap_id"`
	UpdateTime   *time.Time       `json:"time,omitempty"`
	UpdateHeight int64            `json:"height,omitempty"`
	UpdateBlock  *tezos.BlockHash `json:"block,omitempty"`
	UpdateOp     *tezos.OpHash    `json:"op,omitempty"`
	Sender       *tezos.Address   `json:"sender,omitempty"`
	Source       *tezos.Address   `json:"source,omitempty"`
}

// keys
type ExplorerBigmapKey struct {
	Key     micheline.Key       `json:"key"`
	KeyHash tezos.ExprHash      `json:"key_hash"`
	Meta    *ExplorerBigmapMeta `json:"meta,omitempty"`
	Prim    *micheline.Prim     `json:"prim,omitempty"`
}

type ExplorerBigmapKeyList struct {
	list     []ExplorerBigmapKey
	modified time.Time
	expires  time.Time
}

func (l ExplorerBigmapKeyList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l ExplorerBigmapKeyList) LastModified() time.Time      { return l.modified }
func (l ExplorerBigmapKeyList) Expires() time.Time           { return l.expires }

var _ Resource = (*ExplorerBigmapKeyList)(nil)

// values
type ExplorerBigmapValue struct {
	Key       *micheline.Key      `json:"key,omitempty"`
	KeyHash   *tezos.ExprHash     `json:"key_hash,omitempty"`
	Value     *micheline.Value    `json:"value,omitempty"` // omit on removal updates
	Meta      *ExplorerBigmapMeta `json:"meta,omitempty"`
	KeyPrim   *micheline.Prim     `json:"key_prim,omitempty"`
	ValuePrim *micheline.Prim     `json:"value_prim,omitempty"`
	modified  time.Time           `json:"-"`
	expires   time.Time           `json:"-"`
}

func (t ExplorerBigmapValue) LastModified() time.Time { return t.modified }
func (t ExplorerBigmapValue) Expires() time.Time      { return t.expires }

var _ Resource = (*ExplorerBigmapValue)(nil)

type ExplorerBigmapValueList struct {
	list     []ExplorerBigmapValue
	modified time.Time
	expires  time.Time
}

func (l ExplorerBigmapValueList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l ExplorerBigmapValueList) LastModified() time.Time      { return l.modified }
func (l ExplorerBigmapValueList) Expires() time.Time           { return l.expires }

var _ Resource = (*ExplorerBigmapKeyList)(nil)

// storage metadata
type ExplorerStorageMeta struct {
	Contract tezos.Address   `json:"contract"`
	Time     time.Time       `json:"time"`
	Height   int64           `json:"height"`
	Block    tezos.BlockHash `json:"block"`
}

// storage
type ExplorerStorageValue struct {
	Meta     *ExplorerStorageMeta `json:"meta,omitempty"`
	Value    micheline.Value      `json:"value"`
	Prim     *micheline.Prim      `json:"prim,omitempty"`
	Bigmaps  map[string]int64     `json:"bigmaps,omitempty"` // for ContractStorage
	modified time.Time            `json:"-"`
	expires  time.Time            `json:"-"`
}

func (t ExplorerStorageValue) LastModified() time.Time { return t.modified }
func (t ExplorerStorageValue) Expires() time.Time      { return t.expires }

var _ Resource = (*ExplorerBigmapValue)(nil)

// params
type ExplorerParameters struct {
	Entrypoint string          `json:"entrypoint"`
	Call       string          `json:"call"`
	Branch     string          `json:"branch"`
	Id         int             `json:"id"`
	Value      micheline.Value `json:"value"`
	Prim       *micheline.Prim `json:"prim,omitempty"`
}

// updates
type ExplorerBigmapUpdate struct {
	ExplorerBigmapValue                      // full value
	Action              micheline.DiffAction `json:"action"`
	BigmapId            int64                `json:"bigmap_id"`

	// alloc/copy
	KeyType       *micheline.Typedef `json:"key_type,omitempty"`
	ValueType     *micheline.Typedef `json:"value_type,omitempty"`
	KeyTypePrim   *micheline.Prim    `json:"key_type_prim,omitempty"`
	ValueTypePrim *micheline.Prim    `json:"value_type_prim,omitempty"`
	SourceId      int64              `json:"source_big_map,omitempty"`
	DestId        int64              `json:"destination_big_map,omitempty"`
}

type ExplorerBigmapUpdateList struct {
	diff     []ExplorerBigmapUpdate
	modified time.Time
	expires  time.Time
}

func (l ExplorerBigmapUpdateList) MarshalJSON() ([]byte, error) { return json.Marshal(l.diff) }
func (l ExplorerBigmapUpdateList) LastModified() time.Time      { return l.modified }
func (l ExplorerBigmapUpdateList) Expires() time.Time           { return l.expires }

var _ Resource = (*ExplorerBigmapUpdateList)(nil)

func loadBigmap(ctx *ApiContext) *model.BigmapAlloc {
	if id, ok := mux.Vars(ctx.Request)["id"]; !ok || id == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing bigmap id", nil))
	} else {
		i, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid bigmap id", err))
		}
		a, err := ctx.Indexer.LookupBigmapAlloc(ctx, i)
		if err != nil {
			switch err {
			case index.ErrNoBigmapAlloc:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such bigmap", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return a
	}
}

func parseBigmapKey(ctx *ApiContext, typ micheline.OpCode) (micheline.Key, tezos.ExprHash) {
	if k, ok := mux.Vars(ctx.Request)["key"]; !ok || k == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing bigmap key", nil))
	} else {
		expr, err := tezos.ParseExprHash(k)
		if err == nil {
			return micheline.Key{}, expr
		}
		key, err := micheline.ParseKey(typ, k)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid bigmap key", err))
		}
		return key, key.Hash()
	}
}

func ReadBigmap(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc := loadBigmap(ctx)
	return NewExplorerBigmap(ctx, alloc, args), http.StatusOK
}

func ListBigmapKeys(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc := loadBigmap(ctx)

	r := etl.ListRequest{
		BigmapId: alloc.BigmapId,
		Since:    args.BlockHeight,
		Cursor:   args.Cursor,
		Offset:   args.Offset,
		Limit:    ctx.Cfg.ClampExplore(args.Limit),
		Order:    args.Order,
	}

	var (
		items []*model.BigmapKV
		err   error
	)
	if r.Since == 0 {
		items, err = ctx.Indexer.ListBigmapKeys(ctx.Context, r)
	} else {
		items, err = ctx.Indexer.ListHistoricBigmapKeys(ctx.Context, r)
	}
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	resp := &ExplorerBigmapKeyList{
		list:     make([]ExplorerBigmapKey, 0, len(items)),
		expires:  ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
		modified: ctx.Indexer.LookupBlockTime(ctx, alloc.Updated),
	}

	keyType := alloc.GetKeyType()
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		k, err := v.GetKey(keyType)
		if err != nil {
			log.Errorf("explorer: decode bigmap key: %v", err)
			continue
		}
		key := ExplorerBigmapKey{
			Key:     k,
			KeyHash: v.GetKeyHash(),
		}
		if args.WithMeta() {
			key.Meta = &ExplorerBigmapMeta{
				Contract: contract,
				BigmapId: alloc.BigmapId,
			}
		}
		if args.WithPrim() {
			key.Prim = k.PrimPtr()
		}
		if args.WithUnpack() && k.IsPacked() {
			if up, err := k.Unpack(); err == nil {
				key.Key = up
			}
		}

		resp.list = append(resp.list, key)
	}

	return resp, http.StatusOK
}

func ListBigmapValues(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc := loadBigmap(ctx)

	r := etl.ListRequest{
		BigmapId: alloc.BigmapId,
		Since:    args.BlockHeight,
		Cursor:   args.Cursor,
		Offset:   args.Offset,
		Limit:    ctx.Cfg.ClampExplore(args.Limit),
		Order:    args.Order,
	}

	var (
		items []*model.BigmapKV
		err   error
	)
	if r.Since == 0 {
		items, err = ctx.Indexer.ListBigmapKeys(ctx.Context, r)
	} else {
		items, err = ctx.Indexer.ListHistoricBigmapKeys(ctx.Context, r)
	}
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	resp := &ExplorerBigmapValueList{
		list:     make([]ExplorerBigmapValue, 0, len(items)),
		expires:  ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
		modified: ctx.Indexer.LookupBlockTime(ctx, alloc.Updated),
	}

	keyType, valueType := alloc.GetKeyType(), alloc.GetValueType()
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		key, err := v.GetKey(keyType)
		if err != nil {
			log.Errorf("explorer: decode bigmap key: %v", err)
			continue
		}
		keyHash := v.GetKeyHash()
		typedValue := v.GetValue(valueType)
		val := ExplorerBigmapValue{
			Key:     &key,
			KeyHash: &keyHash,
			Value:   &typedValue,
		}
		if args.WithMeta() {
			val.Meta = &ExplorerBigmapMeta{
				Contract: contract,
				BigmapId: alloc.BigmapId,
			}
		}
		if args.WithPrim() {
			val.KeyPrim = key.PrimPtr()
			val.ValuePrim = &typedValue.Value
		}
		if args.WithUnpack() {
			if val.Value.IsPackedAny() {
				if up, err := val.Value.UnpackAll(); err == nil {
					val.Value = &up
				}
			}
			if val.Key.IsPacked() {
				if up, err := val.Key.Unpack(); err == nil {
					val.Key = &up
				}
			}
		}
		resp.list = append(resp.list, val)
	}

	return resp, http.StatusOK
}

func ReadBigmapValue(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	// support key and key_hash
	alloc := loadBigmap(ctx)
	keyType, valType := alloc.GetKeyType(), alloc.GetValueType()
	_, expr := parseBigmapKey(ctx, keyType.OpCode)

	r := etl.ListRequest{
		BigmapId:  alloc.BigmapId,
		BigmapKey: expr,
		Since:     args.BlockHeight,
	}

	var (
		items []*model.BigmapKV
		err   error
	)
	if r.Since == 0 {
		items, err = ctx.Indexer.ListBigmapKeys(ctx.Context, r)
	} else {
		items, err = ctx.Indexer.ListHistoricBigmapKeys(ctx.Context, r)
	}
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}
	if len(items) == 0 {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such bigmap key", err))
	}
	v := items[0]
	key, err := v.GetKey(keyType)
	if err != nil {
		panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
	}

	keyHash := v.GetKeyHash()
	typedValue := v.GetValue(valType)

	resp := &ExplorerBigmapValue{
		Key:      &key,
		KeyHash:  &keyHash,
		Value:    &typedValue,
		modified: ctx.Indexer.LookupBlockTime(ctx, v.Height),
		expires:  ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
	}
	if args.WithMeta() {
		resp.Meta = &ExplorerBigmapMeta{
			Contract: ctx.Indexer.LookupAddress(ctx, alloc.AccountId),
			BigmapId: alloc.BigmapId,
		}
	}
	if args.WithPrim() {
		resp.KeyPrim = key.PrimPtr()
		resp.ValuePrim = &typedValue.Value
	}
	if args.WithUnpack() {
		if resp.Value.IsPackedAny() {
			if up, err := resp.Value.UnpackAll(); err == nil {
				resp.Value = &up
			}
		}
		if resp.Key.IsPacked() {
			if up, err := resp.Key.Unpack(); err == nil {
				resp.Key = &up
			}
		}
	}

	return resp, http.StatusOK
}

func ListBigmapUpdates(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc := loadBigmap(ctx)
	r := etl.ListRequest{
		BigmapId: alloc.BigmapId,
		Since:    args.SinceHeight + 1,
		Until:    args.BlockHeight,
		Cursor:   args.Cursor,
		Offset:   args.Offset,
		Limit:    ctx.Cfg.ClampExplore(args.Limit),
		Order:    args.Order,
	}

	items, err := ctx.Indexer.ListBigmapUpdates(ctx.Context, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	var opCache map[model.OpID]*model.Op
	if args.WithMeta() && len(items) > 0 {
		opIds := make([]uint64, 0)
		for _, v := range items {
			opIds = append(opIds, v.OpId.Value())
		}
		ops, err := ctx.Indexer.LookupOpIds(ctx, opIds)
		if err != nil {
			log.Errorf("%s: missing ops in %#v", ctx.RequestString(), opIds)
		} else {
			opCache = make(map[model.OpID]*model.Op)
			for _, v := range ops {
				opCache[v.RowId] = v
			}
		}
	}

	resp := &ExplorerBigmapUpdateList{
		diff:    make([]ExplorerBigmapUpdate, 0, len(items)),
		expires: ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
	}

	keyType, valType := alloc.GetKeyType(), alloc.GetValueType()
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		upd := ExplorerBigmapUpdate{
			Action:   v.Action,
			BigmapId: v.BigmapId,
		}
		switch v.Action {
		case micheline.DiffActionAlloc:
			kt, vt := v.GetKeyType(), v.GetValueType()
			upd.KeyType = kt.TypedefPtr(micheline.CONST_KEY)
			upd.ValueType = vt.TypedefPtr(micheline.CONST_VALUE)
			if args.WithPrim() {
				upd.KeyTypePrim = &kt.Prim
				upd.ValueTypePrim = &vt.Prim
			}

		case micheline.DiffActionCopy:
			upd.SourceId = int64(v.KeyId)
			upd.DestId = v.BigmapId
			kt, vt := v.GetKeyType(), v.GetValueType()
			upd.KeyType = kt.TypedefPtr(micheline.CONST_KEY)
			upd.ValueType = vt.TypedefPtr(micheline.CONST_VALUE)
			if args.WithPrim() {
				upd.KeyTypePrim = &kt.Prim
				upd.ValueTypePrim = &vt.Prim
			}

		case micheline.DiffActionUpdate:
			key, _ := v.GetKey(keyType)
			keyHash := v.GetKeyHash()
			upd.Key = &key
			upd.KeyHash = &keyHash
			typedValue := v.GetValue(valType)
			upd.Value = &typedValue
			if args.WithPrim() {
				upd.KeyPrim = key.PrimPtr()
				upd.ValuePrim = &typedValue.Value
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
		}
		if args.WithMeta() {
			upd.ExplorerBigmapValue.Meta = &ExplorerBigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   &v.Timestamp,
				UpdateHeight: v.Height,
			}
			bh := ctx.Indexer.LookupBlockHash(ctx.Context, v.Height)
			upd.ExplorerBigmapValue.Meta.UpdateBlock = &bh
			if op, ok := opCache[v.OpId]; ok {
				upd.ExplorerBigmapValue.Meta.UpdateOp = &op.Hash
				snd := ctx.Indexer.LookupAddress(ctx, op.SenderId)
				upd.ExplorerBigmapValue.Meta.Sender = &snd
				if op.CreatorId != 0 {
					src := ctx.Indexer.LookupAddress(ctx, op.CreatorId)
					upd.ExplorerBigmapValue.Meta.Source = &src
				} else {
					upd.ExplorerBigmapValue.Meta.Source = upd.ExplorerBigmapValue.Meta.Sender
				}
			}
		}
		resp.diff = append(resp.diff, upd)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ListBigmapKeyUpdates(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	alloc := loadBigmap(ctx)
	keyType, valType := alloc.GetKeyType(), alloc.GetValueType()
	_, expr := parseBigmapKey(ctx, keyType.OpCode)

	r := etl.ListRequest{
		BigmapId:  alloc.BigmapId,
		BigmapKey: expr,
		Since:     args.SinceHeight + 1,
		Until:     args.BlockHeight,
		Cursor:    args.Cursor,
		Offset:    args.Offset,
		Limit:     ctx.Cfg.ClampExplore(args.Limit),
		Order:     args.Order,
	}

	items, err := ctx.Indexer.ListBigmapUpdates(ctx.Context, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	// load ops when metadata is requested
	var opCache map[model.OpID]*model.Op
	if args.WithMeta() && len(items) > 0 {
		opIds := make([]uint64, 0)
		for _, v := range items {
			opIds = append(opIds, v.OpId.Value())
		}
		ops, err := ctx.Indexer.LookupOpIds(ctx, opIds)
		if err != nil {
			log.Errorf("%s: missing ops in %#v", ctx.RequestString(), opIds)
		} else {
			opCache = make(map[model.OpID]*model.Op)
			for _, v := range ops {
				opCache[v.RowId] = v
			}
		}
	}

	resp := &ExplorerBigmapUpdateList{
		diff:    make([]ExplorerBigmapUpdate, 0, len(items)),
		expires: ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
	}
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		upd := ExplorerBigmapUpdate{
			Action:   v.Action,
			BigmapId: v.BigmapId,
		}
		if args.WithMeta() {
			upd.ExplorerBigmapValue.Meta = &ExplorerBigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   &v.Timestamp,
				UpdateHeight: v.Height,
			}
			bh := ctx.Indexer.LookupBlockHash(ctx.Context, v.Height)
			upd.ExplorerBigmapValue.Meta.UpdateBlock = &bh
			if op, ok := opCache[v.OpId]; ok {
				upd.ExplorerBigmapValue.Meta.UpdateOp = &op.Hash
				snd := ctx.Indexer.LookupAddress(ctx, op.SenderId)
				upd.ExplorerBigmapValue.Meta.Sender = &snd
				if op.CreatorId != 0 {
					src := ctx.Indexer.LookupAddress(ctx, op.CreatorId)
					upd.ExplorerBigmapValue.Meta.Source = &src
				} else {
					upd.ExplorerBigmapValue.Meta.Source = upd.ExplorerBigmapValue.Meta.Sender
				}
			}
		}
		switch v.Action {
		case micheline.DiffActionRemove:
			key, _ := v.GetKey(keyType)
			keyHash := v.GetKeyHash()
			upd.Key = &key
			upd.KeyHash = &keyHash
			if args.WithPrim() {
				upd.KeyPrim = key.PrimPtr()
			}
			if args.WithUnpack() {
				if upd.Key.IsPacked() {
					if up, err := upd.Key.Unpack(); err == nil {
						upd.Key = &up
					}
				}
			}
		case micheline.DiffActionUpdate, micheline.DiffActionCopy:
			key, _ := v.GetKey(keyType)
			keyHash := v.GetKeyHash()
			typedValue := v.GetValue(valType)
			upd.Key = &key
			upd.KeyHash = &keyHash
			upd.Value = &typedValue
			if args.WithPrim() {
				upd.KeyPrim = key.PrimPtr()
				upd.ValuePrim = &typedValue.Value
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
		}

		resp.diff = append(resp.diff, upd)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}
