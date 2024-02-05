// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Bigmap{})
}

var _ server.RESTful = (*Bigmap)(nil)
var _ server.Resource = (*Bigmap)(nil)

type Bigmap struct {
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
	DeletedHeight *int64            `json:"deleted_height"`
	DeletedBlock  *tezos.BlockHash  `json:"deleted_block"`
	DeletedTime   *time.Time        `json:"deleted_time"`
	KeyType       micheline.Typedef `json:"key_type"`
	ValueType     micheline.Typedef `json:"value_type"`
	KeyTypePrim   *micheline.Prim   `json:"key_type_prim,omitempty"`
	ValueTypePrim *micheline.Prim   `json:"value_type_prim,omitempty"`

	expires time.Time
}

func NewBigmap(ctx *server.Context, alloc *model.BigmapAlloc, args server.Options) *Bigmap {
	kt, vt := alloc.GetKeyType(), alloc.GetValueType()
	m := &Bigmap{
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
		expires:       ctx.Expires,
	}
	if alloc.Deleted > 0 {
		m.DeletedHeight = &alloc.Deleted
		tm := ctx.Indexer.LookupBlockTime(ctx, alloc.Deleted)
		m.DeletedTime = &tm
		bh := ctx.Indexer.LookupBlockHash(ctx.Context, alloc.Deleted)
		m.DeletedBlock = &bh
	}
	if args.WithPrim() {
		m.KeyTypePrim = &kt.Prim
		m.ValueTypePrim = &vt.Prim
	}
	return m
}

func (b Bigmap) LastModified() time.Time { return b.UpdatedTime }
func (b Bigmap) Expires() time.Time      { return b.expires }
func (b Bigmap) RESTPrefix() string      { return "/explorer/bigmap" }

func (b Bigmap) RESTPath(r *mux.Router) string {
	path, _ := r.Get("bigmap").URLPath("id", strconv.FormatInt(b.BigmapId, 10))
	return path.String()
}

func (b Bigmap) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Bigmap) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{id}", server.C(ReadBigmap)).Methods("GET").Name("bigmap")
	r.HandleFunc("/{id}/keys", server.C(ListBigmapKeys)).Methods("GET")
	r.HandleFunc("/{id}/values", server.C(ListBigmapValues)).Methods("GET")
	r.HandleFunc("/{id}/updates", server.C(ListBigmapUpdates)).Methods("GET")
	r.HandleFunc("/{id}/{key}/updates", server.C(ListBigmapKeyUpdates)).Methods("GET")
	r.HandleFunc("/{id}/{key}", server.C(ReadBigmapValue)).Methods("GET")
	return nil
}

// bigmap metadata
type BigmapMeta struct {
	Contract     tezos.Address `json:"contract"`
	BigmapId     int64         `json:"bigmap_id"`
	UpdateTime   time.Time     `json:"time"`
	UpdateHeight int64         `json:"height"`
	UpdateOp     tezos.OpHash  `json:"op"`
	Sender       tezos.Address `json:"sender"`
	Source       tezos.Address `json:"source"`
}

// keys
type BigmapKey struct {
	Key     micheline.Key   `json:"key"`
	KeyHash tezos.ExprHash  `json:"hash"`
	Meta    *BigmapMeta     `json:"meta,omitempty"`
	Prim    *micheline.Prim `json:"prim,omitempty"`
}

type BigmapKeyList struct {
	list     []BigmapKey
	modified time.Time
	expires  time.Time
}

func (l BigmapKeyList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l BigmapKeyList) LastModified() time.Time      { return l.modified }
func (l BigmapKeyList) Expires() time.Time           { return l.expires }

var _ server.Resource = (*BigmapKeyList)(nil)

// values
type BigmapValue struct {
	Key       *micheline.Key   `json:"key,omitempty"`   // omit on bigmap clear
	KeyHash   *tezos.ExprHash  `json:"hash,omitempty"`  // omit on bigmap clear
	Value     *micheline.Value `json:"value,omitempty"` // omit on removal updates
	Meta      *BigmapMeta      `json:"meta,omitempty"`
	KeyPrim   *micheline.Prim  `json:"key_prim,omitempty"`
	ValuePrim *micheline.Prim  `json:"value_prim,omitempty"`
	modified  time.Time        `json:"-"`
	expires   time.Time        `json:"-"`
}

func (t BigmapValue) LastModified() time.Time { return t.modified }
func (t BigmapValue) Expires() time.Time      { return t.expires }

var _ server.Resource = (*BigmapValue)(nil)

type BigmapValueList struct {
	list     []BigmapValue
	modified time.Time
	expires  time.Time
}

func (l BigmapValueList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l BigmapValueList) LastModified() time.Time      { return l.modified }
func (l BigmapValueList) Expires() time.Time           { return l.expires }

var _ server.Resource = (*BigmapKeyList)(nil)

// updates
type BigmapUpdate struct {
	BigmapValue                      // full value
	Action      micheline.DiffAction `json:"action"`
	BigmapId    int64                `json:"bigmap_id"`

	// alloc/copy
	KeyType       *micheline.Typedef `json:"key_type,omitempty"`
	ValueType     *micheline.Typedef `json:"value_type,omitempty"`
	KeyTypePrim   *micheline.Prim    `json:"key_type_prim,omitempty"`
	ValueTypePrim *micheline.Prim    `json:"value_type_prim,omitempty"`
	SourceId      int64              `json:"source_big_map,omitempty"`
	DestId        int64              `json:"destination_big_map,omitempty"`
}

type BigmapUpdateList struct {
	diff     []BigmapUpdate
	modified time.Time
	expires  time.Time
}

func (l BigmapUpdateList) MarshalJSON() ([]byte, error) { return json.Marshal(l.diff) }
func (l BigmapUpdateList) LastModified() time.Time      { return l.modified }
func (l BigmapUpdateList) Expires() time.Time           { return l.expires }

var _ server.Resource = (*BigmapUpdateList)(nil)

func loadBigmap(ctx *server.Context) *model.BigmapAlloc {
	if id, ok := mux.Vars(ctx.Request)["id"]; !ok || id == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing bigmap id", nil))
	} else {
		i, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid bigmap id", err))
		}
		a, err := ctx.Indexer.LookupBigmapAlloc(ctx, i)
		if err != nil {
			switch err {
			case model.ErrNoBigmap:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such bigmap", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return a
	}
}

func parseBigmapKey(ctx *server.Context, typ micheline.OpCode) tezos.ExprHash {
	if k, ok := mux.Vars(ctx.Request)["key"]; !ok || k == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing bigmap key", nil))
	} else {
		expr, err := tezos.ParseExprHash(k)
		if err == nil {
			return expr
		}
		key, err := micheline.ParseKey(typ, k)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid bigmap key", err))
		}
		return key.Hash()
	}
}

func ReadBigmap(ctx *server.Context) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc := loadBigmap(ctx)
	return NewBigmap(ctx, alloc, args), http.StatusOK
}

func ListBigmapKeys(ctx *server.Context) (interface{}, int) {
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
		items []*model.BigmapValue
		err   error
	)
	if r.Since == 0 {
		items, err = ctx.Indexer.ListBigmapKeys(ctx.Context, r)
	} else {
		items, err = ctx.Indexer.ListHistoricBigmapKeys(ctx.Context, r)
	}
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read bigmap", err))
	}

	resp := &BigmapKeyList{
		list:     make([]BigmapKey, 0, len(items)),
		expires:  ctx.Expires,
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
		key := BigmapKey{
			Key:     k,
			KeyHash: v.GetKeyHash(),
		}
		if args.WithMeta() {
			key.Meta = &BigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateHeight: v.Height,
				UpdateTime:   ctx.Indexer.LookupBlockTime(ctx, v.Height),
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

func ListBigmapValues(ctx *server.Context) (interface{}, int) {
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
		items []*model.BigmapValue
		err   error
	)
	if r.Since == 0 {
		items, err = ctx.Indexer.ListBigmapKeys(ctx.Context, r)
	} else {
		items, err = ctx.Indexer.ListHistoricBigmapKeys(ctx.Context, r)
	}
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read bigmap", err))
	}

	resp := &BigmapValueList{
		list:     make([]BigmapValue, 0, len(items)),
		expires:  ctx.Expires,
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
		val := BigmapValue{
			Key:     &key,
			KeyHash: &keyHash,
			Value:   &typedValue,
		}
		if args.WithMeta() {
			val.Meta = &BigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateHeight: v.Height,
				UpdateTime:   ctx.Indexer.LookupBlockTime(ctx, v.Height),
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

func ReadBigmapValue(ctx *server.Context) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	// support key and key_hash
	alloc := loadBigmap(ctx)
	keyType, valType := alloc.GetKeyType(), alloc.GetValueType()
	expr := parseBigmapKey(ctx, keyType.OpCode)

	r := etl.ListRequest{
		BigmapId:  alloc.BigmapId,
		BigmapKey: expr,
		Since:     args.BlockHeight,
	}

	var (
		items []*model.BigmapValue
		err   error
	)
	if r.Since == 0 {
		items, err = ctx.Indexer.ListBigmapKeys(ctx.Context, r)
	} else {
		items, err = ctx.Indexer.ListHistoricBigmapKeys(ctx.Context, r)
	}
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read bigmap", err))
	}
	if len(items) == 0 {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such bigmap key", err))
	}
	v := items[0]
	key, err := v.GetKey(keyType)
	if err != nil {
		panic(server.EInternal(server.EC_SERVER, "cannot decode bigmap key", err))
	}

	keyHash := v.GetKeyHash()
	typedValue := v.GetValue(valType)

	resp := &BigmapValue{
		Key:      &key,
		KeyHash:  &keyHash,
		Value:    &typedValue,
		modified: ctx.Indexer.LookupBlockTime(ctx, v.Height),
		expires:  ctx.Expires,
	}
	if args.WithMeta() {
		resp.Meta = &BigmapMeta{
			Contract:     ctx.Indexer.LookupAddress(ctx, alloc.AccountId),
			BigmapId:     alloc.BigmapId,
			UpdateHeight: v.Height,
			UpdateTime:   ctx.Indexer.LookupBlockTime(ctx, v.Height),
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

func ListBigmapUpdates(ctx *server.Context) (interface{}, int) {
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
		panic(server.EInternal(server.EC_DATABASE, "cannot read bigmap", err))
	}

	var opCache map[model.OpID]*model.Op
	if args.WithMeta() && len(items) > 0 {
		opIds := make([]uint64, 0)
		for _, v := range items {
			opIds = append(opIds, v.OpId.U64())
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

	resp := &BigmapUpdateList{
		diff:    make([]BigmapUpdate, 0, len(items)),
		expires: ctx.Expires,
	}

	keyType, valType := alloc.GetKeyType(), alloc.GetValueType()
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		upd := BigmapUpdate{
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
		case micheline.DiffActionRemove:
			// key is empty when entire bigmap is removed
			if len(v.Key) > 0 {
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
			}
		}
		if args.WithMeta() {
			upd.BigmapValue.Meta = &BigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
			}
			if op, ok := opCache[v.OpId]; ok {
				upd.BigmapValue.Meta.UpdateOp = op.Hash
				snd := ctx.Indexer.LookupAddress(ctx, op.SenderId)
				upd.BigmapValue.Meta.Sender = snd
				if op.CreatorId != 0 {
					src := ctx.Indexer.LookupAddress(ctx, op.CreatorId)
					upd.BigmapValue.Meta.Source = src
				} else {
					upd.BigmapValue.Meta.Source = upd.BigmapValue.Meta.Sender
				}
			}
		}
		resp.diff = append(resp.diff, upd)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ListBigmapKeyUpdates(ctx *server.Context) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	alloc := loadBigmap(ctx)
	keyType, valType := alloc.GetKeyType(), alloc.GetValueType()
	expr := parseBigmapKey(ctx, keyType.OpCode)

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
		panic(server.EInternal(server.EC_DATABASE, "cannot read bigmap", err))
	}

	// load ops when metadata is requested
	var opCache map[model.OpID]*model.Op
	if args.WithMeta() && len(items) > 0 {
		opIds := make([]uint64, 0)
		for _, v := range items {
			opIds = append(opIds, v.OpId.U64())
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

	resp := &BigmapUpdateList{
		diff:    make([]BigmapUpdate, 0, len(items)),
		expires: ctx.Expires,
	}
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		upd := BigmapUpdate{
			Action:   v.Action,
			BigmapId: v.BigmapId,
		}
		if args.WithMeta() {
			upd.BigmapValue.Meta = &BigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
			}
			if op, ok := opCache[v.OpId]; ok {
				upd.BigmapValue.Meta.UpdateOp = op.Hash
				snd := ctx.Indexer.LookupAddress(ctx, op.SenderId)
				upd.BigmapValue.Meta.Sender = snd
				if op.CreatorId != 0 {
					src := ctx.Indexer.LookupAddress(ctx, op.CreatorId)
					upd.BigmapValue.Meta.Source = src
				} else {
					upd.BigmapValue.Meta.Source = upd.BigmapValue.Meta.Sender
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
