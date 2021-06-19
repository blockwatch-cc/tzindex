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
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerBigmap{})
}

var _ RESTful = (*ExplorerBigmap)(nil)

type ExplorerBigmap struct {
	Contract        string            `json:"contract"`
	BigmapId        int64             `json:"bigmap_id"`
	NUpdates        int64             `json:"n_updates"`
	NKeys           int64             `json:"n_keys"`
	AllocatedHeight int64             `json:"alloc_height"`
	AllocatedBlock  tezos.BlockHash   `json:"alloc_block"`
	AllocatedTime   time.Time         `json:"alloc_time"`
	UpdatedHeight   int64             `json:"update_height"`
	UpdatedBlock    tezos.BlockHash   `json:"update_block"`
	UpdatedTime     time.Time         `json:"update_time"`
	IsRemoved       bool              `json:"is_removed,omitempty"` // only include when true
	KeyType         micheline.Typedef `json:"key_type"`
	ValueType       micheline.Typedef `json:"value_type"`
	KeyTypePrim     *micheline.Prim   `json:"key_type_prim,omitempty"`
	ValueTypePrim   *micheline.Prim   `json:"value_type_prim,omitempty"`

	expires time.Time `json:"-"`
}

func NewExplorerBigmap(ctx *ApiContext, alloc, last *model.BigmapItem, args Options) *ExplorerBigmap {
	m := &ExplorerBigmap{
		Contract:        ctx.Indexer.LookupAddress(ctx, alloc.AccountId).String(),
		BigmapId:        alloc.BigmapId,
		NUpdates:        last.Counter,
		NKeys:           last.NKeys,
		AllocatedHeight: alloc.Height,
		AllocatedTime:   alloc.Timestamp,
		UpdatedHeight:   last.Height,
		UpdatedTime:     last.Timestamp,
		IsRemoved:       alloc.IsDeleted,
		expires:         ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}
	m.AllocatedBlock = ctx.Indexer.LookupBlockHash(ctx.Context, alloc.Height)
	m.UpdatedBlock = ctx.Indexer.LookupBlockHash(ctx.Context, last.Height)
	var kt, vt micheline.Type
	kt.UnmarshalBinary(alloc.Key)
	vt.UnmarshalBinary(alloc.Value)
	m.KeyType = kt.Typedef(micheline.CONST_KEY)
	m.ValueType = vt.Typedef(micheline.CONST_VALUE)
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

// type
type ExplorerBigmapType struct {
	Contract      string          `json:"contract"`
	BigmapId      int64           `json:"bigmap_id"`
	KeyType       micheline.Type  `json:"key_type"`
	ValueType     micheline.Type  `json:"value_type"`
	KeyTypePrim   *micheline.Prim `json:"key_type_prim,omitempty"`
	ValueTypePrim *micheline.Prim `json:"value_type_prim,omitempty"`
	modified      time.Time       `json:"-"`
}

func (t ExplorerBigmapType) LastModified() time.Time { return t.modified }
func (t ExplorerBigmapType) Expires() time.Time      { return time.Time{} }

var _ Resource = (*ExplorerBigmapType)(nil)

// bigmap metadata
type ExplorerBigmapMeta struct {
	Contract     tezos.Address   `json:"contract"`
	BigmapId     int64           `json:"bigmap_id"`
	UpdateTime   time.Time       `json:"time"`
	UpdateHeight int64           `json:"height"`
	UpdateBlock  tezos.BlockHash `json:"block"`
	UpdateOp     tezos.OpHash    `json:"op"`
	Sender       tezos.Address   `json:"sender"`
	Source       tezos.Address   `json:"source"`
	IsReplaced   *bool           `json:"is_replaced,omitempty"` // empty in op lists
	IsRemoved    *bool           `json:"is_removed,omitempty"`
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
	Contract string          `json:"contract"`
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

func loadBigmap(ctx *ApiContext, withLast bool) (*model.BigmapItem, *model.BigmapItem) {
	if id, ok := mux.Vars(ctx.Request)["id"]; !ok || id == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing bigmap id", nil))
	} else {
		i, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid bigmap id", err))
		}
		a, b, err := ctx.Indexer.LookupBigmap(ctx, i, withLast)
		if err != nil {
			switch err {
			case index.ErrNoBigmapEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such bigmap", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return a, b
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
	// return nil
}

func ReadBigmap(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, last := loadBigmap(ctx, true)
	return NewExplorerBigmap(ctx, alloc, last, args), http.StatusOK
}

func ListBigmapKeys(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, _ := loadBigmap(ctx, false)

	items, err := ctx.Indexer.ListBigmapKeys(ctx.Context,
		alloc.BigmapId,
		args.BlockHeight, // all current active keys when 0, historic active keys when > 0
		tezos.ExprHash{},
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	resp := &ExplorerBigmapKeyList{
		list:    make([]ExplorerBigmapKey, 0, len(items)),
		expires: ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
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

	keyType, _ := alloc.GetKeyType()
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
				Contract:     ctx.Indexer.LookupAddress(ctx, alloc.AccountId),
				BigmapId:     alloc.BigmapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
				UpdateBlock:  ctx.Indexer.LookupBlockHash(ctx.Context, v.Height),
				IsReplaced:   BoolPtr(v.IsReplaced),
				IsRemoved:    BoolPtr(v.IsDeleted),
			}
			if op, ok := opCache[v.OpId]; ok {
				key.Meta.UpdateOp = op.Hash
				key.Meta.Sender = ctx.Indexer.LookupAddress(ctx, op.SenderId)
				if op.CreatorId != 0 {
					key.Meta.Source = ctx.Indexer.LookupAddress(ctx, op.CreatorId)
				} else {
					key.Meta.Source = key.Meta.Sender
				}
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
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ListBigmapValues(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, _ := loadBigmap(ctx, false)

	var typ micheline.Type
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	items, err := ctx.Indexer.ListBigmapKeys(ctx.Context,
		alloc.BigmapId,
		args.BlockHeight, // all current active keys when 0, historic active keys when > 0
		tezos.ExprHash{},
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
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

	resp := &ExplorerBigmapValueList{
		list:    make([]ExplorerBigmapValue, 0, len(items)),
		expires: ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}

	keyType, _ := alloc.GetKeyType()
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		k, err := v.GetKey(keyType)
		if err != nil {
			log.Errorf("explorer: decode bigmap key: %v", err)
			continue
		}
		prim := micheline.Prim{}
		if err = prim.UnmarshalBinary(v.Value); err != nil {
			log.Errorf("explorer: bigmap value unmarshal: %v", err)
		}
		expr := v.GetKeyHash()
		val := ExplorerBigmapValue{
			Key:     &k,
			KeyHash: &expr,
			Value:   micheline.NewValuePtr(typ, prim),
		}
		if args.WithMeta() {
			val.Meta = &ExplorerBigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
				UpdateBlock:  ctx.Indexer.LookupBlockHash(ctx.Context, v.Height),
				IsReplaced:   BoolPtr(v.IsReplaced),
				IsRemoved:    BoolPtr(v.IsDeleted),
			}
			if op, ok := opCache[v.OpId]; ok {
				val.Meta.UpdateOp = op.Hash
				val.Meta.Sender = ctx.Indexer.LookupAddress(ctx, op.SenderId)
				if op.CreatorId != 0 {
					val.Meta.Source = ctx.Indexer.LookupAddress(ctx, op.CreatorId)
				} else {
					val.Meta.Source = val.Meta.Sender
				}
			}
		}
		if args.WithPrim() {
			val.KeyPrim = k.PrimPtr()
			val.ValuePrim = &prim
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
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ReadBigmapValue(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	// support key and key_hash
	alloc, _ := loadBigmap(ctx, false)
	keyType, _ := alloc.GetKeyType()
	_, expr := parseBigmapKey(ctx, keyType.OpCode)

	var typ micheline.Type
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	// log.Infof("Looking for bigmap key %s %s bytes=%x hash=%x bin=%x",
	// 	bkey.Type, bkey.String(), bkey.Bytes(), bkey.Hash, bin)

	items, err := ctx.Indexer.ListBigmapKeys(ctx,
		alloc.BigmapId,
		args.BlockHeight, // current when 0
		expr,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}
	if len(items) == 0 {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such bigmap key", err))
	}
	v := items[0]
	k, err := v.GetKey(keyType)
	if err != nil {
		panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
	}
	// log.Infof("Found bigmap key %s %s bytes=%x hash=%x",
	// k.Type, k.String(), k.Bytes(), k.Hash)
	prim := micheline.Prim{}
	if err = prim.UnmarshalBinary(v.Value); err != nil {
		log.Errorf("explorer: bigmap value unmarshal: %v", err)
	}

	// load ops when metadata is requested
	var opCache map[model.OpID]*model.Op
	if args.WithMeta() && len(items) > 0 {
		ops, err := ctx.Indexer.LookupOpIds(ctx, []uint64{v.OpId.Value()})
		if err != nil {
			log.Errorf("%s: missing op id %d", ctx.RequestString(), v.OpId)
		} else {
			opCache = make(map[model.OpID]*model.Op)
			for _, v := range ops {
				opCache[v.RowId] = v
			}
		}
	}

	expr = v.GetKeyHash()
	resp := &ExplorerBigmapValue{
		Key:      &k,
		KeyHash:  &expr,
		Value:    micheline.NewValuePtr(typ, prim),
		modified: items[0].Timestamp,
		expires:  ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}
	if args.WithMeta() {
		resp.Meta = &ExplorerBigmapMeta{
			Contract:     ctx.Indexer.LookupAddress(ctx, alloc.AccountId),
			BigmapId:     alloc.BigmapId,
			UpdateTime:   v.Timestamp,
			UpdateHeight: v.Height,
			UpdateBlock:  ctx.Indexer.LookupBlockHash(ctx.Context, v.Height),
			IsReplaced:   BoolPtr(v.IsReplaced),
			IsRemoved:    BoolPtr(v.IsDeleted),
		}
		if op, ok := opCache[v.OpId]; ok {
			resp.Meta.UpdateOp = op.Hash
			resp.Meta.Sender = ctx.Indexer.LookupAddress(ctx, op.SenderId)
			if op.CreatorId != 0 {
				resp.Meta.Source = ctx.Indexer.LookupAddress(ctx, op.CreatorId)
			} else {
				resp.Meta.Source = resp.Meta.Sender
			}
		}
	}
	if args.WithPrim() {
		resp.KeyPrim = k.PrimPtr()
		resp.ValuePrim = &prim
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
	alloc, _ := loadBigmap(ctx, false)

	var typ micheline.Type
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	items, err := ctx.Indexer.ListBigmapUpdates(ctx.Context,
		alloc.BigmapId,
		args.SinceHeight+1,
		args.BlockHeight,
		tezos.ExprHash{},
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
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
		expires: ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}

	keyType, _ := alloc.GetKeyType()
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		k, err := v.GetKey(keyType)
		if err != nil {
			panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
		}

		expr := v.GetKeyHash()
		upd := ExplorerBigmapUpdate{
			Action:   v.Action,
			BigmapId: v.BigmapId,
			ExplorerBigmapValue: ExplorerBigmapValue{
				Key:     &k,
				KeyHash: &expr,
			},
		}
		if args.WithMeta() {
			upd.ExplorerBigmapValue.Meta = &ExplorerBigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
				UpdateBlock:  ctx.Indexer.LookupBlockHash(ctx.Context, v.Height),
				IsReplaced:   BoolPtr(v.IsReplaced),
				IsRemoved:    BoolPtr(v.IsDeleted),
			}
			if op, ok := opCache[v.OpId]; ok {
				upd.ExplorerBigmapValue.Meta.UpdateOp = op.Hash
				upd.ExplorerBigmapValue.Meta.Sender = ctx.Indexer.LookupAddress(ctx, op.SenderId)
				if op.CreatorId != 0 {
					upd.ExplorerBigmapValue.Meta.Source = ctx.Indexer.LookupAddress(ctx, op.CreatorId)
				} else {
					upd.ExplorerBigmapValue.Meta.Source = upd.ExplorerBigmapValue.Meta.Sender
				}
			}
		}
		switch v.Action {
		case micheline.DiffActionUpdate, micheline.DiffActionCopy:
			prim := micheline.Prim{}
			if err = prim.UnmarshalBinary(v.Value); err != nil {
				log.Errorf("explorer: bigmap value unmarshal: %v", err)
			}
			upd.Value = micheline.NewValuePtr(typ, prim)
			if args.WithPrim() {
				upd.KeyPrim = k.PrimPtr()
				upd.ValuePrim = &prim
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

func ListBigmapKeyUpdates(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	alloc, _ := loadBigmap(ctx, false)
	keyType, _ := alloc.GetKeyType()
	_, expr := parseBigmapKey(ctx, keyType.OpCode)

	var typ micheline.Type
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	items, err := ctx.Indexer.ListBigmapUpdates(ctx,
		alloc.BigmapId,
		args.SinceHeight+1,
		args.BlockHeight,
		expr,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
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
		expires: ctx.Tip.BestTime.Add(ctx.Params.TimeBetweenBlocks[0]),
	}
	contract := ctx.Indexer.LookupAddress(ctx, alloc.AccountId)
	for _, v := range items {
		k, err := v.GetKey(keyType)
		if err != nil {
			panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
		}

		expr := v.GetKeyHash()
		upd := ExplorerBigmapUpdate{
			Action:   v.Action,
			BigmapId: v.BigmapId,
			ExplorerBigmapValue: ExplorerBigmapValue{
				Key:     &k,
				KeyHash: &expr,
			},
		}
		if args.WithMeta() {
			upd.ExplorerBigmapValue.Meta = &ExplorerBigmapMeta{
				Contract:     contract,
				BigmapId:     alloc.BigmapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
				UpdateBlock:  ctx.Indexer.LookupBlockHash(ctx.Context, v.Height),
				IsReplaced:   BoolPtr(v.IsReplaced),
				IsRemoved:    BoolPtr(v.IsDeleted),
			}
			if op, ok := opCache[v.OpId]; ok {
				upd.ExplorerBigmapValue.Meta.UpdateOp = op.Hash
				upd.ExplorerBigmapValue.Meta.Sender = ctx.Indexer.LookupAddress(ctx, op.SenderId)
				if op.CreatorId != 0 {
					upd.ExplorerBigmapValue.Meta.Source = ctx.Indexer.LookupAddress(ctx, op.CreatorId)
				} else {
					upd.ExplorerBigmapValue.Meta.Source = upd.ExplorerBigmapValue.Meta.Sender
				}
			}
		}
		switch v.Action {
		case micheline.DiffActionUpdate, micheline.DiffActionCopy:
			prim := micheline.Prim{}
			if err = prim.UnmarshalBinary(v.Value); err != nil {
				log.Errorf("explorer: bigmap value unmarshal: %v", err)
			}
			upd.Value = micheline.NewValuePtr(typ, prim)
			if args.WithPrim() {
				upd.KeyPrim = k.PrimPtr()
				upd.ValuePrim = &prim
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
