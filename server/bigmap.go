// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

func init() {
	register(ExplorerBigmap{})
}

var _ RESTful = (*ExplorerBigmap)(nil)

type ExplorerBigmap struct {
	Contract        string          `json:"contract"`
	BigMapId        int64           `json:"bigmap_id"`
	NUpdates        int64           `json:"n_updates"`
	NKeys           int64           `json:"n_keys"`
	AllocatedHeight int64           `json:"alloc_height"`
	AllocatedBlock  chain.BlockHash `json:"alloc_block"`
	AllocatedTime   time.Time       `json:"alloc_time"`
	UpdatedHeight   int64           `json:"update_height"`
	UpdatedBlock    chain.BlockHash `json:"update_block"`
	UpdatedTime     time.Time       `json:"update_time"`

	expires time.Time `json:"-"`
}

func NewExplorerBigMap(ctx *ApiContext, alloc, last *model.BigMapItem) *ExplorerBigmap {
	m := &ExplorerBigmap{
		Contract:        lookupAddress(ctx, alloc.AccountId).String(),
		BigMapId:        alloc.BigMapId,
		NUpdates:        last.Counter,
		NKeys:           last.NKeys,
		AllocatedHeight: alloc.Height,
		AllocatedTime:   alloc.Timestamp,
		UpdatedHeight:   last.Height,
		UpdatedTime:     last.Timestamp,
		expires:         ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}
	if b, err := ctx.Indexer.BlockByHeight(ctx.Context, alloc.Height); err == nil {
		m.AllocatedBlock = b.Hash
	}
	if alloc.Height != last.Height {
		if b, err := ctx.Indexer.BlockByHeight(ctx.Context, last.Height); err == nil {
			m.UpdatedBlock = b.Hash
		}
	} else {
		m.UpdatedBlock = m.AllocatedBlock
	}
	return m
}

func (b ExplorerBigmap) LastModified() time.Time { return b.UpdatedTime }
func (b ExplorerBigmap) Expires() time.Time      { return b.expires }
func (b ExplorerBigmap) RESTPrefix() string      { return "/explorer/bigmap" }

func (b ExplorerBigmap) RESTPath(r *mux.Router) string {
	path, _ := r.Get("bigmap").URLPath("id", strconv.FormatInt(b.BigMapId, 10))
	return path.String()
}

func (b ExplorerBigmap) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b ExplorerBigmap) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{id}", C(ReadBigMap)).Methods("GET").Name("bigmap")
	r.HandleFunc("/{id}/type", C(ReadBigMapType)).Methods("GET")
	r.HandleFunc("/{id}/keys", C(ListBigMapKeys)).Methods("GET")
	r.HandleFunc("/{id}/values", C(ListBigMapValues)).Methods("GET")
	r.HandleFunc("/{id}/updates", C(ListBigMapUpdates)).Methods("GET")
	r.HandleFunc("/{id}/{key}", C(ReadBigMapValue)).Methods("GET")
	r.HandleFunc("/{id}/{key}/updates", C(ListBigMapKeyUpdates)).Methods("GET")
	return nil
}

type ExplorerBigmapKeyValue struct {
	Key       *micheline.Prim `json:"key,omitempty"`
	Value     *micheline.Prim `json:"value,omitempty"`
	KeyType   *micheline.Prim `json:"key_type,omitempty"`
	ValueType *micheline.Prim `json:"value_type,omitempty"`
}

// type
type ExplorerBigmapType struct {
	Contract    string                  `json:"contract"`
	BigMapId    int64                   `json:"bigmap_id"`
	KeyType     micheline.OpCode        `json:"key_type"`
	KeyEncoding micheline.PrimType      `json:"key_encoding"`
	ValueType   micheline.BigMapType    `json:"value_type"`
	Prim        *ExplorerBigmapKeyValue `json:"prim,omitempty"`
	modified    time.Time               `json:"-"`
}

func (t ExplorerBigmapType) LastModified() time.Time { return t.modified }
func (t ExplorerBigmapType) Expires() time.Time      { return time.Time{} }

var _ Resource = (*ExplorerBigmapType)(nil)

// bigmap metadata
type ExplorerBigmapMeta struct {
	Contract     string          `json:"contract"`
	BigMapId     int64           `json:"bigmap_id"`
	UpdateTime   time.Time       `json:"time"`
	UpdateHeight int64           `json:"height"`
	UpdateBlock  chain.BlockHash `json:"block"`
	IsReplaced   *bool           `json:"is_replaced,omitempty"` // empty in op lists
	IsRemoved    *bool           `json:"is_removed,omitempty"`
}

// keys
type ExplorerBigmapKey struct {
	Key         *micheline.BigMapKey `json:"key"`
	KeyHash     chain.ExprHash       `json:"key_hash"`
	KeyBinary   string               `json:"key_binary"`
	KeyUnpacked *micheline.BigMapKey `json:"key_unpacked,omitempty"`
	KeyPretty   string               `json:"key_pretty,omitempty"`
	Meta        ExplorerBigmapMeta   `json:"meta"`
	Prim        *micheline.Prim      `json:"prim,omitempty"`
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
	Key           *micheline.BigMapKey    `json:"key"`
	KeyHash       chain.ExprHash          `json:"key_hash"`
	KeyBinary     string                  `json:"key_binary,omitempty"` // omit on alloc updates
	KeyUnpacked   *micheline.BigMapKey    `json:"key_unpacked,omitempty"`
	KeyPretty     string                  `json:"key_pretty,omitempty"`
	Value         *micheline.BigMapValue  `json:"value,omitempty"` // omit on removal updates
	ValueUnpacked *micheline.BigMapValue  `json:"value_unpacked,omitempty"`
	Meta          ExplorerBigmapMeta      `json:"meta"`
	Prim          *ExplorerBigmapKeyValue `json:"prim,omitempty"`
	modified      time.Time               `json:"-"`
	expires       time.Time               `json:"-"`
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
	Block    chain.BlockHash `json:"block"`
}

// storage
type ExplorerStorageValue struct {
	Meta          ExplorerStorageMeta    `json:"meta"`
	Value         *micheline.BigMapValue `json:"value"`
	ValueUnpacked *micheline.BigMapValue `json:"value_unpacked,omitempty"`
	Prim          *micheline.Prim        `json:"prim,omitempty"`
	modified      time.Time              `json:"-"`
	expires       time.Time              `json:"-"`
}

func (t ExplorerStorageValue) LastModified() time.Time { return t.modified }
func (t ExplorerStorageValue) Expires() time.Time      { return t.expires }

var _ Resource = (*ExplorerBigmapValue)(nil)

// params
type ExplorerParameters struct {
	Entrypoint    string                 `json:"entrypoint"`
	Branch        string                 `json:"branch"`
	Id            int                    `json:"id"`
	Value         *micheline.BigMapValue `json:"value,omitempty"`
	ValueUnpacked *micheline.BigMapValue `json:"value_unpacked,omitempty"`
	Prim          *micheline.Prim        `json:"prim,omitempty"`
}

// updates
type ExplorerBigMapUpdate struct {
	ExplorerBigmapValue                            // full value
	Action              micheline.BigMapDiffAction `json:"action"`

	// alloc/copy
	KeyType     *micheline.OpCode     `json:"key_type,omitempty"`
	KeyEncoding *micheline.PrimType   `json:"key_encoding,omitempty"`
	ValueType   *micheline.BigMapType `json:"value_type,omitempty"`
	SourceId    int64                 `json:"source_big_map,omitempty"`
	DestId      int64                 `json:"destination_big_map,omitempty"`
}

type ExplorerBigMapUpdateList struct {
	diff     []ExplorerBigMapUpdate
	modified time.Time
	expires  time.Time
}

func (l ExplorerBigMapUpdateList) MarshalJSON() ([]byte, error) { return json.Marshal(l.diff) }
func (l ExplorerBigMapUpdateList) LastModified() time.Time      { return l.modified }
func (l ExplorerBigMapUpdateList) Expires() time.Time           { return l.expires }

var _ Resource = (*ExplorerBigMapUpdateList)(nil)

func loadBigmap(ctx *ApiContext, withLast bool) (*model.BigMapItem, *model.BigMapItem) {
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
			case index.ErrNoBigMapEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such bigmap", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return a, b
	}
}

func parseBigmapKey(ctx *ApiContext, typ string) *micheline.BigMapKey {
	if k, ok := mux.Vars(ctx.Request)["key"]; !ok || k == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing bigmap key", nil))
	} else {
		key, err := micheline.ParseBigMapKey(typ, k)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid bigmap key", err))
		}
		return key
	}
	return nil
}

func ReadBigMap(ctx *ApiContext) (interface{}, int) {
	alloc, last := loadBigmap(ctx, true)
	return NewExplorerBigMap(ctx, alloc, last), http.StatusOK
}

func ReadBigMapType(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, _ := loadBigmap(ctx, false)
	args.ParseBlockIdent(ctx)

	resp := &ExplorerBigmapType{
		Contract:    lookupAddress(ctx, alloc.AccountId).String(),
		BigMapId:    alloc.BigMapId,
		KeyType:     alloc.KeyType,
		KeyEncoding: alloc.KeyEncoding,
		ValueType:   micheline.NewBigMapType(),
		modified:    alloc.Timestamp,
	}

	if err := resp.ValueType.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	if args.WithPrim() {
		resp.Prim = &ExplorerBigmapKeyValue{
			// TODO: support complex comparable types
			KeyType: &micheline.Prim{
				OpCode: alloc.KeyType,
				Type:   micheline.PrimNullary,
			},
			ValueType: resp.ValueType.Prim(),
		}
	}

	return resp, http.StatusOK
}

func ListBigMapKeys(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, _ := loadBigmap(ctx, false)
	args.ParseBlockIdent(ctx)
	addr := lookupAddress(ctx, alloc.AccountId).String()

	items, err := ctx.Indexer.ListBigMapKeys(ctx.Context,
		alloc.BigMapId,
		args.BlockHeight, // all current active keys when 0, historic active keys when > 0
		chain.ExprHash{},
		nil,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	resp := &ExplorerBigmapKeyList{
		list:    make([]ExplorerBigmapKey, 0, len(items)),
		expires: ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}

	blockHashes := make(map[int64]chain.BlockHash)

	for _, v := range items {
		k, err := v.GetKey()
		if err != nil {
			log.Errorf("explorer: decode bigmap key: %v", err)
			continue
		}
		h, ok := blockHashes[v.Height]
		if !ok {
			if h, err = ctx.Indexer.BlockHashByHeight(ctx.Context, v.Height); err == nil {
				blockHashes[v.Height] = h
			}
		}

		key := ExplorerBigmapKey{
			Key:       k,
			KeyHash:   chain.NewExprHash(v.KeyHash),
			KeyBinary: k.Encode(),
			Meta: ExplorerBigmapMeta{
				Contract:     addr,
				BigMapId:     alloc.BigMapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
				UpdateBlock:  h,
				IsReplaced:   BoolPtr(v.IsReplaced),
				IsRemoved:    BoolPtr(v.IsDeleted),
			},
		}

		if args.WithPrim() {
			key.Prim = k.Prim()
		}
		if args.WithUnpack() {
			if key.KeyUnpacked, err = k.UnpackKey(); err == nil {
				key.KeyPretty = key.KeyUnpacked.String()
			}
		}

		resp.list = append(resp.list, key)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ListBigMapValues(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, _ := loadBigmap(ctx, false)
	args.ParseBlockIdent(ctx)

	typ := &micheline.Prim{}
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}
	addr := lookupAddress(ctx, alloc.AccountId).String()

	items, err := ctx.Indexer.ListBigMapKeys(ctx.Context,
		alloc.BigMapId,
		args.BlockHeight, // all current active keys when 0, historic active keys when > 0
		chain.ExprHash{},
		nil,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	blockHashes := make(map[int64]chain.BlockHash)

	resp := &ExplorerBigmapValueList{
		list:    make([]ExplorerBigmapValue, 0, len(items)),
		expires: ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}
	for _, v := range items {
		k, err := v.GetKey()
		if err != nil {
			log.Errorf("explorer: decode bigmap key: %v", err)
			continue
		}

		h, ok := blockHashes[v.Height]
		if !ok {
			if h, err = ctx.Indexer.BlockHashByHeight(ctx.Context, v.Height); err == nil {
				blockHashes[v.Height] = h
			}
		}

		prim := &micheline.Prim{}
		if err = prim.UnmarshalBinary(v.Value); err != nil {
			log.Errorf("explorer: bigmap value unmarshal: %v", err)
		}
		val := ExplorerBigmapValue{
			Key:       k,
			KeyHash:   chain.NewExprHash(v.KeyHash),
			KeyBinary: k.Encode(),
			Meta: ExplorerBigmapMeta{
				Contract:     addr,
				BigMapId:     alloc.BigMapId,
				UpdateTime:   v.Timestamp,
				UpdateHeight: v.Height,
				UpdateBlock:  h,
				IsReplaced:   BoolPtr(v.IsReplaced),
				IsRemoved:    BoolPtr(v.IsDeleted),
			},
			Value: &micheline.BigMapValue{
				Type:  typ,
				Value: prim,
			},
		}
		if args.WithPrim() {
			val.Prim = &ExplorerBigmapKeyValue{
				Key:   k.Prim(),
				Value: prim,
			}
		}
		if args.WithUnpack() {
			if prim.IsPackedAny() {
				if p, err := prim.UnpackAny(); err == nil {
					val.ValueUnpacked = &micheline.BigMapValue{
						Type:  p.BuildType(),
						Value: p,
					}
				}
			}
			if k.IsPacked() {
				if val.KeyUnpacked, err = k.UnpackKey(); err == nil {
					val.KeyPretty = val.KeyUnpacked.String()
				}
			}
		}
		resp.list = append(resp.list, val)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ReadBigMapValue(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	// support key and key_hash
	alloc, _ := loadBigmap(ctx, false)
	bkey := parseBigmapKey(ctx, alloc.KeyType.String())
	args.ParseBlockIdent(ctx)
	addr := lookupAddress(ctx, alloc.AccountId).String()

	typ := &micheline.Prim{}
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	// log.Infof("Looking for bigmap key %s %s bytes=%x bash=%x",
	// 	bkey.Type, bkey.String(), bkey.Bytes(), bkey.Hash)

	items, err := ctx.Indexer.ListBigMapKeys(ctx,
		alloc.BigMapId,
		args.BlockHeight, // current when 0
		bkey.Hash,
		bkey.Bytes(),
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

	k, err := items[0].GetKey()
	if err != nil {
		panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
	}
	// log.Infof("Found bigmap key %s %s bytes=%x bash=%x",
	// 	k.Type, k.String(), k.Bytes(), k.Hash)

	h, err := ctx.Indexer.BlockHashByHeight(ctx.Context, v.Height)
	if err != nil {
		panic(EInternal(EC_SERVER, "cannot find block hash", err))
	}

	prim := &micheline.Prim{}
	if err = prim.UnmarshalBinary(v.Value); err != nil {
		log.Errorf("explorer: bigmap value unmarshal: %v", err)
	}

	resp := &ExplorerBigmapValue{
		Key:       k,
		KeyHash:   chain.NewExprHash(v.KeyHash),
		KeyBinary: k.Encode(),
		Meta: ExplorerBigmapMeta{
			Contract:     addr,
			BigMapId:     alloc.BigMapId,
			UpdateTime:   v.Timestamp,
			UpdateHeight: v.Height,
			UpdateBlock:  h,
			IsReplaced:   BoolPtr(v.IsReplaced),
			IsRemoved:    BoolPtr(v.IsDeleted),
		},
		Value: &micheline.BigMapValue{
			Type:  typ,
			Value: prim,
		},
		modified: items[0].Timestamp,
		expires:  ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}
	if args.WithPrim() {
		resp.Prim = &ExplorerBigmapKeyValue{
			Key:   k.Prim(),
			Value: prim,
		}
	}
	if args.WithUnpack() {
		if prim.IsPackedAny() {
			if p, err := prim.UnpackAny(); err == nil {
				resp.ValueUnpacked = &micheline.BigMapValue{
					Type:  p.BuildType(),
					Value: p,
				}
			} else {
				log.Warnf("unpack: %v", err)
			}
		}
		if k.IsPacked() {
			if resp.KeyUnpacked, err = k.UnpackKey(); err == nil {
				resp.KeyPretty = resp.KeyUnpacked.String()
			}
		}
	}

	return resp, http.StatusOK
}

func ListBigMapUpdates(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)
	alloc, _ := loadBigmap(ctx, false)
	args.ParseBlockIdent(ctx)

	typ := &micheline.Prim{}
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}
	addr := lookupAddress(ctx, alloc.AccountId).String()

	items, err := ctx.Indexer.ListBigMapUpdates(ctx.Context,
		alloc.BigMapId,
		args.SinceHeight+1,
		args.BlockHeight,
		chain.ExprHash{},
		nil,
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	blockHashes := make(map[int64]chain.BlockHash)

	resp := &ExplorerBigMapUpdateList{
		diff:    make([]ExplorerBigMapUpdate, 0, len(items)),
		expires: ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}
	for _, v := range items {
		k, err := v.GetKey()
		if err != nil {
			panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
		}

		h, ok := blockHashes[v.Height]
		if !ok {
			if h, err = ctx.Indexer.BlockHashByHeight(ctx.Context, v.Height); err == nil {
				blockHashes[v.Height] = h
			}
		}

		upd := ExplorerBigMapUpdate{
			Action: v.Action,
			ExplorerBigmapValue: ExplorerBigmapValue{
				Key:       k,
				KeyHash:   chain.NewExprHash(v.KeyHash),
				KeyBinary: k.Encode(),
				Meta: ExplorerBigmapMeta{
					Contract:     addr,
					BigMapId:     alloc.BigMapId,
					UpdateTime:   v.Timestamp,
					UpdateHeight: v.Height,
					UpdateBlock:  h,
					IsReplaced:   BoolPtr(v.IsReplaced),
					IsRemoved:    BoolPtr(v.IsDeleted),
				},
			},
		}
		switch v.Action {
		case micheline.BigMapDiffActionUpdate, micheline.BigMapDiffActionCopy:
			prim := &micheline.Prim{}
			upd.Value = &micheline.BigMapValue{
				Type:  typ,
				Value: prim,
			}
			if err = prim.UnmarshalBinary(v.Value); err != nil {
				log.Errorf("explorer: bigmap value unmarshal: %v", err)
			}

			if args.WithPrim() {
				upd.Prim = &ExplorerBigmapKeyValue{
					Key:   k.Prim(),
					Value: prim,
				}
			}
			if args.WithUnpack() {
				if prim.IsPackedAny() {
					if p, err := prim.UnpackAny(); err == nil {
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
		}
		resp.diff = append(resp.diff, upd)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}

func ListBigMapKeyUpdates(ctx *ApiContext) (interface{}, int) {
	args := &ContractRequest{}
	ctx.ParseRequestArgs(args)

	alloc, _ := loadBigmap(ctx, false)
	key := parseBigmapKey(ctx, alloc.KeyType.String())
	args.ParseBlockIdent(ctx)
	addr := lookupAddress(ctx, alloc.AccountId).String()

	typ := &micheline.Prim{}
	if err := typ.UnmarshalBinary(alloc.Value); err != nil {
		log.Errorf("explorer: bigmap type unmarshal: %v", err)
	}

	items, err := ctx.Indexer.ListBigMapUpdates(ctx,
		alloc.BigMapId,
		args.SinceHeight+1,
		args.BlockHeight,
		key.Hash,
		key.Bytes(),
		args.Offset,
		ctx.Cfg.ClampExplore(args.Limit),
	)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read bigmap", err))
	}

	blockHashes := make(map[int64]chain.BlockHash)

	resp := &ExplorerBigMapUpdateList{
		diff:    make([]ExplorerBigMapUpdate, 0, len(items)),
		expires: ctx.Crawler.Time().Add(ctx.Crawler.ParamsByHeight(-1).TimeBetweenBlocks[0]),
	}
	for _, v := range items {
		k, err := v.GetKey()
		if err != nil {
			panic(EInternal(EC_SERVER, "cannot decode bigmap key", err))
		}

		h, ok := blockHashes[v.Height]
		if !ok {
			if h, err = ctx.Indexer.BlockHashByHeight(ctx.Context, v.Height); err == nil {
				blockHashes[v.Height] = h
			}
		}

		upd := ExplorerBigMapUpdate{
			Action: v.Action,
			ExplorerBigmapValue: ExplorerBigmapValue{
				Key:       k,
				KeyHash:   chain.NewExprHash(v.KeyHash),
				KeyBinary: k.Encode(),
				Meta: ExplorerBigmapMeta{
					Contract:     addr,
					BigMapId:     alloc.BigMapId,
					UpdateTime:   v.Timestamp,
					UpdateHeight: v.Height,
					UpdateBlock:  h,
					IsReplaced:   BoolPtr(v.IsReplaced),
					IsRemoved:    BoolPtr(v.IsDeleted),
				},
			},
		}
		switch v.Action {
		case micheline.BigMapDiffActionUpdate, micheline.BigMapDiffActionCopy:
			prim := &micheline.Prim{}
			upd.Value = &micheline.BigMapValue{
				Type:  typ,
				Value: prim,
			}
			if err = prim.UnmarshalBinary(v.Value); err != nil {
				log.Errorf("explorer: bigmap value unmarshal: %v", err)
			}

			if args.WithPrim() {
				upd.Prim = &ExplorerBigmapKeyValue{
					Key:   k.Prim(),
					Value: prim,
				}
			}
			if args.WithUnpack() {
				if prim.IsPackedAny() {
					if p, err := prim.UnpackAny(); err == nil {
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
		}

		resp.diff = append(resp.diff, upd)
		resp.modified = v.Timestamp
	}

	return resp, http.StatusOK
}
