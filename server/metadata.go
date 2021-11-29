// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/echa/code/iso"
	"github.com/echa/config"
	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
)

// keep a cache of all metadata
type (
	metaByAddressMap map[string]*ExplorerMetadata // Address / AssetId
	metaByIdMap      map[uint64]*ExplorerMetadata // AccountID | AssetId << 32
)

var (
	metaByAddressStore atomic.Value
	metaByIdStore      atomic.Value
	metaMutex          sync.Mutex
)

func init() {
	metaByAddressStore.Store(make(metaByAddressMap))
	metaByIdStore.Store(make(metaByIdMap))
}

func purgeMetadataStore() {
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(make(metaByAddressMap))
	metaByIdStore.Store(make(metaByIdMap))
}

func lookupMetadataById(ctx *ApiContext, id model.AccountID, assetId int64, useAssetId bool) (*ExplorerMetadata, bool) {
	if id == 0 || assetId < 0 {
		return nil, false
	}
	metaMap := metaByIdStore.Load().(metaByIdMap)
	if len(metaMap) == 0 {
		_ = loadMetadata(ctx)
		metaMap = metaByIdStore.Load().(metaByIdMap)
	}
	key := id.Value()
	if useAssetId {
		key |= uint64(1)<<63 | uint64(assetId<<32)
	}
	m, ok := metaMap[key]
	return m, ok
}

func lookupMetadataByAddress(ctx *ApiContext, addr tezos.Address, asset int64, useAssetId bool) (*ExplorerMetadata, bool) {
	if !addr.IsValid() || asset < 0 {
		return nil, false
	}
	metaMap := metaByAddressStore.Load().(metaByAddressMap)
	if len(metaMap) == 0 {
		_ = loadMetadata(ctx)
		metaMap = metaByAddressStore.Load().(metaByAddressMap)
	}
	key := addr.String()
	if useAssetId {
		key += "/" + strconv.FormatInt(asset, 10)
	}
	m, ok := metaMap[key]
	return m, ok
}

func allMetadataByAddress(ctx *ApiContext) metaByAddressMap {
	metaMap := metaByAddressStore.Load().(metaByAddressMap)
	if len(metaMap) == 0 {
		_ = loadMetadata(ctx)
		metaMap = metaByAddressStore.Load().(metaByAddressMap)
	}
	return metaMap
}

func allMetadataById(ctx *ApiContext) metaByIdMap {
	metaMap := metaByIdStore.Load().(metaByIdMap)
	if len(metaMap) == 0 {
		_ = loadMetadata(ctx)
		metaMap = metaByIdStore.Load().(metaByIdMap)
	}
	return metaMap
}

func cacheSingleMetadata(ctx *ApiContext, meta *ExplorerMetadata, remove bool) {
	if meta == nil {
		return
	}

	// copy by-address map
	m1 := metaByAddressStore.Load().(metaByAddressMap)
	m2 := make(metaByAddressMap, len(m1)+1)
	for k, v := range m1 {
		if remove && v.Equal(meta) {
			continue
		}
		m2[k] = v
	}
	if !remove {
		m2[meta.StringKey()] = meta
	}

	// copy by-id map
	i1 := metaByIdStore.Load().(metaByIdMap)
	i2 := make(metaByIdMap, len(i1)+1)
	for k, v := range i1 {
		if remove && v.Equal(meta) {
			continue
		}
		i2[k] = v
	}
	if !remove {
		i2[meta.Uint64Key()] = meta
	}

	// replace
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(m2)
	metaByIdStore.Store(i2)
}

func cacheMultiMetadata(meta []*ExplorerMetadata) {
	if len(meta) == 0 {
		return
	}

	// copy by-address map
	m1 := metaByAddressStore.Load().(metaByAddressMap)
	m2 := make(metaByAddressMap, len(m1)+len(meta))
	for k, v := range m1 {
		m2[k] = v
	}

	// copy by-id map
	i1 := metaByIdStore.Load().(metaByIdMap)
	i2 := make(metaByIdMap, len(i1)+len(meta))
	for k, v := range i1 {
		i2[k] = v
	}

	// add new
	for _, v := range meta {
		m2[v.StringKey()] = v
		i2[v.Uint64Key()] = v
	}

	// replace
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(m2)
	metaByIdStore.Store(i2)
}

func loadMetadata(ctx *ApiContext) error {
	addrMap := make(metaByAddressMap)
	idMap := make(metaByIdMap)

	table, err := ctx.Indexer.Table(index.MetadataTableKey)
	if err != nil {
		return fmt.Errorf("metadata: %v", err)
	}

	md := &model.Metadata{}
	err = table.Stream(ctx.Context, pack.Query{}, func(r pack.Row) error {
		if err := r.Decode(md); err != nil {
			return err
		}
		meta := NewExplorerMetadata(md)
		addrMap[meta.StringKey()] = meta
		idMap[meta.Uint64Key()] = meta
		return nil
	})
	if err != nil {
		return fmt.Errorf("loading metadata: %v", err)
	}

	// replace
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(addrMap)
	metaByIdStore.Store(idMap)
	return nil
}

type ExplorerMetadata struct {
	// unique identity
	Address   tezos.Address   // must be valid
	AssetId   *int64          // may be 0
	AccountId model.AccountID // may be 0 for non-indexed accounts
	RowId     uint64          // metadata entry id (used for update/insert)

	// input only
	Parsed map[string]json.RawMessage

	// output only
	Raw []byte

	// filtered properties
	Name           string
	Description    string
	Logo           string
	IsValidator    bool
	Country        iso.Country
	Kind           string
	Status         string
	Symbol         string
	Standard       string
	MinDelegation  float64
	NonDelegatable bool
}

func (m *ExplorerMetadata) Equal(x *ExplorerMetadata) bool {
	if !m.Address.Equal(x.Address) {
		return false
	}
	if (m.AssetId == nil) != (x.AssetId == nil) {
		return false
	}
	if m.AssetId != nil && x.AssetId != nil {
		return *m.AssetId == *x.AssetId
	}
	return true
}

func (m ExplorerMetadata) StringKey() string {
	key := m.Address.String()
	if m.AssetId != nil {
		key += "/" + strconv.FormatInt(*m.AssetId, 10)
	}
	return key
}

func (m ExplorerMetadata) Uint64Key() uint64 {
	key := m.AccountId.Value()
	if m.AssetId != nil {
		key |= uint64(1)<<63 | uint64(*m.AssetId<<32)
	}
	return key
}

func NewExplorerMetadata(m *model.Metadata) *ExplorerMetadata {
	md := &ExplorerMetadata{
		Address:   m.Address.Clone(),
		AccountId: m.AccountId,
	}
	if m.IsAsset {
		id := m.AssetId
		md.AssetId = &id
	}

	// extract filter fields
	if len(m.Content) > 0 {
		// we reuse config library for convenience
		c := config.NewConfig()
		c.UseEnv(false)
		if err := c.ReadConfig(m.Content); err == nil {
			md.Name = c.GetString("alias.name")
			md.Description = c.GetString("alias.description")
			md.Logo = c.GetString("alias.logo")
			md.Country = iso.Country(c.GetString("location.country"))
			md.Kind = c.GetString("alias.kind")
			md.IsValidator = md.Kind == "validator"
			md.Status = c.GetString("baker.status")
			md.Symbol = c.GetString("asset.symbol")
			md.Standard = c.GetString("asset.standard")
			md.MinDelegation = c.GetFloat64("baker.min_delegation")
			md.NonDelegatable = c.GetBool("baker.non_delegatable")
		}
	}

	// build raw output
	var b strings.Builder
	b.WriteString(`{"address":"`)
	b.WriteString(m.Address.String())
	if m.IsAsset {
		b.WriteString(`","asset_id":`)
		b.WriteString(strconv.FormatInt(m.AssetId, 10))
	} else {
		b.WriteRune('"')
	}
	if len(m.Content) > 2 {
		b.WriteRune(',')
		b.Write(m.Content[1 : len(m.Content)-1])
	}
	b.WriteRune('}')
	md.Raw = []byte(b.String())
	return md
}

func (m ExplorerMetadata) MarshalJSON() ([]byte, error) {
	return m.Raw, nil
}

func (m *ExplorerMetadata) UnmarshalJSON(buf []byte) error {
	in := make(map[string]json.RawMessage)
	err := json.Unmarshal(buf, &in)
	if err != nil {
		return err
	}
	m.Parsed = make(map[string]json.RawMessage)
	for n, v := range in {
		switch n {
		case "address":
			str, err := strconv.Unquote(string(v))
			if err != nil {
				return fmt.Errorf("metadata: invalid address: %v", err)
			}
			addr, err := tezos.ParseAddress(str)
			if err != nil {
				return fmt.Errorf("metadata: invalid address: %v", err)
			}
			m.Address = addr
		case "asset_id":
			id, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return fmt.Errorf("metadata: invalid asset_id: %v", err)
			}
			m.AssetId = &id
		default:
			// will decode and validate later
			m.Parsed[n] = v
		}
	}
	return nil
}

func (m *ExplorerMetadata) Validate(ctx *ApiContext) error {
	for n, v := range m.Parsed {
		schema, ok := metadata.GetSchema(n)
		if !ok {
			return fmt.Errorf("metadata: unknown schema %s", n)
		}
		if err := schema.ValidateBytes(v); err != nil {
			return fmt.Errorf("metadata: validating %s failed: %v", n, err)
		}
	}
	return nil
}

func init() {
	register(ExplorerMetadata{})
}

var _ RESTful = (*ExplorerMetadata)(nil)

func (a ExplorerMetadata) LastModified() time.Time {
	return time.Time{}
}

func (a ExplorerMetadata) Expires() time.Time {
	return time.Time{}
}

func (a ExplorerMetadata) RESTPrefix() string {
	return "/metadata"
}

func (a ExplorerMetadata) RESTPath(r *mux.Router) string {
	path, _ := r.Get("meta").URLPath("ident", a.Address.String())
	return path.String()
}

func (a ExplorerMetadata) RegisterDirectRoutes(r *mux.Router) error {
	r.HandleFunc(a.RESTPrefix(), C(ListMetadata)).Methods("GET")
	r.HandleFunc(a.RESTPrefix(), C(CreateMetadata)).Methods("POST")
	r.HandleFunc(a.RESTPrefix(), C(PurgeMetadata)).Methods("DELETE")
	return nil
}

func (b ExplorerMetadata) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/schemas", C(ListMetadataSchemas)).Methods("GET")
	r.HandleFunc("/schemas/{schema}.json", C(ReadMetadataSchema)).Methods("GET")
	r.HandleFunc("/schemas/{schema}", C(ReadMetadataSchema)).Methods("GET")
	r.HandleFunc("/{ident}/{asset_id}", C(ReadMetadata)).Methods("GET")
	r.HandleFunc("/{ident}/{asset_id}", C(UpdateMetadata)).Methods("PUT")
	r.HandleFunc("/{ident}/{asset_id}", C(RemoveMetadata)).Methods("DELETE")
	r.HandleFunc("/{ident}", C(ReadMetadata)).Methods("GET").Name("meta")
	r.HandleFunc("/{ident}", C(UpdateMetadata)).Methods("PUT")
	r.HandleFunc("/{ident}", C(RemoveMetadata)).Methods("DELETE")
	return nil
}

func getMetadataFromUrl(ctx *ApiContext) *ExplorerMetadata {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing address", nil))
	}
	addr, err := tezos.ParseAddress(ident)
	if err != nil {
		panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid address", err))
	}
	var (
		assetId    int64
		useAssetId bool
	)
	if aid, ok := mux.Vars(ctx.Request)["asset_id"]; ok {
		assetId, err = strconv.ParseInt(aid, 10, 64)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid asset id", err))
		}
		useAssetId = true
	}
	meta, ok := lookupMetadataByAddress(ctx, addr, assetId, useAssetId)
	if !ok {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such metadata entry", nil))
	}
	return meta
}

func ReadMetadata(ctx *ApiContext) (interface{}, int) {
	return getMetadataFromUrl(ctx), http.StatusOK
}

func ListMetadataSchemas(ctx *ApiContext) (interface{}, int) {
	return metadata.ListSchemas(), http.StatusOK
}

func ReadMetadataSchema(ctx *ApiContext) (interface{}, int) {
	ident, ok := mux.Vars(ctx.Request)["schema"]
	if !ok || ident == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing schema name", nil))
	}
	schema, ok := metadata.GetSchema(ident)
	if !ok {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, "schema not found", nil))
	}
	return schema, http.StatusOK
}

type MetadataListRequest struct {
	ListRequest
	Kind     *string      `schema:"kind"`
	Status   *string      `schema:"status"`
	Country  *iso.Country `schema:"country"`
	Standard *string      `schema:"standard"`
}

func ListMetadata(ctx *ApiContext) (interface{}, int) {
	args := &MetadataListRequest{ListRequest: ListRequest{Order: pack.OrderAsc}}
	ctx.ParseRequestArgs(args)
	meta := allMetadataByAddress(ctx)

	resp := make([]*ExplorerMetadata, 0, len(meta))
	for _, v := range meta {
		// filter
		if args.Kind != nil && *args.Kind != v.Kind {
			continue
		}
		if args.Status != nil && *args.Status != v.Status {
			continue
		}
		if args.Country != nil && *args.Country != v.Country {
			continue
		}
		if args.Standard != nil && *args.Standard != v.Standard {
			continue
		}
		if args.Cursor > 0 {
			if args.Order == pack.OrderAsc && args.Cursor <= v.RowId {
				continue
			}
			if args.Order == pack.OrderDesc && args.Cursor >= v.RowId {
				continue
			}
		}
		resp = append(resp, v)
	}
	// sort
	if args.Order == pack.OrderAsc {
		sort.Slice(resp, func(i, j int) bool { return resp[i].RowId < resp[j].RowId })
	} else {
		sort.Slice(resp, func(i, j int) bool { return resp[i].RowId > resp[j].RowId })
	}
	// limit and offset
	start := util.Min(int(args.Offset), len(resp))
	end := util.Min(start+int(args.Limit), len(resp))
	if end > 0 {
		resp = resp[start:end]
	}
	return resp, http.StatusOK
}

func CreateMetadata(ctx *ApiContext) (interface{}, int) {
	meta := make([]*ExplorerMetadata, 0)
	ctx.ParseRequestArgs(&meta)

	// 1  validate
	if config.GetBool("metadata.validate") {
		for i := range meta {
			if err := meta[i].Validate(ctx); err != nil {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("%s/%d: %v", meta[i].Address, meta[i].AssetId, err.Error()), nil))
			}
		}
	}

	// 2 fill account ids if exist
	for _, m := range meta {
		if acc, err := ctx.Indexer.LookupAccount(ctx.Context, m.Address); err == nil {
			m.AccountId = acc.RowId
		}
	}

	// 3 upsert
	ups := make([]*model.Metadata, len(meta))
	for i, v := range meta {
		ups[i] = &model.Metadata{
			RowId:     v.RowId,
			AccountId: v.AccountId,
			Address:   v.Address.Clone(),
		}
		if v.AssetId != nil {
			ups[i].IsAsset = true
			ups[i].AssetId = *v.AssetId
		}
		ups[i].Content, _ = json.Marshal(v.Parsed)
	}
	if err := ctx.Indexer.UpsertMetadata(ctx.Context, ups); err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
		default:
			panic(EInternal(EC_DATABASE, "cannot create metadata", err))
		}
	}

	// 4 cache new inserted/updated metadata as new metadata objects
	toCache := make([]*ExplorerMetadata, len(ups))
	for i, v := range ups {
		toCache[i] = NewExplorerMetadata(v)
	}
	cacheMultiMetadata(toCache)

	return toCache, http.StatusCreated
}

func UpdateMetadata(ctx *ApiContext) (interface{}, int) {
	meta := getMetadataFromUrl(ctx)
	upd := &ExplorerMetadata{}
	ctx.ParseRequestArgs(upd)
	if err := upd.Validate(ctx); err != nil {
		panic(EBadRequest(EC_PARAM_INVALID, err.Error(), nil))
	}

	// copy identifiers
	md := &model.Metadata{
		RowId:     meta.RowId,
		Address:   meta.Address,
		AccountId: meta.AccountId,
	}
	if meta.AssetId != nil {
		md.IsAsset = true
		md.AssetId = *meta.AssetId
	}
	md.Content, _ = json.Marshal(upd.Parsed)

	if err := ctx.Indexer.UpdateMetadata(ctx.Context, md); err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
		default:
			panic(EInternal(EC_DATABASE, "cannot update metadata", err))
		}
	}

	// cache and return the updated version
	upd = NewExplorerMetadata(md)
	cacheSingleMetadata(ctx, upd, false)
	return upd, http.StatusOK
}

func RemoveMetadata(ctx *ApiContext) (interface{}, int) {
	meta := getMetadataFromUrl(ctx)
	err := ctx.Indexer.RemoveMetadata(ctx.Context, &model.Metadata{RowId: meta.RowId})
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot remove metadata", err))
	}
	cacheSingleMetadata(ctx, meta, true)
	return nil, http.StatusNoContent
}

func PurgeMetadata(ctx *ApiContext) (interface{}, int) {
	err := ctx.Indexer.PurgeMetadata(ctx.Context)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot purge metadata", err))
	}
	purgeMetadataStore()
	return nil, http.StatusNoContent
}
