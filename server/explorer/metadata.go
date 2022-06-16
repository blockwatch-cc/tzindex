// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

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
	"blockwatch.cc/tzindex/server"
)

// keep a cache of all metadata
type (
	metaByAddressMap map[string]*Metadata // Address / AssetId
	metaByIdMap      map[uint64]*Metadata // AccountID | AssetId << 32
	payoutByBakerMap map[uint64][]uint64  // baker -> payout addresses
)

var (
	metaByAddressStore    atomic.Value
	metaByIdStore         atomic.Value
	payoutByBakerMapStore atomic.Value
	metaMutex             sync.Mutex
)

func init() {
	metaByAddressStore.Store(make(metaByAddressMap))
	metaByIdStore.Store(make(metaByIdMap))
	payoutByBakerMapStore.Store(make(payoutByBakerMap))
}

func purgeMetadataStore() {
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(make(metaByAddressMap))
	metaByIdStore.Store(make(metaByIdMap))
	payoutByBakerMapStore.Store(make(payoutByBakerMap))
}

func ensureMetdataIsLoaded(ctx *server.Context) {
	if len(metaByIdStore.Load().(metaByIdMap)) == 0 {
		_ = loadMetadata(ctx)
	}
}

func lookupMetadataById(ctx *server.Context, id model.AccountID, assetId int64, useAssetId bool) (*Metadata, bool) {
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
	// fail when stripped is empty to prevent return of empty stripped metadata
	if !ok || m.Stripped == nil {
		return nil, false
	}
	return m, ok
}

func lookupMetadataByAddress(ctx *server.Context, addr tezos.Address, asset int64, useAssetId bool) (*Metadata, bool) {
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

func allMetadataByAddress(ctx *server.Context) metaByAddressMap {
	metaMap := metaByAddressStore.Load().(metaByAddressMap)
	if len(metaMap) == 0 {
		_ = loadMetadata(ctx)
		metaMap = metaByAddressStore.Load().(metaByAddressMap)
	}
	return metaMap
}

func allMetadataById(ctx *server.Context) metaByIdMap {
	metaMap := metaByIdStore.Load().(metaByIdMap)
	if len(metaMap) == 0 {
		_ = loadMetadata(ctx)
		metaMap = metaByIdStore.Load().(metaByIdMap)
	}
	return metaMap
}

func cacheSingleMetadata(ctx *server.Context, meta *Metadata, remove bool) {
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

	// copy paymap
	p1 := payoutByBakerMapStore.Load().(payoutByBakerMap)
	p2 := make(payoutByBakerMap, len(p1)+1)
	for k, v := range p1 {
		vv := make([]uint64, 0, len(v))
		for i := range v {
			if remove && v[i] == meta.AccountId.Value() {
				continue
			}
			vv = append(vv, v[i])
		}
		p2[k] = vv
	}
	if !remove {
		for _, v := range meta.ListBakerIds(ctx) {
			// v = the baker
			// ids = its payout address ids
			ids, _ := p2[v]
			p2[v] = append(ids, meta.AccountId.Value())
		}
	}

	// replace
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(m2)
	metaByIdStore.Store(i2)
	payoutByBakerMapStore.Store(p2)
}

func cacheMultiMetadata(ctx *server.Context, meta []*Metadata) {
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

	// copy pay map
	p1 := payoutByBakerMapStore.Load().(payoutByBakerMap)
	p2 := make(payoutByBakerMap, len(p1)+len(meta))
	for k, v := range p1 {
		vv := make([]uint64, len(v))
		copy(vv, v)
		p2[k] = vv
	}

	// add new
	for _, v := range meta {
		m2[v.StringKey()] = v
		i2[v.Uint64Key()] = v
		for _, b := range v.ListBakerIds(ctx) {
			// b = the baker
			// ids = its payout address ids
			ids, _ := p2[b]
			p2[b] = append(ids, v.AccountId.Value())
		}
	}

	// replace
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(m2)
	metaByIdStore.Store(i2)
	payoutByBakerMapStore.Store(p2)
}

func loadMetadata(ctx *server.Context) error {
	addrMap := make(metaByAddressMap)
	idMap := make(metaByIdMap)
	payMap := make(payoutByBakerMap)

	table, err := ctx.Indexer.Table(index.MetadataTableKey)
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}

	md := &model.Metadata{}
	err = table.Stream(ctx.Context, pack.Query{}, func(r pack.Row) error {
		if err := r.Decode(md); err != nil {
			return err
		}
		meta := NewMetadata(md)
		addrMap[meta.StringKey()] = meta
		idMap[meta.Uint64Key()] = meta
		for _, v := range meta.ListBakerIds(ctx) {
			// v = the baker
			// ids = its payout address ids
			ids, _ := payMap[v]
			payMap[v] = append(ids, meta.AccountId.Value())
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("loading metadata: %w", err)
	}

	// replace
	metaMutex.Lock()
	defer metaMutex.Unlock()
	metaByAddressStore.Store(addrMap)
	metaByIdStore.Store(idMap)
	payoutByBakerMapStore.Store(payMap)
	return nil
}

type Metadata struct {
	// unique identity
	Address   tezos.Address   // must be valid
	AssetId   *int64          // may be 0
	AccountId model.AccountID // may be 0 for non-indexed accounts
	RowId     uint64          // metadata entry id (used for update/insert)

	// input only
	Parsed map[string]json.RawMessage

	// output only
	Raw      []byte
	Stripped []byte // alias, asset, baker, location, payout, social

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
	IsPayout       bool
}

var _ Sortable = (*Metadata)(nil)

func (m Metadata) Id() uint64 {
	return m.RowId
}

func (m *Metadata) Equal(x *Metadata) bool {
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

func (m Metadata) StringKey() string {
	key := m.Address.String()
	if m.AssetId != nil {
		key += "/" + strconv.FormatInt(*m.AssetId, 10)
	}
	return key
}

func (m Metadata) Uint64Key() uint64 {
	key := m.AccountId.Value()
	if m.AssetId != nil {
		key |= uint64(1)<<63 | uint64(*m.AssetId<<32)
	}
	return key
}

func (m Metadata) ListBakerIds(ctx *server.Context) []uint64 {
	if !m.IsPayout || m.AccountId == 0 {
		return nil
	}
	c := config.NewConfig()
	c.UseEnv(false)
	err := c.ReadConfig(m.Raw)
	if err != nil {
		return nil
	}
	var (
		pay metadata.Payout
		ids []uint64
	)
	_ = c.Unmarshal("payout", &pay)
	for _, v := range pay.From {
		a, err := ctx.Indexer.LookupAccount(ctx.Context, v)
		if err != nil {
			continue
		}
		ids = append(ids, a.RowId.Value())
	}
	return ids
}

func NewMetadata(m *model.Metadata) *Metadata {
	md := &Metadata{
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
			md.IsPayout = len(c.GetStringSlice("payout.from")) > 0
		}
	}

	// extract short version for anything that has a category and name
	if md.Kind != "" && md.Name != "" {
		var all map[string]json.RawMessage
		_ = json.Unmarshal(m.Content, &all)
		short := make(map[string]json.RawMessage)
		for _, n := range []string{
			"alias",
			"asset",
			"baker",
			"payout",
			"location",
			"social",
		} {
			d, ok := all[n]
			if !ok {
				continue
			}
			short[n] = d
		}
		short["address"] = []byte(strconv.Quote(m.Address.String()))
		if m.IsAsset {
			short["asset_id"] = []byte(strconv.Quote(strconv.FormatInt(m.AssetId, 10)))
		}
		md.Stripped, _ = json.Marshal(short)
		// log.Infof("Stripped MD for %s: %s %v", md.Address, string(md.Stripped), err)
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
	if len(m.Content) > 2 && m.Content[0] == '{' && m.Content[len(m.Content)-1] == '}' {
		b.WriteRune(',')
		b.Write(m.Content[1 : len(m.Content)-1])
	} else {
		log.Errorf("Skipping broken metadata for %s: %s", md.Address, string(m.Content))
	}
	b.WriteRune('}')
	md.Raw = []byte(b.String())
	return md
}

func (m Metadata) MarshalJSON() ([]byte, error) {
	return m.Raw, nil
}

func (m *Metadata) UnmarshalJSON(buf []byte) error {
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
				return fmt.Errorf("metadata: invalid address: %w", err)
			}
			addr, err := tezos.ParseAddress(str)
			if err != nil {
				return fmt.Errorf("metadata: invalid address: %w", err)
			}
			m.Address = addr
		case "asset_id":
			id, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return fmt.Errorf("metadata: invalid asset_id: %w", err)
			}
			m.AssetId = &id
		default:
			// will decode and validate later
			m.Parsed[n] = v
		}
	}
	return nil
}

func (m *Metadata) Validate(ctx *server.Context) error {
	for n, v := range m.Parsed {
		schema, ok := metadata.GetSchema(n)
		if !ok {
			return fmt.Errorf("metadata: unknown schema %s", n)
		}
		if err := schema.ValidateBytes(v); err != nil {
			return fmt.Errorf("metadata: validating %s failed: %w", n, err)
		}
	}
	return nil
}

func (m *Metadata) Short() *ShortMetadata {
	return (*ShortMetadata)(m)
}

type ShortMetadata Metadata

func (m *ShortMetadata) MarshalJSON() ([]byte, error) {
	return m.Stripped, nil
}

func (m *ShortMetadata) Id() uint64 {
	return m.RowId
}

var _ Sortable = (*ShortMetadata)(nil)

func init() {
	server.Register(Metadata{})
}

var _ server.RESTful = (*Metadata)(nil)

func (a Metadata) LastModified() time.Time {
	return time.Time{}
}

func (a Metadata) Expires() time.Time {
	return time.Time{}
}

func (a Metadata) RESTPrefix() string {
	return "/metadata"
}

func (a Metadata) RESTPath(r *mux.Router) string {
	path, _ := r.Get("meta").URLPath("ident", a.Address.String())
	return path.String()
}

func (a Metadata) RegisterDirectRoutes(r *mux.Router) error {
	r.HandleFunc(a.RESTPrefix(), server.C(ListMetadata)).Methods("GET")
	r.HandleFunc(a.RESTPrefix(), server.C(CreateMetadata)).Methods("POST")
	r.HandleFunc(a.RESTPrefix(), server.C(PurgeMetadata)).Methods("DELETE")
	return nil
}

func (b Metadata) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/schemas", server.C(ListMetadataSchemas)).Methods("GET")
	r.HandleFunc("/schemas/{schema}.json", server.C(ReadMetadataSchema)).Methods("GET")
	r.HandleFunc("/schemas/{schema}", server.C(ReadMetadataSchema)).Methods("GET")
	r.HandleFunc("/{ident}/{asset_id}", server.C(ReadMetadata)).Methods("GET")
	r.HandleFunc("/{ident}/{asset_id}", server.C(UpdateMetadata)).Methods("PUT")
	r.HandleFunc("/{ident}/{asset_id}", server.C(RemoveMetadata)).Methods("DELETE")
	r.HandleFunc("/{ident}", server.C(ReadMetadata)).Methods("GET").Name("meta")
	r.HandleFunc("/{ident}", server.C(UpdateMetadata)).Methods("PUT")
	r.HandleFunc("/{ident}", server.C(RemoveMetadata)).Methods("DELETE")
	return nil
}

func getMetadataFromUrl(ctx *server.Context) *Metadata {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing address", nil))
	}
	addr, err := tezos.ParseAddress(ident)
	if err != nil {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
	}
	var (
		assetId    int64
		useAssetId bool
	)
	if aid, ok := mux.Vars(ctx.Request)["asset_id"]; ok {
		assetId, err = strconv.ParseInt(aid, 10, 64)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid asset id", err))
		}
		useAssetId = true
	}
	meta, ok := lookupMetadataByAddress(ctx, addr, assetId, useAssetId)
	if !ok {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such metadata entry", nil))
	}
	return meta
}

func ReadMetadata(ctx *server.Context) (interface{}, int) {
	return getMetadataFromUrl(ctx), http.StatusOK
}

func ListMetadataSchemas(ctx *server.Context) (interface{}, int) {
	return metadata.ListSchemas(), http.StatusOK
}

func ReadMetadataSchema(ctx *server.Context) (interface{}, int) {
	ident, ok := mux.Vars(ctx.Request)["schema"]
	if !ok || ident == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing schema name", nil))
	}
	schema, ok := metadata.GetSchema(ident)
	if !ok {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "schema not found", nil))
	}
	return schema, http.StatusOK
}

type MetadataListRequest struct {
	ListRequest
	Short    bool         `schema:"short"`
	Kind     *string      `schema:"kind"`
	Status   *string      `schema:"status"`
	Country  *iso.Country `schema:"country"`
	Standard *string      `schema:"standard"`
}

func ListMetadata(ctx *server.Context) (interface{}, int) {
	args := &MetadataListRequest{ListRequest: ListRequest{Order: pack.OrderAsc}}
	ctx.ParseRequestArgs(args)
	meta := allMetadataByAddress(ctx)

	resp := make([]Sortable, 0, len(meta))
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
		if args.Short {
			if v.Stripped == nil || v.Kind == "other" {
				continue
			}
			resp = append(resp, v.Short())
		} else {
			resp = append(resp, v)
		}
	}
	// sort
	if args.Order == pack.OrderAsc {
		sort.Slice(resp, func(i, j int) bool { return resp[i].Id() < resp[j].Id() })
	} else {
		sort.Slice(resp, func(i, j int) bool { return resp[i].Id() > resp[j].Id() })
	}
	// limit and offset
	start := util.Min(int(args.Offset), len(resp))
	end := util.Min(start+int(args.Limit), len(resp))
	if end > 0 {
		resp = resp[start:end]
	}
	return resp, http.StatusOK
}

func CreateMetadata(ctx *server.Context) (interface{}, int) {
	meta := make([]*Metadata, 0)
	ctx.ParseRequestArgs(&meta)

	// 1  validate
	if config.GetBool("metadata.validate") {
		for i := range meta {
			if err := meta[i].Validate(ctx); err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("%s/%d: %v", meta[i].Address, meta[i].AssetId, err), nil))
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
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, "cannot create metadata", err))
		}
	}

	// 4 cache new inserted/updated metadata as new metadata objects
	toCache := make([]*Metadata, len(ups))
	for i, v := range ups {
		toCache[i] = NewMetadata(v)
	}
	cacheMultiMetadata(ctx, toCache)

	return toCache, http.StatusCreated
}

func UpdateMetadata(ctx *server.Context) (interface{}, int) {
	meta := getMetadataFromUrl(ctx)
	upd := &Metadata{}
	ctx.ParseRequestArgs(upd)
	if err := upd.Validate(ctx); err != nil {
		panic(server.EBadRequest(server.EC_PARAM_INVALID, err.Error(), nil))
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
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, "cannot update metadata", err))
		}
	}

	// cache and return the updated version
	upd = NewMetadata(md)
	cacheSingleMetadata(ctx, upd, false)
	return upd, http.StatusOK
}

func RemoveMetadata(ctx *server.Context) (interface{}, int) {
	meta := getMetadataFromUrl(ctx)
	err := ctx.Indexer.RemoveMetadata(ctx.Context, &model.Metadata{RowId: meta.RowId})
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot remove metadata", err))
	}
	cacheSingleMetadata(ctx, meta, true)
	return nil, http.StatusNoContent
}

func PurgeMetadata(ctx *server.Context) (interface{}, int) {
	err := ctx.Indexer.PurgeMetadata(ctx.Context)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot purge metadata", err))
	}
	purgeMetadataStore()
	return nil, http.StatusNoContent
}
