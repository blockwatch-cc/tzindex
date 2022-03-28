// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"fmt"
	"math/bits"
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
	Raw []byte

	// filtered properties
	Name           string
	Description    string
	Logo           string
	IsSponsored    bool
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
			md.IsSponsored = c.GetBool("baker.sponsored")
			md.Status = c.GetString("baker.status")
			md.Symbol = c.GetString("asset.symbol")
			md.Standard = c.GetString("asset.standard")
			md.MinDelegation = c.GetFloat64("baker.min_delegation")
			md.NonDelegatable = c.GetBool("baker.non_delegatable")
			md.IsPayout = len(c.GetStringSlice("payout.from")) > 0
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
	r.HandleFunc("/sitemap", server.C(SitemapMetadata)).Methods("GET")
	r.HandleFunc("/describe/{ident}", server.C(DescribeMetadata)).Methods("GET")
	r.HandleFunc("/describe/{ident}/{num}", server.C(DescribeMetadata)).Methods("GET")
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
	Kind     *string      `schema:"kind"`
	Status   *string      `schema:"status"`
	Country  *iso.Country `schema:"country"`
	Standard *string      `schema:"standard"`
}

func ListMetadata(ctx *server.Context) (interface{}, int) {
	args := &MetadataListRequest{ListRequest: ListRequest{Order: pack.OrderAsc}}
	ctx.ParseRequestArgs(args)
	meta := allMetadataByAddress(ctx)

	resp := make([]*Metadata, 0, len(meta))
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

type MetadataDescriptor struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Image       string `json:"image"`
}

const metaDateTime = "2006-01-02 15:04:05"

// Describes an on-chain object with title, description and image.
//
// Supported identifiers
//
// :address      address, contract, token
// :block        block hash or height
// :op           operation hash
// /event/:id    implicit operation
// /cycle/:id    cycle number
// /election/:id election number
//
func DescribeMetadata(ctx *server.Context) (interface{}, int) {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing identifier", nil))
	}
	num, _ := mux.Vars(ctx.Request)["num"]

	var desc MetadataDescriptor

	switch ident {
	case "event":
		ops, err := ctx.Indexer.LookupOp(ctx, num)
		if err != nil {
			switch err {
			case index.ErrNoOpEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such operation", err))
			case index.ErrInvalidOpID:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid event id", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		desc = DescribeOp(ctx, ops)
	default:
		// detect type
		if a, err := tezos.ParseAddress(ident); err == nil {
			desc = DescribeAddress(ctx, a)
		} else if _, err := tezos.ParseBlockHash(ident); err == nil {
			desc = DescribeBlock(ctx, ident)
		} else if _, err := strconv.ParseInt(ident, 10, 64); err == nil {
			desc = DescribeBlock(ctx, ident)
		} else if _, err := tezos.ParseOpHash(ident); err == nil {
			ops := loadOps(ctx)
			desc = DescribeOp(ctx, ops)
		} else {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "unsupported identifier", nil))
		}
	}

	// add title suffix to everything
	if s := config.GetString("metadata.describe.title_suffix"); s != "" {
		desc.Title += " " + s
	}

	// image url prefix in config
	if s := config.GetString("metadata.describe.image_url"); s != "" {
		if !strings.HasPrefix(desc.Image, "http") {
			if !strings.HasPrefix(desc.Image, "/") && !strings.HasSuffix(s, "/") {
				s += "/"
			}
			desc.Image = s + desc.Image
		}
	}

	return desc, http.StatusOK
}

func DescribeAddress(ctx *server.Context, addr tezos.Address) MetadataDescriptor {
	d := MetadataDescriptor{
		Title: addr.Short(),
		Image: config.GetString("metadata.describe.logo"),
	}
	meta, ok := lookupMetadataByAddress(ctx, addr, 0, false)
	if ok {
		// baker, token, other
		d.Title = meta.Name
		d.Description = meta.Description
		if meta.Logo != "" {
			d.Image = meta.Logo
		}
		if d.Title == "" {
			d.Title = fmt.Sprintf("%s", addr.Short())
		}
		if d.Description == "" {
			switch meta.Kind {
			case "validator":
				d.Description = fmt.Sprintf("%s Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					strings.Title(meta.Status), addr)
			case "token":
				d.Description = fmt.Sprintf("%s (%s) token tracker. Address %s. View token activity, supply, numbers of holders and more details.",
					meta.Name, meta.Symbol, addr)
			default:
				d.Description = fmt.Sprintf("Tezos address %s. View account balance, transactions and analytics.", addr)
			}
		}
	} else {
		if bkr, err := ctx.Indexer.LookupBaker(ctx, addr); err == nil {
			if bkr.IsActive {
				d.Description = fmt.Sprintf("Private Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					addr)
			} else {
				d.Description = fmt.Sprintf("Closed Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					addr)
			}
		}
		// normal account fallback
		if acc, err := ctx.Indexer.LookupAccount(ctx, addr); err == nil {
			switch {
			case acc.IsContract:
				d.Description = fmt.Sprintf("Tezos smart contract. Address %s. View contract activity, code and analytics.",
					addr)
			case acc.IsBaker:
				d.Description = fmt.Sprintf("Private Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					addr)
			}
		}
		if d.Description == "" {
			d.Description = fmt.Sprintf("Tezos address %s. View account balance, transactions and analytics.", addr)
		}
	}

	return d
}

func DescribeBlock(ctx *server.Context, ident string) MetadataDescriptor {
	block := loadBlock(ctx)
	d := MetadataDescriptor{
		Title: fmt.Sprintf("Tezos Block %s", util.PrettyInt64(block.Height)),
		Image: config.GetString("metadata.describe.logo"),
	}
	bakerName := ctx.Indexer.LookupAddress(ctx, block.BakerId).String()
	if meta, ok := lookupMetadataById(ctx, block.BakerId, 0, false); ok {
		bakerName = meta.Name
	}
	d.Description = fmt.Sprintf("Tezos block %s – Baked by %s on %s. Hash: %s.",
		util.PrettyInt64(block.Height),
		bakerName,
		block.Timestamp.Format(metaDateTime),
		block.Hash,
	)
	return d
}

func DescribeOp(ctx *server.Context, ops []*model.Op) MetadataDescriptor {
	d := MetadataDescriptor{
		Image: config.GetString("metadata.describe.logo"),
	}

	// use the first op by default, unless its a batched reveal
	op := ops[0]
	if op.Type == model.OpTypeReveal && len(ops) > 1 {
		ops = ops[1:]
		op = ops[0]
	}

	sender := ctx.Indexer.LookupAddress(ctx, op.SenderId).Short()
	if meta, ok := lookupMetadataById(ctx, op.SenderId, 0, false); ok {
		sender = meta.Name
		d.Image = meta.Logo
	}
	receiver := ctx.Indexer.LookupAddress(ctx, op.ReceiverId).Short()
	if meta, ok := lookupMetadataById(ctx, op.ReceiverId, 0, false); ok {
		receiver = meta.Name
	}

	switch op.Type {
	case model.OpTypeActivation:
		d.Title = fmt.Sprintf("Tezos Account %s Activation",
			sender,
		)
		d.Description = fmt.Sprintf("%s XTZ activated on %s (block %s in cycle %s).",
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			util.PrettyInt64(op.Cycle),
		)

	case model.OpTypeDoubleBaking:
		d.Title = fmt.Sprintf("Tezos Double Baking Evidence %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Evidence of double baking in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case model.OpTypeDoubleEndorsement:
		d.Title = fmt.Sprintf("Tezos Double Endorsement Evidence %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Evidence of double endorsing in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case model.OpTypeDoublePreendorsement:
		d.Title = fmt.Sprintf("Tezos Double Preendorsement Evidence %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Evidence of double preendorsing in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case model.OpTypeNonceRevelation:
		d.Title = fmt.Sprintf("Tezos Seed-Nonce Revelation %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Seed nonce revealed by %s on %s (block %s in cycle %s). Baked by %s, reward: %s XTZ.",
			receiver,
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			util.PrettyInt64(op.Cycle),
			sender,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
		)

	case model.OpTypeTransaction:
		if op.IsContract {
			d.Title = fmt.Sprintf("Tezos Contract Call %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Called %s in contract %s on %s. Status: %s.",
				op.Data,
				receiver,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		} else if len(ops) > 1 {
			var sum int64
			for _, v := range ops {
				sum += v.Volume
			}
			d.Title = fmt.Sprintf("Tezos Batch Operation %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Batch-sent %s XTZ from %s on %s. Status: %s.",
				util.PrettyFloat64(ctx.Params.ConvertValue(sum)),
				sender,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		} else {
			d.Title = fmt.Sprintf("Tezos Transaction %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Sent %s XTZ from %s to %s on %s. Status: %s.",
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				sender,
				receiver,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		}

	case model.OpTypeOrigination:
		d.Title = fmt.Sprintf("Tezos Origination %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Sender: %s. Contract: %s. Time %s. Status %s.",
			sender,
			receiver,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeDelegation:
		if op.SenderId == op.BakerId {
			d.Title = fmt.Sprintf("Tezos Baker Registration %s", op.Hash.Short())
			d.Description = fmt.Sprintf("New baker %s registered on %s (cycle %s). Status: %s.",
				sender,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)
		} else if op.BakerId == 0 {
			d.Title = fmt.Sprintf("Tezos Delegation Removal %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Delegator %s left %s on %s (cycle %s). Status: %s.",
				sender,
				receiver,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)

		} else {
			baker := ctx.Indexer.LookupAddress(ctx, op.BakerId).Short()
			if meta, ok := lookupMetadataById(ctx, op.BakerId, 0, false); ok {
				baker = meta.Name
			}
			d.Title = fmt.Sprintf("Tezos Delegation %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Delegation from %s to %s on %s (cycle %s). Status: %s.",
				sender,
				baker,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)
		}

	case model.OpTypeReveal:
		d.Title = fmt.Sprintf("Tezos Public Key Revelation %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Public key revealed by %s on %s (block %s). Key: %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			op.Data,
		)

	case model.OpTypeEndorsement:
		mask, _ := strconv.ParseUint(op.Data, 10, 64)
		d.Title = fmt.Sprintf("Tezos Endorsement %s", op.Hash.Short())
		d.Description = fmt.Sprintf("%s endorsed %d slots in block %s on %s. Deposit: %s XTZ. Reward: %s XTZ.",
			sender,
			bits.OnesCount64(mask),
			util.PrettyInt64(op.Height-1),
			op.Timestamp.Format(metaDateTime),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Deposit)),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
		)

	case model.OpTypeProposal:
		d.Title = fmt.Sprintf("Tezos Proposal Vote %s", op.Hash.Short())
		d.Description = fmt.Sprintf("%s upvoted %s on %s.",
			sender,
			op.Data,
			op.Timestamp.Format(metaDateTime),
		)

	case model.OpTypeBallot:
		data := strings.Split(op.Data, ",")
		d.Title = fmt.Sprintf("Tezos Ballot %s", op.Hash.Short())
		d.Description = fmt.Sprintf("%s voted %s for %s on %s.",
			sender,
			data[1],
			data[0],
			op.Timestamp.Format(metaDateTime),
		)

	// events
	case model.OpTypeBake:
		d.Title = fmt.Sprintf("Tezos Baking Event")
		d.Description = fmt.Sprintf("Baker %s baked block %s on %s.",
			sender,
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
		)
	case model.OpTypeUnfreeze:
		d.Title = fmt.Sprintf("Tezos Unfreeze Event")
		d.Description = fmt.Sprintf("Unfrozen: %.06f XTZ deposits, %.06f XTZ rewards, and %.06f XTZ fees for %s in cycle %s.",
			ctx.Params.ConvertValue(op.Deposit),
			ctx.Params.ConvertValue(op.Reward),
			ctx.Params.ConvertValue(op.Fee),
			sender,
			util.PrettyInt64(op.Cycle),
		)
	case model.OpTypeInvoice:
		d.Title = fmt.Sprintf("Tezos Invoice Event")
		d.Description = fmt.Sprintf("%s received %s XTZ for their contributions to a Tezos protocol upgrade.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
		)
	case model.OpTypeAirdrop:
		d.Title = fmt.Sprintf("Tezos Airdrop Event")
		d.Description = fmt.Sprintf("%s protocol migration airdrop of %s XTZ to %s.",
			"", // TODO
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			receiver,
		)
	case model.OpTypeSeedSlash:
		d.Title = fmt.Sprintf("Tezos Seed Slash Event")
		d.Description = fmt.Sprintf("%s lost %s XTZ rewards and %s XTZ fees in cycle %s.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Fee)),
			util.PrettyInt64(op.Cycle),
		)
	case model.OpTypeMigration:
		d.Title = fmt.Sprintf("Tezos Migration Event")
		d.Description = fmt.Sprintf("Contract %s migrated on block %s.",
			receiver,
			util.PrettyInt64(op.Height),
		)
	}
	return d
}

func SitemapMetadata(ctx *server.Context) (interface{}, int) {
	m := allMetadataByAddress(ctx)
	addrs := make([]string, 0, len(m))
	for n, _ := range m {
		addrs = append(addrs, n)
	}
	return addrs, http.StatusOK
}
