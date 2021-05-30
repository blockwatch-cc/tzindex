// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

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

func cacheSingleMetadata(meta *ExplorerMetadata, remove bool) {
	if meta == nil {
		return
	}

	// copy by-address map
	m2 := make(metaByAddressMap)
	for k, v := range metaByAddressStore.Load().(metaByAddressMap) {
		if remove && v.Equal(meta) {
			continue
		}
		m2[k] = v
	}
	if !remove {
		m2[meta.StringKey()] = meta
	}

	// copy by-id map
	i2 := make(metaByIdMap)
	for k, v := range metaByIdStore.Load().(metaByIdMap) {
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
	m2 := make(metaByAddressMap)
	for k, v := range metaByAddressStore.Load().(metaByAddressMap) {
		m2[k] = v
	}

	// copy by-id map
	i2 := make(metaByIdMap)
	for k, v := range metaByIdStore.Load().(metaByIdMap) {
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
	r.HandleFunc("/sitemap", C(SitemapMetadata)).Methods("GET")
	r.HandleFunc("/describe/{ident}", C(DescribeMetadata)).Methods("GET")
	r.HandleFunc("/describe/{ident}/{num}", C(DescribeMetadata)).Methods("GET")
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
			panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, err.Error(), nil))
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
			panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, err.Error(), nil))
		default:
			panic(EInternal(EC_DATABASE, "cannot update metadata", err))
		}
	}

	// cache and return the updated version
	upd = NewExplorerMetadata(md)
	cacheSingleMetadata(upd, false)
	return upd, http.StatusOK
}

func RemoveMetadata(ctx *ApiContext) (interface{}, int) {
	meta := getMetadataFromUrl(ctx)
	err := ctx.Indexer.RemoveMetadata(ctx.Context, &model.Metadata{RowId: meta.RowId})
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot remove metadata", err))
	}
	cacheSingleMetadata(meta, true)
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
// :address     address, contract, token
// :block       block hash or height
// :op          operation hash
// /event/:id   implicit operation
// /cycle/:id   cycle number
//
func DescribeMetadata(ctx *ApiContext) (interface{}, int) {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing identifier", nil))
	}
	num, _ := mux.Vars(ctx.Request)["num"]

	var desc MetadataDescriptor

	switch ident {
	case "event":
		ops, err := ctx.Indexer.LookupOp(ctx, num)
		if err != nil {
			switch err {
			case index.ErrNoOpEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such operation", err))
			case index.ErrInvalidOpID:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid event id", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		desc = DescribeOp(ctx, ops)
	case "cycle":
		c, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid cycle identifier", nil))
		}
		desc = DescribeCycle(ctx, c)
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
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "unsupported identifier", nil))
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

func DescribeAddress(ctx *ApiContext, addr tezos.Address) MetadataDescriptor {
	d := MetadataDescriptor{
		Title: addr.Short(),
		Image: config.GetString("metadata.describe.logo"),
	}
	meta, ok := lookupMetadataByAddress(ctx, addr, 0, false)
	if ok {
		// baker, token, other
		d.Title = meta.Name
		d.Description = meta.Description
		d.Image = meta.Logo
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
		// normal account fallback
		if acc, err := ctx.Indexer.LookupAccount(ctx, addr); err == nil {
			switch {
			case acc.IsContract:
				d.Description = fmt.Sprintf("Tezos smart contract. Address %s. View contract activity, code and analytics.",
					addr)
			case acc.IsActiveDelegate:
				d.Description = fmt.Sprintf("Private Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					addr)
			case acc.IsDelegate:
				d.Description = fmt.Sprintf("Closed Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					addr)
			}
		}
		if d.Description == "" {
			d.Description = fmt.Sprintf("Tezos address %s. View account balance, transactions and analytics.", addr)
		}
	}

	return d
}

func DescribeBlock(ctx *ApiContext, ident string) MetadataDescriptor {
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

func DescribeOp(ctx *ApiContext, ops []*model.Op) MetadataDescriptor {
	d := MetadataDescriptor{
		Image: config.GetString("metadata.describe.logo"),
	}

	// use the first op by default, unless its a batched reveal
	op := ops[0]
	if op.Type == tezos.OpTypeReveal && len(ops) > 1 {
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
	case tezos.OpTypeActivateAccount:
		d.Title = fmt.Sprintf("Tezos Account %s Activation",
			sender,
		)
		d.Description = fmt.Sprintf("%s XTZ activated on %s (block %s in cycle %s).",
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			util.PrettyInt64(op.Cycle),
		)

	case tezos.OpTypeDoubleBakingEvidence:
		d.Title = fmt.Sprintf("Tezos Double Baking Evidence %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Evidence of double baking in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case tezos.OpTypeDoubleEndorsementEvidence:
		d.Title = fmt.Sprintf("Tezos Double Endorsing Evidence %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Evidence of double endorsing in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case tezos.OpTypeSeedNonceRevelation:
		d.Title = fmt.Sprintf("Tezos Seed-Nonce Revelation %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Seed nonce revealed by %s on %s (block %s in cycle %s). Baked by %s, reward: %s XTZ.",
			receiver,
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			util.PrettyInt64(op.Cycle),
			sender,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
		)

	case tezos.OpTypeTransaction:
		if op.IsContract {
			d.Title = fmt.Sprintf("Tezos Contract Call %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Called %s in contract %s on %s. Status: %s.",
				op.Data,
				receiver,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		} else if op.IsBatch {
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

	case tezos.OpTypeOrigination:
		d.Title = fmt.Sprintf("Tezos Origination %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Sender: %s. Contract: %s. Time %s. Status %s.",
			sender,
			receiver,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case tezos.OpTypeDelegation:
		if op.SenderId == op.DelegateId {
			d.Title = fmt.Sprintf("Tezos Baker Registration %s", op.Hash.Short())
			d.Description = fmt.Sprintf("New baker %s registered on %s (cycle %s). Status: %s.",
				sender,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)
		} else if op.DelegateId == 0 {
			d.Title = fmt.Sprintf("Tezos Delegation Removal %s", op.Hash.Short())
			d.Description = fmt.Sprintf("Delegator %s left %s on %s (cycle %s). Status: %s.",
				sender,
				receiver,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)

		} else {
			baker := ctx.Indexer.LookupAddress(ctx, op.DelegateId).Short()
			if meta, ok := lookupMetadataById(ctx, op.DelegateId, 0, false); ok {
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

	case tezos.OpTypeReveal:
		d.Title = fmt.Sprintf("Tezos Public Key Revelation %s", op.Hash.Short())
		d.Description = fmt.Sprintf("Public key revealed by %s on %s (block %s). Key: %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			op.Data,
		)

	case tezos.OpTypeEndorsement:
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

	case tezos.OpTypeProposals:
		d.Title = fmt.Sprintf("Tezos Proposal Vote %s", op.Hash.Short())
		d.Description = fmt.Sprintf("%s upvoted %s on %s.",
			sender,
			op.Data,
			op.Timestamp.Format(metaDateTime),
		)

	case tezos.OpTypeBallot:
		data := strings.Split(op.Data, ",")
		d.Title = fmt.Sprintf("Tezos Ballot %s", op.Hash.Short())
		d.Description = fmt.Sprintf("%s voted %s for %s on %s.",
			sender,
			data[1],
			data[0],
			op.Timestamp.Format(metaDateTime),
		)

	// events
	case tezos.OpTypeBake:
		d.Title = fmt.Sprintf("Tezos Baking Event")
		d.Description = fmt.Sprintf("Baker %s baked block %s on %s.",
			sender,
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
		)
	case tezos.OpTypeUnfreeze:
		d.Title = fmt.Sprintf("Tezos Unfreeze Event")
		d.Description = fmt.Sprintf("Unfrozen: %.06f XTZ deposits, %.06f XTZ rewards, and %.06f XTZ fees for %s in cycle %s.",
			ctx.Params.ConvertValue(op.Deposit),
			ctx.Params.ConvertValue(op.Reward),
			ctx.Params.ConvertValue(op.Fee),
			sender,
			util.PrettyInt64(op.Cycle),
		)
	case tezos.OpTypeInvoice:
		d.Title = fmt.Sprintf("Tezos Invoice Event")
		d.Description = fmt.Sprintf("%s received %s XTZ for their contributions to a Tezos protocol upgrade.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
		)
	case tezos.OpTypeAirdrop:
		d.Title = fmt.Sprintf("Tezos Airdrop Event")
		d.Description = fmt.Sprintf("%s protocol migration airdrop of %s XTZ to %s.",
			"", // TODO
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			receiver,
		)
	case tezos.OpTypeSeedSlash:
		d.Title = fmt.Sprintf("Tezos Seed Slash Event")
		d.Description = fmt.Sprintf("%s lost %s XTZ rewards and %s XTZ fees in cycle %s.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Fee)),
			util.PrettyInt64(op.Cycle),
		)
	case tezos.OpTypeMigration:
		d.Title = fmt.Sprintf("Tezos Migration Event")
		d.Description = fmt.Sprintf("Contract %s migrated on block %s.",
			receiver,
			util.PrettyInt64(op.Height),
		)
	}
	return d
}

func DescribeCycle(ctx *ApiContext, cycle int64) MetadataDescriptor {
	c := lookupOrBuildCycle(ctx, cycle)
	d := MetadataDescriptor{
		Title: fmt.Sprintf("Tezos Cycle %s on TzStats", util.PrettyInt64(cycle)),
		Image: config.GetString("metadata.describe.logo"),
		Description: fmt.Sprintf("Tezos Cycle %s – Running from %s to %s.",
			util.PrettyInt64(cycle),
			c.StartTime.Format(metaDateTime),
			c.EndTime.Format(metaDateTime),
		),
	}
	return d
}

func SitemapMetadata(ctx *ApiContext) (interface{}, int) {
	m := allMetadataByAddress(ctx)
	addrs := make([]string, 0, len(m))
	for n, _ := range m {
		addrs = append(addrs, n)
	}
	return addrs, http.StatusOK
}
