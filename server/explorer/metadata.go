// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/echa/code/iso"
	"github.com/echa/config"
	"github.com/gorilla/mux"
	lru "github.com/hashicorp/golang-lru"
	"github.com/tidwall/gjson"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var (
	// token: AccountID | AssetId << 32 -> *Metadata
	// address: AccountID -> *Metadata
	metadataCache *lru.TwoQueueCache
)

const MetadataCacheSize = 8192

func init() {
	metadataCache, _ = lru.New2Q(MetadataCacheSize)
}

func purgeMetadataStore() {
	metadataCache.Purge()
}

func lookupAddressIdMetadata(ctx *server.Context, id model.AccountID) (*Metadata, bool) {
	if id == 0 {
		return nil, false
	}
	val, ok := metadataCache.Get(id)
	if ok {
		return val.(*Metadata), true
	}
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		return nil, false
	}
	if table.Stats()[0].TupleCount == 0 {
		return nil, false
	}
	md := &model.Metadata{}
	err = pack.NewQuery("metadata.find").
		WithTable(table).
		AndEqual("account_id", id).
		Execute(ctx.Context, md)
	if err != nil || md.RowId == 0 {
		return nil, false
	}
	m := NewMetadata(md)
	metadataCache.Add(id, m)
	return m, true
}

func lookupTokenIdMetadata(ctx *server.Context, id model.AccountID, tid tezos.Z) (*Metadata, bool) {
	if id == 0 {
		return nil, false
	}
	key := id.U64() | uint64(tid.Int64()<<32) // breaks > 4Bn accounts
	val, ok := metadataCache.Get(key)
	if ok {
		return val.(*Metadata), true
	}
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		return nil, false
	}
	if table.Stats()[0].TupleCount == 0 {
		return nil, false
	}
	md := &model.Metadata{}
	err = pack.NewQuery("metadata.find").
		WithTable(table).
		AndEqual("account_id", id).
		AndEqual("asset_id", tid.Int64()).
		Execute(ctx.Context, md)
	if err != nil || md.RowId == 0 {
		return nil, false
	}
	m := NewMetadata(md)
	metadataCache.Add(key, m)
	return m, true
}

func lookupAddressMetadata(ctx *server.Context, addr tezos.Address) (*Metadata, bool) {
	var id model.AccountID
	id, err := ctx.Indexer.LookupAccountId(ctx, addr)
	if err != nil {
		return nil, false
	}
	return lookupAddressIdMetadata(ctx, id)
}

func lookupTokenMetadata(ctx *server.Context, addr tezos.Token) (*Metadata, bool) {
	var id model.AccountID
	id, err := ctx.Indexer.LookupAccountId(ctx, addr.Contract())
	if err != nil {
		return nil, false
	}
	return lookupTokenIdMetadata(ctx, id, addr.TokenId())
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

func (m Metadata) Key() uint64 {
	if m.AssetId != nil {
		return m.AccountId.U64() | uint64(*m.AssetId<<32) // breaks > 4Bn accounts
	}
	return m.AccountId.U64()
}

func (m Metadata) IsToken() bool {
	return m.AssetId != nil
}

func (m Metadata) IsAddress() bool {
	return m.AssetId == nil
}

func (m Metadata) Equal(x *Metadata) bool {
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

func NewMetadata(m *model.Metadata) *Metadata {
	md := &Metadata{
		RowId:     m.RowId,
		Address:   m.Address.Clone(),
		AccountId: m.AccountId,
	}
	if m.IsAsset {
		id := m.AssetId
		md.AssetId = &id
	}

	// extract filter fields
	if len(m.Content) > 0 {
		c := gjson.ParseBytes(m.Content)
		md.Name = c.Get("alias.name").String()
		md.Description = c.Get("alias.description").String()
		md.Logo = c.Get("alias.logo").String()
		md.Country = iso.Country(c.Get("location.country").String())
		md.Kind = c.Get("alias.kind").String()
		md.IsValidator = md.Kind == "validator"
		md.Status = c.Get("baker.status").String()
		md.Symbol = c.Get("asset.symbol").String()
		md.Standard = c.Get("asset.standard").String()
		md.MinDelegation = c.Get("baker.min_delegation").Float()
		md.NonDelegatable = c.Get("baker.non_delegatable").Bool()
		md.IsPayout = len(c.Get("payout").Array()) > 0
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

func getMetadataFromUrl(ctx *server.Context) (meta *Metadata) {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing address", nil))
	}
	switch tezos.DetectAddressType(ident) {
	case tezos.AddressTypeInvalid:
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", nil))
	case tezos.AddressTypeContract:
		// account or token
		if strings.Contains(ident, "_") {
			addr, err := tezos.ParseToken(ident)
			if err != nil {
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid token address", err))
			}
			meta, ok = lookupTokenMetadata(ctx, addr)
		} else {
			addr, err := tezos.ParseAddress(ident)
			if err != nil {
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
			}
			meta, ok = lookupAddressMetadata(ctx, addr)
		}
	default:
		addr, err := tezos.ParseAddress(ident)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
		}
		meta, ok = lookupAddressMetadata(ctx, addr)
	}
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

func (r MetadataListRequest) HasFilter() bool {
	return r.Kind != nil || r.Status != nil || r.Country != nil || r.Standard != nil
}

func ListMetadata(ctx *server.Context) (interface{}, int) {
	args := &MetadataListRequest{ListRequest: ListRequest{Order: pack.OrderAsc}}
	ctx.ParseRequestArgs(args)
	hasFilter := args.HasFilter()

	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
	}

	q := pack.NewQuery("metadata.list")
	if args.Cursor > 0 {
		if args.Order == pack.OrderAsc {
			q = q.AndGt("row_id", args.Cursor)
		} else {
			q = q.AndLt("row_id", args.Cursor)
		}
	}

	resp := make([]Sortable, 0)
	err = q.WithTable(table).Stream(ctx, func(r pack.Row) error {
		m := &model.Metadata{}
		if err := r.Decode(m); err != nil {
			return err
		}

		// filter encoded JSON
		if hasFilter {
			c := gjson.ParseBytes(m.Content)

			if args.Kind != nil && *args.Kind != c.Get("alias.kind").String() {
				return nil
			}
			if args.Status != nil && *args.Status != c.Get("baker.status").String() {
				return nil
			}
			if args.Country != nil && *args.Country != iso.Country(c.Get("location.country").String()) {
				return nil
			}
			if args.Standard != nil && *args.Standard != c.Get("asset.standard").String() {
				return nil
			}
		}

		if args.Offset > 0 {
			args.Offset--
			return nil
		}

		md := NewMetadata(m)
		if args.Short {
			if md.Stripped != nil && md.Kind != "other" {
				resp = append(resp, md.Short())
			}
		} else {
			resp = append(resp, md)
		}

		if args.Limit > 0 && len(resp) == int(args.Limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		panic(server.EInternal(server.EC_DATABASE, "cannot filter metadata", err))
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
	resp := make([]*Metadata, len(ups))
	for i, v := range ups {
		resp[i] = NewMetadata(v)
		metadataCache.Add(resp[i].Key(), resp[i])
	}
	return resp, http.StatusCreated
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
	metadataCache.Add(upd.Key(), upd)
	return upd, http.StatusOK
}

func RemoveMetadata(ctx *server.Context) (interface{}, int) {
	meta := getMetadataFromUrl(ctx)
	err := ctx.Indexer.RemoveMetadata(ctx.Context, &model.Metadata{RowId: meta.RowId})
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot remove metadata", err))
	}
	metadataCache.Remove(meta.Key())
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
