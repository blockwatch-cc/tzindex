// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"fmt"
	"io"
	"math/bits"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/echa/code/iso"
	"github.com/echa/config"
	"github.com/gorilla/mux"
	"github.com/tidwall/gjson"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
	lru "github.com/hashicorp/golang-lru/v2"
)

var (
	metadataCache *lru.TwoQueueCache[uint64, any]
)

const MetadataCacheSize = 32768

func init() {
	metadataCache, _ = lru.New2Q[uint64, any](MetadataCacheSize)
}

func purgeMetadataStore() {
	metadataCache.Purge()
}

func lookupAddressIdMetadata(ctx *server.Context, id model.AccountID) (*Metadata, bool) {
	if id == 0 {
		return nil, false
	}
	val, ok := metadataCache.Get(id.U64())
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
	metadataCache.Add(id.U64(), m)
	return m, true
}

func lookupTokenIdMetadata(ctx *server.Context, id model.TokenID) ([]byte, bool) {
	if id == 0 {
		return nil, false
	}
	key := id.U64() | (1 << 63)
	val, ok := metadataCache.Get(key)
	if ok {
		return val.([]byte), true
	}
	table, err := ctx.Indexer.Table(model.TokenMetaTableKey)
	if err != nil {
		return nil, false
	}
	if table.Stats()[0].TupleCount == 0 {
		return nil, false
	}
	md := &model.TokenMeta{}
	err = pack.NewQuery("token.metadata.find").
		WithTable(table).
		AndEqual("token", id).
		Execute(ctx, md)
	if err != nil || md.Id == 0 {
		return nil, false
	}
	metadataCache.Add(key, md.Data)
	return md.Data, true
}

func lookupAddressMetadata(ctx *server.Context, addr tezos.Address) (*Metadata, bool) {
	var id model.AccountID
	id, err := ctx.Indexer.LookupAccountId(ctx, addr)
	if err != nil {
		return nil, false
	}
	return lookupAddressIdMetadata(ctx, id)
}

type Metadata struct {
	// unique identity
	RowId     model.MetaID    // metadata entry id (used for update/insert)
	AccountId model.AccountID // may be 0 for non-indexed accounts
	Address   tezos.Address   // must be valid

	// input only
	Parsed map[string]json.RawMessage

	// output only
	Raw      []byte
	Stripped []byte

	// filtered properties
	Name           string
	Description    string
	Logo           string
	IsSponsored    bool
	IsValidator    bool
	Country        iso.Country
	Kind           string
	Status         string
	MinDelegation  float64
	NonDelegatable bool
	IsPayout       bool

	// TODO
	// lastmod
	// expires
}

var _ Sortable = (*Metadata)(nil)

func (m Metadata) Id() uint64 {
	return uint64(m.RowId)
}

func (m Metadata) Key() uint64 {
	return m.AccountId.U64()
}

func (m Metadata) Equal(x *Metadata) bool {
	return m.Address == x.Address
}

func firstOfString(c gjson.Result, keys ...string) string {
	for _, k := range keys {
		if v := c.Get(k).String(); v != "" {
			return v
		}
	}
	return ""
}

func NewMetadata(m *model.Metadata) *Metadata {
	md := &Metadata{
		RowId:     m.RowId,
		AccountId: m.AccountId,
		Address:   m.Address,
	}

	// extract filter fields
	if len(m.Content) > 0 {
		c := gjson.ParseBytes(m.Content)
		md.Name = firstOfString(c, "alias.name", "tzprofile.alias", "tzdomain.name")
		md.Kind = c.Get("alias.kind").String()
		md.Description = firstOfString(c, "alias.description", "tzprofile.description")
		md.Logo = firstOfString(c, "alias.logo", "tzprofile.logo")
		md.Country = iso.Country(c.Get("location.country").String())
		md.IsValidator = md.Kind == "validator"
		md.IsSponsored = c.Get("baker.sponsored").Bool()
		md.Status = c.Get("baker.status").String()
		md.MinDelegation = c.Get("baker.min_delegation").Float()
		md.NonDelegatable = c.Get("baker.non_delegatable").Bool()
		md.IsPayout = len(c.Get("payout").Array()) > 0
	}

	// extract short version for anything that has a kind and name
	if md.Kind != "" && md.Name != "" {
		s := struct {
			Address string `json:"address,omitempty"`
			Name    string `json:"name,omitempty"`
			Kind    string `json:"kind,omitempty"`
			Logo    string `json:"logo,omitempty"`
		}{
			Address: md.Address.String(),
			Name:    md.Name,
			Kind:    md.Kind,
			Logo:    md.Logo,
		}
		md.Stripped, _ = json.Marshal(s)
	}

	// build raw output
	var b strings.Builder
	b.WriteString(`{"address":"`)
	b.WriteString(m.Address.String())
	b.WriteRune('"')
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
		default:
			// will decode and validate later
			m.Parsed[n] = v
		}
	}
	return nil
}

func (m *Metadata) Validate(ctx *server.Context) error {
	// skip for token metadata
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
	if m.Stripped == nil {
		return nil
	}
	return (*ShortMetadata)(m)
}

type ShortMetadata Metadata

func (m *ShortMetadata) MarshalJSON() ([]byte, error) {
	return m.Stripped, nil
}

func (m *ShortMetadata) Id() uint64 {
	return uint64(m.RowId)
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
	r.HandleFunc("/sitemap", server.C(SitemapMetadata)).Methods("GET")
	r.HandleFunc("/describe/{ident}", server.C(DescribeMetadata)).Methods("GET")
	r.HandleFunc("/describe/{ident}/{num}", server.C(DescribeMetadata)).Methods("GET")
	r.HandleFunc("/schemas", server.C(ListMetadataSchemas)).Methods("GET")
	r.HandleFunc("/schemas/{schema}.json", server.C(ReadMetadataSchema)).Methods("GET")
	r.HandleFunc("/schemas/{schema}", server.C(ReadMetadataSchema)).Methods("GET")
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
	addr, err := tezos.ParseAddress(ident)
	if err != nil {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
	}
	meta, ok = lookupAddressMetadata(ctx, addr)
	if !ok {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such metadata entry", nil))
	}
	return meta
}

func loadMetadataFromUrl(ctx *server.Context) *model.Metadata {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing address", nil))
	}
	addr, err := tezos.ParseAddress(ident)
	if err != nil {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid address", err))
	}
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
	}

	m := &model.Metadata{}
	err = pack.NewQuery("metadata.find").
		WithTable(table).
		AndEqual("address", addr).
		Execute(ctx, m)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot create metadata", err))
	}
	if m.RowId == 0 {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such metadata entry", nil))
	}
	return m
}

func loadMetadata(ctx *server.Context, id model.AccountID) (*model.Metadata, bool) {
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		return nil, false
	}

	m := &model.Metadata{}
	err = pack.NewQuery("metadata.find").
		WithTable(table).
		AndEqual("account_id", id).
		Execute(ctx, m)
	if err != nil {
		return nil, false
	}
	if m.RowId == 0 {
		return nil, false
	}
	return m, true
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
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("%s: %v", meta[i].Address, err), nil))
			}
		}
	}

	// 2 fill account ids if exist
	for _, m := range meta {
		if acc, err := ctx.Indexer.LookupAccount(ctx.Context, m.Address); err == nil {
			m.AccountId = acc.RowId
		}
	}
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, "cannot create metadata", err))
		}
	}

	// 3 upsert
	ins := make([]pack.Item, 0, len(meta))
	upd := make([]pack.Item, 0, len(meta))
	for _, v := range meta {
		m, ok := loadMetadata(ctx, v.AccountId)
		if !ok {
			m = &model.Metadata{
				AccountId: v.AccountId,
				Address:   v.Address,
			}
			m.Content, _ = json.Marshal(v.Parsed)
			ins = append(ins, m)
			continue
		}

		// merge
		var current map[string]json.RawMessage
		_ = json.Unmarshal(m.Content, &current)
		for n, vv := range v.Parsed {
			current[n] = vv
		}
		m.Content, _ = json.Marshal(current)
		upd = append(upd, m)
	}

	if err := table.Insert(ctx, ins); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot create metadata", err))
	}
	if err := table.Update(ctx, upd); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot create metadata", err))
	}

	// purge cache
	metadataCache.Purge()
	return nil, http.StatusNoContent
}

func UpdateMetadata(ctx *server.Context) (interface{}, int) {
	m := loadMetadataFromUrl(ctx)
	upd := &Metadata{}
	ctx.ParseRequestArgs(upd)
	if err := upd.Validate(ctx); err != nil {
		panic(server.EBadRequest(server.EC_PARAM_INVALID, err.Error(), nil))
	}

	// merge
	var current map[string]json.RawMessage
	_ = json.Unmarshal(m.Content, &current)
	for n, vv := range upd.Parsed {
		current[n] = vv
	}
	m.Content, _ = json.Marshal(current)

	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot update metadata", err))
	}
	if err := table.Update(ctx, m); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot update metadata", err))
	}

	// purge cache
	metadataCache.Purge()

	return NewMetadata(m), http.StatusOK
}

func RemoveMetadata(ctx *server.Context) (interface{}, int) {
	m := loadMetadataFromUrl(ctx)
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot delete metadata", err))
	}

	// remove all non-automated models
	var current map[string]json.RawMessage
	_ = json.Unmarshal(m.Content, &current)
	for n := range current {
		switch n {
		case "tzprofile", "tzdomain":
			continue
		default:
			delete(current, n)
		}
	}
	if len(current) == 0 {
		err = table.DeleteIds(ctx, []uint64{uint64(m.RowId)})
	} else {
		m.Content, _ = json.Marshal(current)
		err = table.Update(ctx, m)
	}
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot remove metadata", err))
	}

	// purge cache
	metadataCache.Purge()

	return nil, http.StatusNoContent
}

func PurgeMetadata(ctx *server.Context) (interface{}, int) {
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access metadata table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, "cannot delete metadata", err))
		}
	}

	upd := make([]pack.Item, 0)
	del := make([]uint64, 0)
	err = pack.NewQuery("api.metadata.purge").
		WithTable(table).
		Stream(ctx, func(r pack.Row) error {
			m := &model.Metadata{}
			if err := r.Decode(m); err != nil {
				return err
			}
			// remove all non-automated models
			var current map[string]json.RawMessage
			_ = json.Unmarshal(m.Content, &current)
			for n := range current {
				switch n {
				case "tzprofile", "tzdomain":
					continue
				default:
					delete(current, n)
				}
			}
			if len(current) == 0 {
				del = append(del, m.RowId.U64())
			} else {
				m.Content, _ = json.Marshal(current)
				upd = append(upd, m)
			}
			return nil
		})
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read metadata", err))
	}

	if err := table.Update(ctx, upd); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot update metadata", err))
	}
	if err := table.DeleteIds(ctx, del); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot update metadata", err))
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
// # Supported identifiers
//
// :address      address, contract, token
// :block        block hash or height
// :op           operation hash
// /event/:id    implicit operation
// /cycle/:id    cycle number
// /election/:id election number
func DescribeMetadata(ctx *server.Context) (interface{}, int) {
	ident, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || ident == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing identifier", nil))
	}
	num := mux.Vars(ctx.Request)["num"]

	var desc MetadataDescriptor

	switch ident {
	case "event":
		ops, err := ctx.Indexer.LookupOp(ctx, num, etl.ListRequest{})
		if err != nil {
			switch err {
			case model.ErrNoOp:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such operation", err))
			case model.ErrInvalidOpID:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid event id", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		desc = DescribeOp(ctx, ops)
	case "cycle":
		c, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid cycle identifier", nil))
		}
		desc = DescribeCycle(ctx, c)
	case "election":
		c, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid election identifier", nil))
		}
		desc = DescribeElection(ctx, c)
	default:
		// detect type
		if a, err := tezos.ParseAddress(ident); err == nil {
			desc = DescribeAddress(ctx, a)
		} else if _, err := tezos.ParseBlockHash(ident); err == nil {
			desc = DescribeBlock(ctx, ident)
		} else if _, err := strconv.ParseInt(ident, 10, 64); err == nil {
			desc = DescribeBlock(ctx, ident)
		} else if _, err := tezos.ParseOpHash(ident); err == nil {
			ops := loadOps(ctx, &OpsRequest{}, 1)
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
		Title: tezos.Short(addr.String()),
		Image: config.GetString("metadata.describe.logo"),
	}
	meta, ok := lookupAddressMetadata(ctx, addr)
	if ok {
		// baker, token, other
		d.Title = meta.Name
		d.Description = meta.Description
		d.Image = meta.Logo
		if d.Title == "" {
			d.Title = tezos.Short(addr.String())
		}
		if d.Description == "" {
			switch meta.Kind {
			case "validator":
				d.Description = fmt.Sprintf("%s Tezos baker. Address %s. View baking statistics, account activity and analytics.",
					strings.ToTitle(meta.Status), addr)
			case "token":
				d.Description = fmt.Sprintf("%s token ledger. Address %s. View token activity, supply, numbers of holders and more details.",
					meta.Name, addr)
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
	if meta, ok := lookupAddressIdMetadata(ctx, block.BakerId); ok {
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

	sender := tezos.Short(ctx.Indexer.LookupAddress(ctx, op.SenderId).String())
	if meta, ok := lookupAddressIdMetadata(ctx, op.SenderId); ok {
		sender = meta.Name
		d.Image = meta.Logo
	}
	receiver := tezos.Short(ctx.Indexer.LookupAddress(ctx, op.ReceiverId).String())
	if meta, ok := lookupAddressIdMetadata(ctx, op.ReceiverId); ok {
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
		d.Title = fmt.Sprintf("Tezos Double Baking Evidence %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Evidence of double baking in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case model.OpTypeDoubleEndorsement:
		d.Title = fmt.Sprintf("Tezos Double Endorsement Evidence %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Evidence of double endorsing in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case model.OpTypeDoublePreendorsement:
		d.Title = fmt.Sprintf("Tezos Double Preendorsement Evidence %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Evidence of double preendorsing in block %s on %s. Offender: %s. Lost %s XTZ.",
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Burned+op.Volume)),
		)

	case model.OpTypeNonceRevelation:
		d.Title = fmt.Sprintf("Tezos Seed-Nonce Revelation %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Seed nonce revealed by %s on %s (block %s in cycle %s). Baked by %s, reward: %s XTZ.",
			receiver,
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			util.PrettyInt64(op.Cycle),
			sender,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
		)

	case model.OpTypeTransaction:
		switch {
		case op.IsContract:
			d.Title = fmt.Sprintf("Tezos Contract Call %s", tezos.Short(op.Hash.String()))
			d.Description = fmt.Sprintf("Called %s in contract %s on %s. Status: %s.",
				op.Data,
				receiver,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		case len(ops) > 1:
			var sum int64
			for _, v := range ops {
				sum += v.Volume
			}
			d.Title = fmt.Sprintf("Tezos Batch Operation %s", tezos.Short(op.Hash.String()))
			d.Description = fmt.Sprintf("Batch-sent %s XTZ from %s on %s. Status: %s.",
				util.PrettyFloat64(ctx.Params.ConvertValue(sum)),
				sender,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		default:
			d.Title = fmt.Sprintf("Tezos Transaction %s", tezos.Short(op.Hash.String()))
			d.Description = fmt.Sprintf("Sent %s XTZ from %s to %s on %s. Status: %s.",
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				sender,
				receiver,
				op.Timestamp.Format(metaDateTime),
				op.Status,
			)
		}

	case model.OpTypeOrigination:
		d.Title = fmt.Sprintf("Tezos Origination %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Contract: %s. Time %s. Status %s.",
			sender,
			receiver,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeDelegation:
		switch {
		case op.SenderId == op.BakerId:
			d.Title = fmt.Sprintf("Tezos Baker Registration %s", tezos.Short(op.Hash.String()))
			d.Description = fmt.Sprintf("New baker %s registered on %s (cycle %s). Status: %s.",
				sender,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)
		case op.BakerId == 0:
			d.Title = fmt.Sprintf("Tezos Delegation Removal %s", tezos.Short(op.Hash.String()))
			d.Description = fmt.Sprintf("Delegator %s left %s on %s (cycle %s). Status: %s.",
				sender,
				receiver,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)
		default:
			baker := tezos.Short(ctx.Indexer.LookupAddress(ctx, op.BakerId).String())
			if meta, ok := lookupAddressIdMetadata(ctx, op.BakerId); ok {
				baker = meta.Name
			}
			d.Title = fmt.Sprintf("Tezos Delegation %s", tezos.Short(op.Hash.String()))
			d.Description = fmt.Sprintf("Delegation from %s to %s on %s (cycle %s). Status: %s.",
				sender,
				baker,
				op.Timestamp.Format(metaDateTime),
				util.PrettyInt64(op.Cycle),
				op.Status,
			)
		}

	case model.OpTypeReveal:
		d.Title = fmt.Sprintf("Tezos Public Key Revelation %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Public key revealed by %s on %s (block %s). Key: %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			util.PrettyInt64(op.Height),
			op.Data,
		)

	case model.OpTypeEndorsement:
		mask, _ := strconv.ParseUint(op.Data, 10, 64)
		d.Title = fmt.Sprintf("Tezos Endorsement %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("%s endorsed %d slots in block %s on %s. Deposit: %s XTZ. Reward: %s XTZ.",
			sender,
			bits.OnesCount64(mask),
			util.PrettyInt64(op.Height-1),
			op.Timestamp.Format(metaDateTime),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Deposit)),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
		)

	case model.OpTypePreendorsement:
		mask, _ := strconv.ParseUint(op.Data, 10, 64)
		d.Title = fmt.Sprintf("Tezos Pre-Endorsement %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("%s pre-endorsed %d slots in block %s on %s. Deposit: %s XTZ. Reward: %s XTZ.",
			sender,
			bits.OnesCount64(mask),
			util.PrettyInt64(op.Height-1),
			op.Timestamp.Format(metaDateTime),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Deposit)),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
		)

	case model.OpTypeProposal:
		d.Title = fmt.Sprintf("Tezos Proposal Vote %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("%s upvoted %s on %s.",
			sender,
			op.Data,
			op.Timestamp.Format(metaDateTime),
		)

	case model.OpTypeBallot:
		data := strings.Split(op.Data, ",")
		d.Title = fmt.Sprintf("Tezos Ballot %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("%s voted %s for %s on %s.",
			sender,
			data[1],
			data[0],
			op.Timestamp.Format(metaDateTime),
		)

	case model.OpTypeRegisterConstant:
		d.Title = fmt.Sprintf("Tezos Register Global Constant %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Time %s. Status %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeDepositsLimit:
		d.Title = fmt.Sprintf("Tezos Set Deposits Limit %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Baker %s. Time %s. Status %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeRollupOrigination:
		d.Title = fmt.Sprintf("Tezos Rollup Origination %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Rollup %s. Time %s. Status %s.",
			sender,
			receiver,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeRollupTransaction:
		d.Title = fmt.Sprintf("Tezos %s %s", op.Data, tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Rollup %s. Time %s. Status %s.",
			sender,
			receiver,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeVdfRevelation:
		d.Title = fmt.Sprintf("Tezos VDF Nonce Revelation %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Time %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
		)

	case model.OpTypeIncreasePaidStorage:
		d.Title = fmt.Sprintf("Tezos Increase Paid Storage %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Contract %s. Amount %s. Time %s. Status %s.",
			sender,
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeDrainDelegate:
		d.Title = fmt.Sprintf("Tezos Drain Baker %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Baker %s. Amount %s. Time %s. Status %s.",
			receiver, // reward recipient
			sender,   // drained baker
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeUpdateConsensusKey:
		d.Title = fmt.Sprintf("Tezos Update Consensus Key %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Baker %s. Time %s. Status %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeTransferTicket:
		d.Title = fmt.Sprintf("Tezos Ticket Transfer %s", tezos.Short(op.Hash.String()))
		d.Description = fmt.Sprintf("Sender %s. Time %s. Status %s.",
			sender,
			op.Timestamp.Format(metaDateTime),
			op.Status,
		)

	case model.OpTypeSetDelegateParameters:
		d.Title = "Tezos Set Baker Parameters"
		d.Description = fmt.Sprintf("Baker %s. Block %s. Time %s.",
			sender,
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
		)

	// events
	case model.OpTypeBake:
		d.Title = "Tezos Baking Event"
		d.Description = fmt.Sprintf("Baker %s baked block %s on %s.",
			sender,
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
		)
	case model.OpTypeUnfreeze:
		d.Title = "Tezos Unfreeze Event"
		d.Description = fmt.Sprintf("Unfrozen: %.06f XTZ deposits, %.06f XTZ rewards, and %.06f XTZ fees for %s in cycle %s.",
			ctx.Params.ConvertValue(op.Deposit),
			ctx.Params.ConvertValue(op.Reward),
			ctx.Params.ConvertValue(op.Fee),
			sender,
			util.PrettyInt64(op.Cycle),
		)
	case model.OpTypeInvoice:
		d.Title = "Tezos Invoice Event"
		d.Description = fmt.Sprintf("%s received %s XTZ for their contributions to a Tezos protocol upgrade.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
		)
	case model.OpTypeAirdrop:
		d.Title = "Tezos Airdrop Event"
		d.Description = fmt.Sprintf("Protocol migration airdrop of %s XTZ to %s.",
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			receiver,
		)
	case model.OpTypeSeedSlash:
		d.Title = "Tezos Seed Slash Event"
		d.Description = fmt.Sprintf("%s lost %s XTZ rewards and %s XTZ fees in cycle %s.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Reward)),
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Fee)),
			util.PrettyInt64(op.Cycle),
		)
	case model.OpTypeMigration:
		d.Title = "Tezos Migration Event"
		d.Description = fmt.Sprintf("Contract %s migrated on block %s.",
			receiver,
			util.PrettyInt64(op.Height),
		)
	case model.OpTypeSubsidy:
		d.Title = "Tezos Subsidy Event"
		d.Description = fmt.Sprintf("Contract %s received %s XTZ in block %s at %s.",
			receiver,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
		)
	case model.OpTypeDeposit:
		d.Title = "Tezos Deposit Event"
		d.Description = fmt.Sprintf("Baker %s. Cycle %s. Deposits %.06f XTZ.",
			sender,
			util.PrettyInt64(op.Cycle),
			ctx.Params.ConvertValue(op.Deposit),
		)
	case model.OpTypeBonus:
		d.Title = "Tezos Baking Bonus Event"
		d.Description = fmt.Sprintf("Baker %s received %s XTZ bonus for block %s on %s.",
			sender,
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
			util.PrettyInt64(op.Height),
			op.Timestamp.Format(metaDateTime),
		)
	case model.OpTypeReward:
		if op.Burned > 0 {
			d.Title = "Tezos Endorsing Reward Burn Event"
			d.Description = fmt.Sprintf("Baker %s lost %s XTZ reward for cycle %s.",
				sender,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Cycle),
			)
		} else {
			d.Title = "Tezos Endorsing Reward Event"
			d.Description = fmt.Sprintf("Baker %s received %s XTZ reward for cycle %s.",
				sender,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Cycle),
			)
		}
	case model.OpTypeStake:
		if op.IsEvent {
			d.Title = "Tezos Stake Event"
			d.Description = fmt.Sprintf("Baker %s staked %s XTZ at block %s on %s.",
				sender,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Height),
				op.Timestamp.Format(metaDateTime),
			)
		} else {
			d.Title = "Tezos Stake Transaction"
			d.Description = fmt.Sprintf("Sender %s staked %s XTZ with baker %s at block %s on %s.",
				sender,
				receiver,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Height),
				op.Timestamp.Format(metaDateTime),
			)
		}
	case model.OpTypeUnstake:
		if op.IsEvent {
			d.Title = "Tezos Unstake Event"
			d.Description = fmt.Sprintf("Baker %s unstaked %s XTZ at block %s on %s.",
				sender,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Height),
				op.Timestamp.Format(metaDateTime),
			)
		} else {
			d.Title = "Tezos Unstake Transaction"
			d.Description = fmt.Sprintf("Sender %s unstaked %s XTZ from baker %s at block %s on %s.",
				sender,
				receiver,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Height),
				op.Timestamp.Format(metaDateTime),
			)
		}

	case model.OpTypeFinalizeUnstake:
		if op.IsEvent {
			d.Title = "Tezos Finalize Unstake Event"
			d.Description = fmt.Sprintf("Baker %s finalized %s XTZ at block %s on %s.",
				sender,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Height),
				op.Timestamp.Format(metaDateTime),
			)
		} else {
			d.Title = "Tezos Finalize Unstake Transaction"
			d.Description = fmt.Sprintf("Sender %s finalized %s XTZ from baker %s at block %s on %s.",
				sender,
				receiver,
				util.PrettyFloat64(ctx.Params.ConvertValue(op.Volume)),
				util.PrettyInt64(op.Height),
				op.Timestamp.Format(metaDateTime),
			)
		}

	case model.OpTypeStakeSlash:
		d.Title = "Tezos Stake Slash Event"
		d.Description = fmt.Sprintf("Baker %s. Lost %s. Cycle %s. Time %s.",
			ctx.Indexer.LookupAddress(ctx, op.ReceiverId), // offender
			util.PrettyFloat64(ctx.Params.ConvertValue(op.Deposit)),
			util.PrettyInt64(op.Cycle),
			op.Timestamp.Format(metaDateTime),
		)
	}
	return d
}

func DescribeCycle(ctx *server.Context, cycle int64) MetadataDescriptor {
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

func DescribeElection(ctx *server.Context, id int64) MetadataDescriptor {
	election, err := ctx.Indexer.ElectionById(ctx.Context, model.ElectionID(id))
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access election table", err))
		case model.ErrNoElection:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no election", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}
	proposals, err := ctx.Indexer.ProposalsByElection(ctx, election.RowId)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access proposal table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}
	var winner *model.Proposal
	if election.ProposalId > 0 {
		for _, v := range proposals {
			if v.RowId == election.ProposalId {
				winner = v
				break
			}
		}
	} else if len(proposals) > 0 {
		winner = proposals[0]
	}

	if winner == nil {
		return MetadataDescriptor{
			Title: "Tezos Election on TzStats",
			Image: config.GetString("metadata.describe.logo"),
			Description: fmt.Sprintf("Tezos Election – From %s to %s.",
				election.StartTime.Format(metaDateTime),
				election.EndTime.Format(metaDateTime),
			),
		}
	} else {
		return MetadataDescriptor{
			Title: fmt.Sprintf("Tezos Election %s on TzStats", winner.Hash.String()[:8]),
			Image: config.GetString("metadata.describe.logo"),
			Description: fmt.Sprintf("Tezos Election %s – From %s to %s.",
				winner.Hash.String(),
				election.StartTime.Format(metaDateTime),
				election.EndTime.Format(metaDateTime),
			),
		}
	}
}

func SitemapMetadata(ctx *server.Context) (interface{}, int) {
	table, err := ctx.Indexer.Table(model.MetadataTableKey)
	if err != nil {
		return nil, http.StatusNoContent
	}
	if table.Stats()[0].TupleCount == 0 {
		return nil, http.StatusNoContent
	}
	type Alias struct {
		Address tezos.Address `pack:"address"`
	}
	addrs := make([]string, 0)
	alias := &Alias{}
	err = pack.NewQuery("metadata.list").
		WithTable(table).
		Stream(ctx.Context, func(r pack.Row) error {
			if err := r.Decode(alias); err != nil {
				return err
			}
			addrs = append(addrs, alias.Address.String())
			return nil
		})
	if err != nil {
		return nil, http.StatusNoContent
	}
	return addrs, http.StatusOK
}
