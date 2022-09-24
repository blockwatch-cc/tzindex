// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"fmt"
	"github.com/gorilla/mux"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/server"
)

var null = []byte(`null`)

var mimetypes = map[string]string{
	"json": "application/json; charset=utf-8",
	"csv":  "text/csv",
}

func init() {
	server.Register(TableRequest{})
}

var _ server.RESTful = (*TableRequest)(nil)

// build packdb query from request
type TableRequest struct {
	Table   string          `schema:"-"`
	Columns util.StringList `schema:"columns"`
	Limit   uint            `schema:"limit"`
	Cursor  string          `schema:"cursor"`
	Format  string          `schema:"-"`     // from URL
	Order   pack.OrderType  `schema:"order"` // asc/desc
	Verbose bool            `schema:"verbose"`
	// OrderBy string // column name
}

func (t TableRequest) LastModified() time.Time {
	return time.Time{}
}

func (t TableRequest) Expires() time.Time {
	return time.Time{}
}

func (t TableRequest) RESTPrefix() string {
	return "/tables"
}

func (t TableRequest) RESTPath(r *mux.Router) string {
	path, _ := r.Get("tableurl").URLPath("table", t.Table, "format", t.Format)
	return path.String()
}

func (t TableRequest) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t TableRequest) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{table}.{format}", server.C(StreamTable)).Methods("GET").Name("tableurl")
	r.HandleFunc("/{table}", server.C(StreamTable)).Methods("GET")
	return nil
}

func (t *TableRequest) Parse(ctx *server.Context) {
	t.Limit = ctx.Cfg.ClampList(t.Limit)

	// prevent duplicate columns
	if len(t.Columns) > 0 {
		seen := make(map[string]struct{})
		for _, v := range t.Columns {
			if _, ok := seen[v]; ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("duplicate column %s", v), nil))
			}
			seen[v] = struct{}{}
		}
	}

	// read table code from URL
	t.Table = strings.ToLower(mux.Vars(ctx.Request)["table"])

	// read format from URL
	t.Format = strings.ToLower(mux.Vars(ctx.Request)["format"])
	if t.Format == "" {
		t.Format = "json"
	}
	switch t.Format {
	case "json", "csv":
	default:
		panic(server.EBadRequest(server.EC_CONTENTTYPE_UNSUPPORTED, fmt.Sprintf("unsupported format '%s'", t.Format), nil))
	}
}

func StreamTable(ctx *server.Context) (interface{}, int) {
	args := &TableRequest{}
	ctx.ParseRequestArgs(args)
	switch args.Table {
	case "block":
		return StreamBlockTable(ctx, args)
	case "chain":
		return StreamChainTable(ctx, args)
	case "supply":
		return StreamSupplyTable(ctx, args)
	case "op":
		return StreamOpTable(ctx, args)
	case "flow":
		return StreamFlowTable(ctx, args)
	case "contract":
		return StreamContractTable(ctx, args)
	case "account":
		return StreamAccountTable(ctx, args)
	case "rights":
		return StreamRightsTable(ctx, args)
	case "snapshot":
		return StreamSnapshotTable(ctx, args)
	case "election":
		return StreamElectionTable(ctx, args)
	case "proposal":
		return StreamProposalTable(ctx, args)
	case "vote":
		return StreamVoteTable(ctx, args)
	case "ballot":
		return StreamBallotTable(ctx, args)
	case "income":
		return StreamIncomeTable(ctx, args)
	case "bigmaps":
		return StreamBigmapAllocTable(ctx, args)
	case "bigmap_values":
		return StreamBigmapValueTable(ctx, args)
	case "bigmap_updates":
		return StreamBigmapUpdateTable(ctx, args)
	case "constant":
		return StreamConstantTable(ctx, args)
	case "balance":
		return StreamBalanceTable(ctx, args)
	case "event":
		return StreamEventTable(ctx, args)
	default:
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("no such table '%s'", args.Table), nil))
	}
}
