// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tables

import (
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
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
	Table    string          `schema:"-"`
	Columns  util.StringList `schema:"columns"`
	Limit    uint            `schema:"limit"`
	Cursor   string          `schema:"cursor"`
	Format   string          `schema:"-"`     // from URL
	Order    pack.OrderType  `schema:"order"` // asc/desc
	Verbose  bool            `schema:"verbose"`
	Filename string          `schema:"filename"` // for CSV downloads
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
	case model.BlockTableKey:
		return StreamBlockTable(ctx, args)
	case model.ChainTableKey:
		return StreamChainTable(ctx, args)
	case model.SupplyTableKey:
		return StreamSupplyTable(ctx, args)
	case model.OpTableKey:
		return StreamOpTable(ctx, args)
	case model.FlowTableKey:
		return StreamFlowTable(ctx, args)
	case model.ContractTableKey:
		return StreamContractTable(ctx, args)
	case model.AccountTableKey:
		return StreamAccountTable(ctx, args)
	case model.RightsTableKey:
		return StreamRightsTable(ctx, args)
	case model.SnapshotTableKey:
		return StreamSnapshotTable(ctx, args)
	case model.ElectionTableKey, "election":
		args.Table = model.ElectionTableKey
		return StreamElectionTable(ctx, args)
	case model.ProposalTableKey, "proposal":
		args.Table = model.ProposalTableKey
		return StreamProposalTable(ctx, args)
	case model.VoteTableKey, "vote":
		args.Table = model.VoteTableKey
		return StreamVoteTable(ctx, args)
	case model.BallotTableKey, "ballot":
		args.Table = model.BallotTableKey
		return StreamBallotTable(ctx, args)
	case model.IncomeTableKey:
		return StreamIncomeTable(ctx, args)
	case model.BigmapAllocTableKey, "bigmaps":
		args.Table = model.BigmapAllocTableKey
		return StreamBigmapAllocTable(ctx, args)
	case model.BigmapValueTableKey:
		return StreamBigmapValueTable(ctx, args)
	case model.BigmapUpdateTableKey:
		return StreamBigmapUpdateTable(ctx, args)
	case model.ConstantTableKey:
		return StreamConstantTable(ctx, args)
	case model.BalanceTableKey:
		return StreamBalanceTable(ctx, args)
	case model.EventTableKey:
		return StreamEventTable(ctx, args)
	default:
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("no such table '%s'", args.Table), nil))
	}
}
