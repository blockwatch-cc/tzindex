// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/tzindex/etl"

	"github.com/echa/config"
	logpkg "github.com/echa/log"
)

var LoggerMap map[string]logpkg.Logger

func init() {
	register(SystemRequest{})
}

var _ RESTful = (*SystemRequest)(nil)

type SystemRequest struct{}

func (t SystemRequest) LastModified() time.Time {
	return time.Now().UTC()
}

func (t SystemRequest) Expires() time.Time {
	return time.Time{}
}

func (t SystemRequest) RESTPrefix() string {
	return "/system"
}

func (t SystemRequest) RESTPath(r *mux.Router) string {
	return ""
}

func (t SystemRequest) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t SystemRequest) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/tables", C(GetTableStats)).Methods("GET")
	r.HandleFunc("/mem", C(GetMemStats)).Methods("GET")
	r.HandleFunc("/config", C(GetConfig)).Methods("GET")
	r.HandleFunc("/snapshot", C(SnapshotDatabases)).Methods("PUT")
	r.HandleFunc("/flush", C(FlushDatabases)).Methods("PUT")
	r.HandleFunc("/flush_journal", C(FlushJournals)).Methods("PUT")
	r.HandleFunc("/gc", C(GcDatabases)).Methods("PUT")
	r.HandleFunc("/log/{subsystem}/{level}", C(UpdateLog)).Methods("PUT")
	return nil
}

func GetTableStats(ctx *ApiContext) (interface{}, int) {
	return ctx.Indexer.TableStats(), http.StatusOK
}

func GetMemStats(ctx *ApiContext) (interface{}, int) {
	return ctx.Indexer.MemStats(), http.StatusOK
}

func GetConfig(ctx *ApiContext) (interface{}, int) {
	return config.AllSettings(), http.StatusOK
}

func SnapshotDatabases(ctx *ApiContext) (interface{}, int) {
	if err := ctx.Crawler.SnapshotRequest(ctx.Context); err != nil {
		panic(EInternal(EC_DATABASE, "snapshot failed", err))
	}
	return nil, http.StatusNoContent
}

func FlushDatabases(ctx *ApiContext) (interface{}, int) {
	if err := ctx.Indexer.Flush(ctx.Context); err != nil {
		panic(EInternal(EC_DATABASE, "flush failed", err))
	}
	return nil, http.StatusNoContent
}

func FlushJournals(ctx *ApiContext) (interface{}, int) {
	if err := ctx.Indexer.FlushJournals(ctx.Context); err != nil {
		panic(EInternal(EC_DATABASE, "journal flush failed", err))
	}
	return nil, http.StatusNoContent
}

func GcDatabases(ctx *ApiContext) (interface{}, int) {
	if err := ctx.Indexer.GC(ctx.Context, config.GetFloat64("database.gc_ratio")); err != nil {
		panic(EInternal(EC_DATABASE, "gc failed", err))
	}
	return nil, http.StatusNoContent
}

func UpdateLog(ctx *ApiContext) (interface{}, int) {
	sub, _ := mux.Vars(ctx.Request)["subsystem"]
	level, _ := mux.Vars(ctx.Request)["level"]
	lvl := logpkg.ParseLevel(level)
	if lvl == logpkg.LevelInvalid {
		panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("undefined log level '%s'", level), nil))
	}
	var key string
	switch sub {
	case "main":
		key = "MAIN"
	case "blockchain":
		key = "BLOC"
	case "database":
		key = "DATA"
	case "rpc":
		key = "JRPC"
	case "server":
		key = "SRVR"
	case "micheline":
		key = "MICH"
	default:
		panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("undefined subsystem '%s'", sub), nil))
	}
	logger, ok := LoggerMap[key]
	if ok {
		logger.SetLevel(lvl)
	}
	return nil, http.StatusNoContent
}

type RollbackRequest struct {
	Height int64 `schema:"height" json:"height"` // negative height is treated as offset
	Force  bool  `schema:"force"  json:"force"`  // ignore errors
}

func RollbackDatabases(ctx *ApiContext) (interface{}, int) {
	var args RollbackRequest
	ctx.ParseRequestArgs(&args)

	// only supported in failed mode!
	s := ctx.Crawler.Status()
	if s.Status != etl.STATE_FAILED {
		panic(EConflict(EC_RESOURCE_STATE_UNEXPECTED, fmt.Sprintf("rollback unsupported in state '%s'", s.Status), nil))
	}
	if err := ctx.Crawler.Rollback(ctx.Context, args.Height, args.Force); err != nil {
		panic(EInternal(EC_DATABASE, "rollback failed", err))
	}
	return nil, http.StatusNoContent
}
