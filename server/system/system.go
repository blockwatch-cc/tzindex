// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package system

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/server"
	"blockwatch.cc/tzindex/server/explorer"

	"github.com/echa/config"
	logpkg "github.com/echa/log"
)

var LoggerMap map[string]logpkg.Logger

func init() {
	server.Register(SystemRequest{})
}

var _ server.RESTful = (*SystemRequest)(nil)
var _ server.Resource = (*SystemRequest)(nil)

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
	// stats & info
	r.HandleFunc("/config", server.C(GetConfig)).Methods("GET")
	r.HandleFunc("/tables", server.C(GetTableStats)).Methods("GET")
	r.HandleFunc("/caches", server.C(GetCacheStats)).Methods("GET")
	r.HandleFunc("/sysstat", server.C(GetSysStats)).Methods("GET")

	// actions
	r.HandleFunc("/tables/snapshot", server.C(SnapshotDatabases)).Methods("PUT")
	r.HandleFunc("/tables/flush", server.C(FlushDatabases)).Methods("PUT")
	r.HandleFunc("/tables/flush_journal", server.C(FlushJournals)).Methods("PUT")
	r.HandleFunc("/tables/gc", server.C(GcDatabases)).Methods("PUT")
	r.HandleFunc("/tables/dump/{table}/{part}", server.C(DumpTable)).Methods("PUT")
	r.HandleFunc("/caches/purge", server.C(PurgeCaches)).Methods("PUT")
	r.HandleFunc("/log/{subsystem}/{level}", server.C(UpdateLog)).Methods("PUT")
	return nil
}

func GetTableStats(ctx *server.Context) (interface{}, int) {
	return ctx.Indexer.TableStats(), http.StatusOK
}

func GetCacheStats(ctx *server.Context) (interface{}, int) {
	cs := ctx.Crawler.CacheStats()
	for n, v := range ctx.Indexer.CacheStats() {
		cs[n] = v
	}
	return cs, http.StatusOK
}

func GetSysStats(ctx *server.Context) (interface{}, int) {
	s, err := GetSysStat(ctx.Context)
	if err != nil {
		panic(server.EInternal(server.EC_SERVER, "sysstat failed", err))
	}
	return s, http.StatusOK
}

func GetConfig(ctx *server.Context) (interface{}, int) {
	return config.AllSettings(), http.StatusOK
}

func PurgeCaches(ctx *server.Context) (interface{}, int) {
	explorer.PurgeCaches()
	ctx.Indexer.PurgeCaches()
	return nil, http.StatusNoContent
}

func SnapshotDatabases(ctx *server.Context) (interface{}, int) {
	if err := ctx.Crawler.SnapshotRequest(ctx.Context); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "snapshot failed", err))
	}
	return nil, http.StatusNoContent
}

func FlushDatabases(ctx *server.Context) (interface{}, int) {
	if err := ctx.Indexer.Flush(ctx.Context); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "flush failed", err))
	}
	return nil, http.StatusNoContent
}

func FlushJournals(ctx *server.Context) (interface{}, int) {
	if err := ctx.Indexer.FlushJournals(ctx.Context); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "journal flush failed", err))
	}
	return nil, http.StatusNoContent
}

func GcDatabases(ctx *server.Context) (interface{}, int) {
	if err := ctx.Indexer.GC(ctx.Context, config.GetFloat64("database.gc_ratio")); err != nil {
		panic(server.EInternal(server.EC_DATABASE, "gc failed", err))
	}
	return nil, http.StatusNoContent
}

func UpdateLog(ctx *server.Context) (interface{}, int) {
	sub, _ := mux.Vars(ctx.Request)["subsystem"]
	level, _ := mux.Vars(ctx.Request)["level"]
	lvl := logpkg.ParseLevel(level)
	if lvl == logpkg.LevelInvalid {
		panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("undefined log level '%s'", level), nil))
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
	case "report":
		key = "REPO"
	case "micheline":
		key = "MICH"
	default:
		panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("undefined subsystem '%s'", sub), nil))
	}
	logger, ok := LoggerMap[key]
	if ok {
		logger.SetLevel(lvl)
	}
	return nil, http.StatusNoContent
}

func DumpTable(ctx *server.Context) (interface{}, int) {
	tname, _ := mux.Vars(ctx.Request)["table"]
	pname, _ := mux.Vars(ctx.Request)["part"]
	table, err := ctx.Indexer.Table(tname)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such table", nil))
	}

	spath := config.GetString("crawler.snapshot_path")
	if spath == "" {
		panic(server.EForbidden(server.EC_ACCESS_READONLY, "snapshots and dumps disabled, set crawler.snapshot_path to enable", nil))
	}

	switch pname {
	case "blocks", "journal", "table", "index", "data":
		// OK
	default:
		panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("undefined table part '%s'", pname), nil))
	}

	fname := fmt.Sprintf("%s-%s-%s.dump", tname, pname, ctx.Now.Format("2006-01-02T15:04:05"))
	f, err := os.Create(filepath.Join(spath, fname))
	if err != nil {
		panic(server.EInternal(server.EC_SERVER, "cannot create dump file", err))
	}
	defer f.Close()

	switch pname {
	case "blocks":
		err = table.DumpPackBlocks(f, 0)
	case "journal":
		err = table.DumpJournal(f, 0)
	case "table":
		err = table.DumpPackInfo(f, 0, false)
	case "index":
		err = table.DumpIndexPackInfo(f, 0, false)
	case "data":
		for i := 0; ; i++ {
			err2 := table.DumpPack(f, i, 0)
			if err2 == pack.ErrPackNotFound {
				break
			}
			if err2 != nil {
				err = err2
				break
			}
		}
	}
	if err != nil {
		panic(server.EInternal(server.EC_SERVER, "dump failed", err))
	}

	return nil, http.StatusNoContent
}
