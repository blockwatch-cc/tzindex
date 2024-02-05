// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"os"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
	"blockwatch.cc/tzindex/server/explorer"
	"blockwatch.cc/tzindex/server/series"
	"blockwatch.cc/tzindex/server/system"
	"blockwatch.cc/tzindex/server/tables"
	"github.com/echa/config"
	logpkg "github.com/echa/log"
)

var (
	log     = logpkg.NewLogger("MAIN") // main program
	etlLog  = logpkg.NewLogger("ETL ") // blockchain
	dataLog = logpkg.NewLogger("DATA") // database
	jrpcLog = logpkg.NewLogger("RPC ") // json rpc client
	srvrLog = logpkg.NewLogger("API ") // api server
	repoLog = logpkg.NewLogger("REPO") // reports
	michLog = logpkg.NewLogger("MICH") // micheline
)

// Initialize package-global logger variables.
func init() {
	config.SetDefault("log.backend", "stdout")
	config.SetDefault("log.flags", "date,time,micro,utc")

	// assign default loggers
	etl.UseLogger(etlLog)
	cache.UseLogger(etlLog)
	model.UseLogger(etlLog)
	index.UseLogger(etlLog)
	store.UseLogger(dataLog)
	pack.UseLogger(dataLog)
	rpc.UseLogger(jrpcLog)
	server.UseLogger(srvrLog)
	explorer.UseLogger(srvrLog)
	tables.UseLogger(srvrLog)
	series.UseLogger(srvrLog)
	system.UseLogger(srvrLog)
	micheline.UseLogger(michLog)
}

// subsystemLoggers maps each subsystem identifier to its associated logger.
var subsystemLoggers = map[string]logpkg.Logger{
	"MAIN": log,
	"ETL ": etlLog,
	"DATA": dataLog,
	"RPC ": jrpcLog,
	"API ": srvrLog,
	"REPO": repoLog,
	"MICH": michLog,
}

func initLogging() {
	cfg := logpkg.NewConfig()
	cfg.Level = logpkg.ParseLevel(config.GetString("log.level"))
	cfg.Flags = logpkg.ParseFlags(config.GetString("log.flags"))
	cfg.Backend = config.GetString("log.backend")
	cfg.Filename = config.GetString("log.filename")
	cfg.Addr = config.GetString("log.syslog.address")
	cfg.Facility = config.GetString("log.syslog.facility")
	cfg.Ident = config.GetString("log.syslog.ident")
	cfg.FileMode = os.FileMode(config.GetInt("log.filemode"))
	logpkg.Init(cfg)

	log = logpkg.NewLogger("MAIN") // command level

	// create loggers with configured backend
	etlLog = logpkg.NewLogger("ETL ") // blockchain
	etlLog.SetLevel(logpkg.ParseLevel(config.GetString("log.etl")))
	dataLog = logpkg.NewLogger("DATA") // database
	dataLog.SetLevel(logpkg.ParseLevel(config.GetString("log.db")))
	jrpcLog = logpkg.NewLogger("RPC ") // json rpc client
	jrpcLog.SetLevel(logpkg.ParseLevel(config.GetString("log.rpc")))
	srvrLog = logpkg.NewLogger("API ") // api server
	srvrLog.SetLevel(logpkg.ParseLevel(config.GetString("log.server")))
	repoLog = logpkg.NewLogger("REPO") // reports
	repoLog.SetLevel(logpkg.ParseLevel(config.GetString("log.report")))
	michLog = logpkg.NewLogger("MICH") // micheline
	michLog.SetLevel(logpkg.ParseLevel(config.GetString("log.micheline")))

	// assign default loggers
	etl.UseLogger(etlLog)
	cache.UseLogger(etlLog)
	model.UseLogger(etlLog)
	index.UseLogger(etlLog)
	store.UseLogger(dataLog)
	pack.UseLogger(dataLog)
	rpc.UseLogger(jrpcLog)
	server.UseLogger(srvrLog)
	explorer.UseLogger(srvrLog)
	tables.UseLogger(srvrLog)
	server.UseLogger(srvrLog)
	system.UseLogger(srvrLog)
	micheline.UseLogger(michLog)

	// store loggers in map
	subsystemLoggers = map[string]logpkg.Logger{
		"MAIN": log,
		"ETL ": etlLog,
		"DATA": dataLog,
		"RPC ": jrpcLog,
		"SRVR": srvrLog,
		"REPO": repoLog,
		"MICH": michLog,
	}

	// export to server for http control
	system.LoggerMap = subsystemLoggers

	// handle cli flags
	switch {
	case vtrace:
		setLogLevels(logpkg.LevelTrace)
	case vdebug:
		setLogLevels(logpkg.LevelDebug)
	case verbose:
		setLogLevels(logpkg.LevelInfo)
	}
}

// setLogLevel sets the logging level for provided subsystem.  Invalid
// subsystems are ignored.
func setLogLevel(subsystemID string, level logpkg.Level) {
	// Ignore invalid subsystems.
	logger, ok := subsystemLoggers[subsystemID]
	if !ok {
		return
	}

	logger.SetLevel(level)
}

// setLogLevels sets the log level for all subsystem loggers to the passed
// level.
func setLogLevels(level logpkg.Level) {
	for subsystemID := range subsystemLoggers {
		setLogLevel(subsystemID, level)
	}
}
