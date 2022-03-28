// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"os"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
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
	blocLog = logpkg.NewLogger("BLOC") // blockchain
	dataLog = logpkg.NewLogger("DATA") // database
	jrpcLog = logpkg.NewLogger("JRPC") // json rpc client
	srvrLog = logpkg.NewLogger("SRVR") // api server
	michLog = logpkg.NewLogger("MICH") // micheline
)

// Initialize package-global logger variables.
func init() {
	config.SetDefault("logging.backend", "stdout")
	config.SetDefault("logging.flags", "date,time,micro,utc")

	// assign default loggers
	etl.UseLogger(blocLog)
	cache.UseLogger(blocLog)
	model.UseLogger(blocLog)
	index.UseLogger(blocLog)
	metadata.UseLogger(blocLog)
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
	"BLOC": blocLog,
	"DATA": dataLog,
	"JRPC": jrpcLog,
	"SRVR": srvrLog,
	"MICH": michLog,
}

func initLogging() {
	cfg := logpkg.NewConfig()
	cfg.Level = logpkg.ParseLevel(config.GetString("logging.level"))
	cfg.Flags = logpkg.ParseFlags(config.GetString("logging.flags"))
	cfg.Backend = config.GetString("logging.backend")
	cfg.Filename = config.GetString("logging.filename")
	cfg.Addr = config.GetString("logging.syslog.address")
	cfg.Facility = config.GetString("logging.syslog.facility")
	cfg.Ident = config.GetString("logging.syslog.ident")
	cfg.FileMode = os.FileMode(config.GetInt("logging.filemode"))
	logpkg.Init(cfg)

	log = logpkg.NewLogger("MAIN") // command level

	// create loggers with configured backend
	blocLog = logpkg.NewLogger("BLOC") // blockchain
	blocLog.SetLevel(logpkg.ParseLevel(config.GetString("logging.blockchain")))
	dataLog = logpkg.NewLogger("DATA") // database
	dataLog.SetLevel(logpkg.ParseLevel(config.GetString("logging.database")))
	jrpcLog = logpkg.NewLogger("JRPC") // json rpc client
	jrpcLog.SetLevel(logpkg.ParseLevel(config.GetString("logging.rpc")))
	srvrLog = logpkg.NewLogger("SRVR") // api server
	srvrLog.SetLevel(logpkg.ParseLevel(config.GetString("logging.server")))
	michLog = logpkg.NewLogger("MICH") // micheline
	michLog.SetLevel(logpkg.ParseLevel(config.GetString("logging.micheline")))

	// assign default loggers
	etl.UseLogger(blocLog)
	cache.UseLogger(blocLog)
	model.UseLogger(blocLog)
	index.UseLogger(blocLog)
	metadata.UseLogger(blocLog)
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
		"BLOC": blocLog,
		"DATA": dataLog,
		"JRPC": jrpcLog,
		"SRVR": srvrLog,
		"MICH": michLog,
	}

	// export to server for http control
	system.LoggerMap = subsystemLoggers
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
