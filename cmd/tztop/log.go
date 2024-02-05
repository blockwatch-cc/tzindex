// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"os"

	"github.com/echa/config"
	logpkg "github.com/echa/log"
)

var (
	log = logpkg.NewLogger("MAIN") // main program
)

// Initialize package-global logger variables.
func init() {
	config.SetDefault("logging.backend", "stdout")
	config.SetDefault("logging.flags", "date,time,micro,utc")
	config.SetDefault("logging.level", "info")
}

// // subsystemLoggers maps each subsystem identifier to its associated logger.
// var subsystemLoggers = map[string]logpkg.Logger{
// 	"MAIN": log,
// }

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
	// store loggers in map
	// subsystemLoggers = map[string]logpkg.Logger{
	// 	"MAIN": log,
	// }
}

// setLogLevel sets the logging level for provided subsystem.  Invalid
// subsystems are ignored.
// func setLogLevel(subsystemID string, level logpkg.Level) {
// 	// Ignore invalid subsystems.
// 	logger, ok := subsystemLoggers[subsystemID]
// 	if !ok {
// 		return
// 	}

// 	logger.SetLevel(level)
// }

// setLogLevels sets the log level for all subsystem loggers to the passed
// level.
// func setLogLevels(level logpkg.Level) {
// 	for subsystemID := range subsystemLoggers {
// 		setLogLevel(subsystemID, level)
// 	}
// }
