// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/echa/config"
)

var (
	flags   = flag.NewFlagSet(appName, flag.ContinueOnError)
	errExit = errors.New("exit")

	// general options
	verbose     bool
	vtrace      bool
	vdebug      bool
	vstats      int
	maxcpu      int
	showVersion bool
	configFile  string

	// indexer options
	noindex    bool
	nomonitor  bool
	norpc      bool
	noapi      bool
	cors       bool
	validate   bool
	stop       int64
	lightIndex bool
	fullIndex  bool
	notls      bool
	insecure   bool
)

func init() {
	// setup CLI flags
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&vdebug, "vv", false, "debug mode")
	flags.BoolVar(&vtrace, "vvv", false, "trace mode")
	flags.BoolVar(&showVersion, "version", false, "show version")
	flags.StringVar(&configFile, "c", "config.json", "read config from `file`")
	flags.StringVar(&configFile, "config", "config.json", "read config from `file`")
	flags.IntVar(&vstats, "stats", 0, "print statistics every `n` seconds")

	flags.BoolVar(&lightIndex, "light", true, "light mode (use to skip baker and gov data)")
	flags.BoolVar(&fullIndex, "full", false, "full mode (including baker and gov data)")
	flags.BoolVar(&norpc, "norpc", false, "disable RPC client")
	flags.BoolVar(&notls, "notls", false, "disable RPC TLS support (use http)")
	flags.BoolVar(&insecure, "insecure", false, "disable RPC TLS certificate checks (not recommended)")

	flags.BoolVar(&noapi, "noapi", false, "disable API server")
	flags.BoolVar(&noindex, "noindex", false, "disable indexing")
	flags.BoolVar(&nomonitor, "nomonitor", false, "disable block monitor")
	flags.BoolVar(&validate, "validate", false, "validate account balances")
	flags.Int64Var(&stop, "stop", 0, "stop indexing after `height`")
	flags.BoolVar(&cors, "enable-cors", false, "enable API CORS support")

	// go runtime
	config.SetDefault("go.cpu", 0)         // "max number of CPU cores to use (default: all)"
	config.SetDefault("go.gc", 20)         // "trigger GC when used mem grows by N percent"
	config.SetDefault("go.sample_rate", 0) // block and mutex profiling sample rate

	// database
	config.SetDefault("db.path", "./db")                // db base path
	config.SetDefault("db.nosync", false)               // skip fsync, dangerous
	config.SetDefault("db.no_grow_sync", false)         // don't use with ext3/4, good in Docker + XFS
	config.SetDefault("db.no_free_sync", false)         // skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
	config.SetDefault("db.page_size", os.Getpagesize()) // custom db page size (overwrites OS)
	config.SetDefault("db.gc_ratio", 1.0)
	config.SetDefault("db.log_slow_queries", time.Second)
	config.SetDefault("db.max_storage_entry_size", 131072) // contract storage max bytes per record

	// crawling
	config.SetDefault("crawler.queue", 100)
	config.SetDefault("crawler.delay", 1)
	config.SetDefault("crawler.snapshot.path", "./db/snapshots/")
	config.SetDefault("crawler.snapshot.blocks", nil)
	config.SetDefault("crawler.snapshot.interval", 0)

	// HTTP API server
	config.SetDefault("server.addr", "127.0.0.1")
	config.SetDefault("server.port", 8000)
	config.SetDefault("server.scheme", "http")
	config.SetDefault("server.host", "127.0.0.1")
	config.SetDefault("server.name", UserAgent())
	config.SetDefault("server.workers", 64)
	config.SetDefault("server.queue", 128)
	config.SetDefault("server.read_timeout", 5*time.Second)
	config.SetDefault("server.header_timeout", 2*time.Second)
	config.SetDefault("server.write_timeout", 90*time.Second)
	config.SetDefault("server.keepalive", 90*time.Second)
	config.SetDefault("server.shutdown_timeout", 60*time.Second)
	config.SetDefault("server.max_list_count", 500000)
	config.SetDefault("server.default_list_count", 500)
	config.SetDefault("server.max_series_duration", 0)
	config.SetDefault("server.max_explore_count", 100)
	config.SetDefault("server.default_explore_count", 20)
	config.SetDefault("server.cors_enable", false)
	config.SetDefault("server.cors_origin", "*")
	config.SetDefault("server.cors_allow_headers", strings.Join([]string{
		"Authorization",
		"Accept",
		"Content-Type",
		"X-Api-Key",
		"X-Requested-With",
	}, ","))
	config.SetDefault("server.cors_expose_headers", strings.Join([]string{
		"Date",
		"X-Runtime",
		"X-Request-Id",
		"X-Api-Version",
		"X-Network-Id",
		"X-Protocol-Hash",
	}, ","))
	config.SetDefault("server.cors_methods", "GET,PUT,POST,OPTIONS")
	config.SetDefault("server.cors_maxage", 86400*time.Second)
	config.SetDefault("server.cors_credentials", true)
	config.SetDefault("server.cache_control", "public")
	config.SetDefault("server.cache_expires", 30*time.Second)
	config.SetDefault("server.cache_max", 24*time.Hour)

	// REST client
	config.SetDefault("rpc.url", "http://127.0.0.1:8732")
	config.SetDefault("rpc.disable_tls", true)
	config.SetDefault("rpc.insecure_tls", false)
	config.SetDefault("rpc.proxy", "")
	// config.SetDefault("rpc.proxy_user", "")
	// config.SetDefault("rpc.proxy_pass", "")
	config.SetDefault("rpc.dial_timeout", 10*time.Second)
	config.SetDefault("rpc.keepalive", 30*time.Minute)
	config.SetDefault("rpc.idle_timeout", 30*time.Minute)
	config.SetDefault("rpc.response_timeout", 60*time.Minute)
	config.SetDefault("rpc.continue_timeout", 60*time.Second)
	config.SetDefault("rpc.idle_conns", 16)

	// logging
	config.SetDefault("log.progress", 10*time.Second)
	config.SetDefault("log.backend", "stdout")
	config.SetDefault("log.flags", "date,time,micro,utc")
	config.SetDefault("log.level", "info")
	config.SetDefault("log.etl", "info")
	config.SetDefault("log.db", "info")
	config.SetDefault("log.rpc", "info")
	config.SetDefault("log.api", "info")
	config.SetDefault("log.micheline", "info")
}

func loadConfig() error {
	config.SetEnvPrefix(envprefix)
	if configFile != "" {
		config.SetConfigName(configFile)
	}
	realconf := config.ConfigName()
	if _, err := os.Stat(realconf); err == nil {
		if err := config.ReadConfigFile(); err != nil {
			return fmt.Errorf("reading config file %q: %v\n", realconf, err)
		}
		log.Infof("Using config file %s", realconf)
	} else {
		log.Warnf("Missing config file, using default values.")
	}
	return nil
}

func parseFlags() error {
	// split cli args into known and extra
	knownFlags := make([]string, 0)
	extraFlags := make([]string, 0)
	for i := 1; i < len(os.Args); i++ {
		isKnown := flags.Lookup(os.Args[i][1:]) != nil || os.Args[i] == "-h"
		isSingle := true
		if i+1 < len(os.Args) {
			if !strings.HasPrefix(os.Args[i+1], "-") {
				isSingle = false
			}
		}
		if isKnown {
			knownFlags = append(knownFlags, os.Args[i])
			if !isSingle {
				knownFlags = append(knownFlags, os.Args[i+1])
				i++
			}
		} else {
			extraFlags = append(extraFlags, os.Args[i])
			if !isSingle {
				extraFlags = append(extraFlags, os.Args[i+1])
				i++
			}
		}
	}

	if err := flags.Parse(knownFlags); err != nil {
		if err == flag.ErrHelp {
			fmt.Printf("Usage: %s [flags]\n", appName)
			fmt.Println("\nFlags")
			flags.PrintDefaults()
			return errExit
		}
		return err
	}

	if showVersion {
		printVersion()
		return errExit
	}

	// load config file now (before applying extra CLI args so users can override
	// config file settings with cli args )
	if err := loadConfig(); err != nil {
		return err
	}

	// handle other command line flags
	for i := 0; i < len(extraFlags); i++ {
		key, val := extraFlags[i], ""
		if strings.Contains(key, "=") {
			split := strings.Split(key, "=")
			key = split[0]
			val = split[1]
		} else if i+1 < len(extraFlags) && !strings.HasPrefix(extraFlags[i+1], "-") {
			val = extraFlags[i+1]
			i++
		}
		if !strings.HasPrefix(key, "-") {
			return fmt.Errorf("Invalid flag %q: missing dash", key)
		}
		key = strings.TrimPrefix(key, "-")
		if val != "" {
			log.Debugf("Flag %s=%s", key, val)
			config.Set(key, val)
		} else {
			config.Set(key, true) // assume boolean flag
		}
	}
	return nil
}
