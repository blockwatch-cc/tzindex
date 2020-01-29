// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmd

import (
	"github.com/echa/config"
	"time"
)

func init() {
	// database
	config.SetDefault("database.engine", "bolt")
	config.SetDefault("database.path", "./db")
	config.SetDefault("database.nosync", false)
	config.SetDefault("database.gc_ratio", 1.0)
	config.SetDefault("database.log_slow_queries", time.Second)
	config.SetDefault("database.addr_cache_size_log2", 17) // 128k

	// crawling
	config.SetDefault("crawler.queue", 100)
	config.SetDefault("crawler.snapshot_path", "./db/snapshots/")
	config.SetDefault("crawler.snapshot_blocks", nil)
	config.SetDefault("crawler.snapshot_interval", 0)

	// HTTP API server
	config.SetDefault("server.addr", "127.0.0.1")
	config.SetDefault("server.port", 8000)
	config.SetDefault("server.scheme", "http")
	config.SetDefault("server.host", "127.0.0.1")
	config.SetDefault("server.workers", 50)
	config.SetDefault("server.read_timeout", 5*time.Second)
	config.SetDefault("server.header_timeout", 2*time.Second)
	config.SetDefault("server.write_timeout", 15*time.Second)
	config.SetDefault("server.keepalive", 90*time.Second)
	config.SetDefault("server.shutdown_timeout", 15*time.Second)
	config.SetDefault("server.max_list_count", 500000)
	config.SetDefault("server.default_list_count", 500)
	config.SetDefault("server.max_explore_count", 100)
	config.SetDefault("server.default_explore_count", 20)
	config.SetDefault("server.cors_enable", false)
	config.SetDefault("server.cors_origin", "*")
	config.SetDefault("server.cors_allow_headers", "Authorization, Accept, Content-Type, X-Api-Key, X-Requested-With")
	config.SetDefault("server.cors_expose_headers", "Date, X-Runtime, X-Request-Id, X-Api-Version, X-Network-Id, X-Protocol-Hash")
	config.SetDefault("server.cors_methods", "GET, OPTIONS")
	config.SetDefault("server.cors_maxage", "86400")
	config.SetDefault("server.cors_credentials", "true")
	config.SetDefault("server.cache_control", "public")

	// logging
	config.SetDefault("logging.progress", 10*time.Second)
	config.SetDefault("logging.backend", "stdout")
	config.SetDefault("logging.flags", "date,time,micro,utc")
	config.SetDefault("logging.level", "info")
	config.SetDefault("logging.blockchain", "info")
	config.SetDefault("logging.database", "info")
	config.SetDefault("logging.rpc", "info")
	config.SetDefault("logging.server", "info")
	config.SetDefault("logging.micheline", "info")

	// REST client
	config.SetDefault("rpc.host", "127.0.0.1")
	config.SetDefault("rpc.port", 8732)
	config.SetDefault("rpc.disable_tls", false)
	config.SetDefault("rpc.proxy", "")
	// config.SetDefault("rpc.proxy_user", "")
	// config.SetDefault("rpc.proxy_pass", "")
	config.SetDefault("rpc.dial_timeout", 10*time.Second)
	config.SetDefault("rpc.keepalive", 30*time.Minute)
	config.SetDefault("rpc.idle_timeout", 30*time.Minute)
	config.SetDefault("rpc.response_timeout", 60*time.Second)
	config.SetDefault("rpc.continue_timeout", 60*time.Second)
	config.SetDefault("rpc.idle_conns", 16)
}
