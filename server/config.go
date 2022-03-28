// Copyright (c) 2018 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/rpc"
)

type Config struct {
	Crawler *etl.Crawler
	Indexer *etl.Indexer
	Client  *rpc.Client
	Http    HttpConfig
}

func (c Config) ClampList(count uint) uint {
	def := c.Http.DefaultListCount
	max := c.Http.MaxListCount
	if count <= 0 {
		return def
	}
	if max > 0 && count > max {
		return max
	}
	return count
}

func (c Config) ClampList64(count int64) int64 {
	def := int64(c.Http.DefaultListCount)
	max := int64(c.Http.MaxListCount)
	if count <= 0 {
		return def
	}
	if count > max {
		return max
	}
	return count
}

func (c Config) ClampExplore(count uint) uint {
	def := c.Http.DefaultExploreCount
	max := c.Http.MaxExploreCount
	if count <= 0 {
		return def
	}
	if max > 0 && count > max {
		return max
	}
	return count
}

func (c Config) ClampExplore64(count int64) int64 {
	def := int64(c.Http.DefaultExploreCount)
	max := int64(c.Http.MaxExploreCount)
	if count <= 0 {
		return def
	}
	if count > max {
		return max
	}
	return count
}

// HTTP Server Configuration
type HttpConfig struct {
	Addr                string        `json:"addr"`
	Port                int           `json:"port"`
	Scheme              string        `json:"scheme"`
	Host                string        `json:"host"`
	MaxWorkers          int           `json:"max_workers"`
	MaxQueue            int           `json:"max_queue"`
	ReadTimeout         time.Duration `json:"read_timeout"`
	HeaderTimeout       time.Duration `json:"header_timeout"`
	WriteTimeout        time.Duration `json:"write_timeout"`
	KeepAlive           time.Duration `json:"keep_alive"`
	ShutdownTimeout     time.Duration `json:"shutdown_timeout"`
	DefaultListCount    uint          `json:"default_list_count"`
	MaxListCount        uint          `json:"max_list_count"`
	DefaultExploreCount uint          `json:"default_explore_count"`
	MaxExploreCount     uint          `json:"max_explore_count"`
	MaxSeriesDuration   time.Duration `json:"max_series_duration"`
	CorsEnable          bool          `json:"cors_enable"`
	CorsOrigin          string        `json:"cors_origin"`
	CorsAllowHeaders    string        `json:"cors_allow_headers"`
	CorsExposeHeaders   string        `json:"cors_expose_headers"`
	CorsMethods         string        `json:"cors_methods"`
	CorsMaxAge          string        `json:"cors_maxage"`
	CorsCredentials     string        `json:"cors_credentials"`
	CacheEnable         bool          `json:"cache_enable"`
	CacheControl        string        `json:"cache_control"`
	CacheExpires        time.Duration `json:"cache_expires"`
	CacheMaxExpires     time.Duration `json:"cache_max"`
}

func (c HttpConfig) Address() string {
	return net.JoinHostPort(c.Addr, strconv.Itoa(c.Port))
}

func NewHttpConfig() HttpConfig {
	return HttpConfig{
		Addr:                "127.0.0.1",
		Port:                8000,
		Host:                "127.0.0.1",
		Scheme:              "http",
		MaxWorkers:          50,
		MaxQueue:            200,
		HeaderTimeout:       2 * time.Second,  // header timeout
		ReadTimeout:         5 * time.Second,  // header+body timeout
		WriteTimeout:        90 * time.Second, // response deadline
		KeepAlive:           90 * time.Second, // timeout for idle connections
		ShutdownTimeout:     30 * time.Second, // grafecul shutdown deadline
		DefaultListCount:    500,
		MaxListCount:        5000,
		DefaultExploreCount: 20,
		MaxExploreCount:     100,
		MaxSeriesDuration:   90 * 24 * time.Hour,
		CacheExpires:        30 * time.Second,
		CacheMaxExpires:     24 * time.Hour,
	}
}

func (cfg *HttpConfig) Check() error {
	// pre-process config
	if u, err := url.Parse(cfg.Host); err == nil {
		if u.Host != "" {
			cfg.Host = u.Host
		}
		if u.Scheme != "" {
			cfg.Scheme = u.Scheme
		}
	}

	if cfg.Scheme != "https" && cfg.Scheme != "http" {
		cfg.Scheme = "http"
	}

	var hasError bool
	// server IP and port
	if cfg.Addr == "" {
		log.Errorf("Empty API server address")
		hasError = true
	}

	if cfg.Port == 0 {
		log.Errorf("Empty API server port")
		hasError = true
	}

	if cfg.HeaderTimeout <= 0 {
		log.Errorf("Invalid API header timeout %v", cfg.HeaderTimeout)
		hasError = true
	}

	if cfg.ReadTimeout <= 0 {
		log.Errorf("Invalid API read timeout %v", cfg.ReadTimeout)
		hasError = true
	}

	if cfg.WriteTimeout <= 0 {
		log.Errorf("Invalid API write timeout %v", cfg.WriteTimeout)
		hasError = true
	}

	if cfg.KeepAlive <= 0 {
		log.Errorf("Invalid keep alive timeout %v", cfg.KeepAlive)
		hasError = true
	}

	if cfg.ShutdownTimeout <= 0 {
		log.Errorf("Invalid shutdown timeout %v", cfg.ShutdownTimeout)
		hasError = true
	}

	if cfg.Addr == "0.0.0.0" {
		log.Warn("HTTP Server reachable on all interfaces (0.0.0.0)")
	}

	if cfg.Addr == "127.0.0.1" || cfg.Addr == "localhost" {
		log.Warn("HTTP Server reachable on localhost only")
	}

	//  warn when port is used (may be blocked by Safari/iOS (6666, 6667, 6000))
	if cfg.Port == 6666 || cfg.Port == 6667 || cfg.Port == 6000 {
		log.Warn("HTTP Server port may be blocked by Apple Webkit browsers")
	}

	if cfg.Scheme == "" {
		log.Errorf("Empty http scheme")
		hasError = true
	}

	if cfg.Host == "" {
		log.Errorf("Empty http hostname")
		hasError = true
	}

	if hasError {
		return fmt.Errorf("HTTP Server configuration error.")
	}

	return nil
}
