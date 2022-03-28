// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"github.com/echa/config"
)

const (
	timeFormat = "2006-01-02 15:04:05"
	dateFormat = "2006-01-02"
)

// common command-line options
var (
	start string // time
	end   string // time

	// rpc-specific options
	rpcurl   string
	rpcuser  string
	rpcpass  string
	notls    bool
	insecure bool
)

var (
	statedb store.DB
	indexer *etl.Indexer
	cancel  context.CancelFunc
	ctx     context.Context
)

func tableOptions(name string) pack.Options {
	pre := "database." + name + "."
	return pack.Options{
		PackSizeLog2:    config.GetInt(pre + "pack_size_log2"),
		JournalSizeLog2: config.GetInt(pre + "journal_size_log2"),
		CacheSize:       config.GetInt(pre + "cache_size"),
		FillLevel:       config.GetInt(pre + "fill_level"),
	}
}

func indexOptions(name string) pack.Options {
	pre := "database." + name + "_index."
	return pack.Options{
		PackSizeLog2:    config.GetInt(pre + "pack_size_log2"),
		JournalSizeLog2: config.GetInt(pre + "journal_size_log2"),
		CacheSize:       config.GetInt(pre + "cache_size"),
		FillLevel:       config.GetInt(pre + "fill_level"),
	}
}

func openReadOnlyBlockchain() (*etl.Crawler, error) {
	engine := config.GetString("database.engine")
	pathname := config.GetString("database.path")
	log.Infof("Using %s database %s", engine, pathname)
	var err error

	statedb, err = store.Open(engine, filepath.Join(pathname, etl.StateDBName), DBOpts(engine, true, unsafe))
	if err != nil {
		return nil, fmt.Errorf("error opening %s database: %v", etl.StateDBName, err)
	}

	// enabled storage tables
	indexer = etl.NewIndexer(etl.IndexerConfig{
		DBPath:    pathname,
		DBOpts:    DBOpts(engine, true, false),
		StateDB:   statedb,
		Indexes:   enabledIndexes(),
		LightMode: true,
	})

	bc := etl.NewCrawler(etl.CrawlerConfig{
		DB:      statedb,
		Indexer: indexer,
		Client:  nil,
	})

	ctx, cancel = context.WithCancel(context.Background())
	if err := bc.Init(ctx, etl.MODE_INFO); err != nil {
		return nil, fmt.Errorf("error initializing blockchain: %v", err)
	}
	return bc, nil
}

func openReadWriteBlockchain() (*etl.Crawler, error) {
	engine := config.GetString("database.engine")
	pathname := config.GetString("database.path")
	log.Infof("Using %s database %s", engine, pathname)
	var err error

	statedb, err = store.Open(engine, filepath.Join(pathname, etl.StateDBName), DBOpts(engine, false, unsafe))
	if err != nil {
		return nil, fmt.Errorf("error opening %s database: %v", etl.StateDBName, err)
	}

	// enabled storage tables
	indexer = etl.NewIndexer(etl.IndexerConfig{
		DBPath:    pathname,
		DBOpts:    DBOpts(engine, false, false),
		StateDB:   statedb,
		Indexes:   enabledIndexes(),
		LightMode: true,
	})

	bc := etl.NewCrawler(etl.CrawlerConfig{
		DB:      statedb,
		Indexer: indexer,
		Client:  nil,
	})
	ctx, cancel = context.WithCancel(context.Background())
	if err := bc.Init(ctx, etl.MODE_INFO); err != nil {
		return nil, fmt.Errorf("error initializing blockchain: %v", err)
	}
	return bc, nil
}

func timeRange() (from, to time.Time, err error) {
	var f, t util.Time
	f, err = util.ParseTime(start)
	if err != nil {
		return
	}
	t, err = util.ParseTime(end)
	if err != nil {
		return
	}
	if !f.IsZero() && !t.IsZero() && f.After(t) {
		err = fmt.Errorf("time range mismatch (start>end)")
	}
	from, to = f.Time(), t.Time()
	return
}

func newHTTPClient() (*http.Client, error) {
	// Set proxy function if there is a proxy configured.
	var proxyFunc func(*http.Request) (*url.URL, error)
	if purl := config.GetString("rpc.proxy"); purl != "" {
		proxyURL, err := url.Parse(purl)
		if err != nil {
			return nil, err
		}
		proxyFunc = http.ProxyURL(proxyURL)
	}

	// Configure TLS if needed.
	var tlsConfig *tls.Config
	if !config.GetBool("rpc.disable_tls") {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: config.GetBool("rpc.insecure_tls"),
		}
	}

	// 	if len(config.Certificates) > 0 {
	// 		pool := x509.NewCertPool()
	// 		pool.AppendCertsFromPEM(config.Certificates)
	// 		tlsConfig.RootCAs = pool
	// 	}

	client := http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   config.GetDuration("rpc.dial_timeout"),
				KeepAlive: config.GetDuration("rpc.keepalive"),
			}).Dial,
			Proxy:                 proxyFunc,
			TLSClientConfig:       tlsConfig,
			IdleConnTimeout:       config.GetDuration("rpc.idle_timeout"),
			ResponseHeaderTimeout: config.GetDuration("rpc.response_timeout"),
			ExpectContinueTimeout: config.GetDuration("rpc.continue_timeout"),
			MaxIdleConns:          config.GetInt("rpc.idle_conns"),
			MaxIdleConnsPerHost:   config.GetInt("rpc.idle_conns"),
		},
	}
	return &client, nil
}

func newRPCClient() (*rpc.Client, error) {
	c, err := newHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("rpc client: %v", err)
	}
	usetls := !config.GetBool("rpc.disable_tls")
	host, port := config.GetString("rpc.host"), config.GetString("rpc.port")
	baseurl := host
	if port != "" {
		baseurl = net.JoinHostPort(host, port)
	}
	if usetls {
		baseurl = "https://" + baseurl
	} else {
		baseurl = "http://" + baseurl
	}
	rpcclient, err := rpc.NewClient(baseurl+"/"+config.GetString("rpc.path"), c)
	if err != nil {
		return nil, fmt.Errorf("rpc client: %v", err)
	}
	rpcclient.UserAgent = UserAgent()
	return rpcclient, nil
}

func parseRPCFlags() error {
	// overwrite config from flags only if set
	if serverCmd.Flags().Changed("notls") {
		config.Set("rpc.disable_tls", notls)
	}
	if serverCmd.Flags().Changed("insecure") {
		config.Set("rpc.insecure_tls", insecure)
	}
	if serverCmd.Flags().Changed("rpcurl") && rpcurl != "" {
		ux := rpcurl
		if !strings.HasPrefix(ux, "http") {
			if config.GetBool("rpc.disable_tls") {
				ux = "http://" + ux
			} else {
				ux = "https://" + ux
			}
		} else {
			config.Set("rpc.disable_tls", !strings.HasPrefix(ux, "https://"))
		}
		u, err := url.Parse(ux)
		if err != nil {
			return fmt.Errorf("invalid rpc url '%s': %v", rpcurl, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid rpc url '%s'", rpcurl)
		}
		fields := strings.Split(u.Host, ":")
		if len(fields[0]) > 0 {
			config.Set("rpc.host", fields[0])
		}
		if len(fields) > 1 && len(fields[1]) > 0 {
			config.Set("rpc.port", fields[1])
		} else {
			config.Set("rpc.port", "")
		}
		path := strings.TrimPrefix(u.Path, "/")
		if len(path) > 0 && !strings.HasSuffix(path, "/") {
			path = path + "/"
		}
		config.Set("rpc.path", path)
	}
	if serverCmd.Flags().Changed("rpcuser") {
		config.Set("rpc.user", rpcuser)
	}
	if serverCmd.Flags().Changed("rpcpass") {
		config.Set("rpc.pass", rpcpass)
	}
	return nil
}

func enabledIndexes() []model.BlockIndexer {
	return []model.BlockIndexer{
		index.NewAccountIndex(tableOptions("account"), indexOptions("account")),
		index.NewBalanceIndex(tableOptions("balance")),
		index.NewContractIndex(tableOptions("contract"), indexOptions("contract")),
		index.NewConstantIndex(tableOptions("constant"), indexOptions("constant")),
		index.NewBlockIndex(tableOptions("block"), indexOptions("block")),
		index.NewOpIndex(tableOptions("op"), indexOptions("op")),
		index.NewChainIndex(tableOptions("chain")),
		index.NewSupplyIndex(tableOptions("supply")),
		index.NewBigmapIndex(tableOptions("bigmap"), indexOptions("bigmap")),
		index.NewMetadataIndex(tableOptions("metadata"), indexOptions("metadata")),
	}
}
