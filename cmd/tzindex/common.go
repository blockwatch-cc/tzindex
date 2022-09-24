// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"github.com/echa/config"
)

// common command-line options
var (
	// rpc-specific options
	rpcurl   string
	rpcuser  string
	rpcpass  string
	notls    bool
	insecure bool

	// index options
	lightIndex bool
	fullIndex  bool
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
	if lightIndex {
		return []model.BlockIndexer{
			index.NewAccountIndex(tableOptions("account"), indexOptions("account")),
			index.NewBalanceIndex(tableOptions("balance")),
			index.NewContractIndex(tableOptions("contract"), indexOptions("contract")),
			index.NewStorageIndex(tableOptions("storage")),
			index.NewConstantIndex(tableOptions("constant"), indexOptions("constant")),
			index.NewBlockIndex(tableOptions("block")),
			index.NewOpIndex(tableOptions("op")),
			index.NewEventIndex(tableOptions("event")),
			index.NewFlowIndex(tableOptions("flow")),
			index.NewChainIndex(tableOptions("chain")),
			index.NewSupplyIndex(tableOptions("supply")),
			index.NewBigmapIndex(tableOptions("bigmap")),
			index.NewMetadataIndex(tableOptions("metadata"), indexOptions("metadata")),
		}
	} else {
		return []model.BlockIndexer{
			index.NewAccountIndex(tableOptions("account"), indexOptions("account")),
			index.NewBalanceIndex(tableOptions("balance")),
			index.NewContractIndex(tableOptions("contract"), indexOptions("contract")),
			index.NewStorageIndex(tableOptions("storage")),
			index.NewConstantIndex(tableOptions("constant"), indexOptions("constant")),
			index.NewBlockIndex(tableOptions("block")),
			index.NewOpIndex(tableOptions("op")),
			index.NewEventIndex(tableOptions("event")),
			index.NewFlowIndex(tableOptions("flow")),
			index.NewChainIndex(tableOptions("chain")),
			index.NewSupplyIndex(tableOptions("supply")),
			index.NewRightsIndex(tableOptions("right")),
			index.NewSnapshotIndex(tableOptions("snapshot")),
			index.NewIncomeIndex(tableOptions("income")),
			index.NewGovIndex(tableOptions("gov")),
			index.NewBigmapIndex(tableOptions("bigmap")),
			index.NewMetadataIndex(tableOptions("metadata"), indexOptions("metadata")),
		}
	}
}
