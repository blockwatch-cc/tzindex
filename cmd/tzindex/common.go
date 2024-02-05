// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"github.com/echa/config"

	// metadata decoders
	_ "blockwatch.cc/tzindex/etl/metadata/decoder/domain"
	_ "blockwatch.cc/tzindex/etl/metadata/decoder/profile"
)

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
			DisableCompression:    false,
			ForceAttemptHTTP2:     true,
		},
	}
	return &client, nil
}

func newRPCClient() (*rpc.Client, error) {
	c, err := newHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("rpc client: %w", err)
	}
	usetls := !config.GetBool("rpc.disable_tls")
	u, err := url.Parse(config.GetString("rpc.url"))
	if err != nil {
		return nil, err
	}
	if usetls {
		u.Scheme = "https"
	}
	if p := config.GetString("rpc.path"); p != "" {
		u.Path = p
	}
	rpcclient, err := rpc.NewClient(u.String(), c)
	if err != nil {
		return nil, fmt.Errorf("rpc client: %w", err)
	}
	rpcclient.
		WithApiKey(config.GetString("rpc.api_key")).
		WithUserAgent(UserAgent()).
		WithRetry(
			config.GetInt("rpc.retries"),
			config.GetDuration("rpc.retry_delay"),
		)

	return rpcclient, nil
}

func enabledIndexes() (list []model.BlockIndexer) {
	list = append(list,
		index.NewAccountIndex(),
		index.NewBalanceIndex(),
		index.NewContractIndex(),
		index.NewStorageIndex(),
		index.NewConstantIndex(),
		index.NewBlockIndex(),
		index.NewCycleIndex(),
		index.NewOpIndex(),
		index.NewEventIndex(),
		index.NewFlowIndex(),
		index.NewChainIndex(),
		index.NewSupplyIndex(),
		index.NewBigmapIndex(),
		index.NewTicketIndex(),
	)
	if !lightIndex {
		list = append(list,
			index.NewRightsIndex(),
			index.NewSnapshotIndex(),
			index.NewIncomeIndex(),
			index.NewGovIndex(),
		)
	}
	if experimental {
		list = append(list,
			index.NewMetadataIndex(),
			index.NewTokenIndex(),
		)
	}
	return
}
