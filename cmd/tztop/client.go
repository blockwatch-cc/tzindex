// Copyright (c) 2021 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
//
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Config struct {
	BaseURL  string
	Host     string
	Cert     string
	Key      string
	Ca       string
	Insecure bool
}

type Client struct {
	cfg    Config
	client *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.Insecure,
	}
	if cfg.Cert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("Could not load TLS client cert or key %s] %v", cfg.Cert, err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		tlsConfig.BuildNameToCertificate()
	}
	if len(cfg.Ca) > 0 {
		// load from file
		caCert, err := ioutil.ReadFile(cfg.Ca)
		if err != nil {
			return nil, fmt.Errorf("Could not load TLS CA file %s: %v", cfg.Ca, err)
		}
		rootCAs := x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("Failed to add Root CAs to certificate pool")
		}
		tlsConfig.RootCAs = rootCAs
	}
	c := &Client{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
		cfg: cfg,
	}
	return c, nil
}

func (c *Client) get(path string, result interface{}) error {
	req, err := http.NewRequest(http.MethodGet, c.cfg.BaseURL+path, nil)
	if err != nil {
		return err
	}
	if c.cfg.Host != "" {
		req.Host = c.cfg.Host
	}
	res, err := c.client.Do(req)
	if err != nil {
		log.Debugf("query %s: failed with error:  %v", req.URL.String(), err)
		return err
	}
	if res.StatusCode/100 != 2 {
		log.Debugf("query %s: failed with status code %d", req.URL.String(), res.StatusCode)
		return err
	}
	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Debugf("reading failed: %v", err)
		return err
	}
	if err := json.Unmarshal(buf, result); err != nil {
		log.Debugf("unmarshalling failed: %v", err)
		return err
	}
	return nil
}
