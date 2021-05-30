// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"blockwatch.cc/tzgo/tezos"
	"time"
)

func init() {
	LoadSchema(domainNs, []byte(domainSchema), &TezosDomains{})
}

const (
	domainNs     = "domain"
	domainSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzstats.com/metadata/schemas/domain.json",
	"title": "Tezos Domains",
    "description": "Tezos domain reverse record.",
	"type": "object",
	"required": ["address", "name", "owner", "expiry"],
	"properties": {
		"address": {
		  "type": "string"
  		},
		"name": {
		  "type": "string"
  		},
		"owner": {
		  "type": "string"
  		},
		"expiry": {
		  "type": "string",
		  "format": "date-time"
  		},
		"data": {
		  "type": "object"
  		}
	}
}`
)

type TezosDomains struct {
	Address tezos.Address     `json:"address"`
	Name    string            `json:"name"`
	Owner   tezos.Address     `json:"address"`
	Expiry  time.Time         `json:"expiry"`
	Data    map[string]string `json:"data,omitempty"`
}

func (d TezosDomains) Namespace() string {
	return domainNs
}

func (d TezosDomains) Validate() error {
	s, ok := GetSchema(domainNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
