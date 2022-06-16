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
    "description": "Tezos domains alias domains and optional reverse record.",
    "type": "object",
    "required": ["records"],
    "properties": {
        "name": {
          "type": "string",
          "description": "Reverse record domain name."
        },
        "records": {
            "type": "array",
            "description": "List of domain records associated with this account.",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["address", "name", "owner", "expiry"],
                "properties": {
                    "address": {
                      "type": "string",
                      "format": "tzaddress"
                    },
                    "name": {
                      "type": "string"
                    },
                    "owner": {
                      "type": "string",
                      "format": "tzaddress"
                    },
                    "expiry": {
                      "type": "string",
                      "format": "date-time"
                    },
                    "data": {
                      "type": "object"
                    }
                }
            }
        }
    }
}`
)

type TezosDomains struct {
  Name    string               `json:"name"`
  Records []TezosDomainsRecord `json:"records"`
}

type TezosDomainsRecord struct {
  Address tezos.Address     `json:"address"`
  Name    string            `json:"name"`
  Owner   tezos.Address     `json:"owner"`
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
