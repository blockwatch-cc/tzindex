// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"blockwatch.cc/tzgo/tezos"
	"time"
)

func init() {
	LoadSchema(rightsNs, []byte(rightsSchema), &Rights{})
}

const (
	rightsNs     = "rights"
	rightsSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzstats.com/metadata/schemas/rights.json",
	"title": "Rights Info",
    "description": "A list of rights related settings.",
	"type": "object",
	"properties": {
		"date": {
		  "type": "string",
	      "format": "date-time"
  		},
		"rights": {
		  "type": "string"
  		},
		"license": {
		  "type": "string"
  		},
		"minter": {
		  "type": "string",
		  "format": "tzaddress"
  		},
		"authors": {
		  "type": "array",
		  "uniqueItems": true,
		  "items": {
		  	"type": "string"
		  }
		},
		"creators": {
		  "type": "array",
		  "uniqueItems": true,
		  "items": {
		  	"type": "string"
		  }
		},
		"contributors": {
		  "type": "array",
		  "uniqueItems": true,
		  "items": {
		  	"type": "string"
		  }
		},
		"publishers": {
		  "type": "array",
		  "uniqueItems": true,
		  "items": {
		  	"type": "string"
		  }
		}
	}
}`
)

type Rights struct {
	Date         time.Time     `json:"date,omitempty"`
	Rights       string        `json:"rights,omitempty"`
	License      string        `json:"license,omitempty"`
	Minter       tezos.Address `json:"minter,omitempty"`
	Authors      []string      `json:"authors,omitempty"`
	Creators     []string      `json:"creators,omitempty"`
	Contributors []string      `json:"contributors,omitempty"`
	Publishers   []string      `json:"publishers,omitempty"`
}

func (d Rights) Namespace() string {
	return rightsNs
}

func (d Rights) Validate() error {
	s, ok := GetSchema(rightsNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
