// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"time"

	"blockwatch.cc/tzgo/tezos"
)

func init() {
	LoadSchema(updatedNs, []byte(updatedSchema), &Updated{})
}

const (
	updatedNs     = "updated"
	updatedSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzpro.io/metadata/schemas/updated.json",
	"title": "Record Update Information",
    "description": "Info about when this metadata entry was last updated.",
	"type": "object",
	"properties": {
		"hash": {
		  "type": "string",
		  "format": "tzblock"
  		},
		"height": {
		  "type": "number",
          "minimum": 0
  		},
		"time": {
		  "type": "string",
		  "format": "date-time"
  		}
	}
}`
)

type Updated struct {
	Hash   tezos.BlockHash `json:"hash"`
	Height int64           `json:"height"`
	Time   time.Time       `json:"time"`
}

func (d Updated) Namespace() string {
	return updatedNs
}

func (d Updated) Validate() error {
	s, ok := GetSchema(updatedNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
