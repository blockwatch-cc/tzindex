// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
	LoadSchema(assetNs, []byte(assetSchema), &Asset{})
}

const (
	assetNs     = "asset"
	assetSchema = `{
    "$schema": "http://json-schema.org/draft/2019-09/schema#",
    "$id": "https://api.tzpro.io/metadata/schemas/asset.json",
    "title": "Asset Info",
    "description": "A list of asset related settings.",
    "type": "object",
    "required": ["standard"],
    "properties": {
        "standard": {
          "type": "string"
        },
        "tokens": {
          "type": "object",
          "patternProperties": {
            "^[0-9]+$": {
              "type": "object",
              "required": ["name", "symbol", "decimals"],
              "properties": {
                "name": {
                  "type": "string"
                },
                "symbol": {
                  "type": "string"
                },
                "decimals": {
                  "type": "integer",
                  "minimum": 0
                },
                "logo": {
                  "type": "string"
                }
              }
            }
          },
          "additionalProperties": false,
          "minProperties": 1
        },
        "version": {
          "type": "string"
        },
        "homepage": {
          "type": "string",
          "format": "uri",
          "pattern": "^(https?|ipfs)://"
        }
    }
}`
)

// fungible and non-fungible assets
type Token struct {
	Name     string `json:"name"`
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
	Logo     string `json:"logo,omitempty"`
}

type Asset struct {
	Standard string           `json:"standard,omitempty"`
	Tokens   map[string]Token `json:"tokens,omitempty"`
	Version  string           `json:"version,omitempty"`
	Homepage string           `json:"homepage,omitempty"`
}

func (d Asset) Namespace() string {
	return assetNs
}

func (d Asset) Validate() error {
	s, ok := GetSchema(assetNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
