// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
  LoadSchema(assetNs, []byte(assetSchema), &Asset{})
}

const (
  assetNs     = "asset"
  assetSchema = `{
    "$schema": "http://json-schema.org/draft/2019-09/schema#",
    "$id": "https://api.tzstats.com/metadata/schemas/asset.json",
    "title": "Asset Info",
    "description": "A list of asset related settings.",
    "type": "object",
    "required": ["standard", "symbol"],
    "properties": {
        "standard": {
          "type": "string"
        },
        "symbol": {
          "type": "string"
        },
        "decimals": {
          "type": "integer",
          "minimum": 0,
          "maximum": 36
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
type Asset struct {
  Standard string `json:"standard,omitempty"`
  Symbol   string `json:"symbol,omitempty"`
  Decimals int    `json:"decimals,omitempty"`
  Version  string `json:"version,omitempty"`
  Homepage string `json:"homepage,omitempty"`
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
