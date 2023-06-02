// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
	LoadSchema(aliasNs, []byte(aliasSchema), &Alias{})
}

const (
	aliasNs     = "alias"
	aliasSchema = `{
  "$schema": "http://json-schema.org/draft/2019-09/schema#",
  "$id": "https://api.tzstats.com/metadata/schemas/alias.json",
  "title": "Alias",
  "type": "object",
  "required": [ "name", "kind" ],
  "properties": {
    "name": {
      "type": "string",
      "description": "Display name for this address or asset."
    },
    "kind": {
      "type": "string",
      "enum": [
        "validator",
        "tf",
        "payout",
        "merchant",
        "exchange",
        "custodian",
        "token",
        "dex",
        "factory",
        "mintery",
        "oracle",
        "admin",
        "deployer",
        "registry",
        "charity",
        "nft",
        "game",
        "dao",
        "treasury",
        "bridge",
        "lending",
        "farm",
        "insurance",
        "multisig",
        "market",
        "crowdsale",
        "user",
        "contract",
        "collection",
        "other"
      ],
      "description": "A structured type used for filtering."
    },
    "description": {
      "description": "A brief description.",
      "type": "string"
    },
    "category": {
      "description": "A user-defined category.",
      "type": "string"
    },
    "logo": {
      "description": "A filename or URL pointing to a logo image.",
      "type": "string"
    },
    "tags": {
      "type": "array",
      "uniqueItems": true,
      "items": {
        "type": "string"
      }
    }
  }
}`
)

type Alias struct {
	Name        string   `json:"name"`
	Kind        string   `json:"kind"`
	Description string   `json:"description,omitempty"`
	Category    string   `json:"category,omitempty"`
	Logo        string   `json:"logo,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

func (d Alias) Namespace() string {
	return aliasNs
}

func (d Alias) Validate() error {
	s, ok := GetSchema(aliasNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
