// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
    LoadSchema(dexNs, []byte(dexSchema), &Dex{})
}

const (
    dexNs     = "dex"
    dexSchema = `{
    "$schema": "http://json-schema.org/draft/2019-09/schema#",
    "$id": "https://api.tzstats.com/metadata/schemas/dex.json",
    "title": "Dex Info",
    "description": "Metadata for decentralized exchanges.",
    "type": "object",
    "additionalProperties": false,
    "required": [
        "kind",
        "pairs"
    ],
    "properties": {
        "kind": {
            "type": "string",
            "enum": [
                "quipu_v1",
                "quipu_token",
                "quipu_v2",
                "plenty_v1",
                "vortex_v1",
                "aliens_v1",
                "spicy_v1",
                "flame_v1",
                "cpmm_v1",
                "sexp_v1"
            ],
            "description": "An enum type used to identify the trading engine."
        },
        "trading_fee": {
          "type": "number",
          "minimum": 0,
          "maximum": 1
        },
        "exit_fee": {
          "type": "number",
          "minimum": 0,
          "maximum": 1
        },
        "url": {
          "type": "string",
          "format": "uri",
          "pattern": "^(https?|ipfs)://"
        },
        "pairs": {
            "type": "array",
            "minItems": 1,
            "items": {
                "$ref": "#/$defs/pair"
            },
            "description": "List of trading pairs."
        }
    },
    "$defs": {
        "pair": {
            "type": "object",
            "additionalProperties": false,
            "required": [
                "token_a",
                "token_b"
            ],
            "properties": {
                "pair_id": {
                  "type": "integer",
                  "minimum": 0,
                  "description": "The pair_id used in a multi-pool DEX contract (optional)."
                },
                "token_a": {
                  "$ref": "#/$defs/token",
                  "description": "Token A traded in this pool."
                },
                "token_b": {
                  "$ref": "#/$defs/token",
                  "description": "Token B traded in this pool."
                },
                "url": {
                  "type": "string",
                  "format": "uri",
                  "pattern": "^(https?|ipfs)://"
                }
            }
        },
        "token": {
            "type": "object",
            "additionalProperties": false,
            "required": [ "type" ],
            "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "tez",
                    "fa12",
                    "fa2"
                  ]
                },
                "address": {
                    "type": "string",
                    "format": "tzaddress"
                },
                "token_id": {
                  "type": "integer",
                  "minimum": 0
                }
            }
        }
    }
}`
)

// AMM and other decentralized exchanges
type Dex struct {
    Kind       string    `json:"kind"`                  // quipu_v1, quipu_token, quipu_v2, vortex, ..
    TradingFee float64   `json:"trading_fee,omitempty"` // trading fee
    ExitFee    float64   `json:"exit_fee,omitempty"`    // remove liquidity fee
    Url        string    `json:"url,omitempty"`         // homepage
    Pairs      []DexPair `json:"pairs"`                 // trading pairs
}

type DexPair struct {
    PairId *int64   `json:"pair_id,omitempty"` // 0 when single pool dex
    TokenA DexToken `json:"token_a"`
    TokenB DexToken `json:"token_b"`
    Url    string   `json:"url,omitempty"` // deep link
}

type DexToken struct {
    Type    string `json:"type"`               // tez, fa12, fa2
    Address string `json:"address,omitempty"`  // token ledger, only used for FA*
    TokenId *int64 `json:"token_id,omitempty"` // only used for FA2
}

func (d Dex) Namespace() string {
    return dexNs
}

func (d Dex) Validate() error {
    s, ok := GetSchema(dexNs)
    if ok {
        return s.Validate(d)
    }
    return nil
}
