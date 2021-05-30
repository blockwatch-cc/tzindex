// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"fmt"
)

func init() {
	LoadSchema(bakerNs, []byte(bakerSchema), &Baker{})
}

const (
	bakerNs     = "baker"
	bakerSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzstats.com/metadata/schemas/baker.json",
	"title": "Baker Info",
    "description": "A list of baker related settings.",
	"type": "object",
	"required": ["status"],
	"properties": {
		"status": {
		  "type": "string",
		  "enum": ["public","private","closing","closed"]
		},
		"fee": {
		  "type": "number",
		  "minimum": 0,
		  "maximum": 1
		},
		"payout_delay": {
		  "type": "boolean"
		},
		"min_payout": {
		  "type": "number",
		  "minimum": 0
		},
		"min_delegation": {
		  "type": "number",
		  "minimum": 0
		},
		"non_delegatable": {
		  "type": "boolean"
		}
	}
}`
)

// basic baker data for tzstats display
type Baker struct {
	Status         BakerStatus `json:"status,omitempty"`
	Fee            float64     `json:"fee,omitempty"`
	PayoutDelay    bool        `json:"payout_delay,omitempty"`
	MinPayout      float64     `json:"min_payout,omitempty"`
	MinDelegation  float64     `json:"min_delegation,omitempty"`
	NonDelegatable bool        `json:"non_delegatable,omitempty"`
}

func (d Baker) Namespace() string {
	return locationNs
}

func (d Baker) Validate() error {
	s, ok := GetSchema(bakerNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}

type BakerStatus string

const (
	BakerStatusInvalid BakerStatus = ""
	BakerStatusPublic  BakerStatus = "public"
	BakerStatusPrivate BakerStatus = "private"
	BakerStatusClosing BakerStatus = "closing"
	BakerStatusClosed  BakerStatus = "closed"
)

func (s *BakerStatus) UnmarshalText(data []byte) error {
	if ss := ParseBakerStatus(string(data)); ss.IsValid() {
		*s = ss
		return nil
	}
	return fmt.Errorf("invalid baker status '%s'", string(data))
}

func (s BakerStatus) IsValid() bool {
	return s != BakerStatusInvalid
}

func ParseBakerStatus(s string) BakerStatus {
	switch s {
	case "public":
		return BakerStatusPublic
	case "private":
		return BakerStatusPrivate
	case "closing":
		return BakerStatusClosing
	case "closed":
		return BakerStatusClosed
	default:
		return BakerStatusInvalid
	}
}
