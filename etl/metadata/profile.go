// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
	LoadSchema(NsProfile, []byte(profileSchema), &TezosProfile{})
}

const (
	NsProfile     = "tzprofile"
	profileSchema = `{
    "$schema": "http://json-schema.org/draft/2019-09/schema#",
    "$id": "https://api.tzpro.io/metadata/schemas/tzprofile.json",
    "title": "Tezos Profile",
    "description": "Tezos profile record.",
    "type": "object",
    "properties": {
        "alias": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "logo": {
            "type": "string",
            "format": "uri-reference"
        },
        "twitter": {
            "type": "string"
        },
        "ethereum": {
            "type": "string"
        },
        "domain_name": {
            "type": "string"
        },
        "discord": {
            "type": "string"
        },
        "github": {
            "type": "string"
        },
        "website": {
            "type": "string",
            "format": "uri-reference"
        },
        "serial": {
            "type": "integer"
        }
    }
}`
)

type TezosProfile struct {
	Alias       string `json:"alias,omitempty"`
	Description string `json:"description,omitempty"`
	Logo        string `json:"logo,omitempty"`
	Website     string `json:"website,omitempty"`
	Twitter     string `json:"twitter,omitempty"`
	Ethereum    string `json:"ethereum,omitempty"`
	DomainName  string `json:"domain_name,omitempty"`
	Discord     string `json:"discord,omitempty"`
	Github      string `json:"github,omitempty"`
	Serial      uint64 `json:"serial,omitempty"`
}

func (d TezosProfile) Namespace() string {
	return NsProfile
}

func (d TezosProfile) Validate() error {
	s, ok := GetSchema(NsProfile)
	if ok {
		return s.Validate(d)
	}
	return nil
}
