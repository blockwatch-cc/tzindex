// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
	LoadSchema(NsDomain, []byte(domainSchema), &TezosDomains{})
}

const (
	NsDomain     = "tzdomain"
	domainSchema = `{
    "$schema": "http://json-schema.org/draft/2019-09/schema#",
    "$id": "https://api.tzpro.io/metadata/schemas/domain.json",
    "title": "Tezos Domains",
    "description": "Tezos domains reverse record.",
    "type": "object",
    "properties": {
        "name": {
          "type": "string",
          "description": "Reverse record domain name."
        }
    }
}`
)

type TezosDomains struct {
	Name string `json:"name"`
}

func (d TezosDomains) Namespace() string {
	return NsDomain
}

func (d TezosDomains) Validate() error {
	s, ok := GetSchema(NsDomain)
	if ok {
		return s.Validate(d)
	}
	return nil
}
