// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
	LoadSchema(socialNs, []byte(socialSchema), &Social{})
}

const (
	socialNs     = "social"
	socialSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzstats.com/metadata/schemas/social.json",
	"title": "Social Media",
    "description": "A list of social media handles.",
	"type": "object",
	"properties": {
		"twitter": {
		  "type": "string"
  		},
		"instagram": {
		  "type": "string"
  		},
		"reddit": {
		  "type": "string"
  		},
		"github": {
		  "type": "string"
  		},
  		"website": {
  			"type": "string",
  			"format": "uri-reference"
  		}
	}
}`
)

type Social struct {
	Twitter   string `json:"twitter,omitempty"`
	Instagram string `json:"instagram,omitempty"`
	Reddit    string `json:"reddit,omitempty"`
	Github    string `json:"github,omitempty"`
	Website   string `json:"website,omitempty"`
}

func (d Social) Namespace() string {
	return socialNs
}

func (d Social) Validate() error {
	s, ok := GetSchema(socialNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
