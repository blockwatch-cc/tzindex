// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"blockwatch.cc/tzgo/micheline"
)

func init() {
	LoadSchema(tz16Ns, []byte(tz16Schema), &Tz16{})
}

// https://gitlab.com/tzip/tzip/-/blob/master/proposals/tzip-16/tzip-16.md
const (
	tz16Ns     = "tz16"
	tz16Schema = `{
    "$schema": "http://json-schema.org/draft/2019-09/schema#",
    "$id": "https://api.tzpro.io/metadata/schemas/tz16.json",
    "title": "Tzip16",
    "type": "object",
    "properties": {
        "name": { "type": "string" },
        "description": { "type": "string" },
        "version": { "type": "string" },
        "license": {
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "details": { "type": "string" }
            }
        },
        "authors": {
            "type": "array",
            "uniqueItems": true,
            "items": { "type": "string" }
        },
        "homepage": { "type": "string", "format": "uri", "pattern": "^(https?|ipfs)://" },
        "source": {
            "type": "object",
            "properties": {
                "tools": {
                    "type": "array",
                    "uniqueItems": true,
                    "items": { "type": "string" }
                },
                "Details": { "type": "string" }
            }
        },
        "interfaces": {
            "type": "array",
            "uniqueItems": true,
            "items": { "type": "string" }
        }
    }
}`
)

type Tz16 struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Version     string       `json:"version,omitempty"`
	License     *Tz16License `json:"license,omitempty"`
	Authors     []string     `json:"authors,omitempty"`
	Homepage    string       `json:"homepage,omitempty"`
	Source      *Tz16Source  `json:"source,omitempty"`
	Interfaces  []string     `json:"interfaces,omitempty"`
	Errors      []Tz16Error  `json:"errors,omitempty"`
	Views       []Tz16View   `json:"views,omitempty"`
}

type Tz16License struct {
	Name    string `json:"name"`
	Details string `json:"details,omitempty"`
}

type Tz16Source struct {
	Tools    []string `json:"tools"`
	Location string   `json:"location,omitempty"`
}

type Tz16Error struct {
	Error     *micheline.Prim `json:"error,omitempty"`
	Expansion *micheline.Prim `json:"expansion,omitempty"`
	Languages []string        `json:"languages,omitempty"`
	View      string          `json:"view,omitempty"`
}

type Tz16View struct {
	Name            string        `json:"name"`
	Description     string        `json:"description,omitempty"`
	Pure            bool          `json:"pure,omitempty"`
	Implementations []interface{} `json:"implementations,omitempty"`
}

func (d Tz16) Namespace() string {
	return tz16Ns
}

func (d Tz16) Validate() error {
	s, ok := GetSchema(tz16Ns)
	if ok {
		return s.Validate(d)
	}
	return nil
}
