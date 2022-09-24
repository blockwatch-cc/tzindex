// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"encoding/json"
	"fmt"
	"github.com/echa/config"
	"sort"
)

var registry *Registry

func RegisterSchema(s Schema) {
	if registry == nil {
		registry = NewRegistry()
	}
	registry.Register(s)
}

func LoadSchema(name string, buf []byte, tpl interface{}) {
	s, err := NewSchema(name, buf, tpl)
	if err != nil {
		panic(err)
	}
	RegisterSchema(s)
}

func GetSchema(name string) (Schema, bool) {
	if registry == nil {
		return nil, false
	}
	return registry.Get(name)
}

func ListSchemas() []string {
	if registry == nil {
		return nil
	}
	return registry.ListSchemas()
}

type Registry struct {
	schemas map[string]Schema
}

func NewRegistry() *Registry {
	return &Registry{
		schemas: make(map[string]Schema),
	}
}

func (r *Registry) Register(s Schema) {
	r.schemas[s.Namespace()] = s
}

func (r Registry) Get(name string) (Schema, bool) {
	s, ok := r.schemas[name]
	return s, ok
}

func (r Registry) ListSchemas() []string {
	l := make([]string, 0)
	for n := range r.schemas {
		l = append(l, n)
	}
	sort.Strings(l)
	return l
}

func LoadExtensions() error {
	// set a fallback type that's compatible with ForEach()
	config.SetDefault("metadata.extensions", []interface{}{})

	// extract all extensions
	return config.ForEach("metadata.extensions", func(c *config.Config) error {
		ns := c.GetString("namespace")
		buf, err := json.Marshal(c.GetInterface("schema"))
		if err != nil {
			return fmt.Errorf("metadata: reading extension schema %s: %w", ns, err)
		}
		ext, err := NewSchema(ns, buf, nil)
		if err != nil {
			return fmt.Errorf("metadata: loading extension %s: %w", ns, err)
		}
		RegisterSchema(ext)
		log.Infof("Registered %s metadata extension.", ns)
		return nil
	})
}
