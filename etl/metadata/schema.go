// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/qri-io/jsonschema"
)

type schemaImpl struct {
	ns     string
	schema *jsonschema.Schema
	typ    reflect.Type
}

var _ Schema = (*schemaImpl)(nil)

func NewSchema(name string, data []byte, tpl interface{}) (Schema, error) {
	cs := &schemaImpl{
		ns:     name,
		schema: &jsonschema.Schema{},
	}
	if err := json.Unmarshal(data, cs.schema); err != nil {
		return nil, fmt.Errorf("metadata: reading %s schema failed: %v", name, err)
	}
	if tpl != nil {
		cs.typ = reflect.TypeOf(tpl)
	}
	return cs, nil
}

func (s schemaImpl) Namespace() string {
	return s.ns
}

func (s schemaImpl) Validate(data interface{}) error {
	state := s.schema.Validate(context.Background(), data)
	if len(*state.Errs) > 0 {
		return fmt.Errorf((*state.Errs)[0].Error())
	}
	return nil
}

func (s schemaImpl) ValidateBytes(buf []byte) error {
	errs, err := s.schema.ValidateBytes(context.Background(), buf)
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		return fmt.Errorf(errs[0].Error())
	}
	return nil
}

func (s schemaImpl) NewDescriptor() Descriptor {
	if s.typ.Name() == "" {
		return nil
	}
	return reflect.New(s.typ).Interface().(Descriptor)
}

func (s schemaImpl) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.schema)
}
