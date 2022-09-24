// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/server"
)

type Parameters struct {
	Entrypoint string          `json:"entrypoint,omitempty"` // contract
	Value      interface{}     `json:"value,omitempty"`      // contract
	Prim       *micheline.Prim `json:"prim,omitempty"`       // contract
	L2Address  *tezos.Address  `json:"l2_address,omitempty"` // rollup
	Method     string          `json:"method,omitempty"`     // rollup
	Arguments  json.RawMessage `json:"arguments,omitempty"`  // rollup
}

func NewParameters(ctx *server.Context, data []byte, typ micheline.Type, op tezos.OpHash, args server.Options) *Parameters {
	resp := &Parameters{}

	p := &micheline.Parameters{}
	if err := p.UnmarshalBinary(data); err != nil {
		log.Errorf("%s parameter unmarshal: %v", op, err)
		return resp
	}
	// log.Infof("call entrypoint=%s params=%s", p.Entrypoint)
	// log.Infof("call params=%s", p.Value.Dump())
	// log.Infof("call type %s", typ.Dump())

	ep, prim, err := p.MapEntrypoint(typ)
	if err != nil {
		log.Errorf("%s entrypoint %s: %v", op, p.Entrypoint, err)
		log.Errorf("%s params: %s", op, p.Value.Dump())
		return resp
	}

	var wasPacked bool
	if args.WithUnpack() && prim.IsPackedAny() {
		if up, err := prim.UnpackAll(); err == nil {
			wasPacked = true
			prim = up
		}
	}

	// use resolved entrypoint type and real name
	resp.Entrypoint = ep.Name
	typ = ep.Type()

	// strip entrypoint name annot to prevent duplicate nesting
	typ.Prim.Anno = nil

	val := micheline.NewValue(typ, prim)
	if m, err := val.Map(); err == nil {
		resp.Value = m
	} else {
		log.Errorf("%s param translate: %v", op, err)
	}

	// try with type deduction
	if resp.Value == nil && wasPacked {
		val.FixType()
		if m, err := val.Map(); err == nil {
			resp.Value = m
		} else {
			log.Errorf("%s fixed param translate: %v", op, err)
		}
	}

	// be compatible with legacy API
	if resp.Value == nil && !prim.IsNil() {
		resp.Prim = &prim
	}

	if args.WithPrim() {
		resp.Prim = &prim
	}

	return resp
}
