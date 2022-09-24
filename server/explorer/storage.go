// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzindex/server"
)

type Storage struct {
	Value    interface{}     `json:"value,omitempty"`
	Prim     *micheline.Prim `json:"prim,omitempty"`
	modified time.Time       `json:"-"`
	expires  time.Time       `json:"-"`
}

func (t Storage) LastModified() time.Time { return t.modified }
func (t Storage) Expires() time.Time      { return t.expires }

var _ server.Resource = (*Storage)(nil)

func NewStorage(ctx *server.Context, data []byte, typ micheline.Type, mod time.Time, args server.Options) *Storage {
	resp := &Storage{
		modified: mod,
		expires:  ctx.Tip.BestTime.Add(ctx.Params.BlockTime()),
	}

	prim := micheline.Prim{}
	if err := prim.UnmarshalBinary(data); err != nil {
		log.Errorf("storage unmarshal: %v", err)
	}

	var wasPacked bool
	if args.WithUnpack() && prim.IsPackedAny() {
		if up, err := prim.UnpackAll(); err == nil {
			wasPacked = true
			prim = up
		}
	}

	val := micheline.NewValue(typ, prim)
	if m, err := val.Map(); err == nil {
		resp.Value = m
	} else {
		log.Errorf("storage translate: %v", err)
	}

	// try with type deduction
	if resp.Value == nil && wasPacked {
		val.FixType()
		if m, err := val.Map(); err == nil {
			resp.Value = m
		} else {
			log.Errorf("fixed storage translate: %v", err)
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
