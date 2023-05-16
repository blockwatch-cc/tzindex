// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

type Event struct {
	Contract tezos.Address  `json:"contract"`
	Type     micheline.Prim `json:"type"`
	Payload  micheline.Prim `json:"payload"`
	Tag      string         `json:"tag"`
	TypeHash util.U64String `json:"type_hash"`
}

func NewEvent(ctx *server.Context, e *model.Event, args server.Options) *Event {
	ev := &Event{
		Contract: ctx.Indexer.LookupAddress(ctx, e.AccountId),
		Tag:      e.Tag,
		TypeHash: util.U64String(e.TypeHash),
	}
	_ = ev.Type.UnmarshalBinary(e.Type)
	_ = ev.Payload.UnmarshalBinary(e.Payload)
	return ev
}
