// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"fmt"
	"net/http"
	"strings"

	"blockwatch.cc/packdb/pack"
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

func NewEvent(ctx *server.Context, e *model.Event) *Event {
	ev := &Event{
		Contract: ctx.Indexer.LookupAddress(ctx, e.AccountId),
		Tag:      e.Tag,
		TypeHash: util.U64String(e.TypeHash),
	}
	_ = ev.Type.UnmarshalBinary(e.Type)
	_ = ev.Payload.UnmarshalBinary(e.Payload)
	return ev
}

type ContractEventListRequest struct {
	ListRequest
	TypeHash uint64 `schema:"type_hash"`
	Tag      string `schema:"tag"`
}

func ListContractEvents(ctx *server.Context) (interface{}, int) {
	args := &ContractEventListRequest{}
	ctx.ParseRequestArgs(args)
	cc := loadContract(ctx)

	table, err := ctx.Indexer.Table(model.EventTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access event table", err))
	}

	list := make([]*model.Event, 0)
	q := pack.NewQuery("event.list").
		WithTable(table).
		AndEqual("account_id", cc.AccountId).
		WithLimit(int(args.Limit)).
		WithOffset(int(args.Offset)).
		AndGt("row_id", args.Cursor)

	if args.TypeHash > 0 {
		q = q.AndEqual("type_hash", args.TypeHash)
	}
	if args.Tag != "" {
		q = q.AndEqual("tag", args.Tag)
	}

	// filter by time condition
	if mode, val, ok := server.Query(ctx, "time"); ok {
		switch mode {
		case pack.FilterModeGt, pack.FilterModeGte, pack.FilterModeLt, pack.FilterModeLte:
			tm, err := util.ParseTime(val)
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time value %q", val), err))
			}
			height := ctx.Indexer.LookupBlockHeightFromTime(ctx, tm.Time())
			q = q.And("height", mode, height)

		case pack.FilterModeRange:
			from, to, ok := strings.Cut(val, ",")
			if !ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time range value %q", val), nil))
			}
			fromTime, err := util.ParseTime(from)
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid from time value %q", val), err))
			}
			toTime, err := util.ParseTime(to)
			if err != nil {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid to time value %q", val), err))
			}
			q = q.And("height", mode, []int64{
				ctx.Indexer.LookupBlockHeightFromTime(ctx, fromTime.Time()),
				ctx.Indexer.LookupBlockHeightFromTime(ctx, toTime.Time()),
			})

		default:
			panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("invalid time mode %q", mode), nil))
		}
	}

	err = q.Execute(ctx, &list)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list events", err))
	}

	resp := make([]*Event, 0, len(list))
	for _, v := range list {
		resp = append(resp, NewEvent(ctx, v))
	}
	return resp, http.StatusOK
}
