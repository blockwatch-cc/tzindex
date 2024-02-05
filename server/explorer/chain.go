// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

var _ server.Resource = (*Chain)(nil)

type Chain struct {
	model.Chain
}

func (c Chain) LastModified() time.Time {
	return c.Timestamp
}

func (c Chain) Expires() time.Time {
	return time.Time{}
}

func ReadChain(ctx *server.Context) (interface{}, int) {
	id, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || id == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing block identifier", nil))
	}
	switch id {
	case "head":
		c, err := ctx.Indexer.ChainByHeight(ctx, ctx.Crawler.Height())
		if err != nil {
			panic(server.EInternal(server.EC_DATABASE, "cannot read chain data", err))
		}
		return Chain{*c}, http.StatusOK
	default:
		val, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
		}
		c, err := ctx.Indexer.ChainByHeight(ctx, val)
		if err != nil {
			panic(server.EInternal(server.EC_DATABASE, "cannot read chain data", err))
		}
		return Chain{*c}, http.StatusOK
	}
}
