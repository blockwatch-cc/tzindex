// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerConstant{})
}

var _ RESTful = (*ExplorerConstant)(nil)

type ExplorerConstant struct {
	Address     string             `json:"address"`
	Creator     string             `json:"creator"`
	Height      int64              `json:"height"`
	Time        time.Time          `json:"time"`
	StorageSize int64              `json:"storage_size"`
	Value       micheline.Prim     `json:"value"`
	Features    micheline.Features `json:"features"`

	expires time.Time `json:"-"`
}

func NewExplorerConstant(ctx *ApiContext, c *model.Constant) *ExplorerConstant {
	cc := &ExplorerConstant{
		Address:     c.Address.String(),
		Creator:     ctx.Indexer.LookupAddress(ctx, c.CreatorId).String(),
		Height:      c.Height,
		Time:        ctx.Indexer.LookupBlockTime(ctx.Context, c.Height),
		StorageSize: c.StorageSize,
		Features:    c.Features,
	}
	_ = cc.Value.UnmarshalBinary(c.Value)
	return cc
}

func (c ExplorerConstant) LastModified() time.Time {
	return c.Time
}

func (c ExplorerConstant) Expires() time.Time {
	return c.expires
}

func (c ExplorerConstant) RESTPrefix() string {
	return "/explorer/constant"
}

func (c ExplorerConstant) RESTPath(r *mux.Router) string {
	path, _ := r.Get("constant").URLPath("ident", c.Address)
	return path.String()
}

func (c ExplorerConstant) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (c ExplorerConstant) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadConstant)).Methods("GET").Name("constant")
	return nil
}

func loadConstant(ctx *ApiContext) *model.Constant {
	if ccIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || ccIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing constant hash", nil))
	} else {
		hash, err := tezos.ParseExprHash(ccIdent)
		if err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid exprhash", err))
		}
		cc, err := ctx.Indexer.LookupConstant(ctx, hash)
		if err != nil {
			switch err {
			case index.ErrNoConstantEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such constant", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return cc
	}
}

func ReadConstant(ctx *ApiContext) (interface{}, int) {
	cc := loadConstant(ctx)
	return NewExplorerConstant(ctx, cc), http.StatusOK
}
