// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Constant{})
}

var _ server.RESTful = (*Constant)(nil)

type Constant struct {
	Address     string             `json:"address"`
	Creator     string             `json:"creator"`
	Height      int64              `json:"height"`
	Time        time.Time          `json:"time"`
	StorageSize int64              `json:"storage_size"`
	Value       micheline.Prim     `json:"value"`
	Features    micheline.Features `json:"features"`

	expires time.Time `json:"-"`
}

func NewConstant(ctx *server.Context, c *model.Constant) *Constant {
	cc := &Constant{
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

func (c Constant) LastModified() time.Time {
	return c.Time
}

func (c Constant) Expires() time.Time {
	return c.expires
}

func (c Constant) RESTPrefix() string {
	return "/explorer/constant"
}

func (c Constant) RESTPath(r *mux.Router) string {
	path, _ := r.Get("constant").URLPath("ident", c.Address)
	return path.String()
}

func (c Constant) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (c Constant) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadConstant)).Methods("GET").Name("constant")
	return nil
}

func loadConstant(ctx *server.Context) *model.Constant {
	if ccIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || ccIdent == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing constant hash", nil))
	} else {
		hash, err := tezos.ParseExprHash(ccIdent)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid exprhash", err))
		}
		cc, err := ctx.Indexer.LookupConstant(ctx, hash)
		if err != nil {
			switch err {
			case model.ErrNoConstant:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such constant", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return cc
	}
}

func ReadConstant(ctx *server.Context) (interface{}, int) {
	cc := loadConstant(ctx)
	return NewConstant(ctx, cc), http.StatusOK
}
