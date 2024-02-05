// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Pinger{})
}

var _ server.RESTful = (*Pinger)(nil)

type Pinger struct {
	Sequence   int64 `json:"sequence"`
	RequestAt  int64 `json:"client_time"` // whatever client sends
	ResponseAt int64 `json:"server_time"` // unix nanosec
}

func (p Pinger) LastModified() time.Time {
	return time.Now().UTC()
}

func (p Pinger) Expires() time.Time {
	return time.Time{}
}

func (p Pinger) RESTPath(r *mux.Router) string {
	return p.RESTPrefix()
}

func (p Pinger) RESTPrefix() string {
	return "/ping"
}

func (p Pinger) RegisterDirectRoutes(r *mux.Router) error {
	r.HandleFunc(p.RESTPrefix(), server.C(Ping)).Methods("GET")
	return nil
}

func (p Pinger) RegisterRoutes(r *mux.Router) error {
	return nil
}

type PingRequest struct {
	Sequence  int64 `json:"sequence" schema:"sequence"`
	RequestAt int64 `json:"client_time" schema:"client_time"`
}

func Ping(ctx *server.Context) (interface{}, int) {
	args := &PingRequest{}
	ctx.ParseRequestArgs(args)
	if ctx.Server.IsShutdown() {
		panic(server.EServiceUnavailable(server.EC_SERVER, "shutting down", nil))
	}
	resp := &Pinger{
		Sequence:   args.Sequence,
		RequestAt:  args.RequestAt,
		ResponseAt: ctx.Now.UnixNano(),
	}
	return resp, http.StatusOK
}
