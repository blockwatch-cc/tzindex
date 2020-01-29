// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"
)

func init() {
	register(Pinger{})
}

var _ RESTful = (*Pinger)(nil)

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
	r.HandleFunc(p.RESTPrefix(), C(Ping)).Methods("GET")
	return nil
}

func (p Pinger) RegisterRoutes(r *mux.Router) error {
	return nil
}

type PingRequest struct {
	Sequence  int64 `json:"sequence" schema:"sequence"`
	RequestAt int64 `json:"client_time" schema:"client_time"`
}

func Ping(ctx *ApiContext) (interface{}, int) {
	args := &PingRequest{}
	ctx.ParseRequestArgs(args)
	if ctx.Server.IsShutdown() {
		panic(EServiceUnavailable(EC_SERVER, "shutting down", nil))
	}
	resp := &Pinger{
		Sequence:   args.Sequence,
		RequestAt:  args.RequestAt,
		ResponseAt: ctx.Now.UnixNano(),
	}
	return resp, http.StatusOK
}
