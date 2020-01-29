// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"expvar"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"net/http/pprof"
	"time"
)

var (
	schemaDecoder = schema.NewDecoder()
)

type Resource interface {
	LastModified() time.Time
	Expires() time.Time
}

type RESTful interface {
	RESTPrefix() string
	RESTPath(r *mux.Router) string
	RegisterRoutes(r *mux.Router) error
	RegisterDirectRoutes(r *mux.Router) error
}

func BoolPtr(b bool) *bool {
	return &b
}

var models = map[string]RESTful{}

func register(model RESTful) {
	models[model.RESTPrefix()] = model
}

// Generate a new API router with support for HTTP OPTIONS
func NewRouter() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)

	router.PathPrefix("/").HandlerFunc(C(StateOptions)).Methods("OPTIONS")

	for _, m := range models {
		if err := m.RegisterDirectRoutes(router); err != nil {
			log.Fatalf("API cannot register %s route: ", m.RESTPrefix(), err)
		}
		if err := m.RegisterRoutes(router.PathPrefix(m.RESTPrefix()).Subrouter()); err != nil {
			log.Fatalf("API cannot register %s subroutes: ", m.RESTPrefix(), err)
		}
	}

	// register debug routes directly (i.e. without going through dispatcher)
	log.Debugf("Registering debug routes")
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Manually add support for paths linked to by index page at /debug/pprof/
	router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	router.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("/debug/pprof/block", pprof.Handler("block"))
	router.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	router.PathPrefix("/debug/vars").Handler(expvar.Handler())

	router.PathPrefix("/").HandlerFunc(C(NotFound))

	// configure schema (URL parameter) decoding
	schemaDecoder.IgnoreUnknownKeys(true)
	schemaDecoder.ZeroEmpty(true)

	return router
}
