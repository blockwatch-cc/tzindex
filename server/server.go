// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	logpkg "github.com/echa/log"
	"github.com/gorilla/mux"
)

type RestServer struct {
	router   *mux.Router
	srv      *http.Server
	cfg      *Config
	shutdown bool
}

var (
	UserAgent  = "Blockwatch-tzindex/1.0"
	ApiVersion string
	debugHttp  bool
	srv        *RestServer
)

func New(cfg *Config) (*RestServer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config required")
	}

	if err := cfg.Http.Check(); err != nil {
		return nil, err
	}

	debugHttp = log.Level() == logpkg.LevelTrace

	// setup router
	r := NewRouter()
	r.NotFoundHandler = http.HandlerFunc(C(NotFound))
	http.Handle("/", r)

	// setup HTTP/2 server to support HTTP/1.1 and HTTP/2.0
	h2s := &http2.Server{
		MaxHandlers: cfg.Http.MaxWorkers,
		IdleTimeout: cfg.Http.KeepAlive,
	}

	// configure the server, allowing non-TLS HTTP/2.0 a.k.a h2c conns
	// make timeout a bit longer to have headroom for returning 504 errors
	srv = &RestServer{
		cfg:    cfg,
		router: r,
		srv: &http.Server{
			Addr:              cfg.Http.Address(),
			Handler:           h2c.NewHandler(r, h2s),
			ReadHeaderTimeout: cfg.Http.HeaderTimeout,
			ReadTimeout:       cfg.Http.ReadTimeout,
			WriteTimeout:      cfg.Http.WriteTimeout + time.Second,
			IdleTimeout:       cfg.Http.KeepAlive,
			ErrorLog:          log.Logger(),
		},
	}

	return srv, nil
}

func (s *RestServer) IsShutdown() bool {
	return s.shutdown
}

func (s *RestServer) Start() {
	go func() {
		log.Info("Starting HTTP server at ", s.cfg.Http.Address())
		if err := s.srv.ListenAndServe(); err != nil {
			if !s.shutdown {
				log.Fatal(err)
			}
		}
	}()
}

func (s *RestServer) Stop() {
	log.Info("Stopping HTTP server.")
	s.shutdown = true
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Http.ShutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		log.Error(err)
	}
}

func NotFound(ctx *ApiContext) (interface{}, int) {
	r := ctx.Request
	s := fmt.Sprintf("Unrecognized request URL (%s: %s).", r.Method, r.URL.Path)
	panic(ENotFound(EC_NO_ROUTE, s, nil))
}

// Respond to requests with the OPTIONS Method.
func StateOptions(ctx *ApiContext) (interface{}, int) {
	// returning an empty body here, the important part of this method
	// (setting appropriate headers) is done by the HTTP handler wrapper
	return nil, http.StatusOK
}

func C(f ApiCall) func(http.ResponseWriter, *http.Request) {
	return wrapper(f)
}

func wrapper(f ApiCall) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			ctx    context.Context
			cancel context.CancelFunc
		)

		// use configured request timeout as default
		timeout := srv.cfg.Http.ReadTimeout + srv.cfg.Http.WriteTimeout

		// try reading upstream request timeout hint
		if th := r.Header.Get(srv.cfg.Http.TimeoutHeader); th != "" {
			if d, err := strconv.ParseInt(th, 10, 64); err == nil {
				timeout = time.Duration(d) * time.Millisecond
			}
		}
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(r.Context(), timeout)
		} else {
			ctx, cancel = context.WithCancel(r.Context())
		}
		defer cancel()

		api := NewContext(ctx, r, w, f, srv)
		api.serve()
		api.sendResponse()
	}
}
