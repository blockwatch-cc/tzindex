// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@blockwatch.cc

package server

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	logpkg "github.com/echa/log"

	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/rpc"
)

const (
	jsonContentType = "application/json; charset=utf-8"
	headerVersion   = "X-Api-Version"
	headerRuntime   = "X-Runtime"
	trailerError    = "X-Streaming-Error"
	trailerCursor   = "X-Streaming-Cursor"
	trailerCount    = "X-Streaming-Count"
	trailerRuntime  = "X-Streaming-Runtime"
	headerTrailer   = "X-Streaming-Error, X-Streaming-Cursor, X-Streaming-Count, X-Streaming-Runtime"
)

type ApiCall func(*ApiContext) (interface{}, int)

type ApiContext struct {
	context.Context
	// request data
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	RemoteIP       net.IP
	Cfg            *Config
	Server         *RestServer
	Crawler        *etl.Crawler
	Indexer        *etl.Indexer
	Client         *rpc.Client

	// QoS and Debugging
	RequestID string
	Log       logpkg.Logger
	// - operation priority     X-Priority
	// - network QoS label      X-QoS-Label

	// Statistics
	Now         time.Time
	Performance *PerformanceCounter
	// Quota int

	// input
	name string
	f    ApiCall

	// output
	status     int
	isStreamed bool
	result     interface{}
	err        *Error
	done       chan *Error
}

func NewContext(ctx context.Context, r *http.Request, w http.ResponseWriter, f ApiCall, srv *RestServer) *ApiContext {
	now := time.Now().UTC()

	// extract name from func to use in fail method
	name := getCallName(f)

	// log.Infof("New API call %s %s (%s)", r.Method, r.URL.Path, name)

	// get real IP behind Docker Interface X-Real-IP or X-Forwarded-For
	host := r.Header.Get("X-Real-Ip")
	if host == "" {
		host = r.Header.Get("X-Forwarded-For")
	}
	if host == "" {
		host, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	requestId := r.Header.Get("X-Request-ID")
	if requestId == "" {
		requestId = "BW-" + <-idStream
	}

	return &ApiContext{
		Context:        ctx,
		Now:            now,
		RequestID:      requestId,
		Cfg:            srv.cfg,
		Server:         srv,
		Crawler:        srv.cfg.Crawler,
		Indexer:        srv.cfg.Indexer,
		Client:         srv.cfg.Client,
		Request:        r,
		ResponseWriter: w,
		RemoteIP:       net.ParseIP(host),
		Performance:    NewPerformanceCounter(now),
		done:           make(chan *Error, 1),
		f:              f,
		name:           name,
		Log:            log.Clone(requestId),
	}
}

// GET/POST/PATCH/PATCH load data or fail
func (api *ApiContext) ParseRequestArgs(args interface{}) {

	r := api.Request

	if r.Method == http.MethodGet {
		if err := schemaDecoder.Decode(args, r.URL.Query()); err != nil {
			panic(EBadRequest(EC_BAD_URL_QUERY, err.Error(), nil))
		}
	} else {
		// POST, PUT, PATCH, DELETE

		// decode URL arguments
		if err := schemaDecoder.Decode(args, r.URL.Query()); err != nil {
			panic(EBadRequest(EC_BAD_URL_QUERY, err.Error(), nil))
		}
		// JSON overwrites URL arguments
		jsonDecoder := json.NewDecoder(r.Body)
		if err := jsonDecoder.Decode(args); err != nil {
			// ignore empty body errors
			if err != io.EOF {
				panic(EBadRequest(EC_DEMARSHAL_FAILED, err.Error(), nil))
			}
		}
	}
}

// this is executed in a goroutine per call, panics on error
func (api *ApiContext) serve() {
	defer api.complete()
	var status int
	api.result, status = api.f(api)
	if status > 0 {
		api.status = status
	}
}

func (api *ApiContext) complete() {
	// only execute on panic
	if e := recover(); e != nil {
		if debugHttp {
			if api.Request.Header.Get("Content-Type") == "application/json" {
				d, _ := httputil.DumpRequest(api.Request, true)
				api.Log.Trace(string(d))
			} else {
				d, _ := httputil.DumpRequest(api.Request, false)
				api.Log.Trace(string(d))
			}
		}

		// e might not be error type, e.g. when panic is thrown by Go Std Library
		// (e.g. from reflect package)
		switch err := e.(type) {
		case error:
			api.handleError(err)
		default:
			api.handleError(fmt.Errorf("%v", e))
		}
	}
}

func (api *ApiContext) handleError(e error) {
	var re *Error
	switch err := e.(type) {
	case *Error:
		re = err
	case *net.OpError:
		re = EConnectionClosed(EC_NETWORK, "connection closed", err).(*Error)
	case error:
		switch err {
		case context.DeadlineExceeded:
			dl, _ := api.Context.Deadline()
			re = EServiceUnavailable(
				EC_SERVER,
				fmt.Sprintf("request timeout: took=%v max=%v", time.Since(api.Now), dl),
				err).(*Error)
		case context.Canceled:
			re = EConnectionClosed(EC_NETWORK, "context canceled", err).(*Error)
		case syscall.EPIPE:
			re = EConnectionClosed(EC_NETWORK, "connection closed", err).(*Error)
		default:
			re = EInternal(EC_SERVER, "uncaught exception", err).(*Error)
			if b, _ := api.jsonStack(); len(b) > 0 {
				api.Log.Error(string(b))
			}
		}
	default:
		re = EInternal(EC_SERVER, reflect.TypeOf(e).String(), e).(*Error)
	}
	re.SetScope(api.name)
	re.RequestId = api.RequestID
	re.Reason = "" // clear internal error
	api.err = re
	api.status = re.Status
}

func (api *ApiContext) jsonStack() ([]byte, error) {
	trace := debug.Stack()
	api.Log.Debugf("%s", string(trace))
	lines := make([]string, 0, bytes.Count(trace, []byte("\n"))+1)
	for _, v := range bytes.Split(trace, []byte("\n")) {
		if len(v) == 0 {
			continue
		}
		lines = append(lines, string(v))
	}
	js := struct {
		Stack []string `json:"stack"`
	}{
		Stack: lines,
	}
	return json.Marshal(js)
}

func (api *ApiContext) sendResponse() {
	// skip when handler was streaming it's response
	if api.isStreamed {
		// return error response when connection is still alive
		if api.err != nil && (api.err.Cause == nil || api.err.Cause != context.Canceled) {
			api.ResponseWriter.Header().Set(trailerError, api.err.String())
			api.Performance.WriteResponseTrailer(api.ResponseWriter)
		}
		return
	}

	if api.err == nil {
		// make sure to set response headers before writing body
		api.writeResponseHeaders("", "")

		// marshal JSON response into HTTP body
		api.writeResponseBody()
	} else {
		path := strings.Join([]string{
			api.Request.Method,
			api.Request.RequestURI,
			api.Request.Proto,
		}, " ")

		err := api.err
		if err.Cause != nil {
			api.Log.Errorf("%d (%d) %s - %s failed (%s): %v", api.status, err.Code, path, err.Scope, err.Detail, err.Cause)
		} else {
			api.Log.Errorf("%d (%d) %s - %s failed (%s)", api.status, err.Code, path, err.Scope, err.Detail)
		}

		// return error response when connection is still alive
		if err.Cause == nil || err.Cause != context.Canceled {
			api.writeResponseHeaders("", "")
			api.ResponseWriter.Write(err.MarshalIndent())
		}
	}
}

func (api *ApiContext) StreamResponseHeaders(status int, contentType string) {
	api.isStreamed = true
	api.status = status
	api.writeResponseHeaders(contentType, headerTrailer)
	// Attempt to flush the header immediately so the client
	// gets the header information and knows the query was accepted.
	if w, ok := api.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

func (api *ApiContext) StreamTrailer(cursor string, count int, err error) {
	h := api.ResponseWriter.Header()
	h.Set(trailerCursor, cursor)
	h.Set(trailerCount, strconv.Itoa(count))
	if err != nil && err != io.EOF {
		api.handleError(err)
		path := strings.Join([]string{
			api.Request.Method,
			api.Request.RequestURI,
			api.Request.Proto,
		}, " ")
		api.Log.Errorf("streaming %d (%d) %s - %s failed (%s): %v", api.status, api.err.Code, path, api.err.Scope, api.err.Detail, api.err.Cause)
		h.Set(trailerError, api.err.String())
	}
	api.Performance.WriteResponseTrailer(api.ResponseWriter)
}

func (api *ApiContext) writeResponseHeaders(contentType, trailers string) {
	// Note: rate limiting headers are inserted during call init
	w := api.ResponseWriter
	// r := api.Request
	h := w.Header()

	// add request id header
	h.Set("Server", UserAgent)
	h.Set(headerVersion, ApiVersion)
	h.Set("X-Request-Id", api.RequestID)

	// add blockchain info
	tip := api.Crawler.Tip()
	h.Set("X-Network-Id", tip.ChainId.String())
	if l := len(tip.Deployments); l > 0 {
		h.Set("X-Protocol-Hash", tip.Deployments[l-1].Protocol.String())
	}

	// set content type if not already set by request handler function
	if h.Get("Content-Type") == "" && api.status != http.StatusNoContent {
		if contentType == "" {
			contentType = jsonContentType
		}
		h.Set("Content-Type", contentType)
	}

	if trailers != "" {
		h.Set("Trailer", trailers)
	}

	// response creation time
	now := api.Now

	// set CORS header if enabled
	if api.Cfg.Http.CorsEnable {
		if api.Cfg.Http.CorsOrigin == "*" {
			h.Set("Access-Control-Allow-Origin", api.Request.Header.Get("Origin"))
		} else {
			h.Set("Access-Control-Allow-Origin", api.Cfg.Http.CorsOrigin)
		}
		h.Set("Access-Control-Allow-Headers", api.Cfg.Http.CorsAllowHeaders)
		h.Set("Access-Control-Expose-Headers", api.Cfg.Http.CorsExposeHeaders)
		h.Set("Access-Control-Allow-Methods", api.Cfg.Http.CorsMethods)
		h.Set("Access-Control-Allow-Credentials", api.Cfg.Http.CorsCredentials)
		h.Set("Access-Control-Max-Age", api.Cfg.Http.CorsMaxAge)
	}

	// Set cache headers ONLY if ALL of the following applies
	// - caching is enabled in config
	// - request method is GET or OPTIONS (not needed, read-only API)
	// - return status is 2xx
	//
	if api.Cfg.Http.CacheEnable && api.status >= 200 && api.status <= 299 {
		// cache streaming responses from tables and series for 30sec
		expires := 30 * time.Second
		if api.result != nil {
			// disable cache for all regular reponses unless they implement Expires()
			expires = 0
			if res, ok := api.result.(Resource); ok {
				w.Header().Set("Last-Modified", res.LastModified().Format(http.TimeFormat))
				exptime := res.Expires()
				if exptime.IsZero() {
					// anything that never changes like blocks and ops
					expires = 365 * 24 * time.Hour
				} else {
					expires = exptime.Sub(now)
					if expires < 0 {
						expires = 0
					}
				}
			}
		}

		// UTC format: time.RFC1123 (would set timezone string to UTC instead of GMT)
		w.Header().Set("Date", now.Format(http.TimeFormat))
		w.Header().Set("Expires", now.Add(expires).Format(http.TimeFormat))
		w.Header().Set("Cache-Control", api.Cfg.Http.CacheControl+",max-age="+strconv.FormatInt(int64(expires/time.Second), 10))
	} else {
		h.Set("Cache-Control", "max-age=0, no-cache, no-store, must-revalidate")
		h.Set("Pragma", "no-cache")
		h.Set("Date", now.Format(http.TimeFormat))
		h.Set("Expires", now.Format(http.TimeFormat))
	}

	// add performance header
	api.Performance.WriteResponseHeader(api.ResponseWriter)

	w.WriteHeader(api.status)
}

func (api *ApiContext) writeResponseBody() {
	if api.result != nil {
		switch t := api.result.(type) {
		case string:
			api.ResponseWriter.Write([]byte(t))
		case *string:
			api.ResponseWriter.Write([]byte(*t))
		case []byte:
			api.ResponseWriter.Write(t)
		default:
			// marshal and write the result to the HTTP body
			if b, err := json.MarshalIndent(api.result, "", "  "); err != nil {
				api.Log.Errorf("Error sending response: %v in struct %#v", err, api.result)
				e := EInternal(EC_MARSHAL_FAILED, "cannot marshal response", err).(*Error)
				e.SetScope(api.name)
				if api.isStreamed {
					api.ResponseWriter.Header().Set(trailerError, e.String())
				} else {
					api.ResponseWriter.Write(e.MarshalIndent())
				}
			} else {
				api.ResponseWriter.Write(append(b, '\n'))
			}
		}
	}
}

var (
	callNames = make(map[uintptr]string)
	mu        sync.RWMutex
	idStream  chan string
)

func getCallName(f ApiCall) string {
	p := reflect.ValueOf(f).Pointer()
	mu.RLock()
	n, ok := callNames[p]
	mu.RUnlock()
	if ok {
		return n
	}
	name := runtime.FuncForPC(p).Name()
	if idx := strings.LastIndex(name, "."); idx > -1 {
		name = name[idx+1:]
	}
	mu.Lock()
	callNames[p] = name
	mu.Unlock()
	return name
}

func init() {
	// start asynchronous ID generator
	idStream = make(chan string, 100)
	go func(ch chan string) {
		h := sha1.New()
		c := []byte(time.Now().String())
		for {
			h.Write(c)
			ch <- fmt.Sprintf("%x", h.Sum(nil))
		}
	}(idStream)
}
