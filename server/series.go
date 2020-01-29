// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"fmt"
	"github.com/gorilla/mux"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
)

func init() {
	register(SeriesRequest{})
}

var _ RESTful = (*SeriesRequest)(nil)

// build packdb query from request
type SeriesRequest struct {
	Series   string          `schema:"-"`
	Columns  util.StringList `schema:"columns"`
	Collapse util.Duration   `schema:"collapse"`
	From     util.Time       `schema:"start_date"`
	To       util.Time       `schema:"end_date"`
	Limit    uint            `schema:"limit"`
	Verbose  bool            `schema:"verbose"`
	Format   string          `schema:"-"`     // from URL
	Order    pack.OrderType  `schema:"order"` // asc/desc
}

func (t SeriesRequest) LastModified() time.Time {
	return time.Time{}
}

func (t SeriesRequest) Expires() time.Time {
	return time.Time{}
}

func (t SeriesRequest) RESTPrefix() string {
	return "/series"
}

func (t SeriesRequest) RESTPath(r *mux.Router) string {
	path, _ := r.Get("seriesurl").URLPath("series", t.Series, "format", t.Format)
	return path.String()
}

func (t SeriesRequest) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t SeriesRequest) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{series}.{format}", C(StreamSeries)).Methods("GET").Name("seriesurl")
	r.HandleFunc("/{series}", C(StreamSeries)).Methods("GET")
	return nil
}

func (r *SeriesRequest) Parse(ctx *ApiContext) {
	// read schema args
	ctx.ParseRequestArgs(r)

	// prevent duplicate columns
	if len(r.Columns) > 0 {
		seen := make(map[string]struct{})
		for _, v := range r.Columns {
			if _, ok := seen[v]; ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("duplicate column %s", v), nil))
			}
			seen[v] = struct{}{}
		}
	}

	// clamp limit
	r.Limit = ctx.Cfg.ClampList(r.Limit)

	// clamp duration
	if d := r.Collapse.Duration(); d < time.Minute {
		r.Collapse = util.Duration(time.Minute)
	} else if d > 365*24*time.Hour {
		r.Collapse = util.Duration(365 * 24 * time.Hour)
	}

	// ensure time column is included as first item (if not requested otherwise)
	r.Columns.AddUniqueFront("time")

	// read series code from URL
	r.Series, _ = mux.Vars(ctx.Request)["series"]
	r.Series = strings.ToLower(r.Series)

	// read format from URL
	r.Format, _ = mux.Vars(ctx.Request)["format"]
	r.Format = strings.ToLower(r.Format)
	if r.Format == "" {
		r.Format = "json"
	}
	switch r.Format {
	case "json", "csv":
	default:
		panic(EBadRequest(EC_CONTENTTYPE_UNSUPPORTED, fmt.Sprintf("unsupported format '%s'", r.Format), nil))
	}

	// adjust time range to request limit
	window := r.Collapse.Duration()
	switch true {
	case !r.To.IsZero() && !r.From.IsZero():
		// flip start and end when unordered
		if r.To.Before(r.From) {
			r.From, r.To = r.To, r.From
		}
		// truncate from/to to collapse
		r.From = r.From.Truncate(window)
		r.To = r.To.Truncate(window).Add(window)

	case r.From.IsZero() && !r.To.IsZero():
		// adjust start time if not set
		r.To = r.To.Truncate(window).Add(window)
		r.From = r.To.Add(-time.Duration(r.Limit) * window)

	case r.To.IsZero() && !r.From.IsZero():
		// adjust end time if not set
		r.From = r.From.Truncate(window)
		r.To = r.From.Add(time.Duration(r.Limit+1) * window)

	case r.To.IsZero() && r.From.IsZero():
		// set default end to now when both are zero
		r.To = util.NewTime(ctx.Now).Truncate(window).Add(window)
		r.From = r.To.Add(-time.Duration(r.Limit) * window)
	}

	// make sure we never cross realtime
	if now := util.Now(); r.To.After(now) {
		r.To = now.Truncate(window).Add(window)
	}
}

func StreamSeries(ctx *ApiContext) (interface{}, int) {
	args := &SeriesRequest{}
	args.Parse(ctx)
	switch args.Series {
	case "block":
		return StreamBlockSeries(ctx, args)
	case "op":
		return StreamOpSeries(ctx, args)
	case "flow":
		return StreamFlowSeries(ctx, args)
	default:
		panic(ENotFound(EC_RESOURCE_NOTFOUND, fmt.Sprintf("no such series '%s'", args.Series), nil))
		return nil, -1
	}
}
