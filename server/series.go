// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"strings"
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

var null = []byte(`null`)

type FillMode string

const (
	FillModeInvalid FillMode = ""
	FillModeNone    FillMode = "none"
	FillModeNull    FillMode = "null"
	FillModeLast    FillMode = "last"
	FillModeLinear  FillMode = "linear"
	FillModeZero    FillMode = "zero"
)

func ParseFillMode(s string) FillMode {
	switch m := FillMode(strings.ToLower(s)); m {
	case FillModeNone, FillModeNull, FillModeLast, FillModeLinear, FillModeZero:
		return m
	case "":
		return FillModeNone
	default:
		return FillModeInvalid
	}
}

func (m FillMode) IsValid() bool {
	return m != FillModeInvalid
}

func (m FillMode) String() string {
	return string(m)
}

func (m FillMode) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func (m *FillMode) UnmarshalText(data []byte) error {
	mode := ParseFillMode(string(data))
	if !mode.IsValid() {
		return fmt.Errorf("invalid fill mode '%s'", string(data))
	}
	*m = mode
	return nil
}

type SeriesModel interface {
	Time() time.Time
}

// New common series handling below
type SeriesBucket interface {
	// database query builder
	BuildQuery(*ApiContext, *SeriesRequest) pack.Query

	// series handlers
	Init(params *tezos.Params, columns []string, verbose bool)
	Add(m SeriesModel)
	IsEmpty() bool
	Reset()
	Clone() SeriesBucket
	Null(time.Time) SeriesBucket
	Zero(time.Time) SeriesBucket
	Time() time.Time
	SetTime(time.Time) SeriesBucket
	Interpolate(SeriesBucket, time.Time) SeriesBucket

	// series marshallers
	json.Marshaler
	csv.Marshaler
	MarshalJSONVerbose() ([]byte, error)
	MarshalJSONBrief() ([]byte, error)
}

func init() {
	register(SeriesRequest{})
}

var _ RESTful = (*SeriesRequest)(nil)

// build packdb query from request
type SeriesRequest struct {
	Series   string          `schema:"-"`
	Columns  util.StringList `schema:"columns"`
	Collapse util.Duration   `schema:"collapse"`
	FillMode FillMode        `schema:"fill"`
	From     util.Time       `schema:"start_date"`
	To       util.Time       `schema:"end_date"`
	Limit    uint            `schema:"limit"`
	Verbose  bool            `schema:"verbose"`
	Format   string          `schema:"-"`     // from URL
	Order    pack.OrderType  `schema:"order"` // asc/desc

	// internal
	count          int
	query          pack.Query
	model          SeriesModel
	bucket         SeriesBucket
	prevBucket     SeriesBucket
	window         time.Duration
	sign           time.Duration
	nextBucketTime time.Time
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

	// fill default
	if r.FillMode == "" {
		r.FillMode = FillModeZero
	}

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
		r.To = r.From.Add(time.Duration(r.Limit) * window)

	case r.To.IsZero() && r.From.IsZero():
		// set default end to now when both are zero
		r.To = util.NewTime(ctx.Now).Truncate(window).Add(window)
		r.From = r.To.Add(-time.Duration(r.Limit) * window)
	}

	// make sure we never cross realtime
	if now := ctx.Now; r.To.Time().After(now) {
		r.To = util.NewTime(ctx.Now).Truncate(window).Add(window)
	}

	// limit
	if ctx.Cfg.Http.MaxSeriesDuration > 0 && r.To.Time().Sub(r.From.Time()) > ctx.Cfg.Http.MaxSeriesDuration {
		log.Warnf("Duration overflow %s > %s", r.To.Time().Sub(r.From.Time()), ctx.Cfg.Http.MaxSeriesDuration)
		panic(ERequestTooLarge(EC_PARAM_INVALID, "series duration longer than 90 days", nil))
	}

	// log.Infof("SERIES %s %s -> %s %s", r.Series, r.From.Time(), r.To.Time(), r.FillMode)
}

func StreamSeries(ctx *ApiContext) (interface{}, int) {
	args := &SeriesRequest{}
	ctx.ParseRequestArgs(args)
	switch args.Series {
	case "block":
		args.bucket = &BlockSeries{}
		args.model = &model.Block{}
	case "op":
		args.bucket = &OpSeries{}
		args.model = &model.Op{}
	case "flow":
		args.bucket = &FlowSeries{}
		args.model = &model.Flow{}
	case "chain":
		args.bucket = &ChainSeries{}
		args.model = &model.Chain{}
	case "supply":
		args.bucket = &SupplySeries{}
		args.model = &model.Supply{}
	default:
		panic(ENotFound(EC_RESOURCE_NOTFOUND, fmt.Sprintf("no such series '%s'", args.Series), nil))
		return nil, -1
	}
	return args.StreamResponse(ctx)
}

func (args *SeriesRequest) StreamResponse(ctx *ApiContext) (interface{}, int) {
	// parse query args into database query
	args.query = args.bucket.BuildQuery(ctx, args)

	// prepare for source and return type marshalling
	args.bucket.Init(ctx.Params, args.Columns, args.Verbose)
	args.bucket.SetTime(args.From.Time())
	args.prevBucket = args.bucket.Clone().SetTime(args.From.Time())
	args.window = args.Collapse.Duration()
	args.nextBucketTime = args.From.Add(args.window).Time()
	args.sign = 1
	if args.Order == pack.OrderDesc {
		args.sign = -1
	}

	// prepare response stream
	ctx.StreamResponseHeaders(http.StatusOK, mimetypes[args.Format])
	var err error

	switch args.Format {
	case "json":
		err = args.StreamJSON(ctx)
	case "csv":
		err = args.StreamCSV(ctx)
	}

	if err != nil && err != io.EOF {
		log.Debugf("Stream encode: %v", err)
	}

	// write error (except EOF), cursor and count as http trailer
	ctx.StreamTrailer("", args.count, err)

	// streaming return
	return nil, -1
}

func (args *SeriesRequest) StreamJSON(ctx *ApiContext) error {
	var err error
	enc := json.NewEncoder(ctx.ResponseWriter)
	enc.SetIndent("", "")
	enc.SetEscapeHTML(false)

	// open JSON array
	io.WriteString(ctx.ResponseWriter, "[")
	// close JSON array on panic
	defer func() {
		if e := recover(); e != nil {
			io.WriteString(ctx.ResponseWriter, "]")
			panic(e)
		}
	}()

	// keep output status
	var needComma bool

	// stream from database, result order is assumed to be in timestamp order
	err = args.query.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(args.model); err != nil {
			return err
		}

		prevTime := args.prevBucket.Time()
		bucketTime := args.bucket.Time()
		modelTime := args.model.Time()

		// output series value when the next model time has crossed boundary
		if modelTime.Before(args.nextBucketTime) != (args.sign == 1) {
			if args.FillMode != FillModeNone {
				// fill when gap is detected
				if bucketTime.Sub(prevTime)*args.sign > args.window {
					// log.Infof("FILL PRE VALUE GAP %s -> %s", prevTime, bucketTime)
					err = args.Fill(prevTime, bucketTime, func(fill SeriesBucket) error {
						if needComma {
							io.WriteString(ctx.ResponseWriter, ",")
						} else {
							needComma = true
						}
						return enc.Encode(fill)
					})
					if err != nil {
						return err
					}
				}
			}

			// output accumulated data
			// log.Infof("WRITE %s", args.bucket.Time())
			if needComma {
				io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
				// on first write, check if bucket is empty and apply fill mode
				if args.bucket.IsEmpty() && args.FillMode == FillModeNull {
					args.bucket.Null(args.bucket.Time())
				}
			}
			if err := enc.Encode(args.bucket); err != nil {
				return err
			}
			args.count++
			if args.Limit > 0 && args.count == int(args.Limit) {
				return io.EOF
			}

			args.prevBucket = args.bucket.Clone()
			args.bucket.Reset()
		}

		// init next time window from data
		if args.bucket.Time().IsZero() {
			args.bucket.SetTime(modelTime.Truncate(args.window))
			args.nextBucketTime = args.bucket.Time().Add(args.window * args.sign)
			// log.Infof("NEXT %s -> %s", args.bucket.Time(), args.nextBucketTime)
		}

		// accumulate data
		args.bucket.Add(args.model)
		return nil
	})

	// don't handle error here, will be picked up by trailer
	if err == nil {
		// fill gap before last element
		prevTime := args.prevBucket.Time()
		bucketTime := args.bucket.Time()

		if args.FillMode != FillModeNone {
			if bucketTime.Sub(prevTime)*args.sign > args.window {
				// log.Infof("FILL PRE END GAP %s -> %s", prevTime, bucketTime)
				err = args.Fill(prevTime, bucketTime, func(fill SeriesBucket) error {
					if needComma {
						io.WriteString(ctx.ResponseWriter, ",")
					} else {
						needComma = true
					}
					return enc.Encode(fill)
				})
			}
		}

		// output last series element
		// log.Infof("WRITE LAST %s", args.bucket.Time())
		if needComma {
			io.WriteString(ctx.ResponseWriter, ",")
		} else {
			// on first write, check if bucket is empty and apply fill mode
			if args.bucket.IsEmpty() && args.FillMode == FillModeNull {
				args.bucket.Null(args.bucket.Time())
			}
			needComma = true
		}
		err = enc.Encode(args.bucket)
		if err == nil {
			args.count++
		}
		args.prevBucket = args.bucket.Clone()

		// fill end gap
		if args.FillMode != FillModeNone {
			if args.To.Time().Sub(bucketTime)*args.sign > args.window {
				// log.Infof("FILL POST END GAP %s -> %s", bucketTime, args.To)
				err = args.Fill(bucketTime, args.To.Time(), func(fill SeriesBucket) error {
					if needComma {
						io.WriteString(ctx.ResponseWriter, ",")
					} else {
						needComma = true
					}
					return enc.Encode(fill)
				})
			}
		}
	}

	// close JSON bracket
	io.WriteString(ctx.ResponseWriter, "]")
	// ctx.Log.Tracef("JSON encoded %d rows", count)
	return err
}

func (args *SeriesRequest) StreamCSV(ctx *ApiContext) error {
	var err error

	// keep output status
	isFirst := true

	enc := csv.NewEncoder(ctx.ResponseWriter)
	// use custom header columns and order
	if len(args.Columns) > 0 {
		err = enc.EncodeHeader(args.Columns, nil)
	}
	if err != nil {
		return err
	}

	// stream from database, result order is assumed to be in timestamp order
	err = args.query.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(args.model); err != nil {
			return err
		}
		// log.Infof("MODEL AT %s", args.model.Time())

		prevTime := args.prevBucket.Time()
		bucketTime := args.bucket.Time()
		modelTime := args.model.Time()

		// output series value when the next model time has crossed boundary
		if modelTime.Before(args.nextBucketTime) != (args.sign == 1) {
			if args.FillMode != FillModeNone {
				// fill when gap is detected
				if bucketTime.Sub(prevTime)*args.sign > args.window {
					// log.Infof("FILL PRE VALUE GAP %s -> %s", prevTime, bucketTime)
					err = args.Fill(prevTime, bucketTime, func(fill SeriesBucket) error {
						return enc.EncodeRecord(fill)
					})
					if err != nil {
						return err
					}
				}
			}

			// output accumulated data
			// log.Infof("WRITE %s", args.bucket.Time())
			if isFirst {
				// on first write, check if bucket is empty and apply fill mode
				if args.bucket.IsEmpty() && args.FillMode == FillModeNull {
					args.bucket.Null(args.bucket.Time())
				}
				isFirst = false
			}
			if err := enc.EncodeRecord(args.bucket); err != nil {
				return err
			}
			args.count++
			if args.Limit > 0 && args.count == int(args.Limit) {
				return io.EOF
			}

			args.prevBucket = args.bucket.Clone()
			args.bucket.Reset()
		}

		// init next time window from data
		if args.bucket.Time().IsZero() {
			// prepare for next accumulation window
			args.bucket.SetTime(modelTime.Truncate(args.window))
			args.nextBucketTime = args.bucket.Time().Add(args.window * args.sign)
			// log.Infof("NEXT %s -> %s", args.bucket.Time(), args.nextBucketTime)
		}

		// accumulate data
		args.bucket.Add(args.model)
		return nil
	})
	if err == nil {
		// fill gap before last element
		prevTime := args.prevBucket.Time()
		bucketTime := args.bucket.Time()

		if args.FillMode != FillModeNone {
			if bucketTime.Sub(prevTime)*args.sign > args.window {
				// log.Infof("FILL PRE END GAP %s -> %s", prevTime, bucketTime)
				err = args.Fill(prevTime, bucketTime, func(fill SeriesBucket) error {
					return enc.EncodeRecord(fill)
				})
			}
		}

		// output last series element
		// log.Infof("WRITE LAST %s", args.bucket.Time())
		if isFirst {
			// on first write, check if bucket is empty and apply fill mode
			if args.bucket.IsEmpty() && args.FillMode == FillModeNull {
				args.bucket.Null(args.bucket.Time())
			}
			isFirst = false
		}
		err = enc.EncodeRecord(args.bucket)
		if err == nil {
			args.count++
		}
		args.prevBucket = args.bucket.Clone()

		// fill end gap
		if args.FillMode != FillModeNone {
			if args.To.Time().Sub(bucketTime)*args.sign > args.window {
				// log.Infof("FILL POST END GAP %s -> %s", bucketTime, args.To)
				err = args.Fill(bucketTime, args.To.Time(), func(fill SeriesBucket) error {
					return enc.EncodeRecord(fill)
				})
			}
		}
	}

	// ctx.Log.Tracef("CSV Encoded %d rows", count)
	return err
}

func (args *SeriesRequest) Fill(from, to time.Time, fillFunc func(SeriesBucket) error) error {
	window := args.Collapse.Duration()
	for _, ts := range util.StepsBetween(from, to.Truncate(window), window) {
		if args.Limit > 0 && args.count >= int(args.Limit) {
			return io.EOF
		}
		var filled SeriesBucket
		switch args.FillMode {
		case FillModeZero:
			filled = args.bucket.Clone().Zero(ts)
		case FillModeNull:
			filled = args.bucket.Clone().Null(ts)
		case FillModeLast:
			filled = args.prevBucket.Clone().SetTime(ts)
		case FillModeLinear:
			filled = args.bucket.Interpolate(args.prevBucket, ts)
		default:
			filled = args.bucket.Clone().Zero(ts)
		}
		if err := fillFunc(filled); err != nil {
			return err
		}
		args.count++
	}
	return nil
}
