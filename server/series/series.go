// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

var null = []byte(`null`)

var mimetypes = map[string]string{
	"json": "application/json; charset=utf-8",
	"csv":  "text/csv",
}

type FillMode string

const (
	FillModeInvalid FillMode = ""
	FillModeNone    FillMode = "none"
	FillModeNull    FillMode = "null"
	FillModeLast    FillMode = "last"
	FillModeLinear  FillMode = "linear"
	FillModeZero    FillMode = "zero"

	collapseUnits string = "mhdwMy"
)

type Collapse struct {
	Value int
	Unit  rune
}

func (c Collapse) String() string {
	return strconv.Itoa(c.Value) + string(c.Unit)
}

func ParseCollapse(s string) (Collapse, error) {
	var c Collapse
	if len(s) < 1 {
		return c, fmt.Errorf("collapse: invalid value %q", s)
	}
	if u := s[len(s)-1]; !strings.Contains(collapseUnits, string(u)) {
		return c, fmt.Errorf("collapse: invalid unit %q", u)
	} else {
		c.Unit = rune(u)
	}
	if val, err := strconv.Atoi(s[:len(s)-1]); err != nil {
		return c, fmt.Errorf("collapse: %v", err)
	} else {
		c.Value = val
	}
	if c.Value < 0 {
		c.Value = -c.Value
	}
	if c.Value == 0 {
		c.Value = 1
	}
	return c, nil
}

func (c Collapse) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *Collapse) UnmarshalText(data []byte) error {
	cc, err := ParseCollapse(string(data))
	if err != nil {
		return err
	}
	*c = cc
	return nil
}

func (c Collapse) Duration() time.Duration {
	base := time.Minute
	switch c.Unit {
	case 'm':
		base = time.Minute
	case 'h':
		base = time.Hour
	case 'd':
		base = 24 * time.Hour
	case 'w':
		base = 24 * 7 * time.Hour
	case 'M':
		base = 30*24*time.Hour + 629*time.Minute + 28*time.Second // 30.437 days
	case 'y':
		base = 365 * 24 * time.Hour
	}
	return time.Duration(c.Value) * base
}

func (c Collapse) Truncate(t time.Time) time.Time {
	switch c.Unit {
	default:
		// anything below a day is fine for go's time library
		return t.Truncate(c.Duration())
	case 'w':
		// truncate to midnight on first day of week (weekdays are zero-based)
		if c.Value == 1 {
			yy, mm, dd := t.AddDate(0, 0, -int(t.Weekday())).Date()
			return time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
		}

		// round down to n weeks
		_, w := t.ISOWeek()
		w %= c.Value
		yy, mm, dd := t.AddDate(0, 0, int(-t.Weekday())-w*7).Date()
		return time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)

	case 'M':
		// truncate to midnight on first day of month
		yy, mm, _ := t.Date()
		val := yy*12 + int(mm) - 1
		if c.Value > 1 {
			val -= (val % c.Value)
		}
		yy = val / 12
		mm = time.Month(val%12 + 1)
		return time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)

	case 'y':
		// truncate to midnight on first day of year
		yy := t.Year()
		if c.Value > 1 {
			yy -= (yy % c.Value)
		}
		return time.Date(yy, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
}

func (c Collapse) Next(t time.Time, n int) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return c.Truncate(t).Add(time.Duration(n) * c.Duration())
	case 'w':
		// add n*m weeks
		return c.Truncate(t).AddDate(0, 0, n*c.Value*7)
	case 'M':
		// add n*m months
		return c.Truncate(t).AddDate(0, n*c.Value, 0)
	case 'y':
		// add n*m years
		return c.Truncate(t).AddDate(n*c.Value, 0, 0)
	}
}

func (c Collapse) Steps(from, to time.Time, limit uint) []time.Time {
	steps := make([]time.Time, 0)
	if from.After(to) {
		from, to = to, from
	}
	for {
		from = c.Next(from, 1)
		if !from.Before(to) {
			break
		}
		steps = append(steps, from)
		if len(steps) == int(limit) {
			break
		}
	}
	return steps
}

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
	BuildQuery(*server.Context, *SeriesRequest) pack.Query

	// series handlers
	Init(params *rpc.Params, columns []string, verbose bool)
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
	server.Register(SeriesRequest{})
}

var _ server.RESTful = (*SeriesRequest)(nil)

// build packdb query from request
type SeriesRequest struct {
	Series   string          `schema:"-"`
	Columns  util.StringList `schema:"columns"`
	Collapse Collapse        `schema:"collapse"`
	FillMode FillMode        `schema:"fill"`
	From     util.Time       `schema:"start_date"`
	To       util.Time       `schema:"end_date"`
	Limit    uint            `schema:"limit"`
	Verbose  bool            `schema:"verbose"`
	Format   string          `schema:"-"`        // from URL
	Order    pack.OrderType  `schema:"order"`    // asc/desc
	Filename string          `schema:"filename"` // for CSV downloads

	// internal
	count          int
	query          pack.Query
	model          SeriesModel
	bucket         SeriesBucket
	prevBucket     SeriesBucket
	sign           time.Duration
	window         time.Duration
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
	r.HandleFunc("/{series}.{format}", server.C(StreamSeries)).Methods("GET").Name("seriesurl")
	r.HandleFunc("/{series}", server.C(StreamSeries)).Methods("GET")
	return nil
}

func (r *SeriesRequest) Parse(ctx *server.Context) {
	// prevent duplicate columns
	if len(r.Columns) > 0 {
		seen := make(map[string]struct{})
		for _, v := range r.Columns {
			if _, ok := seen[v]; ok {
				panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("duplicate column %s", v), nil))
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

	// ensure time column is included as first item (if not requested otherwise)
	r.Columns.AddUniqueFront("time")

	// read series code from URL
	r.Series = strings.ToLower(mux.Vars(ctx.Request)["series"])

	// read format from URL
	r.Format = strings.ToLower(mux.Vars(ctx.Request)["format"])
	if r.Format == "" {
		r.Format = "json"
	}
	switch r.Format {
	case "json", "csv":
	default:
		panic(server.EBadRequest(server.EC_CONTENTTYPE_UNSUPPORTED, fmt.Sprintf("unsupported format '%s'", r.Format), nil))
	}

	switch {
	case !r.To.IsZero() && !r.From.IsZero():
		// flip start and end when unordered
		if r.To.Before(r.From) {
			r.From, r.To = r.To, r.From
		}
		// truncate from/to to collapse
		r.From = util.NewTime(r.Collapse.Truncate(r.From.Time()))
		r.To = util.NewTime(r.Collapse.Next(r.To.Time(), 1))

	case r.From.IsZero() && !r.To.IsZero():
		// adjust start time if not set
		r.From = util.NewTime(r.Collapse.Next(r.To.Time(), -int(r.Limit)))
		r.To = util.NewTime(r.Collapse.Next(r.To.Time(), 1))

	case r.To.IsZero() && !r.From.IsZero():
		// adjust end time if not set
		r.From = util.NewTime(r.Collapse.Truncate(r.From.Time()))
		r.To = util.NewTime(r.Collapse.Next(r.From.Time(), int(r.Limit)))

	case r.To.IsZero() && r.From.IsZero():
		// set default end to now when both are zero
		r.From = util.NewTime(r.Collapse.Next(ctx.Now, -int(r.Limit-1)))
		r.To = util.NewTime(r.Collapse.Next(ctx.Now, 1))
	}

	// make sure we never cross realtime
	if r.To.Time().After(ctx.Now) {
		r.To = util.NewTime(r.Collapse.Next(ctx.Now, 1))
	}

	// limit
	if ctx.Cfg.Http.MaxSeriesDuration > 0 && r.To.Time().Sub(r.From.Time()) > ctx.Cfg.Http.MaxSeriesDuration {
		log.Warnf("Duration overflow %s > %s", r.To.Time().Sub(r.From.Time()), ctx.Cfg.Http.MaxSeriesDuration)
		panic(server.EBadRequest(server.EC_PARAM_INVALID, fmt.Sprintf("series duration longer than %s", ctx.Cfg.Http.MaxSeriesDuration), nil))
	}

	// log.Infof("SERIES %s collapse=%s %s -> %s mode=%s", r.Series, r.Collapse, r.From.Time(), r.To.Time(), r.FillMode)
}

func StreamSeries(ctx *server.Context) (interface{}, int) {
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
	case "balance":
		args.FillMode = FillModeLast
		args.bucket = &BalanceSeries{}
		args.model = &BalanceModel{
			ctx: ctx.Context,
			idx: ctx.Indexer,
		}
	default:
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, fmt.Sprintf("no such series '%s'", args.Series), nil))
	}
	return args.StreamResponse(ctx)
}

func (args *SeriesRequest) StreamResponse(ctx *server.Context) (interface{}, int) {
	// parse query args into database query
	args.query = args.bucket.BuildQuery(ctx, args)

	// prepare for source and return type marshalling
	args.bucket.Init(ctx.Params, args.Columns, args.Verbose)
	args.bucket.SetTime(args.From.Time())
	args.prevBucket = args.bucket.Clone().SetTime(args.From.Time())
	args.window = args.Collapse.Duration()
	args.nextBucketTime = args.Collapse.Next(args.From.Time(), 1)
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

func (args *SeriesRequest) StreamJSON(ctx *server.Context) error {
	var err error
	enc := json.NewEncoder(ctx.ResponseWriter)
	enc.SetIndent("", "")
	enc.SetEscapeHTML(false)

	// open JSON array
	_, _ = io.WriteString(ctx.ResponseWriter, "[")
	// close JSON array on panic
	defer func() {
		if e := recover(); e != nil {
			_, _ = io.WriteString(ctx.ResponseWriter, "]")
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

		modelTime := args.model.Time()

		// output series value when the next model time has crossed boundary
		if !modelTime.Before(args.nextBucketTime) {
			// fill when gap is detected
			if args.bucket.IsEmpty() {
				gapStart := args.prevBucket.Time()
				gapEnd := args.Collapse.Truncate(modelTime)
				err = args.Fill(gapStart, gapEnd, func(fill SeriesBucket) error {
					if needComma {
						_, _ = io.WriteString(ctx.ResponseWriter, ",")
					} else {
						needComma = true
					}
					return enc.Encode(fill)
				})
				if err != nil {
					return err
				}
				args.prevBucket = args.bucket.Clone().SetTime(gapEnd)
			} else {
				// output accumulated data
				if needComma {
					_, _ = io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}

				if err := enc.Encode(args.bucket); err != nil {
					return err
				}
				args.count++
				if args.Limit > 0 && args.count == int(args.Limit) {
					return io.EOF
				}
				args.prevBucket = args.bucket.Clone()
			}
			args.bucket.Reset()
		}

		// init next time window from data
		if args.bucket.Time().IsZero() {
			args.bucket.SetTime(args.Collapse.Truncate(modelTime))
			args.nextBucketTime = args.Collapse.Next(args.bucket.Time(), int(args.sign))
		}

		// accumulate data
		args.bucket.Add(args.model)
		return nil
	})

	// don't handle error here, will be picked up by trailer
	if err == nil {
		if args.bucket.IsEmpty() {
			// fill gap before last element
			gapStart := args.prevBucket.Time()
			gapEnd := args.Collapse.Truncate(args.model.Time())
			err = args.Fill(gapStart, gapEnd, func(fill SeriesBucket) error {
				if needComma {
					_, _ = io.WriteString(ctx.ResponseWriter, ",")
				} else {
					needComma = true
				}
				return enc.Encode(fill)
			})
		} else {
			// output last series element
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			err = enc.Encode(args.bucket)
			if err == nil {
				args.count++
			}
			args.prevBucket = args.bucket.Clone()
		}
	}

	if err == nil {
		// fill end gap
		err = args.Fill(args.bucket.Time(), args.To.Time(), func(fill SeriesBucket) error {
			if needComma {
				_, _ = io.WriteString(ctx.ResponseWriter, ",")
			} else {
				needComma = true
			}
			return enc.Encode(fill)
		})
	}

	// close JSON bracket
	_, _ = io.WriteString(ctx.ResponseWriter, "]")
	// ctx.Log.Tracef("JSON encoded %d rows", count)
	return err
}

func (args *SeriesRequest) StreamCSV(ctx *server.Context) error {
	var err error

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

		modelTime := args.model.Time()

		// output series value when the next model time has crossed boundary
		if !modelTime.Before(args.nextBucketTime) {
			if args.bucket.IsEmpty() {
				gapStart := args.prevBucket.Time()
				gapEnd := args.Collapse.Truncate(modelTime)
				err = args.Fill(gapStart, gapEnd, func(fill SeriesBucket) error {
					return enc.EncodeRecord(fill)
				})
				if err != nil {
					return err
				}
				args.prevBucket = args.bucket.Clone().SetTime(gapEnd)
			} else {
				// output accumulated data
				if err := enc.EncodeRecord(args.bucket); err != nil {
					return err
				}
				args.count++
				if args.Limit > 0 && args.count == int(args.Limit) {
					return io.EOF
				}
				args.prevBucket = args.bucket.Clone()
			}
			args.bucket.Reset()
		}

		// init next time window from data
		if args.bucket.Time().IsZero() {
			args.bucket.SetTime(args.Collapse.Truncate(modelTime))
			args.nextBucketTime = args.Collapse.Next(args.bucket.Time(), int(args.sign))
		}

		// accumulate data
		args.bucket.Add(args.model)
		return nil
	})

	if err == nil {
		if args.bucket.IsEmpty() {
			// fill gap before last element
			gapStart := args.prevBucket.Time()
			gapEnd := args.Collapse.Truncate(args.model.Time())
			err = args.Fill(gapStart, gapEnd, func(fill SeriesBucket) error {
				return enc.EncodeRecord(fill)
			})
		} else {
			// output last series element
			err = enc.EncodeRecord(args.bucket)
			if err == nil {
				args.count++
			}
			args.prevBucket = args.bucket.Clone()
		}
	}

	if err == nil {
		// fill end gap
		err = args.Fill(args.bucket.Time(), args.To.Time(), func(fill SeriesBucket) error {
			return enc.Encode(fill)
		})
	}

	// ctx.Log.Tracef("CSV Encoded %d rows", count)
	return err
}

func (args *SeriesRequest) Fill(from, to time.Time, fillFunc func(SeriesBucket) error) error {
	for _, ts := range args.Collapse.Steps(from, to, args.Limit) {
		if args.Limit > 0 && args.count >= int(args.Limit) {
			return io.EOF
		}
		var filled SeriesBucket
		switch args.FillMode {
		case FillModeNone:
			return nil
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
