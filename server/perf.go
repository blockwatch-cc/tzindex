// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"net/http"
	"strconv"
	"time"
)

type PerformanceCounter struct {
	start   time.Time
	Runtime time.Duration
}

func NewPerformanceCounter(now time.Time) *PerformanceCounter {
	p := &PerformanceCounter{start: now}
	if now.IsZero() {
		p.StartCall(time.Now().UTC())
	}
	return p
}

func (p *PerformanceCounter) StartCall(now time.Time) {
	p.start = now
}

func (p *PerformanceCounter) EndCall() {
	if p.Runtime == 0 {
		p.Runtime = time.Since(p.start)
	}
}

func (p *PerformanceCounter) WriteResponseHeader(w http.ResponseWriter) {
	p.EndCall()
	rt := strconv.FormatFloat(float64(p.Runtime)/float64(time.Second), 'f', 6, 64)
	w.Header().Set(headerRuntime, rt)
}

func (p *PerformanceCounter) WriteResponseTrailer(w http.ResponseWriter) {
	p.Runtime = time.Since(p.start)
	rt := strconv.FormatFloat(float64(p.Runtime)/float64(time.Second), 'f', 6, 64)
	w.Header().Set(trailerRuntime, rt)
}

func (p *PerformanceCounter) Since() time.Duration {
	return time.Since(p.start)
}
