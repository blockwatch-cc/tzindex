// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build !linux
// +build !linux

package server

import (
	"context"
	"net/url"
	"os"
	"runtime"
	"time"
)

func GetSysStat(ctx context.Context) (SysStat, error) {
	s := SysStat{
		Timestamp: time.Now().UTC(),
	}
	host, _ := os.Hostname()
	s.Hostname = host
	if n := os.Getenv("HOST_HOSTNAME"); n != "" {
		s.ContainerName = host
		u, _ := url.Parse(n)
		s.Hostname = u.Hostname()
	}
	s.NumCpu = runtime.NumCPU()
	s.NumGoroutine = runtime.NumGoroutine()

	// mem + sys
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	s.MemMallocs = memStats.Mallocs
	s.MemFrees = memStats.Frees
	s.MemHeapAlloc = memStats.HeapAlloc
	s.MemStackInuse = memStats.StackInuse

	return s, nil
}
