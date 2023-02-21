// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/echa/config"
)

func main() {
	if err := run(); err != nil {
		if err != errExit {
			fmt.Println("Error:", err)
		}
	}
}

func run() error {
	if err := setup(); err != nil {
		return err
	}
	log.Infof("%s TzIndex -- %s %s", company, version, commit)
	log.Infof("(c) Copyright 2018-2023 %s", company)
	log.Infof("Go version %s", runtime.Version())
	log.Infof("Starting on %d cores", maxcpu)
	return runServer()
}

func setup() error {
	if err := parseFlags(); err != nil {
		return err
	}
	initLogging()

	if fullIndex {
		lightIndex = false
	}

	// init pseudo-random number generator in math package
	// Note: this is not used for cryptographic random numbers,
	//       but may be used by packages
	rand.Seed(time.Now().UnixNano())

	// set max number or CPU cores used for scheduling
	maxcpu = config.GetInt("go.cpu")
	if maxcpu <= 0 {
		maxcpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(maxcpu)

	// Block and transaction processing can cause bursty allocations. This
	// limits the garbage collector from excessively overallocating during
	// bursts. This value was arrived at with the help of profiling live usage.
	gogc := config.GetInt("go.gc")
	if gogc <= 0 {
		gogc = 20
	}
	debug.SetGCPercent(gogc)

	if r := config.GetInt("go.sample_rate"); r > 0 {
		runtime.SetMutexProfileFraction(r)
		runtime.SetBlockProfileRate(r)
	}
	return nil
}
