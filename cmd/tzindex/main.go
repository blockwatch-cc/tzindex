// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

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
	log.Infof("%s %s %s %s", company, appName, version, commit)
	log.Infof("(c) Copyright 2020-2023 %s", company)
	log.Infof("Go version %s", runtime.Version())
	log.Infof("Starting on %d cores", maxcpu)
	startProfiling()
	defer stopProfiling()
	return runServer()
}

func setup() error {
	if err := parseFlags(); err != nil {
		return err
	}
	initLogging()

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
	return nil
}

func startProfiling() {
	if r := config.GetInt("go.sample_rate"); r > 0 {
		runtime.SetMutexProfileFraction(r)
		runtime.SetBlockProfileRate(r)
	}
	if cpuprof != "" {
		f, err := os.Create(cpuprof)
		if err != nil {
			log.Errorf("cannot write cpu profile: %s", err)
		} else {
			log.Info("Profiling CPU usage.")
			_ = pprof.StartCPUProfile(f)
		}
	}
}

func stopProfiling() {
	if cpuprof != "" {
		pprof.StopCPUProfile()
		log.Infof("CPU profile written to %s", cpuprof)
	}

	if blockprof != "" {
		b := pprof.Lookup("block")
		if b != nil {
			f, err := os.Create(blockprof)
			if err != nil {
				log.Errorf("cannot write blocking profile: %s", err)
			} else {
				_ = b.WriteTo(f, 1)
				log.Infof("Lock blocking profile written to %s", blockprof)
			}
		}
	}

	if mutexprof != "" {
		b := pprof.Lookup("mutex")
		if b != nil {
			f, err := os.Create(mutexprof)
			if err != nil {
				log.Errorf("cannot write mutex contention profile: %s", err)
			} else {
				_ = b.WriteTo(f, 1)
				log.Infof("Mutex contention profile written to %s", mutexprof)
			}
		}
	}
}
