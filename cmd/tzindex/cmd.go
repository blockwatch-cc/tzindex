// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

	"github.com/spf13/cobra"

	"github.com/echa/config"
	logpkg "github.com/echa/log"
)

var rootCmd = &cobra.Command{
	Use: appName + " [OPTIONS] [COMMANDS]",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		StartProfiling()
		// overwrite path from command line
		if dbpath != "" {
			config.Set("database.path", dbpath)
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		StopProfiling()
	},
}

var (
	// configuration handling
	conf     string
	testconf bool
	maxcpu   int
	gogc     int
	dbpath   string

	// verbosity levels
	verbose bool
	vdebug  bool
	vtrace  bool

	// runtime profiling
	cpuprof   string
	blockprof string
	mutexprof string
	profrate  int
)

func init() {
	cobra.OnInitialize(initConfig)

	// config
	rootCmd.PersistentFlags().StringVarP(&conf, "config", "c", "", "config file")
	rootCmd.PersistentFlags().BoolVarP(&testconf, "test", "t", false, "test configuration and exit")
	rootCmd.PersistentFlags().IntVar(&maxcpu, "cpus", -1, "max number of logical CPU cores to use (default: all)")
	rootCmd.PersistentFlags().IntVar(&gogc, "gogc", 20, "trigger GC when used mem grows by N percent")
	rootCmd.PersistentFlags().StringVarP(&dbpath, "dbpath", "p", "", "database `path`")

	// verbosity
	rootCmd.PersistentFlags().BoolVar(&verbose, "v", false, "be verbose")
	rootCmd.PersistentFlags().BoolVar(&vdebug, "vv", false, "debug mode")
	rootCmd.PersistentFlags().BoolVar(&vtrace, "vvv", false, "trace mode")

	// profiling
	rootCmd.PersistentFlags().StringVar(&cpuprof, "profile-cpu", "", "write cpu profile to file")
	rootCmd.PersistentFlags().StringVar(&blockprof, "profile-block", "", "write blocking events to file")
	rootCmd.PersistentFlags().StringVar(&mutexprof, "profile-mutex", "", "write mutex contention samples to file")
	rootCmd.PersistentFlags().IntVar(&profrate, "profile-rate", 100, "block/mutex profiling rate in fractions of 100 (e.g. 100 == 1%)")
}

func Run() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("%v\n", err)
		return
	}
}

func initConfig() {
	// set initial log level
	switch true {
	case vtrace:
		setLogLevels(logpkg.LevelTrace)
	case vdebug:
		setLogLevels(logpkg.LevelDebug)
	default:
		setLogLevels(logpkg.LevelInfo)
	}

	// load config
	config.SetEnvPrefix(envprefix)
	if conf != "" {
		config.SetConfigName(conf)
	}
	realconf := config.ConfigName()
	if _, err := os.Stat(realconf); err == nil {
		if err := config.ReadConfigFile(); err != nil {
			fmt.Printf("Could not read config %s: %v\n", realconf, err)
			os.Exit(1)
		}
		log.Infof("Using configuration file %s", realconf)
	} else {
		log.Warnf("Missing config file, using default values.")
	}
	initLogging()

	// overwrite all subsystem levels
	switch true {
	case vtrace:
		setLogLevels(logpkg.LevelTrace)
	case vdebug:
		setLogLevels(logpkg.LevelDebug)
	case verbose:
		setLogLevels(logpkg.LevelInfo)
	}

	if err := parseRPCFlags(); err != nil {
		log.Error(err)
	}

	// TODO: testconf
	if testconf {
		// Check()
		print(config.AllSettings())
		log.Info("Configuration OK.")
		os.Exit(0)
	}

	// set max CPU
	if maxcpu <= 0 {
		maxcpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(maxcpu)
	log.Infof("%s %s %s %s", orgName, appName, version, commit)
	log.Infof("(c) Copyright 2018-2022 %s", company)
	log.Infof("Starting %s on %d cores", UserAgent(), maxcpu)
	log.Infof("Go version %s", runtime.Version())
	runtime.SetBlockProfileRate(profrate)
	runtime.SetMutexProfileFraction(profrate)
	if profrate > 0 {
		log.Debugf("Profiling mutex/blocking at %.2f%% sample rate.", 100.0/float64(profrate))
	}

	// set GC trigger
	if gogc < 0 {
		gogc = 20
	}
	// Block and transaction processing can cause bursty allocations. This
	// limits the garbage collector from excessively overallocating during
	// bursts. This value was arrived at with the help of profiling live
	// usage.
	debug.SetGCPercent(gogc)
}

func print(val interface{}) {
	buf, _ := json.MarshalIndent(val, "", "  ")
	fmt.Printf("%s\n", string(buf))
}

func StartProfiling() {
	if cpuprof != "" {
		f, err := os.Create(cpuprof)
		if err != nil {
			log.Errorf("cannot write cpu profile: %v", err)
		} else {
			log.Info("Profiling CPU usage.")
			_ = pprof.StartCPUProfile(f)
		}
	}
}

func StopProfiling() {
	if cpuprof != "" {
		pprof.StopCPUProfile()
		log.Infof("CPU profile written to %s", cpuprof)
	}

	if blockprof != "" {
		b := pprof.Lookup("block")
		if b != nil {
			f, err := os.Create(blockprof)
			if err != nil {
				log.Errorf("cannot write blocking profile: %v", err)
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
				log.Errorf("cannot write mutex contention profile: %v", err)
			} else {
				_ = b.WriteTo(f, 1)
				log.Infof("Mutex contention profile written to %s", mutexprof)
			}
		}
	}
}
