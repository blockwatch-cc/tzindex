// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/awesome-gocui/gocui"
	"github.com/echa/config"
)

var (
	debug        bool
	quiet        bool
	verbose      bool
	trace        bool
	printVersion bool
	baseURL      string
	host         string
	cert         string
	ca           string
	key          string
	insecure     bool
	interval     time.Duration

	version string = "v0.1"
	commit  string = "dev"
	appDesc string = "Blockwatch tztop - ©2024 Blockwatch Data Inc, All rights reserved."
)

func main() {
	if err := run(); err != nil && err != gocui.ErrQuit {
		log.Error(err)
		os.Exit(1)
	}
}

func run() error {
	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	runCmd.StringVar(&baseURL, "url", "http://localhost:8000", "indexer server url")
	runCmd.StringVar(&host, "host", "", "HTTP hostname override")
	runCmd.StringVar(&cert, "cert", "", "TLS client certificate file")
	runCmd.StringVar(&ca, "ca", "", "TLS trusted certificate authority file")
	runCmd.StringVar(&key, "key", "", "TLS client private key file")
	runCmd.DurationVar(&interval, "interval", time.Second*5, "query interval")
	runCmd.BoolVar(&quiet, "q", false, "enable quiet mode")
	runCmd.BoolVar(&verbose, "v", false, "enable verbose mode")
	runCmd.BoolVar(&debug, "vv", false, "enable debug mode")
	runCmd.BoolVar(&trace, "vvv", false, "enable trace mode")
	runCmd.BoolVar(&insecure, "insecure", false, "don't check TLS server cert")
	runCmd.BoolVar(&printVersion, "version", false, "print version info")

	runCmd.Usage = func() {
		fmt.Fprintf(runCmd.Output(), "Usage: %s [options]:\n", os.Args[0])
		runCmd.PrintDefaults()
	}

	if err := runCmd.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			runCmd.Usage()
			return nil
		}
		return err
	}

	if printVersion {
		fmt.Println(appDesc)
		fmt.Printf("App version: %s %s\n", version, commit)
		fmt.Printf("Go version: %s\n", runtime.Version())
		return nil
	}

	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return fmt.Errorf("Invalid url: %v", err)
	}

	if (cert != "") != (key != "") {
		return fmt.Errorf("Cert and Key must both be specified to enable TLS client support")
	}

	log.Info(appDesc)
	log.Infof("App version: %s %s", version, commit)
	log.Infof("Go version: %s", runtime.Version())

	// set logging level
	switch {
	case quiet:
		config.Set("logging.level", "error")
	case trace:
		config.Set("logging.level", "trace")
	case debug:
		config.Set("logging.level", "debug")
	case verbose:
		config.Set("logging.level", "info")
	}
	initLogging()

	log.Infof("Using API at %s", baseURL)
	if cert != "" {
		log.Infof("Using client TLS cert %s", cert)
	}
	if ca != "" {
		log.Infof("Using custom TLS CA %s", ca)
	}

	client, err := NewClient(Config{
		BaseURL:  baseURL,
		Host:     host,
		Cert:     cert,
		Key:      key,
		Ca:       ca,
		Insecure: insecure,
	})
	if err != nil {
		return err
	}
	// tab, err := client.GetTableStats()
	// fmt.Printf("Resp %#v\n", tab)
	// sys, err := client.GetSysStats()
	// fmt.Printf("Resp %#v\n", sys)

	app, err := NewTop(client, interval)
	if err != nil {
		return err
	}
	if err := app.Display(); err != nil {
		return err
	}
	return nil
}
