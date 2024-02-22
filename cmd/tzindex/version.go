// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"fmt"
	"runtime"
)

var (
	company           = "Blockwatch Data Inc."
	envPrefix         = "TZ"
	appName           = "tzindex"
	apiVersion        = "v018-2024-01-15"
	version    string = "v18.0"
	commit     string = "dev"
)

func UserAgent() string {
	return fmt.Sprintf("%s/%s.%s",
		appName,
		version,
		commit,
	)
}

func printVersion() {
	fmt.Printf("Tezos L1 Indexer by %s\n", company)
	fmt.Printf("Version: %s (%s)\n", version, commit)
	fmt.Printf("API version: %s\n", apiVersion)
	fmt.Printf("Go version: %s\n", runtime.Version())
}
