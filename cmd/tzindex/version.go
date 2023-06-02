// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"fmt"
	"runtime"
)

var (
	company           = "Blockwatch Data Inc."
	orgUrl            = "blockwatch.cc"
	appName           = "tzindex"
	apiVersion        = "v017-2023-05-31"
	version    string = "v17.0"
	commit     string = "dev"
	envprefix         = "TZ"
)

func UserAgent() string {
	return fmt.Sprintf("%s.%s/%s.%s",
		appName,
		orgUrl,
		version,
		commit,
	)
}

func printVersion() {
	fmt.Printf("Tezos TzIndex L1 indexer by %s\n", company)
	fmt.Printf("Version: %s (%s)\n", version, commit)
	fmt.Printf("API version: %s\n", apiVersion)
	fmt.Printf("Go version: %s\n", runtime.Version())
}
