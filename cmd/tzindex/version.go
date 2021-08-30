// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	company           = "Blockwatch Data Inc."
	orgUrl            = "blockwatch.cc"
	orgName           = "Blockwatch"
	appName           = "tzindex"
	apiVersion        = "v010-2021-07-24"
	version    string = "v10.1"
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

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of " + appName,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%s TzIndex OSS %s -- %s\n", orgName, version, commit)
		fmt.Printf("(c) Copyright 2018-2021 -- %s\n", company)
		fmt.Printf("Go version (client): %s\n", runtime.Version())
	},
}
