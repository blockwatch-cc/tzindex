// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"math/rand"
	"time"

	"blockwatch.cc/tzindex/cmd"
)

func main() {
	// init pseudo-random number generator in math package
	// Note: this is not used for cryptographic random numbers,
	//       but may be used by packages
	rand.Seed(time.Now().UnixNano())

	// Run command
	cmd.Run()
}
