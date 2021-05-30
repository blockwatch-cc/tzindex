// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"math/rand"
	"time"
)

func main() {
	// init pseudo-random number generator in math package
	// Note: this is not used for cryptographic random numbers,
	//       but may be used by packages
	rand.Seed(time.Now().UnixNano())

	// Run command
	Run()
}
