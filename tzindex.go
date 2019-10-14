// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

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
