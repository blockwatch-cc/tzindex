// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"time"
)

type Model struct {
	Time  time.Time
	Table TableResponse
	Sys   SysStat
	Tip   Tip
	Error error
}

func (m Model) IsValid() bool {
	return !m.Time.IsZero()
}
