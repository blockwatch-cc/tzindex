// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

type Status struct {
	Status   string  `json:"status"`
	Blocks   int64   `json:"blocks"`
	Indexed  int64   `json:"indexed"`
	Progress float64 `json:"progress"`
}
