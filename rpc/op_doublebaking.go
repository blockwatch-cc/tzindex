// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
)

// Ensure DoubleBaking implements the TypedOperation interface.
var _ TypedOperation = (*DoubleBaking)(nil)

// DoubleBaking represents a double_baking_evidence operation
type DoubleBaking struct {
	Generic
	BH1 json.RawMessage `json:"bh1"`
	BH2 json.RawMessage `json:"bh2"`
}

func (d DoubleBaking) Strip() DoubleBaking {
	dd := d
	dd.Metadata = nil
	return dd
}
