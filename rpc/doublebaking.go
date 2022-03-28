// Copyright (c) 2020-2022 Blockwatch Data Inc.
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
	BH1      json.RawMessage    `json:"bh1"`
	BH2      json.RawMessage    `json:"bh2"`
	Metadata *OperationMetadata `json:"metadata,omitempty"`
}

// Meta returns operation metadata to implement TypedOperation interface.
func (d DoubleBaking) Meta() OperationMetadata {
	return *d.Metadata
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (d DoubleBaking) Fees() BalanceUpdates {
	return d.Metadata.BalanceUpdates
}

func (d DoubleBaking) Strip() DoubleBaking {
	dd := d
	dd.Metadata = nil
	return dd
}
