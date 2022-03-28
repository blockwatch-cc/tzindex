// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
)

// Ensure DoubleEndorsement implements the TypedOperation interface.
var _ TypedOperation = (*DoubleEndorsement)(nil)

// DoubleEndorsement represents a double_endorsement_evidence operation
type DoubleEndorsement struct {
	Generic
	OP1      json.RawMessage    `json:"op1"`
	OP2      json.RawMessage    `json:"op2"`
	Metadata *OperationMetadata `json:"metadata,omitempty"`
}

// Meta returns operation metadata to implement TypedOperation interface.
func (d DoubleEndorsement) Meta() OperationMetadata {
	return *d.Metadata
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (d DoubleEndorsement) Fees() BalanceUpdates {
	return d.Metadata.BalanceUpdates
}

func (d DoubleEndorsement) Strip() DoubleEndorsement {
	dd := d
	dd.Metadata = nil
	return dd
}
