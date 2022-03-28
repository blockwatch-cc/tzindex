// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Activation implements the TypedOperation interface.
var _ TypedOperation = (*Activation)(nil)

// Activation represents a transaction operation
type Activation struct {
	Generic
	Pkh      tezos.Address     `json:"pkh"`
	Secret   tezos.HexBytes    `json:"secret"`
	Metadata OperationMetadata `json:"metadata"`
}

// Meta returns an empty operation metadata to implement TypedOperation interface.
func (a Activation) Meta() OperationMetadata {
	return a.Metadata
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (a Activation) Fees() BalanceUpdates {
	return a.Metadata.BalanceUpdates
}
