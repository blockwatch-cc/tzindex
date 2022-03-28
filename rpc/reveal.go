// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Reveal implements the TypedOperation interface.
var _ TypedOperation = (*Reveal)(nil)

// Reveal represents a reveal operation
type Reveal struct {
	Manager
	PublicKey tezos.Key         `json:"public_key"`
	Metadata  OperationMetadata `json:"metadata"`
}

// Meta returns operation metadata to implement TypedOperation interface.
func (r Reveal) Meta() OperationMetadata {
	return r.Metadata
}

// Result returns operation result to implement TypedOperation interface.
func (r Reveal) Result() OperationResult {
	return r.Metadata.Result
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (t Reveal) Fees() BalanceUpdates {
	return t.Metadata.BalanceUpdates
}
