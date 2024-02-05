// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

// Ensure SetDepositsLimit implements the TypedOperation interface.
var _ TypedOperation = (*SetDepositsLimit)(nil)

// SetDepositsLimit represents a baker deposit limit update operation.
type SetDepositsLimit struct {
	Manager
	Limit    *int64            `json:"limit,string"`
	Metadata OperationMetadata `json:"metadata"`
}

// Meta returns operation metadata to implement TypedOperation interface.
func (r SetDepositsLimit) Meta() OperationMetadata {
	return r.Metadata
}

// Result returns operation result to implement TypedOperation interface.
func (r SetDepositsLimit) Result() OperationResult {
	return r.Metadata.Result
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (r SetDepositsLimit) Fees() BalanceUpdates {
	return r.Metadata.BalanceUpdates
}
