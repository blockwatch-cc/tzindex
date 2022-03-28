// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Transaction implements the TypedOperation interface.
var _ TypedOperation = (*Transaction)(nil)

// Transaction represents a transaction operation
type Transaction struct {
	Manager
	Destination tezos.Address        `json:"destination"`
	Amount      int64                `json:"amount,string"`
	Parameters  micheline.Parameters `json:"parameters"`
	Metadata    OperationMetadata    `json:"metadata"`
}

// Meta returns operation metadata to implement TypedOperation interface.
func (t Transaction) Meta() OperationMetadata {
	return t.Metadata
}

// Result returns operation result to implement TypedOperation interface.
func (t Transaction) Result() OperationResult {
	return t.Metadata.Result
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (t Transaction) Fees() BalanceUpdates {
	return t.Metadata.BalanceUpdates
}

type InternalResult struct {
	Kind        tezos.OpType         `json:"kind"`
	Source      tezos.Address        `json:"source"`
	Nonce       int64                `json:"nonce"`
	Result      OperationResult      `json:"result"`
	Destination tezos.Address        `json:"destination"`    // transaction
	Delegate    tezos.Address        `json:"delegate"`       // delegation
	Parameters  micheline.Parameters `json:"parameters"`     // transaction
	Amount      int64                `json:"amount,string"`  // transaction
	Balance     int64                `json:"balance,string"` // origination
	Script      *micheline.Script    `json:"script"`         // origination
}

// found in block metadata from v010+
type ImplicitResult struct {
	Kind                tezos.OpType      `json:"kind"`
	BalanceUpdates      BalanceUpdates    `json:"balance_updates"`
	ConsumedGas         int64             `json:"consumed_gas,string"`
	Storage             micheline.Prim    `json:"storage"`
	StorageSize         int64             `json:"storage_size,string"`
	OriginatedContracts []tezos.Address   `json:"originated_contracts,omitempty"`
	PaidStorageSizeDiff int64             `json:"paid_storage_size_diff,string"`
	Script              *micheline.Script `json:"script"`
}
