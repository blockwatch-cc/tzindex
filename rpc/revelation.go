// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzindex/chain"
)

// RevelationOp represents a reveal operation
type RevelationOp struct {
	GenericOp
	Source       chain.Address         `json:"source"`
	Fee          int64                 `json:"fee,string"`
	Counter      int64                 `json:"counter,string"`
	GasLimit     int64                 `json:"gas_limit,string"`
	StorageLimit int64                 `json:"storage_limit,string"`
	PublicKey    chain.Hash            `json:"public_key"`
	Metadata     *RevelationOpMetadata `json:"metadata"`
}

// RevelationOpMetadata represents a reveal operation metadata
type RevelationOpMetadata struct {
	BalanceUpdates BalanceUpdates   `json:"balance_updates"` // fee-related
	Result         RevelationResult `json:"operation_result"`
}

// RevelationResult represents a reveal result
type RevelationResult struct {
	ConsumedGas int64            `json:"consumed_gas,string"`
	Status      chain.OpStatus   `json:"status"`
	Errors      []OperationError `json:"errors,omitempty"`
}
