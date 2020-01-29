// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/micheline"
)

// TransactionOp represents a transaction operation
type TransactionOp struct {
	GenericOp
	Source       chain.Address          `json:"source"`
	Destination  chain.Address          `json:"destination"`
	Fee          int64                  `json:"fee,string"`
	Amount       int64                  `json:"amount,string"`
	Counter      int64                  `json:"counter,string"`
	GasLimit     int64                  `json:"gas_limit,string"`
	StorageLimit int64                  `json:"storage_limit,string"`
	Parameters   *micheline.Parameters  `json:"parameters,omitempty"`
	Metadata     *TransactionOpMetadata `json:"metadata"`
}

// TransactionOpMetadata represents a transaction operation metadata
type TransactionOpMetadata struct {
	BalanceUpdates  BalanceUpdates     `json:"balance_updates"` // fee-related
	Result          *TransactionResult `json:"operation_result"`
	InternalResults []*InternalResult  `json:"internal_operation_results,omitempty"`
}

// TransactionResult represents a transaction result
type TransactionResult struct {
	BalanceUpdates      BalanceUpdates       `json:"balance_updates"` // tx or contract related
	ConsumedGas         int64                `json:"consumed_gas,string"`
	Status              chain.OpStatus       `json:"status"`
	Allocated           bool                 `json:"allocated_destination_contract"` // new addr created and payed
	Errors              []OperationError     `json:"errors,omitempty"`
	Storage             *micheline.Prim      `json:"storage,omitempty"`
	StorageSize         int64                `json:"storage_size,string"`
	PaidStorageSizeDiff int64                `json:"paid_storage_size_diff,string"`
	BigMapDiff          micheline.BigMapDiff `json:"big_map_diff,omitempty"`

	// when reused as internal origination result
	OriginatedContracts []chain.Address `json:"originated_contracts,omitempty"`
}

type InternalResult struct {
	GenericOp
	Source      chain.Address         `json:"source"`
	Nonce       int64                 `json:"nonce"`
	Result      *TransactionResult    `json:"result"`
	Destination *chain.Address        `json:"destination,omitempty"` // transaction
	Delegate    *chain.Address        `json:"delegate,omitempty"`    // delegation
	Parameters  *micheline.Parameters `json:"parameters,omitempty"`  // transaction
	Amount      int64                 `json:"amount,string"`         // transaction
	Balance     int64                 `json:"balance,string"`        // origination
	Script      *micheline.Script     `json:"script,omitempty"`      // origination
}
