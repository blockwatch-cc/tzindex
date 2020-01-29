// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

// SeedNonceOp represents a seed_nonce_revelation operation
type SeedNonceOp struct {
	GenericOp
	Level    int64                `json:"level"`
	Nonce    HexBytes             `json:"nonce"`
	Metadata *SeedNonceOpMetadata `json:"metadata"`
}

// SeedNonceOpMetadata represents a transaction operation metadata
type SeedNonceOpMetadata struct {
	BalanceUpdates BalanceUpdates `json:"balance_updates"` // fee-related
}
