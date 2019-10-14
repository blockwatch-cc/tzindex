// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

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
