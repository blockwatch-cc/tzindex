// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure SeedNonce implements the TypedOperation interface.
var _ TypedOperation = (*SeedNonce)(nil)

// SeedNonce represents a seed_nonce_revelation operation
type SeedNonce struct {
	Generic
	Level int64          `json:"level"`
	Nonce tezos.HexBytes `json:"nonce"`
}

// Ensure VdfRevelation implements the TypedOperation interface.
var _ TypedOperation = (*VdfRevelation)(nil)

// VdfRevelation represents a vdf_revelation operation
type VdfRevelation struct {
	Generic
	Solution []tezos.HexBytes `json:"solution"`
}
