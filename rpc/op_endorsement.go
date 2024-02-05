// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Endorsement implements the TypedOperation interface.
var _ TypedOperation = (*Endorsement)(nil)

// Endorsement represents an endorsement operation. This is a
// stripped version for indexing without block_payload_hash.
type Endorsement struct {
	Generic
	Level       int64               `json:"level"`                 // <= v008, v012+
	Endorsement *InlinedEndorsement `json:"endorsement,omitempty"` // v009+
	Slot        int                 `json:"slot"`                  // v009+
	Round       int                 `json:"round"`                 // v012+
	// PayloadHash tezos.PayloadHash   `json:"block_payload_hash"` // v012+
}

func (e Endorsement) GetLevel() int64 {
	if e.Endorsement != nil {
		return e.Endorsement.Operations.Level
	}
	return e.Level
}

// InlinedEndorsement represents and embedded endorsement. This is a
// stripped version for indexing without signature.
type InlinedEndorsement struct {
	Branch     tezos.BlockHash `json:"branch"`     // the double block
	Operations Endorsement     `json:"operations"` // only level and kind are set
	// Signature  tezos.Signature `json:"signature"`
}
