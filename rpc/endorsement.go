// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Endorsement implements the TypedOperation interface.
var _ TypedOperation = (*Endorsement)(nil)

// Endorsement represents an endorsement operation
type Endorsement struct {
	Generic
	Level       int64               `json:"level"`              // <= v008, v012+
	Metadata    OperationMetadata   `json:"metadata"`           // all protocols
	Endorsement *InlinedEndorsement `json:"endorsement"`        // v009+
	Slot        int                 `json:"slot"`               // v009+
	Round       int                 `json:"round"`              // v012+
	PayloadHash tezos.PayloadHash   `json:"block_payload_hash"` // v012+
}

func (e Endorsement) GetLevel() int64 {
	if e.Endorsement != nil {
		return e.Endorsement.Operations.Level
	}
	return e.Level
}

// Meta returns an empty operation metadata to implement TypedOperation interface.
func (e Endorsement) Meta() OperationMetadata {
	return e.Metadata
}

// InlinedEndorsement represents and embedded endorsement
type InlinedEndorsement struct {
	Branch     tezos.BlockHash `json:"branch"`     // the double block
	Operations Endorsement     `json:"operations"` // only level and kind are set
	Signature  tezos.Signature `json:"signature"`
}
