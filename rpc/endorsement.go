// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzindex/chain"
)

// EndorsementOp represents an endorsement operation
type EndorsementOp struct {
	GenericOp
	Level       int64                  `json:"level"`       // <= v008
	Metadata    *EndorsementOpMetadata `json:"metadata"`    // all protocols
	Endorsement *EndorsementContent    `json:"endorsement"` // v009+
	Slot        int                    `json:"slot"`        // v009+
}

func (e EndorsementOp) GetLevel() int64 {
	if e.Endorsement != nil {
		return e.Endorsement.Operations.Level
	}
	return e.Level
}

// EndorsementOpMetadata represents an endorsement operation metadata
type EndorsementOpMetadata struct {
	BalanceUpdates BalanceUpdates `json:"balance_updates"`
	Delegate       chain.Address  `json:"delegate"`
	Slots          []int          `json:"slots"`
}

func (m EndorsementOpMetadata) Address() chain.Address {
	return m.Delegate
}

// v009+
type EndorsementContent struct {
	Branch     string                `json:"branch"`
	Operations EmbeddedEndorsementOp `json:"operations"`
}

// v009+
type EmbeddedEndorsementOp struct {
	Kind  chain.OpType `json:"kind"`
	Level int64        `json:"level"`
}
