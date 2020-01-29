// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzindex/chain"
)

// DoubleEndorsementOp represents a double_endorsement_evidence operation
type DoubleEndorsementOp struct {
	GenericOp
	OP1      DoubleEndorsementEvidence    `json:"op1"`
	OP2      DoubleEndorsementEvidence    `json:"op2"`
	Metadata *DoubleEndorsementOpMetadata `json:"metadata"`
}

// DoubleEndorsementOpMetadata represents double_endorsement_evidence operation metadata
type DoubleEndorsementOpMetadata struct {
	BalanceUpdates BalanceUpdates `json:"balance_updates"`
}

// DoubleEndorsementEvidence represents one of the duplicate endoresements
type DoubleEndorsementEvidence struct {
	Branch     chain.BlockHash `json:"branch"`     // the double block
	Operations EndorsementOp   `json:"operations"` // only level and kind are set
	Signature  string          `json:"signature"`
}
