// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzindex/chain"
)

// EndorsementOp represents an endorsement operation
type EndorsementOp struct {
	GenericOp
	Level    int64                  `json:"level"`
	Metadata *EndorsementOpMetadata `json:"metadata"`
}

// EndorsementOpMetadata represents an endorsement operation metadata
type EndorsementOpMetadata struct {
	BalanceUpdates BalanceUpdates `json:"balance_updates"`
	Delegate       chain.Address  `json:"delegate"`
	Slots          []int          `json:"slots"`
}
