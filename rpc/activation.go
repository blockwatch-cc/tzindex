// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzindex/chain"
)

// AccountActivationOp represents a transaction operation
type AccountActivationOp struct {
	GenericOp
	Pkh      chain.Address                `json:"pkh"`
	Secret   HexBytes                     `json:"secret"`
	Metadata *AccountActivationOpMetadata `json:"metadata"`
}

// AccountActivationOpMetadata represents a transaction operation metadata
type AccountActivationOpMetadata struct {
	BalanceUpdates BalanceUpdates `json:"balance_updates"` // initial funding
}
