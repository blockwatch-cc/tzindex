// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Delegation implements the TypedOperation interface.
var _ TypedOperation = (*Delegation)(nil)

// Delegation represents a transaction operation
type Delegation struct {
	Manager
	Delegate tezos.Address `json:"delegate,omitempty"`
}

// Fees returns fee-related balance updates to implement TypedOperation interface.
func (d Delegation) Fees() BalanceUpdates {
	return d.Metadata.BalanceUpdates
}
