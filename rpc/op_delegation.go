// Copyright (c) 2023 Blockwatch Data Inc.
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

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (d Delegation) Addresses(set *tezos.AddressSet) {
	set.AddUnique(d.Source)
	if d.Delegate.IsValid() {
		set.AddUnique(d.Delegate)
	}
}
