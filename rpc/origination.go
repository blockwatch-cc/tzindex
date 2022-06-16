// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Origination implements the TypedOperation interface.
var _ TypedOperation = (*Origination)(nil)

// Origination represents a contract creation operation
type Origination struct {
	Manager
	ManagerPubkey  tezos.Address     `json:"manager_pubkey"` // proto v1 & >= v4
	ManagerPubkey2 tezos.Address     `json:"managerPubkey"`  // proto v2, v3
	Balance        int64             `json:"balance,string"`
	Spendable      *bool             `json:"spendable"`   // true when missing before v5 Babylon
	Delegatable    *bool             `json:"delegatable"` // true when missing before v5 Babylon
	Delegate       *tezos.Address    `json:"delegate"`
	Script         *micheline.Script `json:"script"`
}

func (o Origination) ManagerAddress() tezos.Address {
	if o.ManagerPubkey2.IsValid() {
		return o.ManagerPubkey2
	}
	return o.ManagerPubkey
}

type BabylonFlags byte

const (
	BabylonSpendable   BabylonFlags = 0x1
	BabylonDelegatable BabylonFlags = 0x2
)

func (o Origination) BabylonFlags(version int) BabylonFlags {
	var flags BabylonFlags
	// in Babylon, these flags always exist, required for upgrades
	if o.Spendable != nil && *o.Spendable {
		flags |= BabylonSpendable
	}
	if o.Delegatable != nil && *o.Delegatable {
		flags |= BabylonDelegatable
	}
	// pre-babylon, they were true when missing,
	// post-delphi they are deprecated
	if version < 5 {
		if o.Spendable == nil || *o.Spendable {
			flags |= BabylonSpendable
		}
		if o.Delegatable == nil || *o.Delegatable {
			flags |= BabylonDelegatable
		}
	}
	return flags
}

func (f BabylonFlags) IsSpendable() bool {
	return f&BabylonSpendable > 0
}

func (f BabylonFlags) IsDelegatable() bool {
	return f&BabylonDelegatable > 0
}

func (f BabylonFlags) CanUpgrade() bool {
	return f.IsSpendable() || !f.IsSpendable() && f.IsDelegatable()
}
