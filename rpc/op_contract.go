// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure ConstantRegistration implements the TypedOperation interface.
var _ TypedOperation = (*ConstantRegistration)(nil)

// ConstantRegistration represents a global constant registration operation
type ConstantRegistration struct {
	Manager
	Value micheline.Prim `json:"value,omitempty"`
}

// Ensure IncreasePaidStorage implements the TypedOperation interface.
var _ TypedOperation = (*IncreasePaidStorage)(nil)

// IncreasePaidStorage represents a transaction operation
type IncreasePaidStorage struct {
	Manager
	Destination tezos.Address `json:"destination"`
	Amount      int64         `json:"amount,string"`
}

// Costs returns operation cost to implement TypedOperation interface.
func (t IncreasePaidStorage) Costs() tezos.Costs {
	res := t.Metadata.Result
	cost := tezos.Costs{
		Fee:     t.Manager.Fee,
		GasUsed: res.Gas(),
	}
	if !t.Result().IsSuccess() {
		return cost
	}
	for _, v := range res.BalanceUpdates {
		if v.Kind != CONTRACT {
			continue
		}
		burn := v.Amount()
		if burn >= 0 {
			continue
		}
		cost.StorageBurn += -burn
	}
	return cost
}
