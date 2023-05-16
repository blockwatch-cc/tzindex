// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure TransferTicket implements the TypedOperation interface.
var _ TypedOperation = (*TransferTicket)(nil)

type TransferTicket struct {
	Manager
	Destination tezos.Address  `json:"destination"`
	Entrypoint  string         `json:"entrypoint"`
	Type        micheline.Prim `json:"ticket_ty"`
	Contents    micheline.Prim `json:"ticket_contents"`
	Ticketer    tezos.Address  `json:"ticket_ticketer"`
	Amount      tezos.Z        `json:"ticket_amount"`
}

// Costs returns operation cost to implement TypedOperation interface.
func (t TransferTicket) Costs() tezos.Costs {
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

func (t TransferTicket) EncodeParameters() micheline.Prim {
	return micheline.NewPair(
		micheline.NewBytes(t.Ticketer.EncodePadded()), // ticketer
		micheline.NewPair(
			t.Type, // type
			micheline.NewPair(
				t.Contents,                       // contents
				micheline.NewNat(t.Amount.Big()), // amount
			),
		),
	)
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (t TransferTicket) Addresses(set *tezos.AddressSet) {
	set.AddUnique(t.Source)
	set.AddUnique(t.Destination)
	set.AddUnique(t.Ticketer)
}
