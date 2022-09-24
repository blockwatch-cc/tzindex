// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Transaction implements the TypedOperation interface.
var _ TypedOperation = (*Transaction)(nil)

// Transaction represents a transaction operation
type Transaction struct {
	Manager
	Destination tezos.Address        `json:"destination"`
	Amount      int64                `json:"amount,string"`
	Parameters  micheline.Parameters `json:"parameters"`
}

func (t Transaction) FindEmbeddedAddresses(addrs *tezos.AddressSet) {
	if !t.Destination.IsContract() {
		return
	}
	collect := func(p micheline.Prim) error {
		switch {
		case len(p.String) == 36 || len(p.String) == 37:
			if a, err := tezos.ParseAddress(p.String); err == nil {
				addrs.AddUnique(a)
			}
			return micheline.PrimSkip
		case tezos.IsAddressBytes(p.Bytes):
			a := tezos.Address{}
			if err := a.UnmarshalBinary(p.Bytes); err == nil {
				addrs.AddUnique(a)
			}
			return micheline.PrimSkip
		default:
			return nil
		}
	}

	// from storage
	_ = t.Metadata.Result.Storage.Walk(collect)

	// from bigmap updates
	for _, v := range t.Metadata.Result.BigmapEvents() {
		if v.Action != micheline.DiffActionUpdate {
			continue
		}
		_ = v.Key.Walk(collect)
		_ = v.Value.Walk(collect)
	}

	// from internal results
	for _, it := range t.Metadata.InternalResults {
		if it.Script != nil {
			_ = it.Script.Storage.Walk(collect)
		}
		_ = it.Result.Storage.Walk(collect)
		for _, v := range it.Result.BigmapEvents() {
			if v.Action != micheline.DiffActionUpdate {
				continue
			}
			_ = v.Key.Walk(collect)
			_ = v.Value.Walk(collect)
		}
	}
}

type InternalResult struct {
	Kind        tezos.OpType         `json:"kind"`
	Source      tezos.Address        `json:"source"`
	Nonce       int64                `json:"nonce"`
	Result      OperationResult      `json:"result"`
	Destination tezos.Address        `json:"destination"`    // transaction
	Delegate    tezos.Address        `json:"delegate"`       // delegation
	Parameters  micheline.Parameters `json:"parameters"`     // transaction
	Amount      int64                `json:"amount,string"`  // transaction
	Balance     int64                `json:"balance,string"` // origination
	Script      *micheline.Script    `json:"script"`         // origination
	Type        micheline.Prim       `json:"type"`           // event
	Payload     micheline.Prim       `json:"payload"`        // event
	Tag         string               `json:"tag"`            // event
}

// found in block metadata from v010+
type ImplicitResult struct {
	Kind                tezos.OpType      `json:"kind"`
	BalanceUpdates      BalanceUpdates    `json:"balance_updates"`
	ConsumedGas         int64             `json:"consumed_gas,string"`
	ConsumedMilliGas    int64             `json:"consumed_milligas,string"`
	Storage             micheline.Prim    `json:"storage"`
	StorageSize         int64             `json:"storage_size,string"`
	OriginatedContracts []tezos.Address   `json:"originated_contracts,omitempty"`
	PaidStorageSizeDiff int64             `json:"paid_storage_size_diff,string"`
	Script              *micheline.Script `json:"script"`
}

func (r ImplicitResult) Gas() int64 {
	if r.ConsumedMilliGas > 0 {
		return r.ConsumedMilliGas / 1000
	}
	return r.ConsumedGas
}

func (r ImplicitResult) MilliGas() int64 {
	if r.ConsumedMilliGas > 0 {
		return r.ConsumedMilliGas
	}
	return r.ConsumedGas * 1000
}
