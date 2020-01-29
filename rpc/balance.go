// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"

	"blockwatch.cc/tzindex/chain"
)

// BalanceUpdate is a variable structure depending on the Kind field
type BalanceUpdate interface {
	BalanceUpdateKind() string
}

// GenericBalanceUpdate holds the common values among all BalanceUpdatesType variants
type GenericBalanceUpdate struct {
	Kind   string `json:"kind"`
	Change int64  `json:"change,string"`
}

// BalanceUpdateKind returns the BalanceUpdateType's Kind field
func (g *GenericBalanceUpdate) BalanceUpdateKind() string {
	return g.Kind
}

// ContractBalanceUpdate is a BalanceUpdatesType variant for Kind=contract
type ContractBalanceUpdate struct {
	GenericBalanceUpdate
	Contract chain.Address `json:"contract"`
}

// FreezerBalanceUpdate is a BalanceUpdatesType variant for Kind=freezer
type FreezerBalanceUpdate struct {
	GenericBalanceUpdate
	Category string        `json:"category"`
	Delegate chain.Address `json:"delegate"`
	Level_   int64         `json:"level"` // wrongly called level, it's cycle
	Cycle_   int64         `json:"cycle"` // v4 fix
}

func (b *FreezerBalanceUpdate) Cycle() int64 {
	if b.Level_ > 0 {
		return b.Level_
	}
	return b.Cycle_
}

// BalanceUpdates is a list of balance update operations
type BalanceUpdates []BalanceUpdate

// UnmarshalJSON implements json.Unmarshaler
func (b *BalanceUpdates) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*b = make(BalanceUpdates, len(raw))

opLoop:
	for i, r := range raw {
		var tmp GenericBalanceUpdate
		if err := json.Unmarshal(r, &tmp); err != nil {
			return err
		}

		switch tmp.Kind {
		case "contract":
			(*b)[i] = &ContractBalanceUpdate{}

		case "freezer":
			(*b)[i] = &FreezerBalanceUpdate{}

		default:
			(*b)[i] = &tmp
			continue opLoop
		}

		if err := json.Unmarshal(r, (*b)[i]); err != nil {
			return err
		}
	}

	return nil
}
