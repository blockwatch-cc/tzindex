// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// BalanceUpdate is a variable structure depending on the Kind field
type BalanceUpdate struct {
	Kind     string `json:"kind"`          // contract, freezer, accumulator, commitment, minted, burned
	Origin   string `json:"origin"`        // block, migration, subsidy
	Category string `json:"category"`      // optional, used on mint, burn, freezer
	Change   int64  `json:"change,string"` // amount, <0 =

	// related debtor or creditor
	Contract  string `json:"contract,omitempty"`  // contract only
	Delegate  string `json:"delegate,omitempty"`  // freezer and burn only
	Committer string `json:"committer,omitempty"` // committer only

	// Ithaca only
	IsParticipationBurn bool `json:"participation"` // burn only
	IsRevelationBurn    bool `json:"revelation"`    // burn only

	// legacy freezer cycle
	Level_ int64 `json:"level"` // wrongly called level, it's cycle
	Cycle_ int64 `json:"cycle"` // v4 fix
}

// Categories
//
// # Mint categories
// - `nonce revelation rewards` is the source of tokens minted to reward delegates for revealing their nonces
// - `double signing evidence rewards` is the source of tokens minted to reward delegates for injecting a double signing evidence
// - `endorsing rewards` is the source of tokens minted to reward delegates for endorsing blocks
// - `baking rewards` is the source of tokens minted to reward delegates for creating blocks
// - `baking bonuses` is the source of tokens minted to reward delegates for validating blocks and including extra endorsements
// - `subsidy` is the source of tokens minted to subsidize the liquidity baking CPMM contract
// - `invoice` is the source of tokens minted to compensate some users who have contributed to the betterment of the chain
// - `commitment` is the source of tokens minted to match commitments made by some users to supply funds for the chain
// - `bootstrap` is analogous to `commitment` but is for internal use or testing.
// - `minted` is only for internal use and may be used to mint tokens for testing.
//
// # Burn categories
// - `storage fees` is the destination of storage fees burned for consuming storage space on the chain
// - `punishments` is the destination of tokens burned as punishment for a delegate that has double baked or double endorsed
// - `lost endorsing rewards` is the destination of rewards that were not distributed to a delegate.
// - `burned` is only for internal use and testing.
//
// # Accumulator categories
// - `block fees` designates the container account used to collect manager operation fees while block's operations are being applied. Other categories may be added in the future.
//
// # Freezer categories
// - `legacy_deposits`, `legacy_fees`, or `legacy_rewards` represent the accounts of frozen deposits, frozen fees or frozen rewards up to protocol HANGZHOU.
// - `deposits` represents the account of frozen deposits in subsequent protocols (replacing the legacy container account `legacy_deposits` above).

func (b BalanceUpdate) Address() tezos.Address {
	a := b.Contract + b.Delegate + b.Committer
	addr, _ := tezos.ParseAddress(a)
	return addr
}

func (b BalanceUpdate) Amount() int64 {
	return b.Change
}

func (b BalanceUpdate) Cycle() int64 {
	if b.Level_ > 0 {
		return b.Level_
	}
	return b.Cycle_
}

// BalanceUpdates is a list of balance update operations
type BalanceUpdates []BalanceUpdate
