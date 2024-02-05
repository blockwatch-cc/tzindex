// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"fmt"

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
	IsParticipationBurn bool `json:"participation,omitempty"` // burn only
	IsRevelationBurn    bool `json:"revelation,omitempty"`    // burn only

	// legacy freezer cycle
	Level_ int64 `json:"level,omitempty"` // wrongly called level, it's cycle
	Cycle_ int64 `json:"cycle,omitempty"` // v4 fix

	// Oxford staking
	Staker struct {
		Contract string `json:"contract,omitempty"` // single: used in ??
		Delegate string `json:"delegate,omitempty"` // single & shared: used in ??
		Baker    string `json:"baker,omitempty"`    // baker: ??
	} `json:"staker"`
	DelayedOp string `json:"delayed_operation_hash,omitempty"`
	Delegator string `json:"delegator,omitempty"` // Oxford+, ??
}

// Categories
// see also docs/alpha/token_management.rst
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
// - `unstaked_deposits` represent tez for which unstaking has been requested.

func (b BalanceUpdate) Address() (addr tezos.Address) {
	switch {
	case len(b.Delegator) > 0:
		// debug
		fmt.Printf("Found BALANCE_UPDATE with NEW delegator field: %#v\n", b)
	case len(b.Contract) > 0:
		addr, _ = tezos.ParseAddress(b.Contract)
	case len(b.Delegate) > 0:
		addr, _ = tezos.ParseAddress(b.Delegate)
	case len(b.Committer) > 0:
		addr, _ = tezos.ParseAddress(b.Committer)
	case len(b.Staker.Contract) > 0:
		addr, _ = tezos.ParseAddress(b.Staker.Contract)
	case len(b.Staker.Delegate) > 0:
		addr, _ = tezos.ParseAddress(b.Staker.Delegate)
	case len(b.Staker.Baker) > 0:
		addr, _ = tezos.ParseAddress(b.Staker.Baker)
	}
	return
}

func (b BalanceUpdate) IsSharedStake() bool {
	return len(b.Staker.Contract) > 0 && len(b.Staker.Delegate) > 0
}

func (b BalanceUpdate) IsBakerStake() bool {
	return len(b.Staker.Contract) == 0 && (len(b.Staker.Delegate)+len(b.Staker.Baker) > 0)
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

// type BalanceOrigin byte

// const (
// 	BalanceOriginInvalid BalanceOrigin = iota
// 	BalanceOriginBlock
// 	BalanceOriginMigration
// 	BalanceOriginSubsidy
// 	BalanceOriginSimulation
// 	BalanceOriginDelayed
// )

// var balanceOriginMap = map[string]BalanceOrigin{
// 	"block":             BalanceOriginBlock,
// 	"migration":         BalanceOriginMigration,
// 	"subsidy":           BalanceOriginSubsidy,
// 	"simulation":        BalanceOriginSimulation,
// 	"delayed_operation": BalanceOriginDelayed,
// }

// type BalanceKind byte

// const (
// 	BalanceKindInvalid BalanceKind = iota
// 	BalanceKindContract
// 	BalanceKindAccumulator
// 	BalanceKindFreezer
// 	BalanceKindMinted
// 	BalanceKindBurned
// 	BalanceKindStaking
// )

// var balanceKindMap = map[string]BalanceKind{
// 	"contract":    BalanceKindContract,
// 	"accumulator": BalanceKindAccumulator,
// 	"freezer":     BalanceKindFreezer,
// 	"minted":      BalanceKindMinted,
// 	"burned":      BalanceKindBurned,
// 	"staking":     BalanceKindStaking,
// }

// type BalanceCategory byte

// const (
// 	BalanceCategoryInvalid BalanceCategory = iota
// 	BalanceCategoryEndorsingRewards
// 	// BalanceCategoryAttestingRewards
// 	BalanceCategoryBakingBonuses
// 	BalanceCategoryBakingRewards
// 	BalanceCategoryBlockFees
// 	BalanceCategoryDeposits
// 	BalanceCategoryRewards
// 	BalanceCategoryFees
// 	BalanceCategoryNonceRevelationRewards
// 	BalanceCategoryStorageFees
// 	BalanceCategoryBonds
// 	BalanceCategoryBootstrap
// 	BalanceCategoryBurned
// 	BalanceCategoryCommitment
// 	BalanceCategoryDelegateDenominator
// 	BalanceCategoryDelegatorNumerator
// 	BalanceCategoryInvoice
// 	BalanceCategoryLostEndorsingRewards
// 	// BalanceCategoryLostAttestingRewards
// 	BalanceCategoryMinted
// 	BalanceCategoryPunishments
// 	BalanceCategorySrPunishments
// 	BalanceCategorySrRewards
// 	BalanceCategorySubsidy
// 	BalanceCategoryUnstakedDeposits
// )

// var balanceCategoryMap = map[string]BalanceCategory{
// 	"endorsing rewards":                   BalanceCategoryEndorsingRewards,
// 	"attesting rewards":                   BalanceCategoryEndorsingRewards,
// 	"baking bonuses":                      BalanceCategoryBakingBonuses,
// 	"baking rewards":                      BalanceCategoryBakingRewards,
// 	"block fees":                          BalanceCategoryBlockFees,
// 	"deposits":                            BalanceCategoryDeposits,
// 	"rewards":                             BalanceCategoryRewards,
// 	"fees":                                BalanceCategoryFees,
// 	"nonce revelation rewards":            BalanceCategoryNonceRevelationRewards,
// 	"storage fees":                        BalanceCategoryStorageFees,
// 	"bonds":                               BalanceCategoryBonds,
// 	"bootstrap":                           BalanceCategoryBootstrap,
// 	"burned":                              BalanceCategoryBurned,
// 	"commitment":                          BalanceCategoryCommitment,
// 	"delegate_denominator":                BalanceCategoryDelegateDenominator,
// 	"delegator_numerator":                 BalanceCategoryDelegatorNumerator,
// 	"invoice":                             BalanceCategoryInvoice,
// 	"lost endorsing rewards":              BalanceCategoryLostEndorsingRewards,
// 	"lost attesting rewards":              BalanceCategoryLostEndorsingRewards,
// 	"minted":                              BalanceCategoryMinted,
// 	"punishments":                         BalanceCategoryPunishments,
// 	"smart_rollup_refutation_punishments": BalanceCategorySrPunishments,
// 	"smart_rollup_refutation_rewards":     BalanceCategorySubsidy,
// 	"subsidy":                             BalanceCategorySubsidy,
// 	"unstaked_deposits":                   BalanceCategoryUnstakedDeposits,
// }
