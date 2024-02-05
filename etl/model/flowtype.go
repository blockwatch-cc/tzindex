// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"fmt"
)

type FlowKind byte

const (
	FlowKindRewards    FlowKind = iota // 0 freezer category
	FlowKindDeposits                   // 1 freezer category
	FlowKindFees                       // 2 freezer category
	FlowKindBalance                    // 3 spendable balance
	FlowKindDelegation                 // 4 delegated balance from other accounts
	FlowKindBond                       // 5 rollup bond
	FlowKindStake                      // 6 staking
	FlowKindInvalid
)

func ParseFlowKind(s string) FlowKind {
	switch s {
	case "rewards":
		return FlowKindRewards
	case "deposits":
		return FlowKindDeposits
	case "fees":
		return FlowKindFees
	case "balance":
		return FlowKindBalance
	case "delegation":
		return FlowKindDelegation
	case "bond":
		return FlowKindBond
	case "stake":
		return FlowKindStake
	default:
		return FlowKindInvalid
	}
}

func (c FlowKind) IsValid() bool {
	return c != FlowKindInvalid
}

func (c FlowKind) String() string {
	switch c {
	case FlowKindRewards:
		return "rewards"
	case FlowKindDeposits:
		return "deposits"
	case FlowKindFees:
		return "fees"
	case FlowKindBalance:
		return "balance"
	case FlowKindDelegation:
		return "delegation"
	case FlowKindBond:
		return "bond"
	case FlowKindStake:
		return "stake"
	default:
		return "invalid"
	}
}

type FlowType byte

const (
	FlowTypeEndorsement           FlowType = iota // 0
	FlowTypeTransaction                           // 1
	FlowTypeOrigination                           // 2
	FlowTypeDelegation                            // 3
	FlowTypeReveal                                // 4
	FlowTypeBaking                                // 5
	FlowTypeNonceRevelation                       // 6
	FlowTypeActivation                            // 7
	FlowTypePenalty                               // 8
	FlowTypeInternal                              // 9 - used for unfreeze
	FlowTypeInvoice                               // 10 - invoice feature
	FlowTypeAirdrop                               // 11 - Babylon Airdrop
	FlowTypeSubsidy                               // 12 - Granada liquidity baking
	FlowTypeRegisterConstant                      // 13 - Hangzhou+
	FlowTypeBonus                                 // 14 - Ithaca+ baking bonus
	FlowTypeReward                                // 15 - Ithaca+ endorsing reward (or slash)
	FlowTypeDeposit                               // 16 - Ithaca+ deposit transfer
	FlowTypeDepositsLimit                         // 17 - Ithaca+
	FlowTypeRollupOrigination                     // 18 - Jakarta+
	FlowTypeRollupTransaction                     // 19 - Jakarta+
	FlowTypeRollupReward                          // 20 - Jakarta+
	FlowTypeRollupPenalty                         // 21 - Jakarta+
	FlowTypePayStorage                            // 22 - Kathmandu+
	FlowTypeUpdateConsensusKey                    // 23 - Lima+
	FlowTypeDrain                                 // 24 - Lima+
	FlowTypeTransferTicket                        // 25 - Jakarta+
	FlowTypeStake                                 // 26 - Oxford+
	FlowTypeUnstake                               // 27 - Oxford+
	FlowTypeFinalizeUnstake                       // 28 - Oxford+
	FlowTypeSetDelegateParameters                 // 29 - Oxford+
	FlowTypeInvalid               = 255
)

var (
	flowTypeStrings = map[FlowType]string{
		FlowTypeEndorsement:           "endorsement",
		FlowTypeTransaction:           "transaction",
		FlowTypeOrigination:           "origination",
		FlowTypeDelegation:            "delegation",
		FlowTypeReveal:                "reveal",
		FlowTypeBaking:                "baking",
		FlowTypeNonceRevelation:       "nonce_revelation",
		FlowTypeActivation:            "activation",
		FlowTypePenalty:               "penalty",
		FlowTypeInternal:              "internal",
		FlowTypeInvoice:               "invoice",
		FlowTypeAirdrop:               "airdrop",
		FlowTypeSubsidy:               "subsidy",
		FlowTypeRegisterConstant:      "register_constant",
		FlowTypeBonus:                 "bonus",
		FlowTypeReward:                "reward",
		FlowTypeDeposit:               "deposit",
		FlowTypeDepositsLimit:         "deposits_limit",
		FlowTypeRollupOrigination:     "rollup_origination",
		FlowTypeRollupTransaction:     "rollup_transaction",
		FlowTypeRollupReward:          "rollup_reward",
		FlowTypeRollupPenalty:         "rollup_penalty",
		FlowTypePayStorage:            "pay_storage",
		FlowTypeUpdateConsensusKey:    "update_consensus_key",
		FlowTypeDrain:                 "drain",
		FlowTypeTransferTicket:        "transfer_ticket",
		FlowTypeStake:                 "stake",
		FlowTypeUnstake:               "unstake",
		FlowTypeFinalizeUnstake:       "finalize_unstake",
		FlowTypeSetDelegateParameters: "set_delegate_parameters",
		FlowTypeInvalid:               "invalid",
	}
	flowTypeReverseStrings = make(map[string]FlowType)
)

func init() {
	for n, v := range flowTypeStrings {
		flowTypeReverseStrings[v] = n
	}
}

func (t *FlowType) UnmarshalText(data []byte) error {
	v := ParseFlowType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid flow type '%s'", string(data))
	}
	*t = v
	return nil
}

func (t *FlowType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseFlowType(s string) FlowType {
	t, ok := flowTypeReverseStrings[s]
	if !ok {
		t = FlowTypeInvalid
	}
	return t
}

func (t FlowType) IsValid() bool {
	return t != FlowTypeInvalid
}

func (t FlowType) String() string {
	return flowTypeStrings[t]
}

func MapFlowType(typ OpType) FlowType {
	switch typ {
	case OpTypeActivation:
		return FlowTypeActivation
	case OpTypeDoubleBaking,
		OpTypeDoubleEndorsement,
		OpTypeDoublePreendorsement:
		return FlowTypePenalty
	case OpTypeTransaction:
		return FlowTypeTransaction
	case OpTypeOrigination:
		return FlowTypeOrigination
	case OpTypeDelegation:
		return FlowTypeDelegation
	case OpTypeReveal:
		return FlowTypeReveal
	case OpTypeEndorsement,
		OpTypePreendorsement:
		return FlowTypeEndorsement
	case OpTypeNonceRevelation, OpTypeVdfRevelation:
		return FlowTypeNonceRevelation
	case OpTypeInvoice:
		return FlowTypeInvoice
	case OpTypeAirdrop:
		return FlowTypeAirdrop
	case OpTypeRegisterConstant:
		return FlowTypeRegisterConstant
	case OpTypeDepositsLimit:
		return FlowTypeDepositsLimit
	case OpTypeRollupOrigination:
		return FlowTypeRollupOrigination
	case OpTypeRollupTransaction:
		return FlowTypeRollupTransaction
	case OpTypeIncreasePaidStorage:
		return FlowTypePayStorage
	case OpTypeUpdateConsensusKey:
		return FlowTypeUpdateConsensusKey
	case OpTypeDrainDelegate:
		return FlowTypeDrain
	case OpTypeTransferTicket:
		return FlowTypeTransferTicket
	case OpTypeStake:
		return FlowTypeStake
	case OpTypeUnstake:
		return FlowTypeUnstake
	case OpTypeFinalizeUnstake:
		return FlowTypeFinalizeUnstake
	case OpTypeSetDelegateParameters:
		return FlowTypeSetDelegateParameters
	default:
		return FlowTypeInvalid
	}
}
