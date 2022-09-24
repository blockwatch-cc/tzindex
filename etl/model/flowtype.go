// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"fmt"
)

type FlowCategory byte

const (
	FlowCategoryRewards    FlowCategory = iota // 0 freezer category
	FlowCategoryDeposits                       // 1 freezer category
	FlowCategoryFees                           // 2 freezer category
	FlowCategoryBalance                        // 3 spendable balance
	FlowCategoryDelegation                     // 4 delegated balance from other accounts
	FlowCategoryBond                           // 5 rollup bond
	FlowCategoryInvalid
)

func ParseFlowCategory(s string) FlowCategory {
	switch s {
	case "rewards":
		return FlowCategoryRewards
	case "deposits":
		return FlowCategoryDeposits
	case "fees":
		return FlowCategoryFees
	case "balance":
		return FlowCategoryBalance
	case "delegation":
		return FlowCategoryDelegation
	case "bond":
		return FlowCategoryBond
	default:
		return FlowCategoryInvalid
	}
}

func (c FlowCategory) IsValid() bool {
	return c != FlowCategoryInvalid
}

func (c FlowCategory) String() string {
	switch c {
	case FlowCategoryRewards:
		return "rewards"
	case FlowCategoryDeposits:
		return "deposits"
	case FlowCategoryFees:
		return "fees"
	case FlowCategoryBalance:
		return "balance"
	case FlowCategoryDelegation:
		return "delegation"
	case FlowCategoryBond:
		return "bond"
	default:
		return "invalid"
	}
}

type FlowType byte

const (
	FlowTypeEndorsement       FlowType = iota // 0
	FlowTypeTransaction                       // 1
	FlowTypeOrigination                       // 2
	FlowTypeDelegation                        // 3
	FlowTypeReveal                            // 4
	FlowTypeBaking                            // 5
	FlowTypeNonceRevelation                   // 6
	FlowTypeActivation                        // 7
	FlowTypePenalty                           // 8
	FlowTypeInternal                          // 9 - used for unfreeze
	FlowTypeInvoice                           // 10 - invoice feature
	FlowTypeAirdrop                           // 11 - Babylon Airdrop
	FlowTypeSubsidy                           // 12 - Granada liquidity baking
	FlowTypeRegisterConstant                  // 13 - Hangzhou+
	FlowTypeBonus                             // 14 - Ithaca+ baking bonus
	FlowTypeReward                            // 15 - Ithaca+ endorsing reward (or slash)
	FlowTypeDeposit                           // 16 - Ithaca+ deposit transfer
	FlowTypeDepositsLimit                     // 17 - Ithaca+
	FlowTypeRollupOrigination                 // 18 - Jakarta+
	FlowTypeRollupTransaction                 // 19 - Jakarta+
	FlowTypeRollupReward                      // 20 - Jakarta+
	FlowTypeRollupPenalty                     // 21 - Jakarta+
	FlowTypePayStorage                        // 22 - Kathmandu+
	FlowTypeInvalid           = 255
)

var (
	flowTypeStrings = map[FlowType]string{
		FlowTypeEndorsement:       "endorsement",
		FlowTypeTransaction:       "transaction",
		FlowTypeOrigination:       "origination",
		FlowTypeDelegation:        "delegation",
		FlowTypeReveal:            "reveal",
		FlowTypeBaking:            "baking",
		FlowTypeNonceRevelation:   "nonce_revelation",
		FlowTypeActivation:        "activation",
		FlowTypePenalty:           "penalty",
		FlowTypeInternal:          "internal",
		FlowTypeInvoice:           "invoice",
		FlowTypeAirdrop:           "airdrop",
		FlowTypeSubsidy:           "subsidy",
		FlowTypeRegisterConstant:  "register_constant",
		FlowTypeBonus:             "bonus",
		FlowTypeReward:            "reward",
		FlowTypeDeposit:           "deposit",
		FlowTypeDepositsLimit:     "deposits_limit",
		FlowTypeRollupOrigination: "rollup_origination",
		FlowTypeRollupTransaction: "rollup_transaction",
		FlowTypeRollupReward:      "rollup_reward",
		FlowTypeRollupPenalty:     "rollup_penalty",
		FlowTypePayStorage:        "pay_storage",
		FlowTypeInvalid:           "invalid",
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
	default:
		return FlowTypeInvalid
	}
}
