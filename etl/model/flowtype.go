// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

type FlowCategory byte

const (
	FlowCategoryRewards    FlowCategory = iota // 0 freezer category
	FlowCategoryDeposits                       // 1 freezer category
	FlowCategoryFees                           // 2 freezer category
	FlowCategoryBalance                        // 3 spendable balance
	FlowCategoryDelegation                     // 4 delegated balance from other accounts
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
	default:
		return "invalid"
	}
}

type FlowType byte

const (
	FlowTypeActivation           FlowType = iota // 0
	FlowTypeDenunciation                         // 1
	FlowTypeTransaction                          // 2
	FlowTypeOrigination                          // 3
	FlowTypeDelegation                           // 4
	FlowTypeReveal                               // 5
	FlowTypeEndorsement                          // 6
	FlowTypeBaking                               // 7
	FlowTypeNonceRevelation                      // 8
	FlowTypeInternal                             // 9 - used for unfreeze
	FlowTypeInvoice                              // 10 - invoice feature
	FlowTypeAirdrop                              // 11 - Babylon Airdrop
	FlowTypeSubsidy                              // 12 - Granada liquidity baking
	FlowTypeConstantRegistration                 // 13 - Hangzhou+
	FlowTypeInvalid
)

func ParseFlowType(s string) FlowType {
	switch s {
	case "activation":
		return FlowTypeActivation
	case "denunciation":
		return FlowTypeDenunciation
	case "transaction":
		return FlowTypeTransaction
	case "origination":
		return FlowTypeOrigination
	case "delegation":
		return FlowTypeDelegation
	case "reveal":
		return FlowTypeReveal
	case "endorsement":
		return FlowTypeEndorsement
	case "baking":
		return FlowTypeBaking
	case "noncerevelation":
		return FlowTypeNonceRevelation
	case "internal":
		return FlowTypeInternal
	case "invoice":
		return FlowTypeInvoice
	case "airdrop":
		return FlowTypeAirdrop
	case "subsidy":
		return FlowTypeSubsidy
	case "constant":
		return FlowTypeConstantRegistration
	default:
		return FlowTypeInvalid
	}
}

func (t FlowType) IsValid() bool {
	return t != FlowTypeInvalid
}

func (t FlowType) String() string {
	switch t {
	case FlowTypeActivation:
		return "activation"
	case FlowTypeDenunciation:
		return "denunciation"
	case FlowTypeTransaction:
		return "transaction"
	case FlowTypeOrigination:
		return "origination"
	case FlowTypeDelegation:
		return "delegation"
	case FlowTypeReveal:
		return "reveal"
	case FlowTypeEndorsement:
		return "endorsement"
	case FlowTypeBaking:
		return "baking"
	case FlowTypeNonceRevelation:
		return "noncerevelation"
	case FlowTypeInternal:
		return "internal"
	case FlowTypeInvoice:
		return "invoice"
	case FlowTypeAirdrop:
		return "airdrop"
	case FlowTypeSubsidy:
		return "subsidy"
	case FlowTypeConstantRegistration:
		return "constant"
	default:
		return "invalid"
	}
}
