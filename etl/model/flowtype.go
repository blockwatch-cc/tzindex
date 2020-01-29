// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

type FlowCategory int

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

type FlowType int

const (
	FlowTypeActivation      FlowType = iota // 0
	FlowTypeDenounciation                   // 1
	FlowTypeTransaction                     // 2
	FlowTypeOrigination                     // 3
	FlowTypeDelegation                      // 4
	FlowTypeReveal                          // 5
	FlowTypeEndorsement                     // 6
	FlowTypeBaking                          // 7
	FlowTypeNonceRevelation                 // 8
	FlowTypeInternal                        // 9 - used for unfreeze
	FlowTypeVest                            // 10 - vests amount into same account (ready to spend)
	FlowTypePour                            // 11 - pours vested amount into other account
	FlowTypeInvoice                         // 12 - invoice feature
	FlowTypeAirdrop                         // 13 - Babylon Airdrop
	FlowTypeInvalid
)

func ParseFlowType(s string) FlowType {
	switch s {
	case "activation":
		return FlowTypeActivation
	case "denounciation":
		return FlowTypeDenounciation
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
	case "vest":
		return FlowTypeVest
	case "pour":
		return FlowTypePour
	case "invoice":
		return FlowTypeInvoice
	case "airdrop":
		return FlowTypeAirdrop
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
	case FlowTypeDenounciation:
		return "denounciation"
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
	case FlowTypeVest:
		return "vest"
	case FlowTypePour:
		return "pour"
	case FlowTypeInvoice:
		return "invoice"
	case FlowTypeAirdrop:
		return "airdrop"
	default:
		return "invalid"
	}
}
