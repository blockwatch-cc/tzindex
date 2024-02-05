// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"fmt"

	"blockwatch.cc/tzgo/tezos"
)

// implicit operation list ids
const (
	OPL_PROTOCOL_UPGRADE = -1 // migration
	OPL_BLOCK_EVENTS     = -1 // block-level events like auto (un)freeze, rewards
	OPL_BLOCK_HEADER     = -1 // implicit operations like liquidity baking
)

// Indexer operation and event type
type OpType byte

// enums are allocated in chronological order with most often used ops first
const (
	OpTypeBake                  OpType = iota // 0 implicit event (bake reward pay)
	OpTypeEndorsement                         // 1
	OpTypeTransaction                         // 2
	OpTypeReveal                              // 3
	OpTypeDelegation                          // 4
	OpTypeOrigination                         // 5
	OpTypeNonceRevelation                     // 6
	OpTypeActivation                          // 7
	OpTypeBallot                              // 8
	OpTypeProposal                            // 9
	OpTypeDoubleBaking                        // 10
	OpTypeDoubleEndorsement                   // 11
	OpTypeUnfreeze                            // 12 implicit event
	OpTypeInvoice                             // 13 implicit event
	OpTypeAirdrop                             // 14 implicit event
	OpTypeSeedSlash                           // 15 implicit event
	OpTypeMigration                           // 16 implicit event
	OpTypeSubsidy                             // 17 v010 liquidity baking
	OpTypeRegisterConstant                    // 18 v011
	OpTypePreendorsement                      // 19 v012
	OpTypeDoublePreendorsement                // 20 v012
	OpTypeDepositsLimit                       // 21 v012
	OpTypeDeposit                             // 22 v012 implicit event (baker deposit)
	OpTypeBonus                               // 23 v012 implicit event (baker extra bonus)
	OpTypeReward                              // 24 v012 implicit event (endorsement reward pay/burn)
	OpTypeRollupOrigination                   // 25 v013
	OpTypeRollupTransaction                   // 26 v013
	OpTypeVdfRevelation                       // 27 v014
	OpTypeIncreasePaidStorage                 // 28 v014
	OpTypeDrainDelegate                       // 29 v015
	OpTypeUpdateConsensusKey                  // 30 v015
	OpTypeTransferTicket                      // 31 v013
	OpTypeStake                               // 32 v018
	OpTypeUnstake                             // 33 v018
	OpTypeFinalizeUnstake                     // 34 v018
	OpTypeSetDelegateParameters               // 35 v018
	OpTypeStakeSlash                          // 36 v018 implicit event (staker slash)
	OpTypeBatch                 = 254         // API output only
	OpTypeInvalid               = 255
)

var (
	opTypeStrings = map[OpType]string{
		OpTypeBake:                  "bake",
		OpTypeEndorsement:           "endorsement",
		OpTypeTransaction:           "transaction",
		OpTypeReveal:                "reveal",
		OpTypeDelegation:            "delegation",
		OpTypeOrigination:           "origination",
		OpTypeNonceRevelation:       "nonce_revelation",
		OpTypeActivation:            "activation",
		OpTypeBallot:                "ballot",
		OpTypeProposal:              "proposal",
		OpTypeDoubleBaking:          "double_baking",
		OpTypeDoubleEndorsement:     "double_endorsement",
		OpTypeUnfreeze:              "unfreeze",
		OpTypeInvoice:               "invoice",
		OpTypeAirdrop:               "airdrop",
		OpTypeSeedSlash:             "seed_slash",
		OpTypeMigration:             "migration",
		OpTypeSubsidy:               "subsidy",
		OpTypeRegisterConstant:      "register_constant",
		OpTypePreendorsement:        "preendorsement",
		OpTypeDoublePreendorsement:  "double_preendorsement",
		OpTypeDepositsLimit:         "deposits_limit",
		OpTypeDeposit:               "deposit",
		OpTypeReward:                "reward",
		OpTypeBonus:                 "bonus",
		OpTypeBatch:                 "batch",
		OpTypeRollupOrigination:     "rollup_origination",
		OpTypeRollupTransaction:     "rollup_transaction",
		OpTypeVdfRevelation:         "vdf_revelation",
		OpTypeIncreasePaidStorage:   "increase_paid_storage",
		OpTypeDrainDelegate:         "drain_delegate",
		OpTypeUpdateConsensusKey:    "update_consensus_key",
		OpTypeTransferTicket:        "transfer_ticket",
		OpTypeStake:                 "stake",
		OpTypeUnstake:               "unstake",
		OpTypeFinalizeUnstake:       "finalize_unstake",
		OpTypeSetDelegateParameters: "set_delegate_parameters",
		OpTypeStakeSlash:            "stake_slash",
		OpTypeInvalid:               "",
	}
	opTypeReverseStrings = make(map[string]OpType)
)

func init() {
	for n, v := range opTypeStrings {
		opTypeReverseStrings[v] = n
	}
}

func (t OpType) IsValid() bool {
	return t != OpTypeInvalid
}

func (t *OpType) UnmarshalText(data []byte) error {
	v := ParseOpType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid operation type '%s'", string(data))
	}
	*t = v
	return nil
}

func (t *OpType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseOpType(s string) OpType {
	t, ok := opTypeReverseStrings[s]
	if !ok {
		t = OpTypeInvalid
	}
	return t
}

func (t OpType) String() string {
	return opTypeStrings[t]
}

func (t OpType) IsEvent() bool {
	switch t {
	case
		OpTypeBake,
		OpTypeUnfreeze,
		OpTypeInvoice,
		OpTypeAirdrop,
		OpTypeSeedSlash,
		OpTypeMigration,
		OpTypeSubsidy,
		OpTypeDeposit,
		OpTypeBonus,
		OpTypeReward,
		OpTypeStakeSlash:
		return true
	default:
		return false
	}
}

func MapOpType(typ tezos.OpType) OpType {
	switch typ {
	case tezos.OpTypeActivateAccount:
		return OpTypeActivation
	case tezos.OpTypeDoubleBakingEvidence:
		return OpTypeDoubleBaking
	case tezos.OpTypeDoubleEndorsementEvidence:
		return OpTypeDoubleEndorsement
	case tezos.OpTypeDoublePreendorsementEvidence:
		return OpTypeDoublePreendorsement
	case tezos.OpTypeProposals:
		return OpTypeProposal
	case tezos.OpTypeBallot:
		return OpTypeBallot
	case tezos.OpTypeTransaction:
		return OpTypeTransaction
	case tezos.OpTypeOrigination:
		return OpTypeOrigination
	case tezos.OpTypeDelegation:
		return OpTypeDelegation
	case tezos.OpTypeReveal:
		return OpTypeReveal
	case tezos.OpTypeEndorsement, tezos.OpTypeEndorsementWithSlot:
		return OpTypeEndorsement
	case tezos.OpTypePreendorsement:
		return OpTypePreendorsement
	case tezos.OpTypeSeedNonceRevelation:
		return OpTypeNonceRevelation
	case tezos.OpTypeRegisterConstant:
		return OpTypeRegisterConstant
	case tezos.OpTypeSetDepositsLimit:
		return OpTypeDepositsLimit
	case tezos.OpTypeVdfRevelation:
		return OpTypeVdfRevelation
	case tezos.OpTypeIncreasePaidStorage:
		return OpTypeIncreasePaidStorage
	case tezos.OpTypeDrainDelegate:
		return OpTypeDrainDelegate
	case tezos.OpTypeUpdateConsensusKey:
		return OpTypeUpdateConsensusKey
	case tezos.OpTypeTransferTicket:
		return OpTypeTransferTicket
	case tezos.OpTypeTxRollupOrigination, tezos.OpTypeSmartRollupOriginate:
		return OpTypeRollupOrigination
	case
		tezos.OpTypeTxRollupSubmitBatch,
		tezos.OpTypeTxRollupCommit,
		tezos.OpTypeTxRollupReturnBond,
		tezos.OpTypeTxRollupFinalizeCommitment,
		tezos.OpTypeTxRollupRemoveCommitment,
		tezos.OpTypeTxRollupRejection,
		tezos.OpTypeTxRollupDispatchTickets,

		tezos.OpTypeSmartRollupAddMessages,
		tezos.OpTypeSmartRollupCement,
		tezos.OpTypeSmartRollupPublish,
		tezos.OpTypeSmartRollupRefute,
		tezos.OpTypeSmartRollupTimeout,
		tezos.OpTypeSmartRollupExecuteOutboxMessage,
		tezos.OpTypeSmartRollupRecoverBond:
		return OpTypeRollupTransaction
	// case OpTypeDalAttestation: // TODO
	// case OpTypeDalPublishSlotHeader: // TODO
	default:
		return OpTypeInvalid
	}
}

func (t OpType) ListId() int {
	switch t {
	case OpTypeBake,
		OpTypeInvoice,
		OpTypeAirdrop,
		OpTypeMigration,
		OpTypeSubsidy,
		OpTypeUnfreeze,
		OpTypeSeedSlash,
		OpTypeDeposit,
		OpTypeBonus,
		OpTypeReward,
		OpTypeStakeSlash:
		return -1
	case OpTypeEndorsement, OpTypePreendorsement:
		return 0
	case OpTypeProposal, OpTypeBallot:
		return 1
	case OpTypeActivation,
		OpTypeDoubleBaking,
		OpTypeDoubleEndorsement,
		OpTypeNonceRevelation,
		OpTypeDoublePreendorsement,
		OpTypeVdfRevelation,
		OpTypeDrainDelegate:
		return 2
	case OpTypeTransaction,
		OpTypeOrigination,
		OpTypeDelegation,
		OpTypeReveal,
		OpTypeRegisterConstant,
		OpTypeDepositsLimit,
		OpTypeRollupOrigination,
		OpTypeRollupTransaction,
		OpTypeIncreasePaidStorage,
		OpTypeUpdateConsensusKey,
		OpTypeTransferTicket,
		OpTypeStake,
		OpTypeUnstake,
		OpTypeFinalizeUnstake,
		OpTypeSetDelegateParameters:
		return 3
	default:
		return -1
	}
}

type OpTypeList []OpType

func (l OpTypeList) IsEmpty() bool {
	return len(l) == 0
}

func (l OpTypeList) Contains(typ OpType) bool {
	for _, v := range l {
		if v == typ {
			return true
		}
	}
	return false
}
