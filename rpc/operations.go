// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzindex/chain"
)

// OperationHeader represents a single operation included into a block
type OperationHeader struct {
	Protocol  chain.ProtocolHash  `json:"protocol"`
	ChainID   chain.ChainIdHash   `json:"chain_id"`
	Hash      chain.OperationHash `json:"hash"`
	Branch    chain.BlockHash     `json:"branch"`
	Contents  Operations          `json:"contents"`
	Signature string              `json:"signature"`
}

// Operation must be implemented by all operations
type Operation interface {
	OpKind() chain.OpType
}

type OperationError struct {
	GenericError
	Contract *chain.Address `json:"contract,omitempty"`
	Amount   int64          `json:"amount,string,omitempty"`
	Balance  int64          `json:"balance,string,omitempty"`
}

// GenericOp is a most generic type
type GenericOp struct {
	Kind chain.OpType `json:"kind"`
}

// OpKind implements Operation
func (e *GenericOp) OpKind() chain.OpType {
	return e.Kind
}

// Operations is a slice of Operation (interface type) with custom JSON unmarshaller
type Operations []Operation

// UnmarshalJSON implements json.Unmarshaler
func (e *Operations) UnmarshalJSON(data []byte) error {
	if data == nil {
		return nil
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*e = make(Operations, len(raw))

opLoop:
	for i, r := range raw {
		if r == nil {
			continue
		}
		var tmp GenericOp
		if err := json.Unmarshal(r, &tmp); err != nil {
			return fmt.Errorf("rpc: generic operation: %v", err)
		}

		switch tmp.Kind {
		// anonymous operations
		case chain.OpTypeActivateAccount:
			(*e)[i] = &AccountActivationOp{}
		case chain.OpTypeDoubleBakingEvidence:
			(*e)[i] = &DoubleBakingOp{}
		case chain.OpTypeDoubleEndorsementEvidence:
			(*e)[i] = &DoubleEndorsementOp{}
		case chain.OpTypeSeedNonceRevelation:
			(*e)[i] = &SeedNonceOp{}
		// manager operations
		case chain.OpTypeTransaction:
			(*e)[i] = &TransactionOp{}
		case chain.OpTypeOrigination:
			(*e)[i] = &OriginationOp{}
		case chain.OpTypeDelegation:
			(*e)[i] = &DelegationOp{}
		case chain.OpTypeReveal:
			(*e)[i] = &RevelationOp{}
		// consensus operations
		case chain.OpTypeEndorsement:
			(*e)[i] = &EndorsementOp{}
		// amendment operations
		case chain.OpTypeProposals:
			(*e)[i] = &ProposalsOp{}
		case chain.OpTypeBallot:
			(*e)[i] = &BallotOp{}

		default:
			log.Warnf("unsupported op '%s'", tmp.Kind)
			(*e)[i] = &tmp
			continue opLoop
		}

		if err := json.Unmarshal(r, (*e)[i]); err != nil {
			return fmt.Errorf("rpc: operation kind %s: %v", tmp.Kind, err)
		}
	}

	return nil
}
