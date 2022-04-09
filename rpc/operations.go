// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Operation represents a single operation or batch of operations included in a block
type Operation struct {
	Hash     tezos.OpHash     `json:"hash"`
	Contents OperationList    `json:"contents"`
	Errors   []OperationError `json:"error,omitempty"`    // mempool only
	Metadata string           `json:"metadata,omitempty"` // contains `too large` when stripped, this is BAD!!
}

// TypedOperation must be implemented by all operations
type TypedOperation interface {
	Kind() tezos.OpType
	Meta() OperationMetadata
	Result() OperationResult
	Fees() BalanceUpdates
}

// OperationError represents data describing an error conditon that lead to a
// failed operation execution.
type OperationError struct {
	GenericError
	// whitelist commonly useful error contents, avoid storing large scripts, etc
	Contract       string          `json:"contract,omitempty"`
	ContractHandle string          `json:"contract_handle,omitempty"`
	BigMap         int64           `json:"big_map,omitempty"`
	Identifier     string          `json:"identifier,omitempty"`
	Location       int64           `json:"location,omitempty"`
	Loc            int64           `json:"loc,omitempty"`
	With           *micheline.Prim `json:"with,omitempty"`
	Amount         int64           `json:"amount,string,omitempty"`
	Balance        int64           `json:"balance,string,omitempty"`
}

// OperationMetadata contains execution receipts for successful and failed
// operations.
type OperationMetadata struct {
	BalanceUpdates BalanceUpdates  `json:"balance_updates,omitempty"` // fee-related
	Result         OperationResult `json:"operation_result"`

	// transaction only
	InternalResults []InternalResult `json:"internal_operation_results,omitempty"`

	// endorsement only
	Delegate            tezos.Address `json:"delegate"`
	Slots               []int         `json:"slots,omitempty"`
	EndorsementPower    int           `json:"endorsement_power,omitempty"`    // v12+
	PreendorsementPower int           `json:"preendorsement_power,omitempty"` // v12+
}

func (m OperationMetadata) Power() int {
	return m.EndorsementPower + len(m.Slots)
}

// Address returns the delegate address for endorsements.
func (m OperationMetadata) Address() tezos.Address {
	return m.Delegate
}

func (m OperationMetadata) Balances() BalanceUpdates {
	return m.BalanceUpdates
}

// OperationResult contains receipts for executed operations, both success and failed.
// This type is a generic container for all possible results. Which fields are actually
// used depends on operation type and performed actions.
type OperationResult struct {
	Status              tezos.OpStatus       `json:"status"`
	BalanceUpdates      BalanceUpdates       `json:"balance_updates"` // burn, etc
	ConsumedGas         int64                `json:"consumed_gas,string"`
	Errors              []OperationError     `json:"errors,omitempty"`
	Allocated           bool                 `json:"allocated_destination_contract"` // tx only
	Storage             micheline.Prim       `json:"storage,omitempty"`              // tx, orig
	OriginatedContracts []tezos.Address      `json:"originated_contracts"`           // orig only
	StorageSize         int64                `json:"storage_size,string"`            // tx, orig, const
	PaidStorageSizeDiff int64                `json:"paid_storage_size_diff,string"`  // tx, orig
	BigmapDiff          micheline.BigmapDiff `json:"big_map_diff,omitempty"`         // tx, orig
	GlobalAddress       tezos.ExprHash       `json:"global_address"`                 // const
}

func (r OperationResult) Balances() BalanceUpdates {
	return r.BalanceUpdates
}

// Generic is the most generic operation type.
type Generic struct {
	OpKind tezos.OpType `json:"kind"`
}

// Manager represents data common for all manager operations.
type Manager struct {
	Generic
	Source       tezos.Address `json:"source"`
	Fee          int64         `json:"fee,string"`
	Counter      int64         `json:"counter,string"`
	GasLimit     int64         `json:"gas_limit,string"`
	StorageLimit int64         `json:"storage_limit,string"`
}

// Kind returns the operation's type. Implements TypedOperation interface.
func (e Generic) Kind() tezos.OpType {
	return e.OpKind
}

// Meta returns an empty operation metadata to implement TypedOperation interface.
func (e Generic) Meta() OperationMetadata {
	return OperationMetadata{}
}

// Result returns an empty operation result to implement TypedOperation interface.
func (e Generic) Result() OperationResult {
	return OperationResult{}
}

// Fees returns an empty balance update list to implement TypedOperation interface.
func (e Generic) Fees() BalanceUpdates {
	return nil
}

// OperationList is a slice of TypedOperation (interface type) with custom JSON unmarshaller
type OperationList []TypedOperation

// UnmarshalJSON implements json.Unmarshaler
func (e *OperationList) UnmarshalJSON(data []byte) error {
	if len(data) <= 2 {
		return nil
	}

	if data[0] != '[' {
		return fmt.Errorf("rpc: expected operation array")
	}

	// fmt.Printf("Decoding ops: %s\n", string(data))
	dec := json.NewDecoder(bytes.NewReader(data))

	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return fmt.Errorf("rpc: %v", err)
	}

	for dec.More() {
		// peek into `{"kind":"...",` field
		start := int(dec.InputOffset()) + 9
		// after first JSON object, decoder pos is at `,`
		if data[start] == '"' {
			start += 1
		}
		end := start + bytes.IndexByte(data[start:], '"')
		kind := tezos.ParseOpType(string(data[start:end]))
		var op TypedOperation
		switch kind {
		// anonymous operations
		case tezos.OpTypeActivateAccount:
			op = &Activation{}
		case tezos.OpTypeDoubleBakingEvidence:
			op = &DoubleBaking{}
		case tezos.OpTypeDoubleEndorsementEvidence,
			tezos.OpTypeDoublePreendorsementEvidence:
			op = &DoubleEndorsement{}
		case tezos.OpTypeSeedNonceRevelation:
			op = &SeedNonce{}

		// consensus operations
		case tezos.OpTypeEndorsement,
			tezos.OpTypeEndorsementWithSlot,
			tezos.OpTypePreendorsement:
			op = &Endorsement{}

		// amendment operations
		case tezos.OpTypeProposals:
			op = &Proposals{}
		case tezos.OpTypeBallot:
			op = &Ballot{}

		// manager operations
		case tezos.OpTypeTransaction:
			op = &Transaction{}
		case tezos.OpTypeOrigination:
			op = &Origination{}
		case tezos.OpTypeDelegation:
			op = &Delegation{}
		case tezos.OpTypeReveal:
			op = &Reveal{}
		case tezos.OpTypeRegisterConstant:
			op = &ConstantRegistration{}
		case tezos.OpTypeSetDepositsLimit:
			op = &SetDepositsLimit{}

		default:
			return fmt.Errorf("rpc: unsupported op %q", kind)
		}

		if err := dec.Decode(op); err != nil {
			return fmt.Errorf("rpc: operation kind %s: %w", kind, err)
		}
		(*e) = append(*e, op)
	}

	return nil
}
