// Copyright (c) 2020-2024 Blockwatch Data Inc.
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

// Addresses lists all Tezos addresses that appear in this operation group. This does
// not include addresses used in contract call parameters, storage updates and tickets.
func (o Operation) Addresses() *tezos.AddressSet {
	set := tezos.NewAddressSet()
	for _, v := range o.Contents {
		v.Addresses(set)
	}
	return set
}

func (o Operation) IsSuccess() bool {
	if len(o.Contents) == 0 {
		return false
	}
	return o.Contents[0].Result().IsSuccess()
}

// TypedOperation must be implemented by all operations
type TypedOperation interface {
	Kind() tezos.OpType
	Meta() OperationMetadata
	Result() OperationResult
	Fees() BalanceUpdates
	Addresses(*tezos.AddressSet)
}

// OperationError represents data describing an error conditon that lead to a
// failed operation execution.
type OperationError struct {
	NodeError
	// whitelist commonly useful error contents, avoid storing large scripts, etc
	Contract       string          `json:"contract,omitempty"`
	ContractHandle string          `json:"contract_handle,omitempty"`
	BigMap         int64           `json:"big_map,omitempty"`
	Identifier     string          `json:"identifier,omitempty"`
	Location       int64           `json:"location,omitempty"`
	Loc            int64           `json:"loc,omitempty"`
	With           *micheline.Prim `json:"with,omitempty"`
	Amount         string          `json:"amount,omitempty"`
	Balance        string          `json:"balance,omitempty"`
}

// OperationMetadata contains execution receipts for successful and failed
// operations.
type OperationMetadata struct {
	BalanceUpdates BalanceUpdates  `json:"balance_updates,omitempty"` // fee-related
	Result         OperationResult `json:"operation_result"`

	// transaction only
	InternalResults []*InternalResult `json:"internal_operation_results,omitempty"`

	// endorsement only
	Delegate            tezos.Address `json:"delegate"`
	Slots               []int         `json:"slots,omitempty"`                // < v12
	EndorsementPower    int           `json:"endorsement_power,omitempty"`    // v12+
	PreendorsementPower int           `json:"preendorsement_power,omitempty"` // v12+
	ConsensusPower      int           `json:"consensus_power,omitempty"`      // v18+
}

func (m OperationMetadata) Power() int {
	// only one of these fields is used per operation depending on protocol
	return m.ConsensusPower + // v18+
		m.EndorsementPower + // v12+
		m.PreendorsementPower + // v12+
		len(m.Slots) // v0+
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
	Status               tezos.OpStatus   `json:"status"`
	BalanceUpdates       BalanceUpdates   `json:"balance_updates"` // burn, etc
	ConsumedGas          int64            `json:"consumed_gas,string"`
	ConsumedMilliGas     int64            `json:"consumed_milligas,string"` // v007+
	Errors               []OperationError `json:"errors,omitempty"`
	Allocated            bool             `json:"allocated_destination_contract"` // tx only
	Storage              micheline.Prim   `json:"storage,omitempty"`              // tx, orig
	OriginatedContracts  []tezos.Address  `json:"originated_contracts"`           // orig only
	StorageSize          int64            `json:"storage_size,string"`            // tx, orig, const
	PaidStorageSizeDiff  int64            `json:"paid_storage_size_diff,string"`  // tx, orig
	BigmapDiff           json.RawMessage  `json:"big_map_diff,omitempty"`         // tx, orig, <v013
	LazyStorageDiff      json.RawMessage  `json:"lazy_storage_diff,omitempty"`    // v008+ tx, orig
	GlobalAddress        tezos.ExprHash   `json:"global_address"`                 // global constant
	TicketUpdatesCorrect []TicketUpdate   `json:"ticket_updates"`                 // v015, correct name on external
	TicketReceipts       []TicketUpdate   `json:"ticket_receipt"`                 // v015, incorrect name on internal

	// v013 tx rollup
	TxRollupResult

	// v016 smart rollup
	SmartRollupResult
}

// Always use this helper to retrieve Ticket updates. This is because due to
// lack of quality control Tezos Lima protocol ended up with 2 distinct names
// for ticket updates in external call receipts versus internal call receipts.
func (r OperationResult) TicketUpdates() []TicketUpdate {
	if len(r.TicketUpdatesCorrect) > 0 {
		return r.TicketUpdatesCorrect
	}
	return r.TicketReceipts
}

func (r OperationResult) BigmapEvents() micheline.BigmapEvents {
	if r.LazyStorageDiff != nil {
		res := make(micheline.LazyEvents, 0)
		if err := json.Unmarshal(r.LazyStorageDiff, &res); err != nil {
			log.Debugf("rpc: lazy decode: %v", err)
		}
		return res.BigmapEvents()
	}
	if r.BigmapDiff != nil {
		res := make(micheline.BigmapEvents, 0)
		if err := json.Unmarshal(r.BigmapDiff, &res); err != nil {
			log.Debugf("rpc: bigmap decode: %v", err)
		}
		return res
	}
	return nil
}

func (r OperationResult) Balances() BalanceUpdates {
	return r.BalanceUpdates
}

func (r OperationResult) IsSuccess() bool {
	return r.Status == tezos.OpStatusApplied
}

func (r OperationResult) Gas() int64 {
	if r.ConsumedMilliGas > 0 {
		var corr int64
		if r.ConsumedMilliGas%1000 > 0 {
			corr++
		}
		return r.ConsumedMilliGas/1000 + corr
	}
	return r.ConsumedGas
}

func (r OperationResult) MilliGas() int64 {
	if r.ConsumedMilliGas > 0 {
		return r.ConsumedMilliGas
	}
	return r.ConsumedGas * 1000
}

// Generic is the most generic operation type.
type Generic struct {
	OpKind   tezos.OpType       `json:"kind"`
	Metadata *OperationMetadata `json:"metadata,omitempty"`
}

// Kind returns the operation's type. Implements TypedOperation interface.
func (e Generic) Kind() tezos.OpType {
	return e.OpKind
}

// Meta returns an empty operation metadata to implement TypedOperation interface.
func (e Generic) Meta() OperationMetadata {
	return *e.Metadata
}

// Result returns an empty operation result to implement TypedOperation interface.
func (e Generic) Result() OperationResult {
	return e.Metadata.Result
}

// Fees returns an empty balance update list to implement TypedOperation interface.
func (e Generic) Fees() BalanceUpdates {
	return e.Metadata.BalanceUpdates
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (e Generic) Addresses(set *tezos.AddressSet) {
	// noop
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

// Limits returns manager operation limits to implement TypedOperation interface.
func (e Manager) Limits() tezos.Limits {
	return tezos.Limits{
		Fee:          e.Fee,
		GasLimit:     e.GasLimit,
		StorageLimit: e.StorageLimit,
	}
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (e Manager) Addresses(set *tezos.AddressSet) {
	set.AddUnique(e.Source)
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
		case tezos.OpTypeDrainDelegate:
			op = &DrainDelegate{}

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
		case tezos.OpTypeIncreasePaidStorage:
			op = &IncreasePaidStorage{}
		case tezos.OpTypeVdfRevelation:
			op = &VdfRevelation{}
		case tezos.OpTypeTransferTicket:
			op = &TransferTicket{}
		case tezos.OpTypeUpdateConsensusKey:
			op = &UpdateConsensusKey{}

			// DEPRECATED: tx rollup operations, kept for testnet backward compatibility
		case tezos.OpTypeTxRollupOrigination,
			tezos.OpTypeTxRollupSubmitBatch,
			tezos.OpTypeTxRollupCommit,
			tezos.OpTypeTxRollupReturnBond,
			tezos.OpTypeTxRollupFinalizeCommitment,
			tezos.OpTypeTxRollupRemoveCommitment,
			tezos.OpTypeTxRollupRejection,
			tezos.OpTypeTxRollupDispatchTickets:
			op = &TxRollup{}

		case tezos.OpTypeSmartRollupOriginate:
			op = &SmartRollupOriginate{}
		case tezos.OpTypeSmartRollupAddMessages:
			op = &SmartRollupAddMessages{}
		case tezos.OpTypeSmartRollupCement:
			op = &SmartRollupCement{}
		case tezos.OpTypeSmartRollupPublish:
			op = &SmartRollupPublish{}
		case tezos.OpTypeSmartRollupRefute:
			op = &SmartRollupRefute{}
		case tezos.OpTypeSmartRollupTimeout:
			op = &SmartRollupTimeout{}
		case tezos.OpTypeSmartRollupExecuteOutboxMessage:
			op = &SmartRollupExecuteOutboxMessage{}
		case tezos.OpTypeSmartRollupRecoverBond:
			op = &SmartRollupRecoverBond{}
		case tezos.OpTypeDalAttestation:
			op = &DalAttestation{}
		case tezos.OpTypeDalPublishSlotHeader:
			op = &DalPublishSlotHeader{}

		default:
			return fmt.Errorf("rpc: unsupported op %q", string(data[start:end]))
		}

		if err := dec.Decode(op); err != nil {
			return fmt.Errorf("rpc: operation kind %s: %w", kind, err)
		}
		(*e) = append(*e, op)
	}

	return nil
}
