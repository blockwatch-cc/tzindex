// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure SmartRollup types implement the TypedOperation interface.
var (
	_ TypedOperation = (*SmartRollupOriginate)(nil)
	_ TypedOperation = (*SmartRollupAddMessages)(nil)
	_ TypedOperation = (*SmartRollupCement)(nil)
	_ TypedOperation = (*SmartRollupPublish)(nil)
	_ TypedOperation = (*SmartRollupRefute)(nil)
	_ TypedOperation = (*SmartRollupTimeout)(nil)
	_ TypedOperation = (*SmartRollupExecuteOutboxMessage)(nil)
	_ TypedOperation = (*SmartRollupRecoverBond)(nil)
)

type SmartRollupResult struct {
	Address          *tezos.Address               `json:"address,omitempty"`            // v016, smart_rollup_originate
	Size             *tezos.Z                     `json:"size,omitempty"`               // v016, smart_rollup_originate
	InboxLevel       int64                        `json:"inbox_level,omitempty"`        // v016, smart_rollup_cement
	StakedHash       *tezos.SmartRollupCommitHash `json:"staked_hash,omitempty"`        // v016, smart_rollup_publish
	PublishedAtLevel int64                        `json:"published_at_level,omitempty"` // v016, smart_rollup_publish
	GameStatus       *GameStatus                  `json:"game_status,omitempty"`        // v016, smart_rollup_refute, smart_rollup_timeout
	Commitment       *tezos.SmartRollupCommitHash `json:"commitment_hash,omitempty"`    // v017, smart_rollup_cement
}

func (r SmartRollupResult) Encode() []byte {
	buf, _ := json.Marshal(r)
	return buf
}

type SmartRollupOriginate struct {
	Manager
	PvmKind          tezos.PvmKind  `json:"pvm_kind"`
	Kernel           tezos.HexBytes `json:"kernel"`
	OriginationProof tezos.HexBytes `json:"origination_proof"`
	ParametersTy     micheline.Prim `json:"parameters_ty"`
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupOriginate) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(*o.Result().Address)
}

func (o SmartRollupOriginate) Encode() []byte {
	type alias struct {
		PvmKind          tezos.PvmKind  `json:"pvm_kind"`
		Kernel           tezos.HexBytes `json:"kernel"`
		OriginationProof tezos.HexBytes `json:"origination_proof"`
		ParametersTy     micheline.Prim `json:"parameters_ty"`
	}
	a := alias{
		PvmKind:          o.PvmKind,
		Kernel:           o.Kernel,
		OriginationProof: o.OriginationProof,
		ParametersTy:     o.ParametersTy,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupAddMessages struct {
	Manager
	Messages []tezos.HexBytes `json:"message"`
}

func (o SmartRollupAddMessages) Encode() []byte {
	type alias struct {
		Messages []tezos.HexBytes `json:"message"`
	}
	a := alias{
		Messages: o.Messages,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupCement struct {
	Manager
	Rollup     tezos.Address                `json:"rollup"`
	Commitment *tezos.SmartRollupCommitHash `json:"commitment,omitempty"` // deprecated in v17
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupCement) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(o.Rollup)
}

func (o SmartRollupCement) Encode() []byte {
	type alias struct {
		Commitment *tezos.SmartRollupCommitHash `json:"commitment,omitempty"`
	}
	a := alias{
		Commitment: o.Commitment,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupCommitment struct {
	CompressedState tezos.SmartRollupStateHash  `json:"compressed_state"`
	InboxLevel      int64                       `json:"inbox_level"`
	Predecessor     tezos.SmartRollupCommitHash `json:"predecessor"`
	NumberOfTicks   tezos.Z                     `json:"number_of_ticks"`
}

type SmartRollupPublish struct {
	Manager
	Rollup     tezos.Address         `json:"rollup"`
	Commitment SmartRollupCommitment `json:"commitment"`
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupPublish) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(o.Rollup)
}

func (o SmartRollupPublish) Encode() []byte {
	type alias struct {
		Commitment SmartRollupCommitment `json:"commitment"`
	}
	a := alias{
		Commitment: o.Commitment,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupRefute struct {
	Manager
	Rollup     tezos.Address         `json:"rollup"`
	Opponent   tezos.Address         `json:"opponent"`
	Refutation SmartRollupRefutation `json:"refutation"`
}

type SmartRollupRefutation struct {
	Kind         string                       `json:"refutation_kind"`
	PlayerHash   *tezos.SmartRollupCommitHash `json:"player_commitment_hash,omitempty"`
	OpponentHash *tezos.SmartRollupCommitHash `json:"opponent_commitment_hash,omitempty"`
	Choice       *tezos.Z                     `json:"choice,omitempty"`
	Step         *SmartRollupRefuteStep       `json:"step,omitempty"`
}

// Step can either be
//
// - []SmartRollupTick
// - SmartRollupInputProof
// - smth else?
//
// There is no indication in the outer parts of the refutation struct that
// suggests how to decode this.
type SmartRollupRefuteStep struct {
	Ticks []SmartRollupTick
	Proof *SmartRollupProof
}

type SmartRollupProof struct {
	PvmStep    tezos.HexBytes         `json:"pvm_step,omitempty"`
	InputProof *SmartRollupInputProof `json:"input_proof,omitempty"`
}

func (s *SmartRollupRefuteStep) UnmarshalJSON(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	switch buf[0] {
	case '[':
		s.Ticks = make([]SmartRollupTick, 0)
		return json.Unmarshal(buf, &s.Ticks)
	case '{':
		s.Proof = &SmartRollupProof{}
		return json.Unmarshal(buf, s.Proof)
	default:
		return fmt.Errorf("Invalid refute step data %q", string(buf))
	}
}

func (s SmartRollupRefuteStep) MarshalJSON() ([]byte, error) {
	if s.Ticks != nil {
		return json.Marshal(s.Ticks)
	}
	if s.Proof != nil {
		return json.Marshal(s.Proof)
	}
	return nil, nil
}

type SmartRollupTick struct {
	State tezos.SmartRollupStateHash `json:"state"`
	Tick  tezos.Z                    `json:"tick"`
}

type SmartRollupInputProof struct {
	Kind    string         `json:"input_proof_kind"`
	Level   int64          `json:"level"`
	Counter tezos.Z        `json:"message_counter"`
	Proof   tezos.HexBytes `json:"serialized_proof"`
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupRefute) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(o.Rollup)
	set.AddUnique(o.Opponent)
}

func (o SmartRollupRefute) Encode() []byte {
	type alias struct {
		Opponent   tezos.Address         `json:"opponent"`
		Refutation SmartRollupRefutation `json:"refutation"`
	}
	a := alias{
		Opponent:   o.Opponent,
		Refutation: o.Refutation,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupTimeout struct {
	Manager
	Rollup  tezos.Address `json:"rollup"`
	Stakers struct {
		Alice tezos.Address `json:"alice"`
		Bob   tezos.Address `json:"bob"`
	} `json:"stakers"`
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupTimeout) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(o.Rollup)
	set.AddUnique(o.Stakers.Alice)
	set.AddUnique(o.Stakers.Bob)
}

func (o SmartRollupTimeout) Encode() []byte {
	type alias struct {
		Stakers struct {
			Alice tezos.Address `json:"alice"`
			Bob   tezos.Address `json:"bob"`
		} `json:"stakers"`
	}
	a := alias{
		Stakers: o.Stakers,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupExecuteOutboxMessage struct {
	Manager
	Rollup             tezos.Address               `json:"rollup"`
	CementedCommitment tezos.SmartRollupCommitHash `json:"cemented_commitment"`
	OutputProof        tezos.HexBytes              `json:"output_proof"`
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupExecuteOutboxMessage) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(o.Rollup)
}

func (o SmartRollupExecuteOutboxMessage) Encode() []byte {
	type alias struct {
		CementedCommitment tezos.SmartRollupCommitHash `json:"cemented_commitment"`
		OutputProof        tezos.HexBytes              `json:"output_proof"`
	}
	a := alias{
		CementedCommitment: o.CementedCommitment,
		OutputProof:        o.OutputProof,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type SmartRollupRecoverBond struct {
	Manager
	Rollup tezos.Address `json:"rollup"`
	Staker tezos.Address `json:"staker"`
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (o SmartRollupRecoverBond) Addresses(set *tezos.AddressSet) {
	set.AddUnique(o.Source)
	set.AddUnique(o.Rollup)
	set.AddUnique(o.Staker)
}

func (o SmartRollupRecoverBond) Encode() []byte {
	type alias struct {
		Rollup tezos.Address `json:"rollup"`
		Staker tezos.Address `json:"staker"`
	}
	a := alias{
		Rollup: o.Rollup,
		Staker: o.Staker,
	}
	buf, _ := json.Marshal(a)
	return buf
}

type GameStatus struct {
	Status string         `json:"status,omitempty"`
	Kind   string         `json:"kind,omitempty"`
	Reason string         `json:"reason,omitempty"`
	Player *tezos.Address `json:"player,omitempty"`
}

func (s *GameStatus) UnmarshalJSON(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	switch buf[0] {
	case '"':
		s.Status = string(buf[1 : len(buf)-1])
	case '{':
		type alias *GameStatus
		type wrapper struct {
			S alias `json:"result"`
		}
		a := wrapper{alias(s)}
		_ = json.Unmarshal(buf, &a)
	default:
		return fmt.Errorf("Invalid game status data %q", string(buf))
	}
	return nil
}
