// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Ensure TxRollup implements the TypedOperation interface.
var _ TypedOperation = (*TxRollup)(nil)

// TxRollup represents any kind of rollup operation
type TxRollup struct {
	// common
	Manager

	// most rollup ops
	Rollup tezos.Address `json:"rollup"`

	// tx_rollup_origination has no data

	// tx_rollup_submit_batch
	Batch TxRollupBatch `json:"-"`

	// tx_rollup_rejection
	Reject TxRollupRejection `json:"-"`

	// tx_rollup_dispatch_tickets
	Dispatch TxRollupDispatch `json:"-"`

	// tx_rollup_commit
	Commit TxRollupCommit `json:"commitment"`
}

type TxRollupResult struct {
	OriginatedRollup tezos.Address `json:"originated_rollup"` // v013 tx_rollup_originate
	Level            int64         `json:"level"`             // v013 ?? here or in metadata??
}

// Addresses adds all addresses used in this operation to the set.
// Implements TypedOperation interface.
func (t TxRollup) Addresses(set *tezos.AddressSet) {
	set.AddUnique(t.Source)
	if a := t.Target(); a.IsValid() {
		set.AddUnique(a)
	}
	if t.Result().OriginatedRollup.IsValid() {
		set.AddUnique(t.Result().OriginatedRollup)
	}
}

func (r *TxRollup) UnmarshalJSON(data []byte) error {
	type alias *TxRollup
	if err := json.Unmarshal(data, alias(r)); err != nil {
		return err
	}
	switch r.Kind() {
	case tezos.OpTypeTxRollupSubmitBatch:
		return json.Unmarshal(data, &r.Batch)
	case tezos.OpTypeTxRollupRejection:
		return json.Unmarshal(data, &r.Reject)
	case tezos.OpTypeTxRollupDispatchTickets:
		return json.Unmarshal(data, &r.Dispatch)
	}
	return nil
}

func (r *TxRollup) Target() tezos.Address {
	if r.Dispatch.TxRollup.IsValid() {
		return r.Dispatch.TxRollup
	}
	return r.Rollup
}

func (r *TxRollup) EncodeParameters() micheline.Parameters {
	params := micheline.Parameters{
		Entrypoint: r.Kind().String(),
	}
	switch r.Kind() {
	case tezos.OpTypeTxRollupSubmitBatch:
		params.Value = r.Batch.Prim()
	case tezos.OpTypeTxRollupCommit:
		params.Value = r.Commit.Prim()
	case tezos.OpTypeTxRollupReturnBond:
		// no data
		params.Value = micheline.InvalidPrim
	case tezos.OpTypeTxRollupFinalizeCommitment,
		tezos.OpTypeTxRollupRemoveCommitment:
		// level in receipt
		params.Value = micheline.NewInt64(r.Metadata.Result.Level)
	case tezos.OpTypeTxRollupRejection:
		params.Value = r.Reject.Prim()
	case tezos.OpTypeTxRollupDispatchTickets:
		params.Value = r.Dispatch.Prim()
	}
	return params
}

type TxRollupBatch struct {
	Content tezos.HexBytes `json:"content"`
	// BurnLimit int64          `json:"burn_limit,string,omitempty"`
}

func DecodeTxRollupBatch(data []byte) (*TxRollupBatch, error) {
	var p micheline.Parameters
	if err := p.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &TxRollupBatch{Content: tezos.HexBytes(p.Value.Bytes)}, nil
}

func (b TxRollupBatch) Prim() micheline.Prim {
	return micheline.NewBytes(b.Content)
}

func (b TxRollupBatch) MarshalJSON() ([]byte, error) {
	type alias TxRollupBatch
	return json.Marshal(alias(b))
}

type TxRollupCommit struct {
	Level           int64    `json:"level"`
	Messages        []string `json:"messages"`
	Predecessor     string   `json:"predecessor,omitempty"`
	InboxMerkleRoot string   `json:"inbox_merkle_root"`
}

func DecodeTxRollupCommit(data []byte) (*TxRollupCommit, error) {
	var p micheline.Parameters
	if err := p.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	c := &TxRollupCommit{
		Level:           p.Value.Args[0].Int.Int64(),
		InboxMerkleRoot: p.Value.Args[3].String,
	}
	for _, v := range p.Value.Args[1].Args {
		c.Messages = append(c.Messages, v.String)
	}
	if p.Value.Args[2].OpCode != micheline.D_NONE {
		c.Predecessor = p.Value.Args[2].String
	}
	return c, nil
}

func (c TxRollupCommit) Prim() micheline.Prim {
	prim := micheline.NewCombPair(
		micheline.NewInt64(c.Level),
		micheline.NewSeq(),
		micheline.NewCode(micheline.D_NONE),
		micheline.NewString(c.InboxMerkleRoot),
	)
	for _, v := range c.Messages {
		prim.Args[1].Args = append(prim.Args[1].Args, micheline.NewString(v))
	}
	if c.Predecessor != "" {
		prim.Args[2] = micheline.NewString(c.Predecessor)
	}
	return prim
}

func (c TxRollupCommit) MarshalJSON() ([]byte, error) {
	type alias TxRollupCommit
	return json.Marshal(alias(c))
}

type TxRollupRejection struct {
	Level                     int64           `json:"level"`
	Message                   json.RawMessage `json:"message,omitempty"`
	MessagePosition           tezos.Z         `json:"message_position"`
	MessagePath               []string        `json:"message_path,omitempty"`
	MessageResultHash         string          `json:"message_result_hash"`
	MessageResultPath         []string        `json:"message_result_path,omitempty"`
	PreviousMessageResult     json.RawMessage `json:"previous_message_result,omitempty"`
	PreviousMessageResultPath []string        `json:"previous_message_result_path,omitempty"`
	Proof                     json.RawMessage `json:"proof,omitempty"`
}

func (r TxRollupRejection) Prim() micheline.Prim {
	buf, _ := json.Marshal(r)
	return micheline.NewBytes(buf)
}

func (r TxRollupRejection) MarshalJSON() ([]byte, error) {
	type alias TxRollupRejection
	return json.Marshal(alias(r))
}

func DecodeTxRollupRejection(data []byte) (*TxRollupRejection, error) {
	var p micheline.Parameters
	if err := p.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	r := &TxRollupRejection{}
	if err := json.Unmarshal(p.Value.Bytes, r); err != nil {
		return nil, err
	}
	return r, nil
}

type TxRollupDispatch struct {
	Level        int64           `json:"level"`
	TxRollup     tezos.Address   `json:"tx_rollup"`
	ContextHash  string          `json:"context_hash"`
	MessageIndex int64           `json:"message_index"`
	TicketsInfo  json.RawMessage `json:"tickets_info"`
}

func (r TxRollupDispatch) Prim() micheline.Prim {
	buf, _ := json.Marshal(r)
	return micheline.NewBytes(buf)
}

func (r TxRollupDispatch) MarshalJSON() ([]byte, error) {
	type alias TxRollupDispatch
	return json.Marshal(alias(r))
}

func DecodeTxRollupDispatch(data []byte) (*TxRollupDispatch, error) {
	var p micheline.Parameters
	if err := p.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	r := &TxRollupDispatch{}
	if err := json.Unmarshal(p.Value.Bytes, r); err != nil {
		return nil, err
	}
	return r, nil
}
