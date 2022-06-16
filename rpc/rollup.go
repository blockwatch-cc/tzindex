// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
    "encoding/json"

    "blockwatch.cc/tzgo/micheline"
    "blockwatch.cc/tzgo/tezos"
)

// Ensure Rollup implements the TypedOperation interface.
var _ TypedOperation = (*Rollup)(nil)

// Rollup represents any kind of rollup operation
type Rollup struct {
    // common
    Manager

    // most rollup ops
    Rollup tezos.Address `json:"rollup"`

    // tx_rollup_origination has no data

    // transfer_ticket contents
    Transfer TransferTicket `json:"-"`

    // tx_rollup_submit_batch
    Batch RollupBatch `json:"-"`

    // tx_rollup_rejection
    Reject RollupRejection `json:"-"`

    // tx_rollup_dispatch_tickets
    Dispatch RollupDispatch `json:"-"`

    // tx_rollup_commit
    Commit RollupCommit `json:"commitment"`

    // // sc_rollup_originate
    // Kind       json.RawMessage `json:"kind"`
    // BootSector json.RawMessage `json:"boot_sector"`
}

func (r *Rollup) UnmarshalJSON(data []byte) error {
    type alias *Rollup
    if err := json.Unmarshal(data, alias(r)); err != nil {
        return err
    }
    switch r.Kind() {
    case tezos.OpTypeTransferTicket:
        return json.Unmarshal(data, &r.Transfer)
    case tezos.OpTypeToruSubmitBatch:
        return json.Unmarshal(data, &r.Batch)
    case tezos.OpTypeToruRejection:
        return json.Unmarshal(data, &r.Reject)
    case tezos.OpTypeToruDispatchTickets:
        return json.Unmarshal(data, &r.Dispatch)
    }
    return nil
}

func (r *Rollup) Target() tezos.Address {
    if r.Transfer.Destination.IsValid() {
        return r.Transfer.Destination
    }
    if r.Dispatch.TxRollup.IsValid() {
        return r.Dispatch.TxRollup
    }
    return r.Rollup
}

func (r *Rollup) EncodeParameters() micheline.Prim {
    switch r.Kind() {
    case tezos.OpTypeTransferTicket:
        return r.Transfer.Prim()
    case tezos.OpTypeToruSubmitBatch:
        return r.Batch.Prim()
    case tezos.OpTypeToruCommit:
        return r.Commit.Prim()
    case tezos.OpTypeToruReturnBond:
        // no data
        return micheline.InvalidPrim
    case tezos.OpTypeToruFinalizeCommitment,
        tezos.OpTypeToruRemoveCommitment:
        // level in receipt
        return micheline.NewInt64(r.Metadata.Level)
    case tezos.OpTypeToruRejection:
        return r.Reject.Prim()
    case tezos.OpTypeToruDispatchTickets:
        return r.Dispatch.Prim()
    default:
        // not supported
        return micheline.InvalidPrim
    }
}

type RollupBatch struct {
    Content tezos.HexBytes `json:"content"`
    // BurnLimit int64          `json:"burn_limit,string,omitempty"`
}

func DecodeRollupBatch(data []byte) (*RollupBatch, error) {
    var p micheline.Prim
    if err := p.UnmarshalBinary(data); err != nil {
        return nil, err
    }
    return &RollupBatch{Content: tezos.HexBytes(p.Bytes)}, nil
}

func (b RollupBatch) Prim() micheline.Prim {
    return micheline.NewBytes(b.Content)
}

func (b RollupBatch) MarshalJSON() ([]byte, error) {
    type alias RollupBatch
    return json.Marshal(alias(b))
}

type TransferTicket struct {
    Destination tezos.Address  `json:"destination"`
    Entrypoint  string         `json:"entrypoint"`
    Type        micheline.Prim `json:"ticket_ty"`
    Contents    micheline.Prim `json:"ticket_contents"`
    Ticketer    tezos.Address  `json:"ticket_ticketer"`
    Amount      tezos.Z        `json:"ticket_amount"`
}

func DecodeTransferTicket(data []byte) (*TransferTicket, error) {
    var p micheline.Prim
    if err := p.UnmarshalBinary(data); err != nil {
        return nil, err
    }
    return &TransferTicket{
        Type:     p.Args[0],
        Contents: p.Args[1].Args[1].Args[0],
        Ticketer: tezos.NewAddress(tezos.AddressTypeContract, p.Args[1].Args[0].Bytes),
        Amount:   tezos.Z(*p.Args[1].Args[1].Args[1].Int),
    }, nil
}

func (t TransferTicket) Prim() micheline.Prim {
    return micheline.NewPair(
        micheline.TicketType(t.Type).Prim,
        micheline.TicketValue(t.Contents, t.Ticketer, t.Amount),
    )
}

func (t TransferTicket) MarshalJSON() ([]byte, error) {
    val := micheline.NewValue(
        micheline.TicketType(t.Type),
        micheline.TicketValue(t.Contents, t.Ticketer, t.Amount),
    )
    return val.MarshalJSON()
}

type RollupCommit struct {
    Level           int64        `json:"level"`
    Messages        []tezos.Hash `json:"messages"`
    Predecessor     *tezos.Hash  `json:"predecessor,omitempty"`
    InboxMerkleRoot tezos.Hash   `json:"inbox_merkle_root"`
}

func DecodeRollupCommit(data []byte) (*RollupCommit, error) {
    var p micheline.Prim
    if err := p.UnmarshalBinary(data); err != nil {
        return nil, err
    }
    c := &RollupCommit{
        Level:           p.Args[0].Int.Int64(),
        InboxMerkleRoot: tezos.NewHash(tezos.HashTypeToruInbox, p.Args[3].Bytes),
    }
    for _, v := range p.Args[1].Args {
        c.Messages = append(c.Messages, tezos.NewHash(tezos.HashTypeToruMessageResult, v.Bytes))
    }
    if p.Args[2].OpCode != micheline.D_NONE {
        pre := tezos.NewHash(tezos.HashTypeToruCommitment, p.Args[2].Bytes)
        c.Predecessor = &pre
    }
    return c, nil
}

func (c RollupCommit) Prim() micheline.Prim {
    prim := micheline.NewCombPair(
        micheline.NewInt64(c.Level),
        micheline.NewSeq(),
        micheline.NewCode(micheline.D_NONE),
        micheline.NewBytes(c.InboxMerkleRoot.Hash),
    )
    for _, v := range c.Messages {
        prim.Args[1].Args = append(prim.Args[1].Args, micheline.NewBytes(v.Hash))
    }
    if c.Predecessor != nil {
        prim.Args[2] = micheline.NewBytes(c.Predecessor.Hash)
    }
    return prim
}

func (c RollupCommit) MarshalJSON() ([]byte, error) {
    type alias RollupCommit
    return json.Marshal(alias(c))
}

type RollupRejection struct {
    Level                     int64           `json:"level"`
    Message                   json.RawMessage `json:"commitment,omitempty"`
    MessagePosition           tezos.Z         `json:"message_position"`
    MessagePath               []tezos.Hash    `json:"message_path,omitempty"`
    MessageResultHash         tezos.Hash      `json:"message_result_hash"`
    MessageResultPath         []tezos.Hash    `json:"message_result_path,omitempty"`
    PreviousMessageResult     json.RawMessage `json:"previous_message_result,omitempty"`
    PreviousMessageResultPath []tezos.Hash    `json:"previous_message_result_path,omitempty"`
    Proof                     json.RawMessage `json:"proof,omitempty"`
}

func (r RollupRejection) Prim() micheline.Prim {
    buf, _ := json.Marshal(r)
    return micheline.NewBytes(buf)
}

func (r RollupRejection) MarshalJSON() ([]byte, error) {
    return json.Marshal(r)
}

func DecodeRollupRejection(data []byte) (*RollupRejection, error) {
    var p micheline.Prim
    if err := p.UnmarshalBinary(data); err != nil {
        return nil, err
    }
    r := &RollupRejection{}
    if err := json.Unmarshal(p.Bytes, r); err != nil {
        return nil, err
    }
    return r, nil
}

type RollupDispatch struct {
    Level        int64           `json:"level"`
    TxRollup     tezos.Address   `json:"tx_rollup"`
    ContextHash  tezos.Hash      `json:"context_hash"`
    MessageIndex int64           `json:"message_index"`
    TicketsInfo  json.RawMessage `json:"tickets_info"`
}

func (r RollupDispatch) Prim() micheline.Prim {
    buf, _ := json.Marshal(r)
    return micheline.NewBytes(buf)
}

func (r RollupDispatch) MarshalJSON() ([]byte, error) {
    type alias RollupDispatch
    return json.Marshal(alias(r))
}

func DecodeRollupDispatch(data []byte) (*RollupDispatch, error) {
    var p micheline.Prim
    if err := p.UnmarshalBinary(data); err != nil {
        return nil, err
    }
    r := &RollupDispatch{}
    if err := json.Unmarshal(p.Bytes, r); err != nil {
        return nil, err
    }
    return r, nil
}
