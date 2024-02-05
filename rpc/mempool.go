// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"encoding/json"
)

// Mempool represents mempool operations
type Mempool struct {
	Applied       []*Operation `json:"applied"`
	Refused       []*Operation `json:"refused"`
	Outdated      []*Operation `json:"outdated"` // v012+
	BranchRefused []*Operation `json:"branch_refused"`
	BranchDelayed []*Operation `json:"branch_delayed"`
	Unprocessed   []*Operation `json:"unprocessed"`
}

// GetMempool returns mempool pending operations
func (c *Client) GetMempool(ctx context.Context) (*Mempool, error) {
	var mem Mempool
	if err := c.Get(ctx, "chains/main/mempool/pending_operations", &mem); err != nil {
		return nil, err
	}
	return &mem, nil
}

type PendingOperation Operation

func (o *PendingOperation) UnmarshalJSON(data []byte) error {
	return unmarshalMultiTypeJSONArray(data, &o.Hash, (*Operation)(o))
}

func (o PendingOperation) MarshalJSON() ([]byte, error) {
	return marshalMultiTypeJSONArray(o.Hash, (Operation)(o))
}

func (m *Mempool) UnmarshalJSON(data []byte) error {
	type mempool struct {
		Applied       []*Operation        `json:"applied"`
		Refused       []*PendingOperation `json:"refused"`
		Outdated      []*PendingOperation `json:"outdated"`
		BranchRefused []*PendingOperation `json:"branch_refused"`
		BranchDelayed []*PendingOperation `json:"branch_delayed"`
		Unprocessed   []*PendingOperation `json:"unprocessed"`
	}
	mp := mempool{}
	if err := json.Unmarshal(data, &mp); err != nil {
		return err
	}
	// applied is the correct type
	m.Applied = mp.Applied
	mp.Applied = mp.Applied[:0]

	// type-convert the rest
	type convert struct {
		A *[]*PendingOperation
		B *[]*Operation
	}
	for _, v := range []convert{
		{&mp.Refused, &m.Refused},
		{&mp.Outdated, &m.Outdated},
		{&mp.BranchRefused, &m.BranchRefused},
		{&mp.BranchDelayed, &m.BranchDelayed},
		{&mp.Unprocessed, &m.Unprocessed},
	} {
		if l := len(*v.A); l > 0 {
			*v.B = make([]*Operation, l)
			for i := range *v.A {
				(*v.B)[i] = (*Operation)((*v.A)[i])
			}
			*v.A = (*v.A)[:0]
		}
	}
	return nil
}
