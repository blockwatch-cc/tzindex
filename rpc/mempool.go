// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
)

// MempoolOperations represents mempool operations
type MempoolOperations struct {
	Applied       []*OperationHeader             `json:"applied"`
	Refused       []*OperationHeaderWithErrorAlt `json:"refused"`
	BranchRefused []*OperationHeaderWithErrorAlt `json:"branch_refused"`
	BranchDelayed []*OperationHeaderWithErrorAlt `json:"branch_delayed"`
	Unprocessed   []*OperationHeaderAlt          `json:"unprocessed"`
}

// GetMempoolPendingOperations returns mempool pending operations
func (c *Client) GetMempoolPendingOperations(ctx context.Context) (*MempoolOperations, error) {
	var ops MempoolOperations
	if err := c.Get(ctx, "chains/"+c.ChainID+"/mempool/pending_operations", &ops); err != nil {
		return nil, err
	}
	return &ops, nil
}

type OperationHeaderAlt OperationHeader

// UnmarshalJSON implements json.Unmarshaler
func (o *OperationHeaderAlt) UnmarshalJSON(data []byte) error {
	return unmarshalNamedJSONArray(data, &o.Hash, (*OperationHeader)(o))
}

// OperationHeaderWithError represents unsuccessful operation
type OperationHeaderWithError struct {
	OperationHeader
	Error Errors `json:"error"`
}

// OperationHeaderWithErrorAlt is a named array encoded OperationWithError with hash as a first array member.
// See OperationAltList for details
type OperationHeaderWithErrorAlt OperationHeaderWithError

// UnmarshalJSON implements json.Unmarshaler
func (o *OperationHeaderWithErrorAlt) UnmarshalJSON(data []byte) error {
	return unmarshalNamedJSONArray(data, &o.Hash, (*OperationHeaderWithError)(o))
}
