// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
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
