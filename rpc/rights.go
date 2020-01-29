// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"time"

	"blockwatch.cc/tzindex/chain"
)

// BakingRight holds information about the right to bake a specific Tezos block
type BakingRight struct {
	Delegate      chain.Address `json:"delegate"`
	Level         int64         `json:"level"`
	Priority      int           `json:"priority"`
	EstimatedTime time.Time     `json:"estimated_time"`
}

// EndorsingRight holds information about the right to endorse a specific Tezos block
type EndorsingRight struct {
	Delegate      chain.Address `json:"delegate"`
	Level         int64         `json:"level"`
	EstimatedTime time.Time     `json:"estimated_time"`
	Slots         []int         `json:"slots"`
}

type SnapshotIndex struct {
	LastRoll     []string `json:"last_roll"`
	Nonces       []string `json:"nonces"`
	RandomSeed   string   `json:"random_seed"`
	RollSnapshot int64    `json:"roll_snapshot"`
	Cycle        int64    `json:"cycle"`
}

// GetBakingRights returns information about a Tezos block baking rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-baking-rights
func (c *Client) GetBakingRights(ctx context.Context, blockID chain.BlockHash) ([]BakingRight, error) {
	rights := make([]BakingRight, 0, 64)
	u := fmt.Sprintf("chains/%s/blocks/%s/helpers/baking_rights?all", c.ChainID, blockID)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetBakingRightsHeight returns information about a Tezos block baking rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-baking-rights
func (c *Client) GetBakingRightsHeight(ctx context.Context, height int64) ([]BakingRight, error) {
	rights := make([]BakingRight, 0, 64)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/baking_rights?all", c.ChainID, height)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetBakingRightsCycle returns information about a Tezos baking rights for an entire cycle.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-baking-rights
func (c *Client) GetBakingRightsCycle(ctx context.Context, height, cycle int64) ([]BakingRight, error) {
	rights := make([]BakingRight, 0, 64*4096)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/baking_rights?all&cycle=%d", c.ChainID, height, cycle)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetEndorsingRights returns information about a Tezos block endorsing rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetEndorsingRights(ctx context.Context, blockID chain.BlockHash) ([]EndorsingRight, error) {
	rights := make([]EndorsingRight, 0, 32)
	u := fmt.Sprintf("chains/%s/blocks/%s/helpers/endorsing_rights?all", c.ChainID, blockID)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetEndorsingRightsHeight returns information about a Tezos block endorsing rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetEndorsingRightsHeight(ctx context.Context, height int64) ([]EndorsingRight, error) {
	rights := make([]EndorsingRight, 0, 32)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/endorsing_rights?all", c.ChainID, height)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetEndorsingRightsCycle returns information about a Tezos endorsing rights for an entire cycle
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetEndorsingRightsCycle(ctx context.Context, height, cycle int64) ([]EndorsingRight, error) {
	rights := make([]EndorsingRight, 0, 32*4096)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/endorsing_rights?all&cycle=%d", c.ChainID, height, cycle)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetSnapshotIndexCycle returns information about a Tezos roll snapshot
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetSnapshotIndexCycle(ctx context.Context, height, cycle int64) (*SnapshotIndex, error) {
	idx := &SnapshotIndex{Cycle: cycle}
	u := fmt.Sprintf("chains/%s/blocks/%d/context/raw/json/cycle/%d", c.ChainID, height, cycle)
	if err := c.Get(ctx, u, idx); err != nil {
		return nil, err
	}
	return idx, nil
}
