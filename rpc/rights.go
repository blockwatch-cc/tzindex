// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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

func (r BakingRight) Address() chain.Address {
	return r.Delegate
}

// EndorsingRight holds information about the right to endorse a specific Tezos block
type EndorsingRight struct {
	Delegate      chain.Address `json:"delegate"`
	Level         int64         `json:"level"`
	EstimatedTime time.Time     `json:"estimated_time"`
	Slots         []int         `json:"slots"`
}

func (r EndorsingRight) Address() chain.Address {
	return r.Delegate
}

type SnapshotIndex struct {
	LastRoll     []string `json:"last_roll"`
	Nonces       []string `json:"nonces"`
	RandomSeed   string   `json:"random_seed"`
	RollSnapshot int64    `json:"roll_snapshot"`
	Cycle        int64    `json:"cycle"`
}

type SnapshotRoll struct {
	RollId   int64
	OwnerKey chain.Key
}

func (r *SnapshotRoll) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] != '[' || data[len(data)-1] != ']' {
		return fmt.Errorf("SnapshotRoll: invalid json array '%s'", string(data))
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	return r.decode(unpacked)
}

func (r SnapshotRoll) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	buf = strconv.AppendInt(buf, r.RollId, 10)
	buf = append(buf, ',')
	buf = strconv.AppendQuote(buf, r.OwnerKey.String())
	buf = append(buf, ']')
	return buf, nil
}

func (r *SnapshotRoll) decode(unpacked []interface{}) error {
	if l := len(unpacked); l != 2 {
		return fmt.Errorf("SnapshotRoll: invalid json array len %d", l)
	}
	id, err := strconv.ParseInt(unpacked[0].(json.Number).String(), 10, 64)
	if err != nil {
		return fmt.Errorf("SnapshotRoll: invalid roll id: %v", err)
	}
	if err = r.OwnerKey.UnmarshalText([]byte(unpacked[1].(string))); err != nil {
		return err
	}
	r.RollId = id
	return nil
}

type SnapshotOwners struct {
	Height int64          `json:"height"`
	Cycle  int64          `json:"cycle"`
	Index  int64          `json:"index"`
	Rolls  []SnapshotRoll `json:"rolls"`
}

// GetBakingRights returns information about a Tezos block baking rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-baking-rights
func (c *Client) GetBakingRights(ctx context.Context, blockID chain.BlockHash) ([]BakingRight, error) {
	rights := make([]BakingRight, 0, 64)
	u := fmt.Sprintf("chains/%s/blocks/%s/helpers/baking_rights?all=true&max_priority=63", c.ChainID, blockID)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetBakingRightsHeight returns information about a Tezos block baking rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-baking-rights
func (c *Client) GetBakingRightsHeight(ctx context.Context, height int64) ([]BakingRight, error) {
	rights := make([]BakingRight, 0, 64)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/baking_rights?all=true&max_priority=63", c.ChainID, height)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetBakingRightsCycle returns information about a Tezos baking rights for an entire cycle.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-baking-rights
func (c *Client) GetBakingRightsCycle(ctx context.Context, height, cycle int64) ([]BakingRight, error) {
	rights := make([]BakingRight, 0, 64*4096)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/baking_rights?all=true&cycle=%d&max_priority=63", c.ChainID, height, cycle)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetEndorsingRights returns information about a Tezos block endorsing rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetEndorsingRights(ctx context.Context, blockID chain.BlockHash) ([]EndorsingRight, error) {
	rights := make([]EndorsingRight, 0, 32)
	u := fmt.Sprintf("chains/%s/blocks/%s/helpers/endorsing_rights?all=true", c.ChainID, blockID)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetEndorsingRightsHeight returns information about a Tezos block endorsing rights
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetEndorsingRightsHeight(ctx context.Context, height int64) ([]EndorsingRight, error) {
	rights := make([]EndorsingRight, 0, 32)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/endorsing_rights?all=true", c.ChainID, height)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// GetEndorsingRightsCycle returns information about a Tezos endorsing rights for an entire cycle
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-helpers-endorsing-rights
func (c *Client) GetEndorsingRightsCycle(ctx context.Context, height, cycle int64) ([]EndorsingRight, error) {
	rights := make([]EndorsingRight, 0, 32*4096)
	u := fmt.Sprintf("chains/%s/blocks/%d/helpers/endorsing_rights?all=true&cycle=%d", c.ChainID, height, cycle)
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

// GetSnapshotRollOwners returns information about a Tezos roll snapshot ownership
// [[0,"p2pk67wVncLFS1DQDm2gVR45sYCzQSXTtqn3bviNYXVCq6WRoqtxHXL"]]
//   roll - baker pubkey
// /chains/main/blocks/901121/context/raw/json/rolls/owner/snapshot/220/15/?depth=1
func (c *Client) GetSnapshotRollOwners(ctx context.Context, height, cycle, index int64) (*SnapshotOwners, error) {
	owners := &SnapshotOwners{Height: height, Cycle: cycle, Index: index}
	u := fmt.Sprintf("chains/%s/blocks/%d/context/raw/json/rolls/owner/snapshot/%d/%d?depth=1", c.ChainID, height, cycle, index)
	if err := c.Get(ctx, u, &owners.Rolls); err != nil {
		return nil, err
	}
	return owners, nil
}
