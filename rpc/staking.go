// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
)

type StakingParameters struct {
	Cycle int64   `json:"cycle"`
	Limit tezos.Z `json:"limit_of_staking_over_baking_millionth"`
	Edge  tezos.Z `json:"edge_of_baking_over_staking_billionth"`
}

// GetActiveStakingParams returns a delegate's current staking setup
func (c *Client) GetActiveStakingParams(ctx context.Context, addr tezos.Address, id BlockID) (*StakingParameters, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/context/delegates/%s/active_staking_parameters", id, addr)
	p := &StakingParameters{}
	if err := c.Get(ctx, u, p); err != nil {
		return nil, err
	}
	return p, nil
}

// GetPendingStakingParams returns a delegate's future staking setup
func (c *Client) GetPendingStakingParams(ctx context.Context, addr tezos.Address, id BlockID) ([]StakingParameters, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/context/delegates/%s/pending_staking_parameters", id, addr)
	list := make([]StakingParameters, 0, 5)
	if err := c.Get(ctx, u, &list); err != nil {
		return nil, err
	}
	return list, nil
}

type FrozenDeposit struct {
	Cycle   int64   `json:"cycle"`
	Deposit tezos.Z `json:"deposit"`
}

// GetUnstakedFrozenDeposits returns a delegate's unstaked frozen deposits
func (c *Client) GetUnstakedFrozenDeposits(ctx context.Context, addr tezos.Address, id BlockID) ([]FrozenDeposit, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/context/delegates/%s/unstaked_frozen_deposits", id, addr)
	list := make([]FrozenDeposit, 0, 7)
	if err := c.Get(ctx, u, &list); err != nil {
		return nil, err
	}
	return list, nil
}
