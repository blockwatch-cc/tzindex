// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
    "context"
    "fmt"

    "blockwatch.cc/tzgo/tezos"
)

// Delegate holds information about an active delegate
type Delegate struct {
    // extra info
    Delegate tezos.Address `json:"-"`
    Height   int64         `json:"-"`
    Block    string        `json:"-"`

    // tezos data
    Deactivated          bool            `json:"deactivated"`
    Balance              int64           `json:"balance,string"`
    DelegatedContracts   []tezos.Address `json:"delegated_contracts"`
    FrozenBalance        int64           `json:"frozen_balance,string"`
    FrozenBalanceByCycle []CycleBalance  `json:"frozen_balance_by_cycle"`
    GracePeriod          int64           `json:"grace_period"`
    StakingBalance       int64           `json:"staking_balance,string"`
    DelegatedBalance     int64           `json:"delegated_balance,string"`
    VotingPower          Int64orString   `json:"voting_power"`

    // v012+
    FullBalance           int64 `json:"full_balance,string"`
    FrozenDeposits        int64 `json:"frozen_deposits,string"`
    CurrentFrozenDeposits int64 `json:"current_frozen_deposits,string"`
    FrozenDepositsLimit   int64 `json:"frozen_deposits_limit,string"`
}

type CycleBalance struct {
    Cycle   int64 `json:"cycle"`
    Deposit int64 `json:"deposit,string"`
    Fees    int64 `json:"fees,string"`
    Rewards int64 `json:"rewards,string"`
}

type DelegateList []tezos.Address

// ListActiveDelegates returns information about all active delegates at a block.
func (c *Client) ListActiveDelegates(ctx context.Context, id BlockID) (DelegateList, error) {
    p, err := c.GetParams(ctx, id)
    if err != nil {
        return nil, err
    }
    selector := "active=true"
    if p.Version >= 13 {
        selector = "with_minimal_stake=true"
    }
    delegates := make(DelegateList, 0)
    u := fmt.Sprintf("chains/main/blocks/%s/context/delegates?%s", id, selector)
    if err := c.Get(ctx, u, &delegates); err != nil {
        return nil, err
    }
    return delegates, nil
}

// GetDelegate returns information about a delegate at a specific height.
func (c *Client) GetDelegate(ctx context.Context, addr tezos.Address, id BlockID) (*Delegate, error) {
    delegate := &Delegate{
        Delegate: addr,
        Height:   id.Int64(),
        Block:    id.String(),
    }
    u := fmt.Sprintf("chains/main/blocks/%s/context/delegates/%s", id, addr)
    if err := c.Get(ctx, u, &delegate); err != nil {
        return nil, err
    }
    return delegate, nil
}
