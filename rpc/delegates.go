// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"strconv"

	"blockwatch.cc/tzindex/chain"
)

// Delegate holds information about an active delegate
type Delegate struct {
	Delegate             chain.Address   `json:"-"`
	Height               int64           `json:"-"`
	Deactivated          bool            `json:"deactivated"`
	Balance              int64           `json:"balance,string"`
	DelegatedContracts   []chain.Address `json:"delegated_contracts"`
	FrozenBalance        int64           `json:"frozen_balance,string"`
	FrozenBalanceByCycle []CycleBalance  `json:"frozen_balance_by_cycle"`
	GracePeriod          int64           `json:"grace_period"`
	StakingBalance       int64           `json:"staking_balance,string"`
}

type CycleBalance struct {
	Cycle   int64 `json:"cycle"`
	Deposit int64 `json:"deposit,string"`
	Fees    int64 `json:"fees,string"`
	Rewards int64 `json:"rewards,string"`
}

// DelegateList contains a list of delegates
type DelegateList []chain.Address

// ListActiveDelegates returns information about all active delegates at a block
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-context-delegates
func (c *Client) ListActiveDelegates(ctx context.Context, height int64) (DelegateList, error) {
	delegates := make(DelegateList, 0)
	u := fmt.Sprintf("chains/%s/blocks/%d/context/delegates?active=1", c.ChainID, height)
	if err := c.Get(ctx, u, &delegates); err != nil {
		return nil, err
	}
	return delegates, nil
}

// GetDelegateStatus returns information about a delegate at a specific height
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-context-delegates-pkh
func (c *Client) GetDelegateStatus(ctx context.Context, addr chain.Address, height int64) (*Delegate, error) {
	delegate := &Delegate{
		Delegate: addr,
		Height:   height,
	}
	u := fmt.Sprintf("chains/%s/blocks/%d/context/delegates/%s", c.ChainID, height, addr)
	if err := c.Get(ctx, u, &delegate); err != nil {
		return nil, err
	}
	return delegate, nil
}

// GetDelegateBalance returns a delegate's balance http://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id-context-delegates-pkh-balance
func (c *Client) GetDelegateBalance(ctx context.Context, addr chain.Address) (int64, error) {
	u := fmt.Sprintf("chains/%s/blocks/head/context/delegates/%s/balance", c.ChainID, addr)
	var bal string
	err := c.Get(ctx, u, &bal)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(bal, 10, 64)
}
