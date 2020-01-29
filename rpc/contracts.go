// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"strconv"

	"blockwatch.cc/tzindex/chain"
)

// Contracts holds a list of addresses
type Contracts []chain.Address

// GetContracts returns a list of all known contracts at head
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-contracts
func (c *Client) GetContracts(ctx context.Context) (Contracts, error) {
	contracts := make(Contracts, 0)
	u := fmt.Sprintf("chains/%s/blocks/head/context/contracts", c.ChainID)
	if err := c.Get(ctx, u, &contracts); err != nil {
		return nil, err
	}
	return contracts, nil
}

// GetContractsHeight returns a list of all known contracts at height
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-contracts
func (c *Client) GetContractsHeight(ctx context.Context, height int64) (Contracts, error) {
	u := fmt.Sprintf("chains/%s/blocks/%d/context/contracts", c.ChainID, height)
	contracts := make(Contracts, 0)
	if err := c.Get(ctx, u, &contracts); err != nil {
		return nil, err
	}
	return contracts, nil
}

// GetContractBalance returns the current balance of a contract at head
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-contracts-contract-id-balance
func (c *Client) GetContractBalance(ctx context.Context, addr chain.Address) (int64, error) {
	u := fmt.Sprintf("chains/%s/blocks/head/context/contracts/%s/balance", c.ChainID, addr)
	var bal string
	err := c.Get(ctx, u, &bal)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(bal, 10, 64)
}

// GetContractBalanceHeight returns the current balance of a contract at height
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-contracts-contract-id-balance
func (c *Client) GetContractBalanceHeight(ctx context.Context, addr chain.Address, height int64) (int64, error) {
	u := fmt.Sprintf("chains/%s/blocks/%d/context/contracts/%s/balance", c.ChainID, height, addr)
	var bal string
	err := c.Get(ctx, u, &bal)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(bal, 10, 64)
}
