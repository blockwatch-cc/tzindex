// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"blockwatch.cc/tzindex/chain"
)

// Block holds information about a Tezos block
type Block struct {
	Protocol   chain.ProtocolHash   `json:"protocol"`
	ChainId    chain.ChainIdHash    `json:"chain_id"`
	Hash       chain.BlockHash      `json:"hash"`
	Header     BlockHeader          `json:"header"`
	Metadata   BlockMetadata        `json:"metadata"`
	Operations [][]*OperationHeader `json:"operations"`
}

func (b Block) GetLevel() int64 {
	return b.Header.Level
}

func (b Block) GetTimestamp() time.Time {
	return b.Header.Timestamp
}

func (b Block) GetVersion() int {
	return b.Header.Proto
}

func (b Block) GetCycle() int64 {
	if b.Metadata.LevelInfo != nil {
		return b.Metadata.LevelInfo.Cycle
	}
	if b.Metadata.Level != nil {
		return b.Metadata.Level.Cycle
	}
	return 0
}

func (b Block) GetVotingPeriodKind() chain.VotingPeriodKind {
	if b.Metadata.VotingPeriodInfo != nil {
		return b.Metadata.VotingPeriodInfo.VotingPeriod.Kind
	}
	if b.Metadata.VotingPeriodKind != nil {
		return *b.Metadata.VotingPeriodKind
	}
	return chain.VotingPeriodInvalid
}

func (b Block) GetVotingPeriod() int64 {
	if b.Metadata.VotingPeriodInfo != nil {
		return b.Metadata.VotingPeriodInfo.VotingPeriod.Index
	}
	if b.Metadata.Level != nil {
		return b.Metadata.Level.VotingPeriod
	}
	return 0
}

// InvalidBlock represents invalid block hash along with the errors that led to it being declared invalid
type InvalidBlock struct {
	Block chain.BlockHash `json:"block"`
	Level int64           `json:"level"`
	Error Errors          `json:"error"`
}

// BlockHeader is a part of the Tezos block data
type BlockHeader struct {
	ChainId          *chain.ChainIdHash  `json:"chain_id,omitempty"`
	Hash             *chain.BlockHash    `json:"hash,omitempty"`
	Level            int64               `json:"level"`
	Proto            int                 `json:"proto"`
	Predecessor      chain.BlockHash     `json:"predecessor"`
	Timestamp        time.Time           `json:"timestamp"`
	ValidationPass   int                 `json:"validation_pass"`
	OperationsHash   string              `json:"operations_hash"`
	Fitness          []HexBytes          `json:"fitness"`
	Context          string              `json:"context"`
	Priority         int                 `json:"priority"`
	ProofOfWorkNonce HexBytes            `json:"proof_of_work_nonce"`
	SeedNonceHash    *chain.NonceHash    `json:"seed_nonce_hash"`
	Signature        string              `json:"signature"`
	Content          *BlockContent       `json:"content,omitempty"`
	Protocol         *chain.ProtocolHash `json:"protocol,omitempty"`
}

// BlockContent is part of block 1 header that seeds the initial context
type BlockContent struct {
	Command    string             `json:"command"`
	Protocol   chain.ProtocolHash `json:"hash"`
	Fitness    []HexBytes         `json:"fitness"`
	Parameters *GenesisData       `json:"protocol_parameters"`
}

// OperationListLength is a part of the BlockMetadata
type OperationListLength struct {
	MaxSize int `json:"max_size"`
	MaxOp   int `json:"max_op"`
}

// BlockLevel is a part of BlockMetadata
type BlockLevel struct {
	Level              int64 `json:"level"`
	LevelPosition      int64 `json:"level_position"`
	Cycle              int64 `json:"cycle"`
	CyclePosition      int64 `json:"cycle_position"`
	ExpectedCommitment bool  `json:"expected_commitment"`

	// deprecated in v008
	VotingPeriod         int64 `json:"voting_period"`
	VotingPeriodPosition int64 `json:"voting_period_position"`
}

type VotingPeriod struct {
	Index         int64                  `json:"index"`
	Kind          chain.VotingPeriodKind `json:"kind"`
	StartPosition int64                  `json:"start_position"`
}

type VotingPeriodInfo struct {
	Position     int64        `json:"position"`
	Remaining    int64        `json:"remaining"`
	VotingPeriod VotingPeriod `json:"voting_period"`
}

// BlockMetadata is a part of the Tezos block data
type BlockMetadata struct {
	Protocol               chain.ProtocolHash     `json:"protocol"`
	NextProtocol           chain.ProtocolHash     `json:"next_protocol"`
	TestChainStatus        TestChainStatus        `json:"-"`
	MaxOperationsTTL       int                    `json:"max_operations_ttl"`
	MaxOperationDataLength int                    `json:"max_operation_data_length"`
	MaxBlockHeaderLength   int                    `json:"max_block_header_length"`
	MaxOperationListLength []*OperationListLength `json:"max_operation_list_length"`
	Baker                  chain.Address          `json:"baker"`
	NonceHash              *chain.NonceHash       `json:"nonce_hash"`
	ConsumedGas            int64                  `json:"consumed_gas,string"`
	Deactivated            []chain.Address        `json:"deactivated"`
	BalanceUpdates         BalanceUpdates         `json:"balance_updates"`

	// deprecated in v008
	Level            *BlockLevel             `json:"level"`
	VotingPeriodKind *chain.VotingPeriodKind `json:"voting_period_kind"`

	// v008
	LevelInfo        *BlockLevel       `json:"level_info"`
	VotingPeriodInfo *VotingPeriodInfo `json:"voting_period_info"`
}

// UnmarshalJSON unmarshals the BlockMetadata JSON
func (bhm *BlockMetadata) UnmarshalJSON(data []byte) error {
	type suppressJSONUnmarshaller BlockMetadata
	if err := json.Unmarshal(data, (*suppressJSONUnmarshaller)(bhm)); err != nil {
		return err
	}

	var tmp struct {
		TestChainStatus json.RawMessage `json:"test_chain_status"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	tcs, err := unmarshalTestChainStatus(tmp.TestChainStatus)
	if err != nil {
		return err
	}

	bhm.TestChainStatus = tcs

	return nil
}

// GetBlock returns information about a Tezos block
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id
func (c *Client) GetBlock(ctx context.Context, blockID chain.BlockHash) (*Block, error) {
	var block Block
	u := fmt.Sprintf("chains/%s/blocks/%s", c.ChainID, blockID)
	if err := c.Get(ctx, u, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

// GetBlockHeight returns information about a Tezos block
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id
func (c *Client) GetBlockHeight(ctx context.Context, height int64) (*Block, error) {
	var block Block
	u := fmt.Sprintf("chains/%s/blocks/%d", c.ChainID, height)
	if err := c.Get(ctx, u, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

// GetTips returns hashes of the current chain tip blocks, first in the array is the
// current main chain.
// https://tezos.gitlab.io/mainnet/api/rpc.html#chains-chain-id-blocks
func (c *Client) GetTips(ctx context.Context, depth int, head chain.BlockHash) ([][]chain.BlockHash, error) {
	if depth == 0 {
		depth = 1
	}
	tips := make([][]chain.BlockHash, 0, 10)
	var u string
	if head.IsValid() {
		u = fmt.Sprintf("chains/%s/blocks?length=%d&head=%s", c.ChainID, depth, head)
	} else {
		u = fmt.Sprintf("chains/%s/blocks?length=%d", c.ChainID, depth)
	}
	if err := c.Get(ctx, u, &tips); err != nil {
		return nil, err
	}
	return tips, nil
}

// GetTipHeader returns main chain tip's block header.
// https://tezos.gitlab.io/mainnet/api/rpc.html#chains-chain-id-blocks
func (c *Client) GetTipHeader(ctx context.Context) (*BlockHeader, error) {
	var head BlockHeader
	u := fmt.Sprintf("chains/%s/blocks/head/header", c.ChainID)
	if err := c.Get(ctx, u, &head); err != nil {
		return nil, err
	}
	return &head, nil
}

// GetBlockPredHashes returns the block id's (hashes) of count preceeding blocks.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-chains-chain-id-blocks
func (c *Client) GetBlockPredHashes(ctx context.Context, blockID chain.BlockHash, count int) ([]chain.BlockHash, error) {
	if count <= 0 {
		count = 1
	}
	blockIds := make([][]chain.BlockHash, 0, count)
	u := fmt.Sprintf("chains/%s/blocks?length=%d&head=%s", c.ChainID, count, blockID)
	if err := c.Get(ctx, u, &blockIds); err != nil {
		return nil, err
	}
	return blockIds[0], nil
}

// GetInvalidBlocks lists blocks that have been declared invalid along with the errors that led to them being declared invalid.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-chains-chain-id-invalid-blocks
func (c *Client) GetInvalidBlocks(ctx context.Context) ([]*InvalidBlock, error) {
	var invalidBlocks []*InvalidBlock
	if err := c.Get(ctx, "chains/"+c.ChainID+"/invalid_blocks", &invalidBlocks); err != nil {
		return nil, err
	}
	return invalidBlocks, nil
}

// GetInvalidBlock returns a single invalid block with the errors that led to it being declared invalid.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-chains-chain-id-invalid-blocks-block-hash
func (c *Client) GetInvalidBlock(ctx context.Context, blockID chain.BlockHash) (*InvalidBlock, error) {
	var invalidBlock InvalidBlock
	u := fmt.Sprintf("chains/%s/invalid_blocks/%s", c.ChainID, blockID)
	if err := c.Get(ctx, u, &invalidBlock); err != nil {
		return nil, err
	}
	return &invalidBlock, nil
}
