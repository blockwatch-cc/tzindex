// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"time"

	"blockwatch.cc/tzgo/tezos"
)

// Block holds information about a Tezos block
type Block struct {
	ChainId    tezos.ChainIdHash `json:"chain_id"`
	Hash       tezos.BlockHash   `json:"hash"`
	Header     BlockHeader       `json:"header"`
	Metadata   BlockMetadata     `json:"metadata"`
	Operations [][]*Operation    `json:"operations"`
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

func (b Block) GetLevelInfo() LevelInfo {
	if b.Metadata.LevelInfo != nil {
		return *b.Metadata.LevelInfo
	}
	if b.Metadata.Level != nil {
		return *b.Metadata.Level
	}
	return LevelInfo{}
}

// only works for mainnet when before Edo or for all nets after Edo
// due to fixed constants used
func (b Block) GetVotingInfo() VotingPeriodInfo {
	if b.Metadata.VotingPeriodInfo != nil {
		return *b.Metadata.VotingPeriodInfo
	}
	if b.Metadata.Level != nil {
		return VotingPeriodInfo{
			VotingPeriod: VotingPeriod{
				Index: b.Metadata.Level.VotingPeriod,
				Kind:  *b.Metadata.VotingPeriodKind,
			},
		}
	}
	return VotingPeriodInfo{}
}

func (b Block) GetVotingPeriodKind() tezos.VotingPeriodKind {
	if b.Metadata.VotingPeriodInfo != nil {
		return b.Metadata.VotingPeriodInfo.VotingPeriod.Kind
	}
	if b.Metadata.VotingPeriodKind != nil {
		return *b.Metadata.VotingPeriodKind
	}
	return tezos.VotingPeriodInvalid
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

func (b Block) IsProtocolUpgrade() bool {
	return !b.Metadata.Protocol.Equal(b.Metadata.NextProtocol)
}

// InvalidBlock represents invalid block hash along with the errors that led to it being declared invalid
type InvalidBlock struct {
	Block tezos.BlockHash `json:"block"`
	Level int64           `json:"level"`
	Error Errors          `json:"error"`
}

// BlockHeader is a part of the Tezos block data
type BlockHeader struct {
	Level                     int64             `json:"level"`
	Proto                     int               `json:"proto"`
	Predecessor               tezos.BlockHash   `json:"predecessor"`
	Timestamp                 time.Time         `json:"timestamp"`
	Fitness                   []tezos.HexBytes  `json:"fitness"`
	PayloadHash               tezos.PayloadHash `json:"payload_hash"`
	PayloadRound              int               `json:"payload_round"`
	Priority                  int               `json:"priority"`
	ProofOfWorkNonce          tezos.HexBytes    `json:"proof_of_work_nonce"`
	Content                   *BlockContent     `json:"content,omitempty"`
	LiquidityBakingEscapeVote bool              `json:"liquidity_baking_escape_vote"`
}

// BlockContent is part of block 1 header that seeds the initial context
type BlockContent struct {
	Parameters *GenesisData `json:"protocol_parameters"`
}

// BlockLevel is a part of BlockMetadata
type LevelInfo struct {
	Level int64 `json:"level"`
	Cycle int64 `json:"cycle"`
	// deprecated in v008
	VotingPeriod int64 `json:"voting_period"`
}

type VotingPeriod struct {
	Index int64                  `json:"index"`
	Kind  tezos.VotingPeriodKind `json:"kind"`
}

type VotingPeriodInfo struct {
	VotingPeriod VotingPeriod `json:"voting_period"`
}

// BlockMetadata is a part of the Tezos block data
type BlockMetadata struct {
	Protocol         tezos.ProtocolHash `json:"protocol"`
	NextProtocol     tezos.ProtocolHash `json:"next_protocol"`
	MaxOperationsTTL int                `json:"max_operations_ttl"`
	Baker            tezos.Address      `json:"baker"`
	Proposer         tezos.Address      `json:"proposer"`
	ConsumedGas      int64              `json:"consumed_gas,string"`
	Deactivated      []tezos.Address    `json:"deactivated"`
	BalanceUpdates   BalanceUpdates     `json:"balance_updates"`

	// deprecated in v008
	Level            *LevelInfo              `json:"level"`
	VotingPeriodKind *tezos.VotingPeriodKind `json:"voting_period_kind"`

	// v008
	LevelInfo        *LevelInfo        `json:"level_info"`
	VotingPeriodInfo *VotingPeriodInfo `json:"voting_period_info"`

	// v010
	ImplicitOperationsResults []ImplicitResult `json:"implicit_operations_results"`
	LiquidityBakingEscapeEma  int64            `json:"liquidity_baking_escape_ema"`
}

// GetBlock returns information about a Tezos block
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id
func (c *Client) GetBlock(ctx context.Context, id BlockID) (*Block, error) {
	var block Block
	u := fmt.Sprintf("chains/main/blocks/%s", id)
	if err := c.Get(ctx, u, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

// GetBlockheader returns information about a Tezos block header
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-header-id
func (c *Client) GetBlockHeader(ctx context.Context, id BlockID) (*BlockHeader, error) {
	var head BlockHeader
	u := fmt.Sprintf("chains/main/blocks/%s/header", id)
	if err := c.Get(ctx, u, &head); err != nil {
		return nil, err
	}
	return &head, nil
}

// GetTips returns hashes of the current chain tip blocks, first in the array is the
// current main chain.
// https://tezos.gitlab.io/mainnet/api/rpc.html#chains-chain-id-blocks
func (c *Client) GetTips(ctx context.Context, depth int, head tezos.BlockHash) ([][]tezos.BlockHash, error) {
	if depth == 0 {
		depth = 1
	}
	tips := make([][]tezos.BlockHash, 0, 10)
	var u string
	if head.IsValid() {
		u = fmt.Sprintf("chains/main/blocks?length=%d&head=%s", depth, head)
	} else {
		u = fmt.Sprintf("chains/main/blocks?length=%d", depth)
	}
	if err := c.Get(ctx, u, &tips); err != nil {
		return nil, err
	}
	return tips, nil
}

// GetTipHeader returns the head block's header.
// https://tezos.gitlab.io/mainnet/api/rpc.html#chains-chain-id-blocks
func (c *Client) GetTipHeader(ctx context.Context) (*BlockHeader, error) {
	var head BlockHeader
	u := fmt.Sprintf("chains/main/blocks/head/header")
	if err := c.Get(ctx, u, &head); err != nil {
		return nil, err
	}
	return &head, nil
}
