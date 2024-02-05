// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// Block holds information about a Tezos block. This is a
// stripped version for indexing without signatures.
type Block struct {
	Protocol   tezos.ProtocolHash `json:"protocol"`
	ChainId    tezos.ChainIdHash  `json:"chain_id"`
	Hash       tezos.BlockHash    `json:"hash"`
	Header     BlockHeader        `json:"header"`
	Metadata   BlockMetadata      `json:"metadata"`
	Operations [4][]*Operation    `json:"operations"`
}

func (b Block) GetProtocol() tezos.ProtocolHash {
	return b.Protocol
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

func (b Block) GetCyclePosition() int64 {
	return b.GetLevelInfo().CyclePosition
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
	// only for <v008
	if lvl := b.Metadata.Level; lvl != nil {
		return VotingPeriodInfo{
			VotingPeriod: VotingPeriod{
				Index: lvl.VotingPeriod,
				Kind:  *b.Metadata.VotingPeriodKind,
			},
			Position:  lvl.VotingPeriodPosition,
			Remaining: 32767 - lvl.VotingPeriodPosition, // v2..7
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

func (b Block) IsAiActivationUpgrade() bool {
	if b.Header.AdaptiveIssuanceActivationCycle == nil {
		return false
	}
	if *b.Header.AdaptiveIssuanceActivationCycle != b.GetCycle() {
		return false
	}
	return b.GetCyclePosition() == 0
}

func (b Block) IsProtocolUpgrade() bool {
	return !b.Metadata.Protocol.Equal(b.Metadata.NextProtocol)
}

func (b Block) Invoices() (upd []BalanceUpdate, ok bool) {
	list := b.Metadata.BalanceUpdates
	if len(list) == 0 || list[0].Category != "invoice" {
		return
	}
	for _, v := range list[1:] {
		if v.Category != "" {
			break
		}
		if v.Kind != "contract" {
			break
		}
		if v.Origin != "migration" {
			break
		}
		upd = append(upd, v)
		ok = true
	}
	return
}

// InvalidBlock represents invalid block hash along with the errors that led to it being declared invalid
type InvalidBlock struct {
	Block tezos.BlockHash `json:"block"`
	Level int64           `json:"level"`
	Error Errors          `json:"error"`
}

// BlockHeader is a part of the Tezos block data
type BlockHeader struct {
	Level                           int64             `json:"level"`
	Proto                           int               `json:"proto"`
	Predecessor                     tezos.BlockHash   `json:"predecessor"`
	Timestamp                       time.Time         `json:"timestamp"`
	Fitness                         []tezos.HexBytes  `json:"fitness"`
	PayloadHash                     tezos.PayloadHash `json:"payload_hash"`
	PayloadRound                    int               `json:"payload_round"`
	Priority                        int               `json:"priority"`
	ProofOfWorkNonce                tezos.HexBytes    `json:"proof_of_work_nonce"`
	Content                         *BlockContent     `json:"content,omitempty"`
	LiquidityBakingEscapeVote       bool              `json:"liquidity_baking_escape_vote"`
	LiquidityBakingToggleVote       tezos.FeatureVote `json:"liquidity_baking_toggle_vote"`
	AdaptiveIssuanceVote            tezos.FeatureVote `json:"adaptive_issuance_vote"`
	AdaptiveIssuanceActivationCycle *int64            `json:"adaptive_issuance_activation_cycle"`

	// only present when header is fetched explicitly
	// Hash     tezos.BlockHash    `json:"hash"`
	// Protocol tezos.ProtocolHash `json:"protocol"`
	ChainId tezos.ChainIdHash `json:"chain_id"`
}

func (h BlockHeader) LbVote() tezos.FeatureVote {
	if h.LiquidityBakingToggleVote.IsValid() {
		return h.LiquidityBakingToggleVote
	}
	// sic! bool flag has opposite meaning
	if h.LiquidityBakingEscapeVote {
		return tezos.FeatureVoteOff
	}
	return tezos.FeatureVoteOn
}

// BlockContent is part of block 1 header that seeds the initial context
type BlockContent struct {
	Parameters *GenesisData `json:"protocol_parameters"`
}

// BlockLevel is a part of BlockMetadata
type LevelInfo struct {
	Level int64 `json:"level"`
	Cycle int64 `json:"cycle"`
	// <v008
	VotingPeriod         int64 `json:"voting_period"`
	CyclePosition        int64 `json:"cycle_position"`
	VotingPeriodPosition int64 `json:"voting_period_position"`
	ExpectedCommitment   bool  `json:"expected_commitment"`
}

type VotingPeriod struct {
	Index         int64                  `json:"index"`
	Kind          tezos.VotingPeriodKind `json:"kind"`
	StartPosition int64                  `json:"start_position"`
}

type VotingPeriodInfo struct {
	VotingPeriod VotingPeriod `json:"voting_period"`
	Position     int64        `json:"position"`
	Remaining    int64        `json:"remaining"`
}

// BlockMetadata is a part of the Tezos block data
type BlockMetadata struct {
	Protocol         tezos.ProtocolHash `json:"protocol"`
	NextProtocol     tezos.ProtocolHash `json:"next_protocol"`
	MaxOperationsTTL int                `json:"max_operations_ttl"`
	Baker            tezos.Address      `json:"baker"`
	Proposer         tezos.Address      `json:"proposer"`
	ConsumedGas      int64              `json:"consumed_gas,string"`
	ConsumedMilliGas int64              `json:"consumed_milli_gas,string"`
	Deactivated      []tezos.Address    `json:"deactivated"`
	BalanceUpdates   BalanceUpdates     `json:"balance_updates"`

	// <v008
	Level            *LevelInfo              `json:"level"`
	VotingPeriodKind *tezos.VotingPeriodKind `json:"voting_period_kind"`

	// v008+
	LevelInfo        *LevelInfo        `json:"level_info"`
	VotingPeriodInfo *VotingPeriodInfo `json:"voting_period_info"`

	// v010+
	ImplicitOperationsResults []*ImplicitResult `json:"implicit_operations_results"`
	LiquidityBakingEscapeEma  int64             `json:"liquidity_baking_escape_ema"`
	LiquidityBakingToggleEma  int64             `json:"liquidity_baking_toggle_ema"`
	AdaptiveIssuanceEma       int64             `json:"adaptive_issuance_vote_ema"`

	// v015+
	ProposerConsensusKey tezos.Address `json:"proposer_consensus_key"`
	BakerConsensusKey    tezos.Address `json:"baker_consensus_key"`
}

func (m BlockMetadata) GetLbEma() int64 {
	return m.LiquidityBakingEscapeEma + m.LiquidityBakingToggleEma
}

func (m *BlockMetadata) GetLevel() int64 {
	if m.LevelInfo != nil {
		return m.LevelInfo.Level
	}
	return m.Level.Level
}

func (m BlockMetadata) Gas() int64 {
	if m.ConsumedMilliGas > 0 {
		return m.ConsumedMilliGas / 1000
	}
	return m.ConsumedGas
}

func (m BlockMetadata) MilliGas() int64 {
	if m.ConsumedMilliGas > 0 {
		return m.ConsumedMilliGas
	}
	return m.ConsumedGas * 1000
}

// GetBlock returns information about a Tezos block
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-block-id
func (c *Client) GetBlock(ctx context.Context, id BlockID) (*Block, error) {
	var block Block
	u := fmt.Sprintf("chains/main/blocks/%s?metadata=always", id)
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

// GetBlockMetadata returns a block metadata.
// https://tezos.gitlab.io/mainnet/api/rpc.html#chains-chain-id-blocks
func (c *Client) GetBlockMetadata(ctx context.Context, id BlockID) (*BlockMetadata, error) {
	var meta BlockMetadata
	u := fmt.Sprintf("chains/main/blocks/%s/metadata?metadata=always", id)
	if err := c.Get(ctx, u, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
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
	u := "chains/main/blocks/head/header"
	if err := c.Get(ctx, u, &head); err != nil {
		return nil, err
	}
	return &head, nil
}

// Pulls storage from archive node and replaces storage embedded into originated scripts.
// For implicit (block-level) originations generated by protocol upgrades we pull the
// entire script. This is necessary because in script bigmap pointers are not replaced.
func (b *Block) UpdateAllOriginatedScripts(ctx context.Context, c *Client) error {
	// genesis block has no ops
	if len(b.Operations[0]) == 0 {
		return nil
	}
	// handle ops in list 3
	for _, op := range b.Operations[3] {
		for _, o := range op.Contents {
			if !o.Result().IsSuccess() {
				continue
			}
			switch o.Kind() {
			case tezos.OpTypeOrigination:
				if err := updateOriginationScript(ctx, c, o, b.Hash); err != nil {
					log.Errorf("script update for origination %d > %s: %v",
						b.GetLevel(), op.Hash, err)
				}
			case tezos.OpTypeTransaction:
				tx := o.(*Transaction)
				for _, iop := range tx.Metadata.InternalResults {
					if iop.Kind != tezos.OpTypeOrigination {
						continue
					}
					if err := updateInternalOriginationScript(ctx, c, iop, b.Hash); err != nil {
						log.Errorf("script update for internal origination %d > %s: %v",
							b.GetLevel(), op.Hash, err)
					}
				}
			}
		}
	}

	// also visit implicit tx from block headers
	return b.UpdateHeaderOriginatedScripts(ctx, c)
}

func (b *Block) UpdateHeaderOriginatedScripts(ctx context.Context, c *Client) error {
	// handle implicit block header results (i.e. liquidity baking)
	for _, ires := range b.Metadata.ImplicitOperationsResults {
		if ires.Kind != tezos.OpTypeOrigination {
			continue
		}
		if err := updateImplicitOriginationScript(ctx, c, ires, b.Hash); err != nil {
			log.Errorf("script update for implicit origination in block %d: %v", b.GetLevel(), err)
		}
	}
	return nil
}

func updateOriginationScript(ctx context.Context, c *Client, o TypedOperation, id BlockID) error {
	org := o.(*Origination)
	if org.Script == nil {
		// skip early delegation accounts because they are script-less contracts
		return nil
	}
	addr := org.Metadata.Result.OriginatedContracts[0]
	if !org.Script.IsValid() || org.Script.Code.Code.ContainsOpCode(micheline.H_CONSTANT) {
		// refetch script when it uses global constants
		s, err := c.GetContractScript(ctx, addr, id)
		if err != nil {
			return err
		}
		org.Script = s
	}
	if org.Script.Code.Storage.ContainsOpCode(micheline.T_BIG_MAP) {
		// only refetch storage value when script uses bigmaps
		store, err := c.GetContractStorage(ctx, addr, id)
		if err != nil {
			return err
		}
		org.Script.Storage = store
	}
	return nil
}

func updateInternalOriginationScript(ctx context.Context, c *Client, iop *InternalResult, id BlockID) error {
	if iop.Script == nil {
		// skip early delegation accounts because they are script-less contracts
		return nil
	}
	addr := iop.Result.OriginatedContracts[0]
	if !iop.Script.IsValid() || iop.Script.Code.Code.ContainsOpCode(micheline.H_CONSTANT) {
		// refetch script when it uses global constants
		s, err := c.GetContractScript(ctx, addr, id)
		if err != nil {
			return err
		}
		iop.Script = s
	}
	if iop.Script.Code.Storage.ContainsOpCode(micheline.T_BIG_MAP) {
		// only refetch storage value when script uses bigmaps
		store, err := c.GetContractStorage(ctx, addr, id)
		if err != nil {
			return err
		}
		iop.Script.Storage = store
	}
	return nil
}

func updateImplicitOriginationScript(ctx context.Context, c *Client, ires *ImplicitResult, id BlockID) error {
	addr := ires.OriginatedContracts[0]
	script, err := c.GetContractScript(ctx, addr, id)
	if err != nil {
		return err
	}
	ires.Script = script
	return nil
}
