// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/tezos"
)

type Constants struct {
	NoRewardCycles               int64    `json:"no_reward_cycles"`
	SecurityDepositRampUpCycles  int64    `json:"security_deposit_ramp_up_cycles"`
	PreservedCycles              int64    `json:"preserved_cycles"`
	BlocksPerCycle               int64    `json:"blocks_per_cycle"`
	BlocksPerCommitment          int64    `json:"blocks_per_commitment"`
	BlocksPerRollSnapshot        int64    `json:"blocks_per_roll_snapshot"`
	BlocksPerVotingPeriod        int64    `json:"blocks_per_voting_period"`
	TimeBetweenBlocks            []string `json:"time_between_blocks"`
	EndorsersPerBlock            int      `json:"endorsers_per_block"`
	HardGasLimitPerOperation     int64    `json:"hard_gas_limit_per_operation,string"`
	HardGasLimitPerBlock         int64    `json:"hard_gas_limit_per_block,string"`
	ProofOfWorkThreshold         int64    `json:"proof_of_work_threshold,string"`
	ProofOfWorkNonceSize         int      `json:"proof_of_work_nonce_size"`
	TokensPerRoll                int64    `json:"tokens_per_roll,string"`
	MichelsonMaximumTypeSize     int      `json:"michelson_maximum_type_size"`
	SeedNonceRevelationTip       int64    `json:"seed_nonce_revelation_tip,string"`
	OriginationSize              int64    `json:"origination_size"`
	OriginationBurn              int64    `json:"origination_burn,string"`
	BlockSecurityDeposit         int64    `json:"block_security_deposit,string"`
	EndorsementSecurityDeposit   int64    `json:"endorsement_security_deposit,string"`
	CostPerByte                  int64    `json:"cost_per_byte,string"`
	HardStorageLimitPerOperation int64    `json:"hard_storage_limit_per_operation,string"`
	TestChainDuration            int64    `json:"test_chain_duration,string"`
	MaxOperationDataLength       int      `json:"max_operation_data_length"`
	MaxProposalsPerDelegate      int      `json:"max_proposals_per_delegate"`
	MaxRevelationsPerBlock       int      `json:"max_revelations_per_block"`
	NonceLength                  int      `json:"nonce_length"`

	// New in Bablyon v005
	MinProposalQuorum int64 `json:"min_proposal_quorum"`
	QuorumMin         int64 `json:"quorum_min"`
	QuorumMax         int64 `json:"quorum_max"`

	// Emmy+ v1
	InitialEndorsers           int `json:"initial_endorsers"`
	DelayPerMissingEndorsement int `json:"delay_per_missing_endorsement,string"`

	// New in Carthage v006 (Emmy+ v2)
	BakingRewardPerEndorsement_v6 [2]int64 `json:"-"`
	EndorsementReward_v6          [2]int64 `json:"-"`

	// Broken by v6
	BlockReward_v1       int64 `json:"block_reward,string"` // default unmarshal
	EndorsementReward_v1 int64 `json:"-"`

	// New in v7
	MaxAnonOpsPerBlock int `json:"max_anon_ops_per_block"` // was max_revelations_per_block

	// New in v10
	LiquidityBakingEscapeEmaThreshold int64 `json:"liquidity_baking_escape_ema_threshold"`
	LiquidityBakingSubsidy            int64 `json:"liquidity_baking_subsidy,string"`
	LiquidityBakingSunsetLevel        int64 `json:"liquidity_baking_sunset_level"`
	MinimalBlockDelay                 int   `json:"minimal_block_delay,string"`

	// New in v11
	MaxMichelineNodeCount          int      `json:"max_micheline_node_count"`
	MaxMichelineBytesLimit         int      `json:"max_micheline_bytes_limit"`
	MaxAllowedGlobalConstantsDepth int      `json:"max_allowed_global_constants_depth"`
	CacheLayout                    []string `json:"cache_layout"`

	// New in v12
	BlocksPerStakeSnapshot                           int64       `json:"blocks_per_stake_snapshot"`
	BakingRewardFixedPortion                         int64       `json:"baking_reward_fixed_portion,string"`
	BakingRewardBonusPerSlot                         int64       `json:"baking_reward_bonus_per_slot,string"`
	EndorsingRewardPerSlot                           int64       `json:"endorsing_reward_per_slot,string"`
	MaxOperationsTimeToLive                          int64       `json:"max_operations_time_to_live"`
	DelayIncrementPerRound                           int         `json:"delay_increment_per_round,string"`
	ConsensusCommitteeSize                           int         `json:"consensus_committee_size"`
	ConsensusThreshold                               int         `json:"consensus_threshold"`
	MinimalParticipationRatio                        tezos.Ratio `json:"minimal_participation_ratio"`
	MaxSlashingPeriod                                int64       `json:"max_slashing_period"`
	FrozenDepositsPercentage                         int         `json:"frozen_deposits_percentage"`
	DoubleBakingPunishment                           int64       `json:"double_baking_punishment,string"`
	RatioOfFrozenDepositsSlashedPerDoubleEndorsement tezos.Ratio `json:"ratio_of_frozen_deposits_slashed_per_double_endorsement"`
}

func (c Constants) HaveV6Rewards() bool {
	return c.BakingRewardPerEndorsement_v6[0] > 0
}

func (c Constants) HaveV12Rewards() bool {
	return c.BakingRewardFixedPortion > 0
}

func (c Constants) GetBlockReward() int64 {
	switch {
	case c.HaveV12Rewards():
		return c.BakingRewardFixedPortion + c.BakingRewardBonusPerSlot*int64(c.ConsensusCommitteeSize-c.ConsensusThreshold)
	case c.HaveV6Rewards():
		return c.BakingRewardPerEndorsement_v6[0] * int64(c.EndorsersPerBlock)
	default:
		return c.BlockReward_v1
	}
}

func (c Constants) GetEndorsementReward() int64 {
	switch {
	case c.HaveV12Rewards():
		return c.EndorsingRewardPerSlot
	case c.HaveV6Rewards():
		return c.EndorsementReward_v6[0]
	default:
		return c.EndorsementReward_v1
	}
}

type v1_const struct {
	BlockReward       int64 `json:"block_reward,string"`
	EndorsementReward int64 `json:"endorsement_reward,string"`
}

type v6_const struct {
	BakingRewardPerEndorsement []string `json:"baking_reward_per_endorsement"`
	EndorsementReward          []string `json:"endorsement_reward"`
}

func (c *Constants) UnmarshalJSON(buf []byte) error {
	type X Constants
	cc := X{}
	if err := json.Unmarshal(buf, &cc); err != nil {
		return fmt.Errorf("parsing constants: %w", err)
	}
	// try extra unmarshal
	v1 := v1_const{}
	v6 := v6_const{}
	if err := json.Unmarshal(buf, &v1); err == nil {
		cc.BlockReward_v1 = v1.BlockReward
		cc.EndorsementReward_v1 = v1.EndorsementReward
	} else if err := json.Unmarshal(buf, &v6); err == nil {
		for i, v := range v6.BakingRewardPerEndorsement {
			val, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("parsing v6 constant baking_reward.. '%s': %w", string(buf), err)
			}
			cc.BakingRewardPerEndorsement_v6[i] = val
		}
		for i, v := range v6.EndorsementReward {
			val, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("parsing v6 constant endorsement_reward '%s': %w", string(buf), err)
			}
			cc.EndorsementReward_v6[i] = val
		}
	}
	*c = Constants(cc)
	return nil
}

// GetConstants returns chain configuration constants at block id
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-constants
func (c *Client) GetConstants(ctx context.Context, id BlockID) (con Constants, err error) {
	u := fmt.Sprintf("chains/main/blocks/%s/context/constants", id)
	err = c.Get(ctx, u, &con)
	return
}

// GetParams returns a translated parameters structure for the current
// network at block id.
func (c *Client) GetParams(ctx context.Context, id BlockID) (*tezos.Params, error) {
	p, err := c.ResolveChainConfig(ctx)
	if err != nil {
		return nil, err
	}
	height := id.Int64()
	if height < 0 {
		head, err := c.GetBlockHeader(ctx, id)
		if err != nil {
			return nil, err
		}
		height = head.Level
	}
	return p.ForHeight(height), nil
}

func (c Constants) MapToChainParams() *tezos.Params {
	p := tezos.NewParams()
	p.NoRewardCycles = c.NoRewardCycles
	p.SecurityDepositRampUpCycles = c.SecurityDepositRampUpCycles
	p.PreservedCycles = c.PreservedCycles
	p.BlocksPerCycle = c.BlocksPerCycle
	p.BlocksPerCommitment = c.BlocksPerCommitment
	p.BlocksPerRollSnapshot = c.BlocksPerRollSnapshot
	p.BlocksPerVotingPeriod = c.BlocksPerVotingPeriod
	p.EndorsersPerBlock = c.EndorsersPerBlock
	p.HardGasLimitPerOperation = c.HardGasLimitPerOperation
	p.HardGasLimitPerBlock = c.HardGasLimitPerBlock
	p.ProofOfWorkThreshold = c.ProofOfWorkThreshold
	p.ProofOfWorkNonceSize = c.ProofOfWorkNonceSize
	p.TokensPerRoll = c.TokensPerRoll
	p.MichelsonMaximumTypeSize = c.MichelsonMaximumTypeSize
	p.SeedNonceRevelationTip = c.SeedNonceRevelationTip
	p.OriginationSize = c.OriginationSize
	p.OriginationBurn = c.OriginationBurn
	p.BlockSecurityDeposit = c.BlockSecurityDeposit
	p.EndorsementSecurityDeposit = c.EndorsementSecurityDeposit
	p.BlockReward = c.GetBlockReward()
	p.EndorsementReward = c.GetEndorsementReward()
	if c.HaveV6Rewards() {
		p.BlockRewardV6 = c.BakingRewardPerEndorsement_v6
		p.EndorsementRewardV6 = c.EndorsementReward_v6
	}
	p.CostPerByte = c.CostPerByte
	p.HardStorageLimitPerOperation = c.HardStorageLimitPerOperation
	p.TestChainDuration = c.TestChainDuration
	p.MaxOperationDataLength = c.MaxOperationDataLength
	p.MaxProposalsPerDelegate = c.MaxProposalsPerDelegate
	p.MaxRevelationsPerBlock = c.MaxRevelationsPerBlock
	p.NonceLength = c.NonceLength
	p.MinProposalQuorum = c.MinProposalQuorum
	p.QuorumMin = c.QuorumMin
	p.QuorumMax = c.QuorumMax
	p.MaxAnonOpsPerBlock = c.MaxAnonOpsPerBlock
	p.LiquidityBakingEscapeEmaThreshold = c.LiquidityBakingEscapeEmaThreshold
	p.LiquidityBakingSubsidy = c.LiquidityBakingSubsidy
	p.LiquidityBakingSunsetLevel = c.LiquidityBakingSunsetLevel
	p.MinimalBlockDelay = time.Duration(c.MinimalBlockDelay) * time.Second

	for i, v := range c.TimeBetweenBlocks {
		if i > 1 {
			break
		}
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Errorf("parsing TimeBetweenBlocks: %w", err)
		} else {
			p.TimeBetweenBlocks[i] = time.Duration(val) * time.Second
		}
	}

	// v11
	p.MaxMichelineNodeCount = c.MaxMichelineNodeCount
	p.MaxMichelineBytesLimit = c.MaxMichelineBytesLimit
	p.MaxAllowedGlobalConstantsDepth = c.MaxAllowedGlobalConstantsDepth
	p.CacheLayout = c.CacheLayout

	// New in v12
	p.BlocksPerStakeSnapshot = c.BlocksPerStakeSnapshot
	p.BakingRewardFixedPortion = c.BakingRewardFixedPortion
	p.BakingRewardBonusPerSlot = c.BakingRewardBonusPerSlot
	p.EndorsingRewardPerSlot = c.EndorsingRewardPerSlot
	p.MaxOperationsTTL = c.MaxOperationsTimeToLive
	p.DelayIncrementPerRound = time.Duration(c.DelayIncrementPerRound) * time.Second
	p.ConsensusCommitteeSize = c.ConsensusCommitteeSize
	p.ConsensusThreshold = c.ConsensusThreshold
	p.MinimalParticipationRatio = c.MinimalParticipationRatio
	p.MaxSlashingPeriod = c.MaxSlashingPeriod
	p.FrozenDepositsPercentage = c.FrozenDepositsPercentage
	p.DoubleBakingPunishment = c.DoubleBakingPunishment
	p.RatioOfFrozenDepositsSlashedPerDoubleEndorsement = c.RatioOfFrozenDepositsSlashedPerDoubleEndorsement
	return p
}
