// Copyright (c) 2020-2021 Blockwatch Data Inc.
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
	PreservedCycles              int64    `json:"preserved_cycles"`
	BlocksPerCycle               int64    `json:"blocks_per_cycle"`
	BlocksPerCommitment          int64    `json:"blocks_per_commitment"`
	BlocksPerRollSnapshot        int64    `json:"blocks_per_roll_snapshot"`
	BlocksPerVotingPeriod        int64    `json:"blocks_per_voting_period"`
	TimeBetweenBlocks            []string `json:"time_between_blocks"`
	EndorsersPerBlock            int      `json:"endorsers_per_block"`
	HardGasLimitPerOperation     int64    `json:"hard_gas_limit_per_operation,string"`
	HardGasLimitPerBlock         int64    `json:"hard_gas_limit_per_block,string"`
	TokensPerRoll                int64    `json:"tokens_per_roll,string"`
	MichelsonMaximumTypeSize     int      `json:"michelson_maximum_type_size"`
	SeedNonceRevelationTip       int64    `json:"seed_nonce_revelation_tip,string"`
	OriginationSize              int64    `json:"origination_size"`
	OriginationBurn              int64    `json:"origination_burn,string"`
	BlockSecurityDeposit         int64    `json:"block_security_deposit,string"`
	EndorsementSecurityDeposit   int64    `json:"endorsement_security_deposit,string"`
	CostPerByte                  int64    `json:"cost_per_byte,string"`
	HardStorageLimitPerOperation int64    `json:"hard_storage_limit_per_operation,string"`
	MaxOperationDataLength       int      `json:"max_operation_data_length"`
	ConsensusCommitteeSize       int      `json:"consensus_committee_size"`
	ConsensusThreshold           int      `json:"consensus_threshold"`

	// New in Bablyon v005
	MinProposalQuorum int64 `json:"min_proposal_quorum"`
	QuorumMin         int64 `json:"quorum_min"`
	QuorumMax         int64 `json:"quorum_max"`

	// New in Carthage v006 (Emmy+ v2)
	BakingRewardPerEndorsement_v6 [2]int64 `json:"-"`
	EndorsementReward_v6          [2]int64 `json:"-"`

	// Broken by v6
	BlockReward_v1       int64 `json:"block_reward,string"` // default unmarshal
	EndorsementReward_v1 int64 `json:"-"`

	// New in v10
	MinimalBlockDelay int `json:"minimal_block_delay,string"`

	// New in v12
	BlocksPerStakeSnapshot   int64 `json:"blocks_per_stake_snapshot"`
	BakingRewardFixedPortion int64 `json:"baking_reward_fixed_portion,string"`
	BakingRewardBonusPerSlot int64 `json:"baking_reward_bonus_per_slot,string"`
	EndorsingRewardPerSlot   int64 `json:"endorsing_reward_per_slot,string"`
	FrozenDepositsPercentage int   `json:"frozen_deposits_percentage"`
	MaxOperationsTimeToLive  int64 `json:"max_operations_time_to_live"`
	DelayIncrementPerRound   int   `json:"delay_increment_per_round,string"`

	// New in v13
	CyclesPerVotingPeriod int64 `json:"cycles_per_voting_period"`

	// New in v15
	MinimalStake int64 `json:"minimal_stake,string"` // replaces tokens_per_roll
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
		return fmt.Errorf("parsing constants: %v", err)
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
				return fmt.Errorf("parsing v6 constant baking_reward.. '%s': %v", string(buf), err)
			}
			cc.BakingRewardPerEndorsement_v6[i] = val
		}
		for i, v := range v6.EndorsementReward {
			val, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("parsing v6 constant endorsement_reward '%s': %v", string(buf), err)
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
	if !c.ChainId.IsValid() {
		id, err := c.GetChainId(ctx)
		if err != nil {
			return nil, err
		}
		c.ChainId = id
	}
	meta, err := c.GetBlockMetadata(ctx, id)
	if err != nil {
		return nil, err
	}
	con, err := c.GetConstants(ctx, id)
	if err != nil {
		return nil, err
	}
	return con.Params().
		ForNetwork(c.ChainId).
		ForProtocol(meta.Protocol).
		ForHeight(meta.GetLevel()), nil
}

func (c Constants) Params() *tezos.Params {
	p := tezos.NewParams()
	p.MinimalStake = c.TokensPerRoll + c.MinimalStake // either/or

	p.PreservedCycles = c.PreservedCycles
	p.BlocksPerCycle = c.BlocksPerCycle
	p.BlocksPerCommitment = c.BlocksPerCommitment
	p.BlocksPerSnapshot = c.BlocksPerRollSnapshot + c.BlocksPerStakeSnapshot // either/or

	// timing
	for i, v := range c.TimeBetweenBlocks {
		if i > 1 {
			break
		}
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Errorf("parsing TimeBetweenBlocks: %v", err)
		} else {
			p.MinimalBlockDelay = time.Duration(val) * time.Second
		}
	}
	p.MinimalBlockDelay += time.Duration(c.MinimalBlockDelay) * time.Second // either/or
	p.DelayIncrementPerRound = time.Duration(c.DelayIncrementPerRound) * time.Second

	// rewards
	p.SeedNonceRevelationTip = c.SeedNonceRevelationTip
	p.BlockReward = c.GetBlockReward()
	p.EndorsementReward = c.GetEndorsementReward()
	if c.HaveV6Rewards() {
		p.BlockRewardV6 = c.BakingRewardPerEndorsement_v6
		p.EndorsementRewardV6 = c.EndorsementReward_v6
	}
	p.BakingRewardFixedPortion = c.BakingRewardFixedPortion
	p.BakingRewardBonusPerSlot = c.BakingRewardBonusPerSlot
	p.EndorsingRewardPerSlot = c.EndorsingRewardPerSlot

	// costs
	p.OriginationSize = c.OriginationSize
	p.OriginationBurn = c.OriginationBurn
	p.BlockSecurityDeposit = c.BlockSecurityDeposit
	p.EndorsementSecurityDeposit = c.EndorsementSecurityDeposit
	p.CostPerByte = c.CostPerByte

	// limits
	p.MichelsonMaximumTypeSize = c.MichelsonMaximumTypeSize
	p.EndorsersPerBlock = c.EndorsersPerBlock
	p.HardGasLimitPerOperation = c.HardGasLimitPerOperation
	p.HardGasLimitPerBlock = c.HardGasLimitPerBlock
	p.HardStorageLimitPerOperation = c.HardStorageLimitPerOperation
	p.MaxOperationDataLength = c.MaxOperationDataLength
	p.MaxOperationsTTL = c.MaxOperationsTimeToLive
	p.ConsensusCommitteeSize = c.ConsensusCommitteeSize
	p.ConsensusThreshold = c.ConsensusThreshold
	p.FrozenDepositsPercentage = c.FrozenDepositsPercentage

	// votinog
	p.MinProposalQuorum = c.MinProposalQuorum
	p.QuorumMin = c.QuorumMin
	p.QuorumMax = c.QuorumMax
	p.BlocksPerVotingPeriod = c.BlocksPerVotingPeriod
	if p.BlocksPerVotingPeriod == 0 {
		p.CyclesPerVotingPeriod = c.CyclesPerVotingPeriod
		p.BlocksPerVotingPeriod = c.CyclesPerVotingPeriod * c.BlocksPerCycle
	} else {
		p.CyclesPerVotingPeriod = c.BlocksPerVotingPeriod / c.BlocksPerCycle
	}

	return p
}
