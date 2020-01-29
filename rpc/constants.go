// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/tzindex/chain"
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
	BlockReward                  int64    `json:"block_reward,string"`
	EndorsementReward            int64    `json:"endorsement_reward,string"`
	CostPerByte                  int64    `json:"cost_per_byte,string"`
	HardStorageLimitPerOperation int64    `json:"hard_storage_limit_per_operation,string"`
	TestChainDuration            int64    `json:"test_chain_duration,string"`
	MaxOperationDataLength       int      `json:"max_operation_data_length"`
	MaxProposalsPerDelegate      int      `json:"max_proposals_per_delegate"`
	MaxRevelationsPerBlock       int      `json:"max_revelations_per_block"`
	NonceLength                  int      `json:"nonce_length"`

	// Zeronet only
	InitialEndorsers           int `json:"initial_endorsers"`
	DelayPerMissingEndorsement int `json:"delay_per_missing_endorsement,string"`

	// New in Bablyon v005
	MinProposalQuorum int64 `json:"min_proposal_quorum"`
	QuorumMin         int64 `json:"quorum_min"`
	QuorumMax         int64 `json:"quorum_max"`
}

// GetConstants returns chain configuration constants at a block hash
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-constants
func (c *Client) GetConstants(ctx context.Context, blockID chain.BlockHash) (Constants, error) {
	var con Constants
	u := fmt.Sprintf("chains/%s/blocks/%s/context/constants", c.ChainID, blockID)
	if err := c.Get(ctx, u, &con); err != nil {
		return con, err
	}
	return con, nil
}

// GetConstantsHeight returns chain configuration constants at a block height
// https://tezos.gitlab.io/tezos/api/rpc.html#get-block-id-context-constants
func (c *Client) GetConstantsHeight(ctx context.Context, height int64) (Constants, error) {
	var con Constants
	u := fmt.Sprintf("chains/%s/blocks/%d/context/constants", c.ChainID, height)
	if err := c.Get(ctx, u, &con); err != nil {
		return con, err
	}
	return con, nil
}

func (c Constants) MapToChainParams() *chain.Params {
	p := chain.NewParams()
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
	p.BlockReward = c.BlockReward
	p.EndorsementReward = c.EndorsementReward
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

	for i, v := range c.TimeBetweenBlocks {
		if i > 1 {
			break
		}
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Errorf("parsing TimeBetweenBlocks: %v", err)
		} else {
			p.TimeBetweenBlocks[i] = time.Duration(val) * time.Second
		}
	}

	return p
}
