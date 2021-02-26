// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

import (
	"time"
)

type Params struct {
	// chain identity, not part of RPC
	Name        string       `json:"name"`
	Network     string       `json:"network"`
	Symbol      string       `json:"symbol"`
	Deployment  int          `json:"deployment"` // as activated on chain
	Version     int          `json:"version"`    // as implemented
	ChainId     ChainIdHash  `json:"chain_id"`
	Protocol    ProtocolHash `json:"protocol"`
	StartHeight int64        `json:"start_height"`
	EndHeight   int64        `json:"end_height"`
	Decimals    int          `json:"decimals"`
	Token       int64        `json:"units"` // atomic units per token

	NoRewardCycles               int64            `json:"no_reward_cycles"`                // from mainnet genesis
	SecurityDepositRampUpCycles  int64            `json:"security_deposit_ramp_up_cycles"` // increase 1/64th each cycle
	PreservedCycles              int64            `json:"preserved_cycles"`
	BlocksPerCycle               int64            `json:"blocks_per_cycle"`
	BlocksPerCommitment          int64            `json:"blocks_per_commitment"`
	BlocksPerRollSnapshot        int64            `json:"blocks_per_roll_snapshot"`
	BlocksPerVotingPeriod        int64            `json:"blocks_per_voting_period"`
	TimeBetweenBlocks            [2]time.Duration `json:"time_between_blocks"`
	EndorsersPerBlock            int              `json:"endorsers_per_block"`
	HardGasLimitPerOperation     int64            `json:"hard_gas_limit_per_operation"`
	HardGasLimitPerBlock         int64            `json:"hard_gas_limit_per_block"`
	ProofOfWorkThreshold         int64            `json:"proof_of_work_threshold"`
	ProofOfWorkNonceSize         int              `json:"proof_of_work_nonce_size"`
	TokensPerRoll                int64            `json:"tokens_per_roll"`
	MichelsonMaximumTypeSize     int              `json:"michelson_maximum_type_size"`
	SeedNonceRevelationTip       int64            `json:"seed_nonce_revelation_tip"`
	OriginationSize              int64            `json:"origination_size"`
	OriginationBurn              int64            `json:"origination_burn"`
	BlockSecurityDeposit         int64            `json:"block_security_deposit"`
	EndorsementSecurityDeposit   int64            `json:"endorsement_security_deposit"`
	BlockReward                  int64            `json:"block_reward"`
	EndorsementReward            int64            `json:"endorsement_reward"`
	BlockRewardV6                [2]int64         `json:"block_rewards_v6"`
	EndorsementRewardV6          [2]int64         `json:"endorsement_rewards_v6"`
	CostPerByte                  int64            `json:"cost_per_byte"`
	HardStorageLimitPerOperation int64            `json:"hard_storage_limit_per_operation"`
	TestChainDuration            int64            `json:"test_chain_duration"`
	MaxOperationDataLength       int              `json:"max_operation_data_length"`
	MaxProposalsPerDelegate      int              `json:"max_proposals_per_delegate"`
	MaxRevelationsPerBlock       int              `json:"max_revelations_per_block"`
	NonceLength                  int              `json:"nonce_length"`

	// New in Bablyon v005
	MinProposalQuorum int64 `json:"min_proposal_quorum"`
	QuorumMin         int64 `json:"quorum_min"`
	QuorumMax         int64 `json:"quorum_max"`

	// New in Delphi v007
	MaxAnonOpsPerBlock int `json:"max_anon_ops_per_block"` // was max_revelations_per_block

	// hidden invoice feature
	Invoices map[string]int64 `json:"invoices,omitempty"`

	// extra features to follow protocol upgrades
	SilentSpendable      bool  `json:"silent_spendable"` // contracts are spendable/delegatable without flag set
	HasOriginationBug    bool  `json:"has_origination_bug"`
	ReactivateByTx       bool  `json:"reactivate_by_tx"`
	OperationTagsVersion int   `json:"operation_tags_version"`
	NumVotingPeriods     int   `json:"num_voting_periods"`
	StartBlockOffset     int64 `json:"vote_block_offset"` // correct voting start/end detection
}

func NewParams() *Params {
	return &Params{
		Name:             "Tezos",
		Network:          "",
		Symbol:           "XTZ",
		StartHeight:      -1,
		EndHeight:        -1,
		Decimals:         6,
		Token:            1000000,
		NumVotingPeriods: 4,
	}
}

// convertAmount converts a floating point number, which may or may not be representable
// as an integer, to an integer type by rounding to the nearest integer.
// This is performed consistent with the General Decimal Arithmetic spec
// and according to IEEE 754-2008 roundTiesToEven
func (p *Params) ConvertAmount(value float64) int64 {
	sign := int64(1)
	if value < 0 {
		sign = -1
	}
	f := value * float64(p.Token)
	i := int64(f)
	rem := (f - float64(i)) * float64(sign)
	if rem > 0.5 || rem == 0.5 && i*sign%2 == 1 {
		i += sign
	}
	return i
}

func (p *Params) ConvertValue(amount int64) float64 {
	return float64(amount) / float64(p.Token)
}

func (p *Params) IsCycleStart(height int64) bool {
	return height > 0 && (height-1)%p.BlocksPerCycle == 0
}

func (p *Params) IsCycleEnd(height int64) bool {
	return height > 0 && height%p.BlocksPerCycle == 0
}

func (p *Params) CycleFromHeight(height int64) int64 {
	if height == 0 {
		return 0
	}
	return (height - 1) / p.BlocksPerCycle
}

func (p *Params) CycleStartHeight(cycle int64) int64 {
	return cycle*p.BlocksPerCycle + 1
}

func (p *Params) CycleEndHeight(cycle int64) int64 {
	return (cycle + 1) * p.BlocksPerCycle
}

func (p *Params) SnapshotBlock(cycle, index int64) int64 {
	if cycle < p.PreservedCycles+2 {
		return 0
	}
	return p.CycleStartHeight(cycle-(p.PreservedCycles+2)) + (index+1)*p.BlocksPerRollSnapshot - 1
}

func (p *Params) VotingStartCycleFromHeight(height int64) int64 {
	currentCycle := p.CycleFromHeight(height)
	offset := (height - p.StartBlockOffset) % p.BlocksPerVotingPeriod
	return currentCycle - offset/p.BlocksPerCycle
}

func (p *Params) IsVoteStart(height int64) bool {
	return height > 0 && (height-p.StartBlockOffset-1)%p.BlocksPerVotingPeriod == 0
}

func (p *Params) IsVoteEnd(height int64) bool {
	return height > 0 && (height-p.StartBlockOffset)%p.BlocksPerVotingPeriod == 0
}

func (p *Params) MaxBlockReward() int64 {
	return p.BlockReward + p.EndorsementReward*int64(p.EndorsersPerBlock)
}

func (p *Params) ContainsHeight(height int64) bool {
	// treat -1 as special height query that matches open interval params only
	return (height < 0 && p.EndHeight < 0) ||
		(p.StartHeight <= height && (p.EndHeight < 0 || p.EndHeight >= height))
}

func (p *Params) IsMainnet() bool {
	return p.ChainId.IsEqual(Mainnet)
}
