// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"time"

	"blockwatch.cc/tzgo/tezos"
)

// Params contains a subset of protocol configuration settings that are relevant
// for dapps and most indexers. For additional protocol data, call rpc.GetCustomConstants()
// with a custom data struct.
type Params struct {
	// chain identity, not part of RPC
	Network     string             `json:"network,omitempty"`
	Symbol      string             `json:"symbol,omitempty"`
	Deployment  int                `json:"deployment"`
	Version     int                `json:"version"`
	ChainId     tezos.ChainIdHash  `json:"chain_id"`
	Protocol    tezos.ProtocolHash `json:"protocol"`
	StartHeight int64              `json:"start_height"` // protocol start (may be != cycle start!!)
	EndHeight   int64              `json:"end_height"`   // protocol end (may be != cycle end!!)
	Decimals    int                `json:"decimals"`

	// sizes
	MinimalStake        int64 `json:"minimal_stake,omitempty"`
	PreservedCycles     int64 `json:"preserved_cycles,omitempty"`
	BlocksPerCycle      int64 `json:"blocks_per_cycle,omitempty"`
	BlocksPerCommitment int64 `json:"blocks_per_commitment,omitempty"`
	BlocksPerSnapshot   int64 `json:"blocks_per_snapshot,omitempty"`

	// timing
	MinimalBlockDelay      time.Duration `json:"minimal_block_delay,omitempty"`
	DelayIncrementPerRound time.Duration `json:"delay_increment_per_round,omitempty"`

	// rewards
	BlockReward              int64 `json:"block_reward,omitempty"`
	EndorsementReward        int64 `json:"endorsement_reward,omitempty"`
	BakingRewardFixedPortion int64 `json:"baking_reward_fixed_portion,omitempty"`
	BakingRewardBonusPerSlot int64 `json:"baking_reward_bonus_per_slot,omitempty"`
	EndorsingRewardPerSlot   int64 `json:"endorsing_reward_per_slot,omitempty"`
	LiquidityBakingSubsidy   int64 `json:"liquidity_baking_subsidy,omitempty"`
	SeedNonceRevelationTip   int64 `json:"seed_nonce_revelation_tip,omitempty"`

	// costs
	CostPerByte                int64 `json:"cost_per_byte,omitempty"`
	OriginationSize            int64 `json:"origination_size,omitempty"`
	OriginationBurn            int64 `json:"origination_burn,omitempty"`
	BlockSecurityDeposit       int64 `json:"block_security_deposit,omitempty"`
	EndorsementSecurityDeposit int64 `json:"endorsement_security_deposit,omitempty"`

	// limits
	EndorsersPerBlock              int   `json:"endorsers_per_block,omitempty"`
	MaxOperationsTTL               int64 `json:"max_operations_ttl,omitempty"`
	ConsensusCommitteeSize         int   `json:"consensus_committee_size,omitempty"`
	ConsensusThreshold             int   `json:"consensus_threshold,omitempty"`
	FrozenDepositsPercentage       int   `json:"frozen_deposits_percentage,omitempty"`          // <v18
	LimitOfDelegationOverBaking    int64 `json:"limit_of_delegation_over_baking,omitempty"`     // v18+
	GlobalLimitOfStakingOverBaking int64 `json:"global_limit_of_staking_over_baking,omitempty"` // v18+
	MaxSlashingPeriod              int64 `json:"max_slashing_period,omitempty"`

	// voting
	BlocksPerVotingPeriod int64 `json:"blocks_per_voting_period,omitempty"`
	MinProposalQuorum     int64 `json:"min_proposal_quorum,omitempty"`
	QuorumMin             int64 `json:"quorum_min,omitempty"`
	QuorumMax             int64 `json:"quorum_max,omitempty"`

	// extra features to follow protocol upgrades
	NumVotingPeriods int64 `json:"num_votes,omitempty"` // 5 after v008, 4 before
	StartOffset      int64 `json:"start_offset"`        // correction for cycle start
	StartCycle       int64 `json:"start_cycle"`         // correction cycle lengths
	// VoteBlockOffset  int64 `json:"vote_block_offset,omitempty"`  // correction for Edo + Florence Mainnet-only +1 bug
}

func NewParams() *Params {
	return &Params{
		Network:          "unknown",
		Symbol:           "XTZ",
		Decimals:         6,
		NumVotingPeriods: 5,          // initial:4, v008+:5
		MaxOperationsTTL: 240,        // initial:60, v011+:120, v016+:240
		BlocksPerCycle:   8192,       // meaningful default v16+
		MinimalStake:     6000000000, // v12+
		StartHeight:      -1,
		EndHeight:        -1,
		StartCycle:       -1,
	}
}

func (p *Params) WithChainId(id tezos.ChainIdHash) *Params {
	p.ChainId = id
	if p.Network == "unknown" {
		switch id {
		case Mainnet:
			p.Network = "Mainnet"
		case Ghostnet:
			p.Network = "Ghostnet"
		case Nairobinet:
			p.Network = "Nairobinet"
		case Oxfordnet:
			p.Network = "Oxfordnet"
		}
	}
	return p
}

func (p *Params) WithProtocol(h tezos.ProtocolHash) *Params {
	var ok bool
	p.Protocol = h
	p.Version, ok = Versions[h]
	if !ok {
		var max int
		for _, v := range Versions {
			if v < max {
				continue
			}
			max = v
		}
		p.Version = max + 1
		Versions[h] = p.Version
	}
	if p.Version < 8 {
		p.NumVotingPeriods = 4
	}
	return p
}

func (p *Params) WithDeployment(v int) *Params {
	p.Deployment = v
	return p
}

func (p *Params) WithNetwork(n string) *Params {
	if p.Network == "unknown" {
		p.Network = n
	}
	return p
}

func (p *Params) WithIssuance(i Issuance) *Params {
	p.BlockReward = i.BakingReward + i.BakingBonus*int64(p.ConsensusCommitteeSize-p.ConsensusThreshold)
	p.EndorsementReward = i.AttestingReward
	p.BakingRewardFixedPortion = i.BakingReward
	p.BakingRewardBonusPerSlot = i.BakingBonus
	p.EndorsingRewardPerSlot = i.AttestingReward
	p.LiquidityBakingSubsidy = i.LBSubsidy
	p.SeedNonceRevelationTip = i.SeedNonceTip
	return p
}

// convertAmount converts a floating point number, which may or may not be representable
// as an integer, to an integer type by rounding to the nearest integer.
// This is performed consistent with the General Decimal Arithmetic spec
// and according to IEEE 754-2008 roundTiesToEven
func (p Params) ConvertAmount(value float64) int64 {
	sign := int64(1)
	if value < 0 {
		sign = -1
	}
	f := value * 1000000.0
	i := int64(f)
	rem := (f - float64(i)) * float64(sign)
	if rem > 0.5 || rem == 0.5 && i*sign%2 == 1 {
		i += sign
	}
	return i
}

func (p Params) ConvertValue(amount int64) float64 {
	return float64(amount) / 1000000.0
}

// Functions that work without start/end
func (p Params) SnapshotBaseCycle(c int64) int64 {
	var offset int64 = 2
	if p.Version >= 12 {
		offset = 1
	}
	return c - (p.PreservedCycles + offset)
}

// post v18 this is no longer available, must query dynamic inflation from RPC
func (p Params) MaxBlockReward() int64 {
	if p.Version < 12 {
		return p.BlockReward + p.EndorsementReward*int64(p.EndorsersPerBlock)
	}
	return p.BlockReward + p.EndorsementReward*int64(p.ConsensusCommitteeSize)
}

func (p Params) BlockTime() time.Duration {
	return p.MinimalBlockDelay
}

func (p Params) NumEndorsers() int {
	return p.EndorsersPerBlock + p.ConsensusCommitteeSize
}

func (p Params) IsMainnet() bool {
	return p.ChainId.Equal(Mainnet)
}

func (p Params) IsPostBabylon() bool {
	return p.IsMainnet() && p.Version >= 5
}

func (p Params) IsPreBabylonHeight(height int64) bool {
	return p.IsMainnet() && height < 655360
}

func (p Params) IsPreIthacaNetworkAtStart() bool {
	return p.IsMainnet() || p.ChainId.Equal(Ghostnet)
}

// Functions requiring start/end to be available
func (p Params) HeightToCycle(height int64) int64 {
	// FIX granada early start
	s := p.StartCycle
	if height == 1589248 && p.IsMainnet() {
		s--
	}
	return s + (height-(p.StartHeight-p.StartOffset))/p.BlocksPerCycle
}

func (p Params) CycleStartHeight(c int64) int64 {
	return p.StartHeight - p.StartOffset + (c-p.StartCycle)*p.BlocksPerCycle
}

func (p Params) CycleEndHeight(c int64) int64 {
	return p.CycleStartHeight(c) + p.BlocksPerCycle - 1
}

func (p Params) CyclePosition(height int64) int64 {
	pos := (height - (p.StartHeight - p.StartOffset)) % p.BlocksPerCycle
	if pos < 0 {
		pos += p.BlocksPerCycle
	}
	return pos
}

func (p Params) IsCycleStart(height int64) bool {
	return height > 0 && (height == 1 || p.CyclePosition(height) == 0)
}

func (p Params) IsCycleEnd(height int64) bool {
	return p.CyclePosition(height)+1 == p.BlocksPerCycle
}

func (p Params) SnapshotBlock(cycle int64, index int) int64 {
	base := p.SnapshotBaseCycle(cycle)
	if base < 0 {
		return 0
	}
	offset := int64(index+1) * p.BlocksPerSnapshot
	if offset > p.BlocksPerCycle {
		offset = p.BlocksPerCycle
	}
	return p.CycleStartHeight(base) + offset - 1
}

func (p Params) SnapshotIndex(height int64) int {
	// FIX granada early start
	if height == 1589248 && p.IsMainnet() {
		return 15
	}
	return int((p.CyclePosition(height)+1)/p.BlocksPerSnapshot) - 1
}

// treat -1 as special height query that matches open interval params only
// don't correct for start offset here
func (p Params) ContainsHeight(height int64) bool {
	return (height < 0 && p.EndHeight < 0) ||
		(p.StartHeight <= height && (p.EndHeight < 0 || p.EndHeight >= height))
}

// only works one way, but that's enough for use in proxy
func (p Params) ContainsCycle(c int64) bool {
	// FIX granada early start
	s := p.StartCycle
	if c == 387 && p.IsMainnet() {
		s--
	}
	return s <= c
}
