// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Explorer{})
}

var _ server.RESTful = (*Explorer)(nil)

func PurgeCaches() {
	purgeTipStore()
	purgeMetadataStore()
}

type Explorer struct{}

func (e Explorer) LastModified() time.Time {
	return time.Now().UTC()
}

func (e Explorer) Expires() time.Time {
	return time.Time{}
}

func (e Explorer) RESTPrefix() string {
	return "/explorer"
}

func (e Explorer) RESTPath(r *mux.Router) string {
	return e.RESTPrefix()
}

func (e Explorer) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Explorer) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/tip", server.C(GetBlockchainTip)).Methods("GET")
	r.HandleFunc("/protocols", server.C(GetBlockchainProtocols)).Methods("GET")
	r.HandleFunc("/config/{height}", server.C(GetBlockchainConfig)).Methods("GET")
	r.HandleFunc("/status", server.C(GetStatus)).Methods("GET")
	return nil
}

func GetStatus(ctx *server.Context) (interface{}, int) {
	return ctx.Crawler.Status(), http.StatusOK
}

type BlockchainTip struct {
	Name        string             `json:"name"`
	Network     string             `json:"network"`
	Symbol      string             `json:"symbol"`
	ChainId     tezos.ChainIdHash  `json:"chain_id"`
	GenesisTime time.Time          `json:"genesis_time"`
	BestHash    tezos.BlockHash    `json:"block_hash"`
	Timestamp   time.Time          `json:"timestamp"`
	Height      int64              `json:"height"`
	Cycle       int64              `json:"cycle"`
	Protocol    tezos.ProtocolHash `json:"protocol"`

	TotalAccounts  int64 `json:"total_accounts"`
	TotalContracts int64 `json:"total_contracts"`
	FundedAccounts int64 `json:"funded_accounts"`
	DustAccounts   int64 `json:"dust_accounts"`
	DustDelegators int64 `json:"dust_delegators"`
	TotalOps       int64 `json:"total_ops"`
	Delegators     int64 `json:"delegators"`
	Bakers         int64 `json:"bakers"`
	Rolls          int64 `json:"rolls"`
	RollOwners     int64 `json:"roll_owners"`

	Supply *Supply           `json:"supply"`
	Status etl.CrawlerStatus `json:"status"`

	expires time.Time `json:"-"`
}

func (t BlockchainTip) LastModified() time.Time {
	return t.Timestamp
}

func (t BlockchainTip) Expires() time.Time {
	return t.expires
}

var (
	tipStore atomic.Value
	tipMutex sync.Mutex
)

func init() {
	tipStore.Store(&BlockchainTip{})
}

func purgeTipStore() {
	tipMutex.Lock()
	defer tipMutex.Unlock()
	tipStore.Store(&BlockchainTip{})
}

func getTip(ctx *server.Context) *BlockchainTip {
	ct := ctx.Tip
	tip := tipStore.Load().(*BlockchainTip)
	if tip.Height < ct.BestHeight {
		tipMutex.Lock()
		defer tipMutex.Unlock()
		// load fresh tip again, locking may have waited
		ct = ctx.Crawler.Tip()
		tip = tipStore.Load().(*BlockchainTip)
		if tip.Height < ct.BestHeight {
			tip = buildBlockchainTip(ctx, ct)
		}
		tipStore.Store(tip)
	}
	return tip
}

func GetBlockchainTip(ctx *server.Context) (interface{}, int) {
	// copy tip and update status
	tip := getTip(ctx)
	t := *tip
	t.Status = ctx.Crawler.Status()
	return t, http.StatusOK
}

func buildBlockchainTip(ctx *server.Context, tip *model.ChainTip) *BlockchainTip {
	params := ctx.Params
	// most recent chain data (medium expensive)
	ch, err := ctx.Indexer.ChainByHeight(ctx.Context, tip.BestHeight)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read tip chain state", err))
	}

	// most recent supply (medium expensive)
	supply, err := ctx.Indexer.SupplyByHeight(ctx.Context, tip.BestHeight)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read tip supply", err))
	}

	return &BlockchainTip{
		Name:        tip.Name,
		Network:     params.Network,
		Symbol:      tip.Symbol,
		ChainId:     tip.ChainId,
		GenesisTime: tip.GenesisTime,
		BestHash:    tip.BestHash,
		Timestamp:   tip.BestTime,
		Height:      tip.BestHeight,
		Protocol:    tip.Deployments[len(tip.Deployments)-1].Protocol,

		Cycle:          ch.Cycle,
		TotalOps:       ch.TotalOps,
		TotalAccounts:  ch.TotalAccounts,
		TotalContracts: ch.TotalContracts,
		FundedAccounts: ch.FundedAccounts,
		DustAccounts:   ch.DustAccounts,
		DustDelegators: ch.DustDelegators,
		Delegators:     ch.ActiveDelegators,
		Bakers:         ch.ActiveBakers,
		Rolls:          ch.Rolls,
		RollOwners:     ch.RollOwners,

		Supply: &Supply{
			Supply: *supply,
			params: params,
		},
		Status: ctx.Crawler.Status(),

		// expires when next block is expected
		expires: tip.BestTime.Add(params.BlockTime()),
	}
}

func GetBlockchainProtocols(ctx *server.Context) (interface{}, int) {
	ct := ctx.Crawler.Tip()
	return ct.Deployments, http.StatusOK
}

type BlockchainConfig struct {
	// chain identity
	Name        string             `json:"name"`
	Network     string             `json:"network"`
	Symbol      string             `json:"symbol"`
	ChainId     tezos.ChainIdHash  `json:"chain_id"`
	Deployment  int                `json:"deployment"`
	Version     int                `json:"version"`
	Protocol    tezos.ProtocolHash `json:"protocol"`
	StartHeight int64              `json:"start_height"`
	EndHeight   int64              `json:"end_height"`
	Decimals    int                `json:"decimals"`
	Token       int64              `json:"units"`
	// extra
	NumVotingPeriods int   `json:"num_voting_periods"`
	MaxOperationsTTL int64 `json:"max_operations_ttl"`

	// may change with protocol updates
	BlockReward                       float64     `json:"block_reward"`
	BlockSecurityDeposit              float64     `json:"block_security_deposit,omitempty"`
	BlocksPerCommitment               int64       `json:"blocks_per_commitment"`
	BlocksPerCycle                    int64       `json:"blocks_per_cycle"`
	BlocksPerSnapshot                 int64       `json:"blocks_per_snapshot,omitempty"`
	BlocksPerVotingPeriod             int64       `json:"blocks_per_voting_period"`
	CostPerByte                       int64       `json:"cost_per_byte"`
	EndorsementReward                 float64     `json:"endorsement_reward"`
	EndorsementSecurityDeposit        float64     `json:"endorsement_security_deposit,omitempty"`
	EndorsersPerBlock                 int         `json:"endorsers_per_block,omitempty"`
	HardGasLimitPerBlock              int64       `json:"hard_gas_limit_per_block"`
	HardGasLimitPerOperation          int64       `json:"hard_gas_limit_per_operation"`
	HardStorageLimitPerOperation      int64       `json:"hard_storage_limit_per_operation"`
	MaxOperationDataLength            int         `json:"max_operation_data_length"`
	MaxProposalsPerDelegate           int         `json:"max_proposals_per_delegate"`
	MaxRevelationsPerBlock            int         `json:"max_revelations_per_block,omitempty"`
	MichelsonMaximumTypeSize          int         `json:"michelson_maximum_type_size"`
	NonceLength                       int         `json:"nonce_length"`
	OriginationBurn                   float64     `json:"origination_burn,omitempty"`
	OriginationSize                   int64       `json:"origination_size,omitempty"`
	PreservedCycles                   int64       `json:"preserved_cycles"`
	ProofOfWorkNonceSize              int         `json:"proof_of_work_nonce_size"`
	ProofOfWorkThreshold              int64       `json:"proof_of_work_threshold"`
	SeedNonceRevelationTip            float64     `json:"seed_nonce_revelation_tip"`
	TimeBetweenBlocks                 *[2]int     `json:"time_between_blocks,omitempty"`
	TokensPerRoll                     float64     `json:"tokens_per_roll"`
	TestChainDuration                 int64       `json:"test_chain_duration,omitempty"`
	MinProposalQuorum                 int64       `json:"min_proposal_quorum,omitempty"`
	QuorumMin                         int64       `json:"quorum_min,omitempty"`
	QuorumMax                         int64       `json:"quorum_max,omitempty"`
	BlockRewardV6                     *[2]float64 `json:"block_rewards_v6,omitempty"`
	EndorsementRewardV6               *[2]float64 `json:"endorsement_rewards_v6,omitempty"`
	MaxAnonOpsPerBlock                int         `json:"max_anon_ops_per_block,omitempty"`
	LiquidityBakingEscapeEmaThreshold int64       `json:"liquidity_baking_escape_ema_threshold,omitempty"`
	LiquidityBakingSubsidy            int64       `json:"liquidity_baking_subsidy,omitempty"`
	LiquidityBakingSunsetLevel        int64       `json:"liquidity_baking_sunset_level,omitempty"`
	MinimalBlockDelay                 int         `json:"minimal_block_delay"`

	// New in Hangzhou v011
	MaxMichelineNodeCount          int `json:"max_micheline_node_count,omitempty"`
	MaxMichelineBytesLimit         int `json:"max_micheline_bytes_limit,omitempty"`
	MaxAllowedGlobalConstantsDepth int `json:"max_allowed_global_constants_depth,omitempty"`

	// New in Ithaca v012
	BakingRewardFixedPortion                         int64        `json:"baking_reward_fixed_portion,omitempty"`
	BakingRewardBonusPerSlot                         int64        `json:"baking_reward_bonus_per_slot,omitempty"`
	EndorsingRewardPerSlot                           int64        `json:"endorsing_reward_per_slot,omitempty"`
	DelayIncrementPerRound                           int          `json:"delay_increment_per_round,omitempty"`
	ConsensusCommitteeSize                           int          `json:"consensus_committee_size,omitempty"`
	ConsensusThreshold                               int          `json:"consensus_threshold,omitempty"`
	MinimalParticipationRatio                        *tezos.Ratio `json:"minimal_participation_ratio,omitempty"`
	MaxSlashingPeriod                                int64        `json:"max_slashing_period,omitempty"`
	FrozenDepositsPercentage                         int          `json:"frozen_deposits_percentage,omitempty"`
	DoubleBakingPunishment                           int64        `json:"double_baking_punishment,omitempty"`
	RatioOfFrozenDepositsSlashedPerDoubleEndorsement *tezos.Ratio `json:"ratio_of_frozen_deposits_slashed_per_double_endorsement,omitempty"`

	timestamp time.Time `json:"-"`
	expires   time.Time `json:"-"`
}

func (c BlockchainConfig) LastModified() time.Time {
	return c.timestamp
}

func (c BlockchainConfig) Expires() time.Time {
	return c.expires
}

func GetBlockchainConfig(ctx *server.Context) (interface{}, int) {
	var height int64 = -1
	if s, ok := mux.Vars(ctx.Request)["height"]; ok && s != "" {
		var err error
		height, err = strconv.ParseInt(s, 10, 64)
		switch true {
		case s == "head":
			height = ctx.Tip.BestHeight
		case err != nil:
			panic(server.EBadRequest(server.EC_PARAM_INVALID, "invalid height", err))
		}
	}
	p := ctx.Crawler.ParamsByHeight(height)
	cfg := BlockchainConfig{
		Name:                              p.Name,
		Network:                           p.Network,
		Symbol:                            p.Symbol,
		ChainId:                           p.ChainId,
		Deployment:                        p.Deployment,
		Version:                           p.Version,
		Protocol:                          p.Protocol,
		StartHeight:                       p.StartHeight,
		EndHeight:                         p.EndHeight,
		Decimals:                          p.Decimals,
		Token:                             p.Token,
		NumVotingPeriods:                  p.NumVotingPeriods,
		MaxOperationsTTL:                  p.MaxOperationsTTL,
		BlockReward:                       p.ConvertValue(p.BlockReward),
		BlockSecurityDeposit:              p.ConvertValue(p.BlockSecurityDeposit),
		BlocksPerCommitment:               p.BlocksPerCommitment,
		BlocksPerCycle:                    p.BlocksPerCycle,
		BlocksPerSnapshot:                 p.SnapshotBlocks(),
		BlocksPerVotingPeriod:             p.BlocksPerVotingPeriod,
		CostPerByte:                       p.CostPerByte,
		EndorsementReward:                 p.ConvertValue(p.EndorsementReward),
		EndorsementSecurityDeposit:        p.ConvertValue(p.EndorsementSecurityDeposit),
		EndorsersPerBlock:                 p.EndorsersPerBlock,
		HardGasLimitPerBlock:              p.HardGasLimitPerBlock,
		HardGasLimitPerOperation:          p.HardGasLimitPerOperation,
		HardStorageLimitPerOperation:      p.HardStorageLimitPerOperation,
		MaxOperationDataLength:            p.MaxOperationDataLength,
		MaxProposalsPerDelegate:           p.MaxProposalsPerDelegate,
		MaxRevelationsPerBlock:            p.MaxRevelationsPerBlock,
		MichelsonMaximumTypeSize:          p.MichelsonMaximumTypeSize,
		NonceLength:                       p.NonceLength,
		OriginationBurn:                   p.ConvertValue(p.OriginationBurn),
		OriginationSize:                   p.OriginationSize,
		PreservedCycles:                   p.PreservedCycles,
		ProofOfWorkNonceSize:              p.ProofOfWorkNonceSize,
		ProofOfWorkThreshold:              p.ProofOfWorkThreshold,
		SeedNonceRevelationTip:            p.ConvertValue(p.SeedNonceRevelationTip),
		TokensPerRoll:                     p.ConvertValue(p.TokensPerRoll),
		TestChainDuration:                 p.TestChainDuration,
		MinProposalQuorum:                 p.MinProposalQuorum,
		QuorumMin:                         p.QuorumMin,
		QuorumMax:                         p.QuorumMax,
		MaxAnonOpsPerBlock:                p.MaxAnonOpsPerBlock,
		LiquidityBakingEscapeEmaThreshold: p.LiquidityBakingEscapeEmaThreshold,
		LiquidityBakingSubsidy:            p.LiquidityBakingSubsidy,
		LiquidityBakingSunsetLevel:        p.LiquidityBakingSunsetLevel,
		MinimalBlockDelay:                 int(p.MinimalBlockDelay / time.Second),
		MaxMichelineNodeCount:             p.MaxMichelineNodeCount,
		MaxMichelineBytesLimit:            p.MaxMichelineBytesLimit,
		MaxAllowedGlobalConstantsDepth:    p.MaxAllowedGlobalConstantsDepth,
		BakingRewardFixedPortion:          p.BakingRewardFixedPortion,
		BakingRewardBonusPerSlot:          p.BakingRewardBonusPerSlot,
		EndorsingRewardPerSlot:            p.EndorsingRewardPerSlot,
		DelayIncrementPerRound:            int(p.DelayIncrementPerRound / time.Second),
		ConsensusCommitteeSize:            p.ConsensusCommitteeSize,
		ConsensusThreshold:                p.ConsensusThreshold,
		MaxSlashingPeriod:                 p.MaxSlashingPeriod,
		FrozenDepositsPercentage:          p.FrozenDepositsPercentage,
		DoubleBakingPunishment:            p.DoubleBakingPunishment,

		timestamp: ctx.Tip.BestTime,
		expires:   ctx.Tip.BestTime.Add(p.BlockTime()),
	}

	if p.TimeBetweenBlocks[0] > 0 {
		cfg.TimeBetweenBlocks = &[2]int{
			int(p.TimeBetweenBlocks[0] / time.Second),
			int(p.TimeBetweenBlocks[1] / time.Second),
		}
	}
	if p.BlockRewardV6[0] > 0 {
		cfg.BlockRewardV6 = &[2]float64{
			p.ConvertValue(p.BlockRewardV6[0]),
			p.ConvertValue(p.BlockRewardV6[1]),
		}
		cfg.EndorsementRewardV6 = &[2]float64{
			p.ConvertValue(p.EndorsementRewardV6[0]),
			p.ConvertValue(p.EndorsementRewardV6[1]),
		}
	}

	if p.MinimalParticipationRatio.Float64() > 0 {
		cfg.MinimalParticipationRatio = &p.MinimalParticipationRatio
	}
	if p.RatioOfFrozenDepositsSlashedPerDoubleEndorsement.Float64() > 0 {
		cfg.RatioOfFrozenDepositsSlashedPerDoubleEndorsement = &p.RatioOfFrozenDepositsSlashedPerDoubleEndorsement
	}

	return cfg, http.StatusOK
}

// configurable marshalling helper
type Supply struct {
	model.Supply
	params *tezos.Params // blockchain amount conversion
}

func (s *Supply) MarshalJSON() ([]byte, error) {
	supply := struct {
		RowId               uint64    `json:"row_id"`
		Height              int64     `json:"height"`
		Cycle               int64     `json:"cycle"`
		Timestamp           time.Time `json:"time"`
		Total               float64   `json:"total"`
		Activated           float64   `json:"activated"`
		Unclaimed           float64   `json:"unclaimed"`
		Circulating         float64   `json:"circulating"`
		Liquid              float64   `json:"liquid"`
		Delegated           float64   `json:"delegated"`
		Staking             float64   `json:"staking"`
		Shielded            float64   `json:"shielded"`
		ActiveDelegated     float64   `json:"active_delegated"`
		ActiveStaking       float64   `json:"active_staking"`
		InactiveDelegated   float64   `json:"inactive_delegated"`
		InactiveStaking     float64   `json:"inactive_staking"`
		Minted              float64   `json:"minted"`
		MintedBaking        float64   `json:"minted_baking"`
		MintedEndorsing     float64   `json:"minted_endorsing"`
		MintedSeeding       float64   `json:"minted_seeding"`
		MintedAirdrop       float64   `json:"minted_airdrop"`
		MintedSubsidy       float64   `json:"minted_subsidy"`
		Burned              float64   `json:"burned"`
		BurnedDoubleBaking  float64   `json:"burned_double_baking"`
		BurnedDoubleEndorse float64   `json:"burned_double_endorse"`
		BurnedOrigination   float64   `json:"burned_origination"`
		BurnedAllocation    float64   `json:"burned_allocation"`
		BurnedStorage       float64   `json:"burned_storage"`
		BurnedExplicit      float64   `json:"burned_explicit"`
		BurnedSeedMiss      float64   `json:"burned_seed_miss"`
		BurnedAbsence       float64   `json:"burned_absence"`
		Frozen              float64   `json:"frozen"`
		FrozenDeposits      float64   `json:"frozen_deposits"`
		FrozenRewards       float64   `json:"frozen_rewards"`
		FrozenFees          float64   `json:"frozen_fees"`
	}{
		RowId:               s.RowId,
		Height:              s.Height,
		Cycle:               s.Cycle,
		Timestamp:           s.Timestamp,
		Total:               s.params.ConvertValue(s.Total),
		Activated:           s.params.ConvertValue(s.Activated),
		Unclaimed:           s.params.ConvertValue(s.Unclaimed),
		Circulating:         s.params.ConvertValue(s.Circulating),
		Liquid:              s.params.ConvertValue(s.Liquid),
		Delegated:           s.params.ConvertValue(s.Delegated),
		Staking:             s.params.ConvertValue(s.Staking),
		Shielded:            s.params.ConvertValue(s.Shielded),
		ActiveDelegated:     s.params.ConvertValue(s.ActiveDelegated),
		ActiveStaking:       s.params.ConvertValue(s.ActiveStaking),
		InactiveDelegated:   s.params.ConvertValue(s.InactiveDelegated),
		InactiveStaking:     s.params.ConvertValue(s.InactiveStaking),
		Minted:              s.params.ConvertValue(s.Minted),
		MintedBaking:        s.params.ConvertValue(s.MintedBaking),
		MintedEndorsing:     s.params.ConvertValue(s.MintedEndorsing),
		MintedSeeding:       s.params.ConvertValue(s.MintedSeeding),
		MintedAirdrop:       s.params.ConvertValue(s.MintedAirdrop),
		MintedSubsidy:       s.params.ConvertValue(s.MintedSubsidy),
		Burned:              s.params.ConvertValue(s.Burned),
		BurnedDoubleBaking:  s.params.ConvertValue(s.BurnedDoubleBaking),
		BurnedDoubleEndorse: s.params.ConvertValue(s.BurnedDoubleEndorse),
		BurnedOrigination:   s.params.ConvertValue(s.BurnedOrigination),
		BurnedAllocation:    s.params.ConvertValue(s.BurnedAllocation),
		BurnedStorage:       s.params.ConvertValue(s.BurnedStorage),
		BurnedExplicit:      s.params.ConvertValue(s.BurnedExplicit),
		BurnedSeedMiss:      s.params.ConvertValue(s.BurnedSeedMiss),
		BurnedAbsence:       s.params.ConvertValue(s.BurnedAbsence),
		Frozen:              s.params.ConvertValue(s.Frozen),
		FrozenDeposits:      s.params.ConvertValue(s.FrozenDeposits),
		FrozenRewards:       s.params.ConvertValue(s.FrozenRewards),
		FrozenFees:          s.params.ConvertValue(s.FrozenFees),
	}
	return json.Marshal(supply)
}
