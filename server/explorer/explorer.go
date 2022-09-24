// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/pack"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Explorer{})
}

var _ server.RESTful = (*Explorer)(nil)

func PurgeCaches() {
	purgeCycleStore()
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
	r.HandleFunc("/config/{ident}", server.C(GetBlockchainConfig)).Methods("GET")
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
	TotalRollups   int64 `json:"total_rollups"`
	FundedAccounts int64 `json:"funded_accounts"`
	DustAccounts   int64 `json:"dust_accounts"`
	DustDelegators int64 `json:"dust_delegators"`
	TotalOps       int64 `json:"total_ops"`
	Delegators     int64 `json:"delegators"`
	Bakers         int64 `json:"bakers"`

	NewAccounts30d     int64   `json:"new_accounts_30d"`
	ClearedAccounts30d int64   `json:"cleared_accounts_30d"`
	FundedAccounts30d  int64   `json:"funded_accounts_30d"`
	Inflation1Y        float64 `json:"inflation_1y"`
	InflationRate1Y    float64 `json:"inflation_rate_1y"`

	Health int `json:"health"`

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
	oneDay := 24 * time.Hour
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

	// for 30d block data (expensive call)
	growth, err := ctx.Indexer.GrowthByDuration(ctx.Context, tip.BestTime, 30*oneDay)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read growth data", err))
	}

	// for annualized inflation (medium expensive)
	supply365, err := ctx.Indexer.SupplyByTime(ctx.Context, supply.Timestamp.Add(-365*oneDay))
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read last year supply", err))
	}
	supplyDays := int64(supply.Timestamp.Truncate(oneDay).Sub(supply365.Timestamp.Truncate(oneDay)) / oneDay)

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
		TotalRollups:   ch.TotalRollups,
		FundedAccounts: ch.FundedAccounts,
		DustAccounts:   ch.DustAccounts,
		DustDelegators: ch.DustDelegators,
		Delegators:     ch.ActiveDelegators,
		Bakers:         ch.ActiveBakers,

		NewAccounts30d:     growth.NewAccounts,
		ClearedAccounts30d: growth.ClearedAccounts,
		FundedAccounts30d:  growth.FundedAccounts,
		Inflation1Y:        params.ConvertValue(supply.Total - supply365.Total),
		InflationRate1Y:    annualizedPercent(supply.Total, supply365.Total, supplyDays),

		// track health over the past 128 blocks
		Health: estimateHealth(ctx, tip.BestHeight, 127),

		Supply: &Supply{
			Supply: *supply,
			params: params,
		},
		Status: ctx.Crawler.Status(),

		// expires when next block is expected
		expires: tip.BestTime.Add(params.BlockTime()),
	}
}

func annualizedPercent(a, b, days int64) float64 {
	if days == 0 {
		days = 1
	}
	diff := float64(a) - float64(b)
	return diff / float64(days) * 365 / float64(b) * 100.0
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

	TokensPerRoll       float64 `json:"tokens_per_roll"`
	PreservedCycles     int64   `json:"preserved_cycles"`
	BlocksPerCommitment int64   `json:"blocks_per_commitment"`
	BlocksPerCycle      int64   `json:"blocks_per_cycle"`
	BlocksPerSnapshot   int64   `json:"blocks_per_snapshot,omitempty"`

	// timing
	TimeBetweenBlocks      *[2]int `json:"time_between_blocks,omitempty"`
	MinimalBlockDelay      int     `json:"minimal_block_delay"`
	DelayIncrementPerRound int     `json:"delay_increment_per_round,omitempty"`

	// rewards
	BlockReward              float64     `json:"block_reward"`
	EndorsementReward        float64     `json:"endorsement_reward"`
	BlockRewardV6            *[2]float64 `json:"block_rewards_v6,omitempty"`
	EndorsementRewardV6      *[2]float64 `json:"endorsement_rewards_v6,omitempty"`
	SeedNonceRevelationTip   float64     `json:"seed_nonce_revelation_tip"`
	BakingRewardFixedPortion int64       `json:"baking_reward_fixed_portion,omitempty"`
	BakingRewardBonusPerSlot int64       `json:"baking_reward_bonus_per_slot,omitempty"`
	EndorsingRewardPerSlot   int64       `json:"endorsing_reward_per_slot,omitempty"`

	// costs
	OriginationBurn            float64 `json:"origination_burn,omitempty"`
	OriginationSize            int64   `json:"origination_size,omitempty"`
	CostPerByte                int64   `json:"cost_per_byte"`
	BlockSecurityDeposit       float64 `json:"block_security_deposit,omitempty"`
	EndorsementSecurityDeposit float64 `json:"endorsement_security_deposit,omitempty"`
	FrozenDepositsPercentage   int     `json:"frozen_deposits_percentage,omitempty"`

	// limits
	MichelsonMaximumTypeSize     int   `json:"michelson_maximum_type_size"`
	EndorsersPerBlock            int   `json:"endorsers_per_block,omitempty"`
	HardGasLimitPerBlock         int64 `json:"hard_gas_limit_per_block"`
	HardGasLimitPerOperation     int64 `json:"hard_gas_limit_per_operation"`
	HardStorageLimitPerOperation int64 `json:"hard_storage_limit_per_operation"`
	MaxOperationDataLength       int   `json:"max_operation_data_length"`
	MaxOperationsTTL             int64 `json:"max_operations_ttl"`
	ConsensusCommitteeSize       int   `json:"consensus_committee_size,omitempty"`
	ConsensusThreshold           int   `json:"consensus_threshold,omitempty"`

	// voting
	NumVotingPeriods      int   `json:"num_voting_periods"`
	BlocksPerVotingPeriod int64 `json:"blocks_per_voting_period"`
	MinProposalQuorum     int64 `json:"min_proposal_quorum,omitempty"`
	QuorumMin             int64 `json:"quorum_min,omitempty"`
	QuorumMax             int64 `json:"quorum_max,omitempty"`

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
	b := loadBlock(ctx)
	p := ctx.Crawler.ParamsByHeight(b.Height)
	cfg := BlockchainConfig{
		Name:        p.Name,
		Network:     p.Network,
		Symbol:      p.Symbol,
		ChainId:     p.ChainId,
		Deployment:  p.Deployment,
		Version:     p.Version,
		Protocol:    p.Protocol,
		StartHeight: p.StartHeight,
		EndHeight:   p.EndHeight,
		Decimals:    p.Decimals,
		Token:       p.Token,

		// main
		TokensPerRoll:       p.ConvertValue(p.TokensPerRoll),
		PreservedCycles:     p.PreservedCycles,
		BlocksPerCommitment: p.BlocksPerCommitment,
		BlocksPerCycle:      p.BlocksPerCycle,
		BlocksPerSnapshot:   p.SnapshotBlocks(),

		// timing
		MinimalBlockDelay:      int(p.MinimalBlockDelay / time.Second),
		DelayIncrementPerRound: int(p.DelayIncrementPerRound / time.Second),

		// rewards
		BlockReward:              p.ConvertValue(p.BlockReward),
		EndorsementReward:        p.ConvertValue(p.EndorsementReward),
		SeedNonceRevelationTip:   p.ConvertValue(p.SeedNonceRevelationTip),
		BakingRewardFixedPortion: p.BakingRewardFixedPortion,
		BakingRewardBonusPerSlot: p.BakingRewardBonusPerSlot,
		EndorsingRewardPerSlot:   p.EndorsingRewardPerSlot,

		// costs
		CostPerByte:                p.CostPerByte,
		OriginationBurn:            p.ConvertValue(p.OriginationBurn),
		OriginationSize:            p.OriginationSize,
		BlockSecurityDeposit:       p.ConvertValue(p.BlockSecurityDeposit),
		EndorsementSecurityDeposit: p.ConvertValue(p.EndorsementSecurityDeposit),
		FrozenDepositsPercentage:   p.FrozenDepositsPercentage,

		// limits
		MichelsonMaximumTypeSize:     p.MichelsonMaximumTypeSize,
		EndorsersPerBlock:            p.EndorsersPerBlock,
		HardGasLimitPerBlock:         p.HardGasLimitPerBlock,
		HardGasLimitPerOperation:     p.HardGasLimitPerOperation,
		HardStorageLimitPerOperation: p.HardStorageLimitPerOperation,
		MaxOperationDataLength:       p.MaxOperationDataLength,
		MaxOperationsTTL:             p.MaxOperationsTTL,
		ConsensusCommitteeSize:       p.ConsensusCommitteeSize,
		ConsensusThreshold:           p.ConsensusThreshold,

		// voting
		NumVotingPeriods:      p.NumVotingPeriods,
		BlocksPerVotingPeriod: p.BlocksPerVotingPeriod,
		MinProposalQuorum:     p.MinProposalQuorum,
		QuorumMin:             p.QuorumMin,
		QuorumMax:             p.QuorumMax,

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

	return cfg, http.StatusOK
}

// Estimates network health based on past on-chain observations.
//
// Result [0..100]
//
// # Factor                  Penalty      Comment
//
// missed priorities       2            also translates past due blocks into missed prio
// missed endorsements     50/size
// double-x                10           TODO
//
// Decay function: x^(1/n)
func estimateHealth(ctx *server.Context, height, history int64) int {
	nowheight := ctx.Tip.BestHeight
	params := ctx.Params
	isSync := ctx.Crawler.Status().Status == etl.STATE_SYNCHRONIZED
	health := 100.0
	const (
		missedRoundPenalty = 2.0
		doubleSignPenalty  = 10.0
	)
	var missedEndorsePenalty = 50.0 / float64(params.EndorsersPerBlock+params.ConsensusCommitteeSize) / float64(history)

	blocks, err := ctx.Indexer.Table(index.BlockTableKey)
	if err != nil {
		log.Errorf("health: block table: %v", err)
		return 0
	}
	b := &model.Block{}
	err = pack.NewQuery("health.blocks").
		WithTable(blocks).
		WithDesc().
		WithLimit(int(history)).
		AndGt("height", height-history).
		Stream(ctx.Context, func(r pack.Row) error {
			if err := r.Decode(b); err != nil {
				return err
			}
			// skip blocks past height (may happen during sync)
			if b.Height > height {
				return nil
			}

			// more weight for recent blocks
			weight := 1.0 / float64(height-b.Height+1)

			// priority penalty
			health -= float64(b.Round) * missedRoundPenalty * weight
			// if b.Round > 0 {
			// log.Warnf("Health penalty %.3f due to %d missed priorities at block %d",
			// 	float64(b.Priority)*missedRoundPenalty*weight, b.Priority, b.Height)
			// }

			// endorsement penalty, don't count endorsements for the most recent block
			if b.Height < nowheight {
				missed := float64(params.EndorsersPerBlock + params.ConsensusCommitteeSize - b.NSlotsEndorsed)
				health -= missed * missedEndorsePenalty * weight
				// if missed > 0 {
				// 	log.Warnf("Health penalty %.3f due to %d missed endorsements at block %d",
				// 		missed*missedEndorsePenalty*weight, int(missed), b.Height)
				// }
			}

			return nil
		})
	if err != nil {
		log.Errorf("health: block stream: %v", err)
	}

	// check if next block is past due and estimate expected priority
	if height == nowheight && isSync {
		delay := ctx.Now.Sub(ctx.Tip.BestTime)
		t1 := params.BlockTime()
		t2 := params.TimeBetweenBlocks[1]
		if t2 == 0 {
			t2 = t1
		}
		if delay > t1 {
			var round int = int((delay-t1+t2/2)/t2) + 1
			if params.Version >= 12 {
				round = 1
				for d := delay - t1; d > 0; d -= t1 + time.Duration(round-1)*params.DelayIncrementPerRound {
					round++
				}
			}
			health -= float64(round) * missedRoundPenalty
			// log.Warnf("Health penalty %.3f due to %s overdue next block %d [%d]",
			// 	float64(estprio)*missedRoundPenalty, delay, nowheight+1, estprio)
		}
	}

	if health < 0 {
		health = 0
	}
	return int(math.Round(health))
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
		ActiveStake         float64   `json:"active_stake"`
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
		BurnedRollup        float64   `json:"burned_rollup"`
		Frozen              float64   `json:"frozen"`
		FrozenDeposits      float64   `json:"frozen_deposits"`
		FrozenRewards       float64   `json:"frozen_rewards"`
		FrozenFees          float64   `json:"frozen_fees"`
		FrozenBonds         float64   `json:"frozen_bonds"`
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
		ActiveStake:         s.params.ConvertValue(s.ActiveStake),
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
		BurnedRollup:        s.params.ConvertValue(s.BurnedRollup),
		Frozen:              s.params.ConvertValue(s.Frozen),
		FrozenDeposits:      s.params.ConvertValue(s.FrozenDeposits),
		FrozenRewards:       s.params.ConvertValue(s.FrozenRewards),
		FrozenFees:          s.params.ConvertValue(s.FrozenFees),
		FrozenBonds:         s.params.ConvertValue(s.FrozenBonds),
	}
	return json.Marshal(supply)
}
