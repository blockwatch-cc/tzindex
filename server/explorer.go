// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"math"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/packdb/pack"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(Explorer{})
}

var _ RESTful = (*Explorer)(nil)

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
	r.HandleFunc("/tip", C(GetBlockchainTip)).Methods("GET")
	r.HandleFunc("/config/{height}", C(GetBlockchainConfig)).Methods("GET")
	r.HandleFunc("/status", C(GetStatus)).Methods("GET")
	return nil
}

// generic list request
type ExplorerListRequest struct {
	Limit  uint `schema:"limit"`
	Offset uint `schema:"offset"`
}

func GetStatus(ctx *ApiContext) (interface{}, int) {
	return ctx.Crawler.Status(), http.StatusOK
}

type BlockchainTip struct {
	Name        string             `json:"name"`
	Network     string             `json:"network"`
	Symbol      string             `json:"symbol"`
	ChainId     chain.ChainIdHash  `json:"chain_id"`
	GenesisTime time.Time          `json:"genesis_time"`
	BestHash    chain.BlockHash    `json:"block_hash"`
	Timestamp   time.Time          `json:"timestamp"`
	Height      int64              `json:"height"`
	Cycle       int64              `json:"cycle"`
	Deployments []model.Deployment `json:"deployments"`

	TotalAccounts  int64 `json:"total_accounts"`
	FundedAccounts int64 `json:"funded_accounts"`
	TotalOps       int64 `json:"total_ops"`
	Delegators     int64 `json:"delegators"`
	Delegates      int64 `json:"delegates"`
	Rolls          int64 `json:"rolls"`
	RollOwners     int64 `json:"roll_owners"`

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

func GetBlockchainTip(ctx *ApiContext) (interface{}, int) {
	tip := buildBlockchainTip(ctx, ctx.Crawler.Tip())
	return tip, http.StatusOK
}

func (t BlockchainTip) LastModified() time.Time {
	return t.Timestamp
}

func (t BlockchainTip) Expires() time.Time {
	return t.expires
}

func buildBlockchainTip(ctx *ApiContext, tip *model.ChainTip) *BlockchainTip {
	oneDay := 24 * time.Hour
	params := ctx.Crawler.ParamsByHeight(-1)
	// most recent chain data (medium expensive)
	ch, err := ctx.Indexer.ChainByHeight(ctx.Context, tip.BestHeight)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read tip chain state", err))
	}

	// most recent supply (medium expensive)
	supply, err := ctx.Indexer.SupplyByHeight(ctx.Context, tip.BestHeight)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read tip supply", err))
	}

	// for 30d block data (expensive call)
	growth, err := ctx.Indexer.GrowthByDuration(ctx.Context, tip.BestTime, 30*oneDay)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read growth data", err))
	}

	// for annualized inflation (medium expensive)
	supply365, err := ctx.Indexer.SupplyByTime(ctx.Context, supply.Timestamp.Add(-365*oneDay))
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read last year supply", err))
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
		Deployments: tip.Deployments,

		Cycle:          ch.Cycle,
		TotalOps:       ch.TotalOps,
		TotalAccounts:  ch.TotalAccounts,
		FundedAccounts: ch.FundedAccounts,
		Delegators:     ch.ActiveDelegators,
		Delegates:      ch.ActiveDelegates,
		Rolls:          ch.Rolls,
		RollOwners:     ch.RollOwners,

		NewAccounts30d:     growth.NewAccounts,
		ClearedAccounts30d: growth.ClearedAccounts,
		FundedAccounts30d:  growth.FundedAccounts,
		Inflation1Y:        params.ConvertValue(supply.Total - supply365.Total),
		InflationRate1Y:    annualizedPercent(supply.Total, supply365.Total, supplyDays),

		// track health over the past 128 blocks
		Health: estimateHealth(ctx, tip.BestHeight, 127),

		Supply: &Supply{
			Supply:  *supply,
			verbose: true,
			params:  params,
		},
		Status: ctx.Crawler.Status(),

		// expires when next block is expected
		expires: tip.BestTime.Add(params.TimeBetweenBlocks[0]),
	}
}

func annualizedPercent(a, b, days int64) float64 {
	if days == 0 {
		days = 1
	}
	diff := float64(a) - float64(b)
	return diff / float64(days) * 365 / float64(b) * 100.0
}

type BlockchainConfig struct {
	// chain identity
	Name        string             `json:"name"`
	Network     string             `json:"network"`
	Symbol      string             `json:"symbol"`
	ChainId     chain.ChainIdHash  `json:"chain_id"`
	Deployment  int                `json:"deployment"`
	Version     int                `json:"version"`
	Protocol    chain.ProtocolHash `json:"protocol"`
	StartHeight int64              `json:"start_height"`
	EndHeight   int64              `json:"end_height"`

	// fixed
	NoRewardCycles              int64 `json:"no_reward_cycles"`
	SecurityDepositRampUpCycles int64 `json:"security_deposit_ramp_up_cycles"`
	Decimals                    int   `json:"decimals"`
	Token                       int64 `json:"units"`

	// may change with protocol updates
	BlockReward                  float64 `json:"block_reward"`
	BlockSecurityDeposit         float64 `json:"block_security_deposit"`
	BlocksPerCommitment          int64   `json:"blocks_per_commitment"`
	BlocksPerCycle               int64   `json:"blocks_per_cycle"`
	BlocksPerRollSnapshot        int64   `json:"blocks_per_roll_snapshot"`
	BlocksPerVotingPeriod        int64   `json:"blocks_per_voting_period"`
	CostPerByte                  int64   `json:"cost_per_byte"`
	EndorsementReward            float64 `json:"endorsement_reward"`
	EndorsementSecurityDeposit   float64 `json:"endorsement_security_deposit"`
	EndorsersPerBlock            int     `json:"endorsers_per_block"`
	HardGasLimitPerBlock         int64   `json:"hard_gas_limit_per_block"`
	HardGasLimitPerOperation     int64   `json:"hard_gas_limit_per_operation"`
	HardStorageLimitPerOperation int64   `json:"hard_storage_limit_per_operation"`
	MaxOperationDataLength       int     `json:"max_operation_data_length"`
	MaxProposalsPerDelegate      int     `json:"max_proposals_per_delegate"`
	MaxRevelationsPerBlock       int     `json:"max_revelations_per_block"`
	MichelsonMaximumTypeSize     int     `json:"michelson_maximum_type_size"`
	NonceLength                  int     `json:"nonce_length"`
	OriginationBurn              float64 `json:"origination_burn"`
	OriginationSize              int64   `json:"origination_size"`
	PreservedCycles              int64   `json:"preserved_cycles"`
	ProofOfWorkNonceSize         int     `json:"proof_of_work_nonce_size"`
	ProofOfWorkThreshold         int64   `json:"proof_of_work_threshold"`
	SeedNonceRevelationTip       float64 `json:"seed_nonce_revelation_tip"`
	TimeBetweenBlocks            [2]int  `json:"time_between_blocks"`
	TokensPerRoll                float64 `json:"tokens_per_roll"`
	TestChainDuration            int64   `json:"test_chain_duration"`
	MinProposalQuorum            int64   `json:"min_proposal_quorum"`
	QuorumMin                    int64   `json:"quorum_min"`
	QuorumMax                    int64   `json:"quorum_max"`

	timestamp time.Time `json:"-"`
	expires   time.Time `json:"-"`
}

func (c BlockchainConfig) LastModified() time.Time {
	return c.timestamp
}

func (c BlockchainConfig) Expires() time.Time {
	return c.expires
}

func GetBlockchainConfig(ctx *ApiContext) (interface{}, int) {
	var height int64 = -1
	if s, ok := mux.Vars(ctx.Request)["height"]; ok && s != "" {
		var err error
		height, err = strconv.ParseInt(s, 10, 64)
		switch true {
		case s == "head":
			height = ctx.Crawler.Height()
		case err != nil:
			panic(EBadRequest(EC_PARAM_INVALID, "invalid height", err))
		}
	}
	p := ctx.Crawler.ParamsByHeight(height)
	cfg := BlockchainConfig{
		Name:                         p.Name,
		Network:                      p.Network,
		Symbol:                       p.Symbol,
		ChainId:                      p.ChainId,
		Deployment:                   p.Deployment,
		Version:                      p.Version,
		Protocol:                     p.Protocol,
		StartHeight:                  p.StartHeight,
		EndHeight:                    p.EndHeight,
		NoRewardCycles:               p.NoRewardCycles,
		SecurityDepositRampUpCycles:  p.SecurityDepositRampUpCycles,
		Decimals:                     p.Decimals,
		Token:                        p.Token,
		BlockReward:                  p.ConvertValue(p.BlockReward),
		BlockSecurityDeposit:         p.ConvertValue(p.BlockSecurityDeposit),
		BlocksPerCommitment:          p.BlocksPerCommitment,
		BlocksPerCycle:               p.BlocksPerCycle,
		BlocksPerRollSnapshot:        p.BlocksPerRollSnapshot,
		BlocksPerVotingPeriod:        p.BlocksPerVotingPeriod,
		CostPerByte:                  p.CostPerByte,
		EndorsementReward:            p.ConvertValue(p.EndorsementReward),
		EndorsementSecurityDeposit:   p.ConvertValue(p.EndorsementSecurityDeposit),
		EndorsersPerBlock:            p.EndorsersPerBlock,
		HardGasLimitPerBlock:         p.HardGasLimitPerBlock,
		HardGasLimitPerOperation:     p.HardGasLimitPerOperation,
		HardStorageLimitPerOperation: p.HardStorageLimitPerOperation,
		MaxOperationDataLength:       p.MaxOperationDataLength,
		MaxProposalsPerDelegate:      p.MaxProposalsPerDelegate,
		MaxRevelationsPerBlock:       p.MaxRevelationsPerBlock,
		MichelsonMaximumTypeSize:     p.MichelsonMaximumTypeSize,
		NonceLength:                  p.NonceLength,
		OriginationBurn:              p.ConvertValue(p.OriginationBurn),
		OriginationSize:              p.OriginationSize,
		PreservedCycles:              p.PreservedCycles,
		ProofOfWorkNonceSize:         p.ProofOfWorkNonceSize,
		ProofOfWorkThreshold:         p.ProofOfWorkThreshold,
		SeedNonceRevelationTip:       p.ConvertValue(p.SeedNonceRevelationTip),
		TimeBetweenBlocks: [2]int{
			int(p.TimeBetweenBlocks[0] / time.Second),
			int(p.TimeBetweenBlocks[1] / time.Second),
		},
		TokensPerRoll:     p.ConvertValue(p.TokensPerRoll),
		TestChainDuration: p.TestChainDuration,
		MinProposalQuorum: p.MinProposalQuorum,
		QuorumMin:         p.QuorumMin,
		QuorumMax:         p.QuorumMax,
		timestamp:         ctx.Crawler.Time(),
		expires:           ctx.Crawler.Time().Add(p.TimeBetweenBlocks[0]),
	}
	return cfg, http.StatusOK
}

// Estimates network health based on past on-chain observations.
//
// Result [0..100]
//
// Factor                  Penalty      Comment
//
// missed priorities       2            also translates past due blocks into missed prio
// missed endorsements     0.5
// orphan block            5
// double-x                10           TODO
//
// Decay function: x^(1/n)
//
func estimateHealth(ctx *ApiContext, height, history int64) int {
	nowheight := ctx.Crawler.Height()
	params := ctx.Crawler.ParamsByHeight(-1)
	isSync := ctx.Crawler.Status().Status == etl.STATE_SYNCHRONIZED
	health := 100.0
	const (
		missedPriorityPenalty = 2.0
		missedEndorsePenalty  = 0.5
		orphanPenalty         = 5.0
		doubleSignPenalty     = 10.0
	)

	blocks, err := ctx.Indexer.Table(index.BlockTableKey)
	if err != nil {
		log.Errorf("health: block table: %v", err)
		return 0
	}
	b := &model.Block{}
	err = blocks.Stream(ctx.Context, pack.Query{
		Name:  "health.blocks",
		Order: pack.OrderDesc,
		Limit: int(history),
		Conditions: pack.ConditionList{pack.Condition{
			Field: blocks.Fields().Find("h"), // search for height (include orphans)
			Mode:  pack.FilterModeGt,
			Value: height - history,
		}},
	}, func(r pack.Row) error {
		if err := r.Decode(b); err != nil {
			return err
		}
		// skip blocks past height (may happen during sync)
		if b.Height > height {
			return nil
		}

		// more weight for recent blocks
		weight := 1.0 / float64(height-b.Height+1)

		// orphan penalty
		if b.IsOrphan {
			health -= orphanPenalty * weight
			// don't proceed when orphan
			return nil
		}

		// priority penalty
		health -= float64(b.Priority) * missedPriorityPenalty * weight

		// endorsement penalty, don't count endorsements for the most recent block
		if b.Height < nowheight {
			missed := float64(params.EndorsersPerBlock - b.NSlotsEndorsed)
			health -= missed * missedEndorsePenalty * weight
		}

		return nil
	})
	if err != nil {
		log.Errorf("health: block stream: %v", err)
	}

	// check if next block is past due and estimate expected priority
	if height == nowheight && isSync {
		delay := ctx.Now.Sub(ctx.Crawler.Time())
		t1 := params.TimeBetweenBlocks[0]
		t2 := params.TimeBetweenBlocks[1]
		if t2 == 0 {
			t2 = t1
		}
		if delay > t1 {
			estprio := (delay-t1+t2/2)/t2 + 1
			health -= float64(estprio) * missedPriorityPenalty
		}
	}

	if health < 0 {
		health = 0
	}
	return int(math.Round(health))
}
