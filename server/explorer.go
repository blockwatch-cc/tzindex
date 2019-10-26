// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
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
	return time.Now().UTC().Add(10 * time.Second)
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
	Limit  int `schema:"limit"`
	Offset int `schema:"offset"`
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

	Health float64 `json:"health"`

	Supply *Supply           `json:"supply"`
	Status etl.CrawlerStatus `json:"status"`
}

func GetBlockchainTip(ctx *ApiContext) (interface{}, int) {
	tip := buildBlockchainTip(ctx, ctx.Crawler.Tip())
	return tip, http.StatusOK
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
	supplyDays := int64(supply.Timestamp.Sub(supply365.Timestamp) / oneDay)

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

		// TODO: implement formula and track health over time
		Health: 100,

		Supply: &Supply{
			Supply:  *supply,
			verbose: true,
			params:  params,
		},
		Status: ctx.Crawler.Status(),
	}
}

func annualizedPercent(a, b, days int64) float64 {
	diff := float64(a) - float64(b)
	return diff / float64(days) * 365 / float64(b) * 100.0
}

type BlockchainConfig struct {
	// chain identity
	Name        string             `json:"name"`
	Network     string             `json:"network"`
	Symbol      string             `json:"symbol"`
	ChainId     chain.ChainIdHash  `json:"chain_id"`
	Version     int                `json:"version"`
	Protocol    chain.ProtocolHash `json:"protocol"`
	StartHeight int64              `json:"start_height"`
	EndHeight   int64              `json:"end_height"`

	// fixed
	NoRewardCycles              int64 `json:"no_reward_cycles"`                // from mainnet genesis
	SecurityDepositRampUpCycles int64 `json:"security_deposit_ramp_up_cycles"` // increase 1/64th each cycle
	Decimals                    int   `json:"decimals"`
	Token                       int64 `json:"units"` // atomic units per token

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
	ProofOfWorkThreshold         uint64  `json:"proof_of_work_threshold"`
	SeedNonceRevelationTip       float64 `json:"seed_nonce_revelation_tip"`
	TimeBetweenBlocks            [2]int  `json:"time_between_blocks"`
	TokensPerRoll                float64 `json:"tokens_per_roll"`
	TestChainDuration            int64   `json:"test_chain_duration"`
	MinProposalQuorum            int64   `json:"min_proposal_quorum"`
	QuorumMin                    int64   `json:"quorum_min"`
	QuorumMax                    int64   `json:"quorum_max"`
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
	}
	return cfg, http.StatusOK
}
