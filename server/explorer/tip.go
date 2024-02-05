// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

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
	GhostAccounts  int64 `json:"ghost_accounts"`
	TotalOps       int64 `json:"total_ops"`
	Delegators     int64 `json:"delegators"`
	Stakers        int64 `json:"stakers"`
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
		GhostAccounts:  ch.GhostAccounts,
		Delegators:     ch.ActiveDelegators,
		Stakers:        ch.ActiveStakers,
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
		expires: ctx.Expires,
	}
}

func annualizedPercent(a, b, days int64) float64 {
	if days == 0 {
		days = 1
	}
	diff := float64(a) - float64(b)
	return diff / float64(days) * 365 / float64(b) * 100.0
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

	blocks, err := ctx.Indexer.Table(model.BlockTableKey)
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
			//  float64(b.Priority)*missedRoundPenalty*weight, b.Priority, b.Height)
			// }

			// endorsement penalty, don't count endorsements for the most recent block
			if b.Height < nowheight {
				missed := float64(params.EndorsersPerBlock + params.ConsensusCommitteeSize - b.NSlotsEndorsed)
				health -= missed * missedEndorsePenalty * weight
				// if missed > 0 {
				//  log.Warnf("Health penalty %.3f due to %d missed endorsements at block %d",
				//      missed*missedEndorsePenalty*weight, int(missed), b.Height)
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
		t1 := params.MinimalBlockDelay
		t2 := params.DelayIncrementPerRound
		if delay > t1 {
			round := 1
			for d := delay - t1; d > 0; d -= t1 + time.Duration(round-1)*t2 {
				round++
			}
			health -= float64(round) * missedRoundPenalty
			// log.Warnf("Health penalty %.3f due to %s overdue next block %d [%d]",
			//  float64(estprio)*missedRoundPenalty, delay, nowheight+1, estprio)
		}
	}

	if health < 0 {
		health = 0
	}
	return int(math.Round(health))
}
