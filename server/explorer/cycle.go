// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/tzindex/server"
)

// keep a cache of past cycles to avoid expensive database lookups
type (
	cycleMap map[int64]*Cycle
)

var (
	cycleMapStore atomic.Value
	cycleMutex    sync.Mutex
)

func init() {
	server.Register(Cycle{})
	cycleMapStore.Store(make(cycleMap))
}

func purgeCycleStore() {
	cycleMutex.Lock()
	defer cycleMutex.Unlock()
	cycleMapStore.Store(make(cycleMap))
}

// use read-mostly cache for complete cycles
func lookupOrBuildCycle(ctx *server.Context, id int64) *Cycle {
	if id < 0 {
		return nil
	}
	m := cycleMapStore.Load().(cycleMap)
	c, ok := m[id]
	if !ok {
		// lazy load under lock to avoid duplicate calls while building
		cycleMutex.Lock()
		defer cycleMutex.Unlock()
		// check again after aquiring the lock
		m = cycleMapStore.Load().(cycleMap)
		c, ok = m[id]
		if !ok {
			c = NewCycle(ctx, id)
			// cycles are final when complete and when snapshot was taken
			if c != nil && c.IsComplete && c.IsSnapshot {
				m2 := make(cycleMap) // create a new map
				for k, v := range m {
					m2[k] = v // copy all data
				}
				m2[id] = c // add new cycle data
				cycleMapStore.Store(m2)
			}
		}
	}
	// always return a copy
	cc := &Cycle{}
	*cc = *c
	return cc
}

var _ server.RESTful = (*Cycle)(nil)

type Cycle struct {
	Cycle       int64     `json:"cycle"`
	StartHeight int64     `json:"start_height"`
	EndHeight   int64     `json:"end_height"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`
	Progress    float64   `json:"progress"`
	IsComplete  bool      `json:"is_complete"` // all blocks baked, cycle complete
	IsSnapshot  bool      `json:"is_snapshot"` // snapshot and rights for cycle N+7 exist
	IsActive    bool      `json:"is_active"`   // cycle is in progress

	// this cycle staking data (at snapshot block or last available block)
	SnapshotHeight   int64     `json:"snapshot_height"` // -1 when no snapshot
	SnapshotIndex    int       `json:"snapshot_index"`  // -1 when no snapshot
	SnapshotTime     time.Time `json:"snapshot_time"`   // zero when no snapshot
	EligibleBakers   int64     `json:"eligible_bakers"`
	ActiveDelegators int64     `json:"active_delegators"`
	ActiveStakers    int64     `json:"active_stakers"`
	ActiveBakers     int64     `json:"active_bakers"`
	StakingSupply    float64   `json:"staking_supply"`
	StakingPercent   float64   `json:"staking_percent"` // of total supply

	// health data across all blocks in cycle (empty for future cycles)
	UniqueBakers       int     `json:"unique_bakers"`
	MissedRounds       int     `json:"missed_rounds"`
	MissedEndorsements int     `json:"missed_endorsements"`
	N2Baking           int     `json:"n_double_baking"`
	N2Endorsement      int     `json:"n_double_endorsement"`
	SolveTimeMin       int     `json:"solvetime_min"`
	SolveTimeMax       int     `json:"solvetime_max"`
	SolveTimeMean      float64 `json:"solvetime_mean"`
	RoundMin           int     `json:"round_min"`
	RoundMax           int     `json:"round_max"`
	RoundMean          float64 `json:"round_mean"`
	EndorsementRate    float64 `json:"endorsement_rate"`
	EndorsementsMin    int     `json:"endorsements_min"`
	EndorsementsMax    int     `json:"endorsements_max"`
	EndorsementsMean   float64 `json:"endorsements_mean"`
	SeedNonceRate      float64 `json:"seed_rate"` // from ops
	WorstBakedBlock    int64   `json:"worst_baked_block"`
	WorstEndorsedBlock int64   `json:"worst_endorsed_block"`

	// issuance
	BlockReward              int64 `json:"block_reward"`
	BlockBonusPerSlot        int64 `json:"block_bonus_per_slot"`
	MaxBlockReward           int64 `json:"max_block_reward"`
	EndorsementRewardPerSlot int64 `json:"endorsement_reward_per_slot"`
	NonceRevelationReward    int64 `json:"nonce_revelation_reward"`
	VdfRevelationReward      int64 `json:"vdf_revelation_reward"`
	LBSubsidy                int64 `json:"lb_subsidy"`

	// snapshot cycle who defined rights for this cycle
	SnapshotCycle *Cycle `json:"snapshot_cycle,omitempty"`

	// future cycle who's rights are defined by this cycle
	FollowerCycle *Cycle `json:"follower_cycle,omitempty"`

	// cache hint
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func NewCycle(ctx *server.Context, id int64) *Cycle {
	// get latest params
	p := ctx.Params

	// get current status
	nowheight := ctx.Tip.BestHeight
	nowcycle := p.HeightToCycle(nowheight)

	// get params that were or are active at cycle (future safe, will return latest)
	p = ctx.Crawler.ParamsByCycle(id)

	// this cycle start/end
	start, end := p.CycleStartHeight(id), p.CycleEndHeight(id)

	// Ithaca changes the distance
	var offset int64 = 2
	if p.Version >= 12 {
		offset = 1
	}
	numEndorsers := p.EndorsersPerBlock + p.ConsensusCommitteeSize

	// fill common data for every cycle state (past, active, future)
	ec := &Cycle{
		Cycle:              id,
		StartHeight:        start,
		EndHeight:          end,
		Progress:           0,
		IsComplete:         end < nowheight, // only complete when last endorsements are fixed
		IsSnapshot:         id <= nowcycle-offset,
		IsActive:           id == nowcycle,
		SnapshotHeight:     -1,
		SnapshotIndex:      -1,
		WorstBakedBlock:    -1, // when all are qual
		WorstEndorsedBlock: -1, // when all are equal
	}

	// set times
	if ec.IsComplete {
		ec.StartTime = ctx.Indexer.LookupBlockTime(ctx.Context, start)
		ec.EndTime = ctx.Indexer.LookupBlockTime(ctx.Context, end)
	} else {
		nowtime := ctx.Tip.BestTime
		ec.StartTime = ctx.Indexer.LookupBlockTime(ctx.Context, start)
		if ec.StartTime.IsZero() {
			ec.StartTime = nowtime.Add(time.Duration(start-nowheight) * p.BlockTime())
		}
		ec.EndTime = nowtime.Add(time.Duration(end-nowheight) * p.BlockTime())
	}

	var (
		maxEndorse int        // scaled to current blocks in cycle
		maxSeeds   int        // unscaled, full value (since requirement is from snapshot)
		snapHeight int64 = -1 // selected or latest snapshot block
	)

	maxSeeds = int(p.BlocksPerCycle / p.BlocksPerCommitment)
	if ec.IsComplete {
		ec.Progress = 100
		maxEndorse = numEndorsers * int(p.BlocksPerCycle)
		snapHeight = end
	} else if ec.IsActive {
		ec.Progress = float64((nowheight-start)*100) / float64(p.BlocksPerCycle)
		// latest block cannot have an endorsement yet, so we don't require it
		// otherwise the formula would be (nowheight - start + 1)*p.EndorsersPerBlock
		maxEndorse = int(nowheight-start) * numEndorsers
		snapHeight = nowheight - (nowheight % p.BlocksPerSnapshot)
	}

	// load cycle from index db and fill stats, don't fail
	if ic, err := ctx.Indexer.CycleByNum(ctx, id); err == nil {
		ec.SnapshotHeight = ic.SnapshotHeight
		snapHeight = ic.SnapshotHeight
		ec.SnapshotIndex = ic.SnapshotIndex
		ec.SnapshotTime = ctx.Indexer.LookupBlockTime(ctx, ec.SnapshotHeight)
		ec.MissedRounds = ic.MissedRounds
		ec.MissedEndorsements = ic.MissedEndorsements
		ec.WorstBakedBlock = ic.WorstBakedBlock
		ec.WorstEndorsedBlock = ic.WorstEndorsedBlock
		ec.N2Baking = ic.Num2Baking
		ec.N2Endorsement = ic.Num2Endorsement

		ec.UniqueBakers = ic.UniqueBakers
		ec.SolveTimeMin = ic.SolveTimeMin
		ec.SolveTimeMax = ic.SolveTimeMax
		ec.SolveTimeMean = float64(ic.SolveTimeSum) / float64(p.BlocksPerCycle)
		ec.RoundMin = ic.RoundMin
		ec.RoundMax = ic.RoundMax
		ec.RoundMean = float64(ic.MissedRounds) / float64(p.BlocksPerCycle)
		ec.EndorsementsMin = ic.EndorsementsMin
		ec.EndorsementsMax = ic.EndorsementsMax
		ec.EndorsementsMean = float64(maxEndorse-ic.MissedEndorsements) / float64(p.BlocksPerCycle)

		ec.BlockReward = ic.BlockReward
		ec.BlockBonusPerSlot = ic.BlockBonusPerSlot
		ec.MaxBlockReward = ic.MaxBlockReward
		ec.EndorsementRewardPerSlot = ic.EndorsementRewardPerSlot
		ec.NonceRevelationReward = ic.NonceRevelationReward
		ec.VdfRevelationReward = ic.VdfRevelationReward
		ec.LBSubsidy = ic.LBSubsidy

		if maxSeeds > 0 {
			ec.SeedNonceRate = float64(ic.NumSeeds*100) / float64(maxSeeds)
		}
	}

	// on active or complete cycles
	if snapHeight <= nowheight {
		// scale endorsement rate to current progress
		if maxEndorse > 0 {
			ec.EndorsementRate = float64(maxEndorse-ec.MissedEndorsements) * 100 / float64(maxEndorse)
		}

		// pull rolls and supply from chain and supply table (no need to fetch snapshot)
		// determine height from snapshot block, if not exist, use latest snapshot
		// ignore loading errors because height may be in the future

		if chain, err := ctx.Indexer.ChainByHeight(ctx.Context, snapHeight); err == nil {
			ec.ActiveDelegators = chain.ActiveDelegators
			ec.ActiveStakers = chain.ActiveStakers
			ec.ActiveBakers = chain.ActiveBakers
			ec.EligibleBakers = chain.EligibleBakers
		}
		if supply, err := ctx.Indexer.SupplyByHeight(ctx.Context, snapHeight); err == nil {
			ec.StakingSupply = p.ConvertValue(supply.ActiveStake)
			if supply.Total > 0 {
				ec.StakingPercent = float64(supply.ActiveStake*100) / float64(supply.Total)
			}
		}
	}

	return ec
}

func (c Cycle) LastModified() time.Time {
	if c.FollowerCycle != nil && c.FollowerCycle.IsComplete {
		return c.FollowerCycle.EndTime
	}
	return c.lastmod
}

func (c Cycle) Expires() time.Time {
	return c.expires
}

func (c Cycle) RESTPrefix() string {
	return "/explorer/cycle"
}

func (c Cycle) RESTPath(r *mux.Router) string {
	path, _ := r.Get("cycle").URLPath("cycle", strconv.FormatInt(c.Cycle, 10))
	return path.String()
}

func (c Cycle) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (c Cycle) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{cycle}", server.C(ReadCycle)).Methods("GET").Name("cycle")
	return nil
}

func parseCycle(ctx *server.Context) int64 {
	// from number or string
	if id, ok := mux.Vars(ctx.Request)["cycle"]; !ok || id == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing cycle identifier", nil))
	} else {
		switch {
		case id == "head":
			p := ctx.Params
			return p.HeightToCycle(ctx.Tip.BestHeight)
		default:
			cycle, err := strconv.ParseInt(id, 10, 64)
			if err != nil || cycle < 0 {
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid cycle", err))
			}
			return cycle
		}
	}
}

func ReadCycle(ctx *server.Context) (interface{}, int) {
	id := parseCycle(ctx)
	p := ctx.Indexer.ParamsByCycle(id)
	tiptime := ctx.Tip.BestTime

	// compose cycle data from N, N-7 and N+7
	cycle := lookupOrBuildCycle(ctx, id)

	// Ithaca changes the distance
	var offset int64 = 2
	if p.Version >= 12 {
		offset = 1
	}

	// snapshot cycle who defined rights for this cycle
	cycle.SnapshotCycle = lookupOrBuildCycle(ctx, id-(p.PreservedCycles+offset))

	// future cycle who's rights are defined by this cycle
	cycle.FollowerCycle = lookupOrBuildCycle(ctx, id+(p.PreservedCycles+offset))

	// set cache expiry
	cycle.expires = ctx.Expires
	cycle.lastmod = tiptime

	if cycle.FollowerCycle != nil && cycle.FollowerCycle.IsComplete {
		cycle.expires = ctx.Now.Add(ctx.Cfg.Http.CacheMaxExpires)
	}

	return cycle, http.StatusOK
}
