// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"math/bits"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"

	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

// keep a cache of past cycles to avoid expensive database lookups
type (
	cycleMap map[int64]*ExplorerCycle
)

var (
	cycleMapStore atomic.Value
	cycleMutex    sync.Mutex
)

func init() {
	register(ExplorerCycle{})
	cycleMapStore.Store(make(cycleMap))
}

func purgeCycleStore() {
	cycleMutex.Lock()
	defer cycleMutex.Unlock()
	cycleMapStore.Store(make(cycleMap))
}

// use read-mostly cache for complete cycles
func lookupOrBuildCycle(ctx *ApiContext, id int64) *ExplorerCycle {
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
			c = NewExplorerCycle(ctx, id)
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
	cc := &ExplorerCycle{}
	*cc = *c
	return cc
}

var _ RESTful = (*ExplorerCycle)(nil)

type ExplorerCycle struct {
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
	SnapshotIndex    int64     `json:"snapshot_index"`  // -1 when no snapshot
	SnapshotTime     time.Time `json:"snapshot_time"`   // zero when no snapshot
	Rolls            int64     `json:"rolls"`
	RollOwners       int64     `json:"roll_owners"`
	ActiveDelegators int64     `json:"active_delegators"`
	ActiveBakers     int64     `json:"active_bakers"`
	StakingSupply    float64   `json:"staking_supply"`
	StakingPercent   float64   `json:"staking_percent"` // of total supply

	// health data across all blocks in cycle (empty for future cycles)
	WorkingBakers      int     `json:"working_bakers"`
	WorkingEndorsers   int     `json:"working_endorsers"` // from ops
	MissedPriorities   int     `json:"missed_priorities"`
	MissedEndorsements int     `json:"missed_endorsements"`
	N2Baking           int     `json:"n_double_baking"`
	N2Endorsement      int     `json:"n_double_endorsement"`
	NOrphans           int     `json:"n_orphans"`
	SolveTimeMin       int64   `json:"solvetime_min"`
	SolveTimeMax       int64   `json:"solvetime_max"`
	SolveTimeMean      float64 `json:"solvetime_mean"`
	PriorityMin        int64   `json:"priority_min"`
	PriorityMax        int64   `json:"priority_max"`
	PriorityMean       float64 `json:"priority_mean"`
	EndorsementRate    float64 `json:"endorsement_rate"`
	EndorsementsMin    int64   `json:"endorsements_min"`
	EndorsementsMax    int64   `json:"endorsements_max"`
	EndorsementsMean   float64 `json:"endorsements_mean"`
	SeedNonceRate      float64 `json:"seed_rate"` // from ops
	WorstBakedBlock    int64   `json:"worst_baked_block"`
	WorstEndorsedBlock int64   `json:"worst_endorsed_block"`

	// snapshot cycle who defined rights for this cycle
	SnapshotCycle *ExplorerCycle `json:"snapshot_cycle,omitempty"`

	// future cycle who's rights are defined by this cycle
	FollowerCycle *ExplorerCycle `json:"follower_cycle,omitempty"`

	// cache hint
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func NewExplorerCycle(ctx *ApiContext, id int64) *ExplorerCycle {
	// get latest params
	p := ctx.Params

	// get params that were or are active at cycle (future safe, will return latest)
	if cycleStart := p.CycleStartHeight(id); !p.ContainsHeight(cycleStart) {
		p = ctx.Crawler.ParamsByHeight(cycleStart)
	}

	// get current status
	nowheight := ctx.Tip.BestHeight
	nowcycle := p.CycleFromHeight(nowheight)

	// this cycle start/end
	start, end := p.CycleStartHeight(id), p.CycleEndHeight(id)

	// fill common data for every cycle state (past, active, future)
	ec := &ExplorerCycle{
		Cycle:              id,
		StartHeight:        start,
		EndHeight:          end,
		Progress:           0,
		IsComplete:         end < nowheight, // only complete when last endorsements are fixed
		IsSnapshot:         id <= nowcycle-2,
		IsActive:           id == nowcycle,
		SnapshotHeight:     -1,
		SnapshotIndex:      -1,
		WorstBakedBlock:    -1, // when all are qual
		WorstEndorsedBlock: -1, //when all are equal
	}

	// set times
	if ec.IsComplete {
		ec.StartTime = ctx.Indexer.LookupBlockTime(ctx.Context, start)
		ec.EndTime = ctx.Indexer.LookupBlockTime(ctx.Context, end)
	} else {
		nowtime := ctx.Tip.BestTime
		ec.StartTime = ctx.Indexer.LookupBlockTime(ctx.Context, start)
		if ec.StartTime.IsZero() {
			ec.StartTime = nowtime.Add(time.Duration(start-nowheight) * p.TimeBetweenBlocks[0])
		}
		ec.EndTime = nowtime.Add(time.Duration(end-nowheight) * p.TimeBetweenBlocks[0])
	}

	var (
		uniqueAccountsMap = make(map[model.AccountID]struct{})
		prioStats         = vec.IntegerReducer{}
		endorseStats      = vec.IntegerReducer{}
		timeStats         = vec.IntegerReducer{}

		worstPriority     int = 0
		worstEndorsements int = p.EndorsersPerBlock

		maxEndorse int        // scaled to current blocks in cycle
		maxSeeds   int        // unscaled, full value (since requirement is from snapshot)
		snapHeight int64 = -1 // selected or latest snapshot block
	)

	maxSeeds = int(p.BlocksPerCycle / p.BlocksPerCommitment)
	if ec.IsComplete {
		ec.Progress = 100
		maxEndorse = p.EndorsersPerBlock * int(p.BlocksPerCycle)
		snapHeight = end
	} else if ec.IsActive {
		ec.Progress = float64(nowheight%p.BlocksPerCycle*100) / float64(p.BlocksPerCycle)
		// latest block cannot have an endorsement yet, so we don't require it
		// otherwise the formula would be (nowheight - start + 1)*p.EndorsersPerBlock
		maxEndorse = int(nowheight-start) * p.EndorsersPerBlock
		snapHeight = nowheight - (nowheight % p.BlocksPerRollSnapshot)
	}

	if snapHeight <= nowheight {
		// walk all blocks in cycle to update cycle fields and identify snapshot block
		blocks, err := ctx.Indexer.Table(index.BlockTableKey)
		if err != nil {
			log.Errorf("cycle: block table: %v", err)
		}
		b := &model.Block{}
		err = blocks.Stream(ctx.Context,
			pack.NewQuery("cycle.blocks", blocks).
				WithFields("Z", "o", "h", "B", "p", "d", "s").
				AndEqual("cycle", id),
			func(r pack.Row) error {
				if err := r.Decode(b); err != nil {
					return err
				}

				if b.IsOrphan {
					ec.NOrphans++
					// don't proceed when orphan
					return nil
				}

				// find snapshot block
				if b.IsCycleSnapshot {
					snapHeight = b.Height
					ec.SnapshotHeight = b.Height
					ec.SnapshotIndex = ((b.Height - start) / p.BlocksPerRollSnapshot)
					ec.SnapshotTime = ctx.Indexer.LookupBlockTime(ctx, b.Height)
				}

				// collect unique bakers
				if b.BakerId > 0 {
					uniqueAccountsMap[b.BakerId] = struct{}{}
				}

				// sum misses and ops
				ec.MissedPriorities += b.Priority

				// collect stats
				prioStats.Add(int64(b.Priority))
				timeStats.Add(int64(b.Solvetime))

				// update worst blocks
				if b.Priority > worstPriority {
					worstPriority = b.Priority
					ec.WorstBakedBlock = b.Height
				}

				// don't count endorsements for the current block
				if b.Height != nowheight {
					nEndorse := bits.OnesCount32(b.SlotsEndorsed)
					ec.MissedEndorsements += p.EndorsersPerBlock - nEndorse
					endorseStats.Add(int64(nEndorse))
					if nEndorse < worstEndorsements {
						worstEndorsements = nEndorse
						ec.WorstEndorsedBlock = b.Height
					}
				}

				return nil
			})
		if err != nil {
			log.Errorf("cycle: block stream: %v", err)
		}
		ec.WorkingBakers = len(uniqueAccountsMap)
		ec.SolveTimeMin = timeStats.Min()
		ec.SolveTimeMax = timeStats.Max()
		ec.SolveTimeMean = timeStats.Mean()
		ec.PriorityMin = prioStats.Min()
		ec.PriorityMax = prioStats.Max()
		ec.PriorityMean = prioStats.Mean()
		ec.EndorsementsMin = endorseStats.Min()
		ec.EndorsementsMax = endorseStats.Max()
		ec.EndorsementsMean = endorseStats.Mean()

		// scale endorsement rate to current progress
		if maxEndorse > 0 {
			ec.EndorsementRate = float64(maxEndorse-ec.MissedEndorsements) * 100 / float64(maxEndorse)
		}

		// load active endorsers from ops
		uniqueAccountsMap = make(map[model.AccountID]struct{})
		ops, err := ctx.Indexer.Table(index.OpTableKey)
		if err != nil {
			log.Errorf("cycle: op table: %v", err)
			return ec
		}
		op := &model.Op{}
		err = pack.NewQuery("cycle.endorse_ops", ops).
			WithFields("S").
			AndRange(
				"height",
				start+1, // Note: endorsements are always sent one block later!
				end+1,   // safe when cycle is still active
			).
			AndEqual("type", tezos.OpTypeEndorsement).
			Stream(ctx.Context, func(r pack.Row) error {
				if err := r.Decode(op); err != nil {
					return err
				}
				uniqueAccountsMap[op.SenderId] = struct{}{}
				return nil
			})
		if err != nil {
			log.Errorf("cycle: op stream: %v", err)
		}
		ec.WorkingEndorsers = len(uniqueAccountsMap)

		// seed nonces are send as operations and we expect one commitment
		// for every 32nd block produced in the cycle before, they need to be sent
		// by the bakers who produced block%32==0 in the previous cycle
		seeds, err := pack.NewQuery("cycle.seeds", ops).
			AndRange("height", start, end).
			AndEqual("type", tezos.OpTypeSeedNonceRevelation).
			Count(ctx.Context)
		if err != nil {
			log.Errorf("cycle: op count: %v", err)
		}
		if maxSeeds > 0 {
			ec.SeedNonceRate = float64(seeds*100) / float64(maxSeeds)
		}

		// walk all ops to count unique 2bake/2endorse events
		// count unique double bake and endorse events
		bake2 := make(map[int64]struct{})    // height
		endorse2 := make(map[int64]struct{}) // height
		err = pack.NewQuery("cycle.denounce_ops", ops).
			WithFields("t", "a").
			AndEqual("cycle", id).
			AndIn("type", []uint8{
				uint8(tezos.OpTypeDoubleBakingEvidence),
				uint8(tezos.OpTypeDoubleEndorsementEvidence),
			}).
			Stream(ctx.Context, func(r pack.Row) error {
				if err := r.Decode(op); err != nil {
					return err
				}
				switch op.Type {
				case tezos.OpTypeDoubleBakingEvidence:
					bhs := make([]rpc.BlockHeader, 0)
					if err := json.Unmarshal([]byte(op.Data), &bhs); err != nil {
						return err
					}
					bake2[bhs[0].Level] = struct{}{}
				case tezos.OpTypeDoubleEndorsementEvidence:
					dops := make([]rpc.DoubleEndorsementEvidence, 0)
					if err := json.Unmarshal([]byte(op.Data), &dops); err != nil {
						return err
					}
					endorse2[dops[0].Operations.Level] = struct{}{}
				}
				return nil
			})
		if err != nil {
			log.Errorf("cycle: op stream 2: %v", err)
		}
		ec.N2Baking = len(bake2)
		ec.N2Endorsement = len(endorse2)

		// pull rolls and supply from chain and supply table (no need to fetch snapshot)
		// determine height from snapshot block, if not exist, use latest snapshot
		// ignore loading errors because height may be in the future

		if chain, err := ctx.Indexer.ChainByHeight(ctx.Context, snapHeight); err == nil {
			ec.ActiveDelegators = chain.ActiveDelegators
			ec.ActiveBakers = chain.ActiveDelegates
			ec.Rolls = chain.Rolls
			ec.RollOwners = chain.RollOwners
		}
		if supply, err := ctx.Indexer.SupplyByHeight(ctx.Context, snapHeight); err == nil {
			ec.StakingSupply = p.ConvertValue(supply.ActiveStaking)
			if supply.Total > 0 {
				ec.StakingPercent = float64(supply.ActiveStaking*100) / float64(supply.Total)
			}
		}
	}

	return ec
}

func (c ExplorerCycle) LastModified() time.Time {
	if c.FollowerCycle != nil && c.FollowerCycle.IsComplete {
		return c.FollowerCycle.EndTime
	}
	return c.lastmod
}

func (c ExplorerCycle) Expires() time.Time {
	if c.FollowerCycle != nil && c.FollowerCycle.IsComplete {
		return time.Now().UTC().Add(maxCacheExpires)
	}
	return c.expires
}

func (c ExplorerCycle) RESTPrefix() string {
	return "/explorer/cycle"
}

func (c ExplorerCycle) RESTPath(r *mux.Router) string {
	path, _ := r.Get("cycle").URLPath("ident", strconv.FormatInt(c.Cycle, 10))
	return path.String()
}

func (c ExplorerCycle) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (c ExplorerCycle) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadCycle)).Methods("GET").Name("cycle")
	return nil
}

func parseCycle(ctx *ApiContext) int64 {
	// from number or string
	if id, ok := mux.Vars(ctx.Request)["ident"]; !ok || id == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing cycle identifier", nil))
	} else {
		switch true {
		case id == "head":
			p := ctx.Params
			return p.CycleFromHeight(ctx.Tip.BestHeight)
		default:
			cycle, err := strconv.ParseInt(id, 10, 64)
			if err != nil || cycle < 0 {
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid cycle", err))
			}
			return cycle
		}
	}
	return 0
}

func ReadCycle(ctx *ApiContext) (interface{}, int) {
	id := parseCycle(ctx)
	p := ctx.Params
	tiptime := ctx.Tip.BestTime

	// compose cycle data from N, N-7 and N+7
	cycle := lookupOrBuildCycle(ctx, id)

	// snapshot cycle who defined rights for this cycle
	cycle.SnapshotCycle = lookupOrBuildCycle(ctx, id-(p.PreservedCycles+2))

	// future cycle who's rights are defined by this cycle
	cycle.FollowerCycle = lookupOrBuildCycle(ctx, id+(p.PreservedCycles+2))

	// set cache expiry
	cycle.expires = tiptime.Add(p.TimeBetweenBlocks[0])
	cycle.lastmod = tiptime

	return cycle, http.StatusOK
}
