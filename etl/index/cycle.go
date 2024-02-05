// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package index

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
	"blockwatch.cc/tzindex/rpc"
)

const CycleIndexKey = "cycle"

type CycleIndex struct {
	db     *pack.DB
	table  *pack.Table
	unique map[model.AccountID]struct{}
	cycle  *model.Cycle
}

var _ model.BlockIndexer = (*CycleIndex)(nil)

func NewCycleIndex() *CycleIndex {
	return &CycleIndex{}
}

func (idx *CycleIndex) DB() *pack.DB {
	return idx.db
}

func (idx *CycleIndex) Tables() []*pack.Table {
	return []*pack.Table{
		idx.table,
	}
}

func (idx *CycleIndex) Key() string {
	return CycleIndexKey
}

func (idx *CycleIndex) Name() string {
	return CycleIndexKey + " index"
}

func (idx *CycleIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Cycle{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *CycleIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Cycle{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *CycleIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *CycleIndex) Close() error {
	for _, v := range idx.Tables() {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", v.Name(), err)
			}
		}
	}
	idx.table = nil
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *CycleIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	p := block.Params

	// skip genesis
	if block.Height == 0 {
		return nil
	}

	// init on cycle start
	if block.Height == 1 || block.TZ.IsCycleStart() {
		idx.initCycle(block)
	}

	// on first load use last saved state
	if idx.cycle == nil {
		if err := idx.prepareCycle(ctx, block, builder); err != nil {
			return fmt.Errorf("cycle: prepare C_%d: %v", block.Cycle, err)
		}
	}

	// mark snapshot (in old cycle!)
	if snap := block.TZ.Snapshot; snap != nil && snap.Base >= 0 {
		// be sensitive to cycle length changes when back-dating blocks
		params := block.Params
		if block.Parent != nil {
			params = block.Parent.Params
		}
		snapHeight := params.SnapshotBlock(snap.Cycle, snap.Index)
		c, err := idx.loadCycle(ctx, snap.Base)
		if err != nil {
			log.Warnf("cycle: C_%d setting snapshot block: %v", snap.Base, err)
		} else {
			c.SnapshotHeight = snapHeight
			c.SnapshotIndex = snap.Index
			if err := idx.table.Update(ctx, c); err != nil {
				log.Warnf("cycle: C_%d update snapshot: %v", snap.Base, err)
			}
		}
	}

	// handle solve time
	if idx.cycle.StartHeight == block.Height {
		idx.cycle.SolveTimeMin = block.Solvetime
		idx.cycle.SolveTimeMax = block.Solvetime
	} else {
		idx.cycle.SolveTimeMin = util.Min(idx.cycle.SolveTimeMin, block.Solvetime)
		idx.cycle.SolveTimeMax = util.Max(idx.cycle.SolveTimeMax, block.Solvetime)
	}
	idx.cycle.SolveTimeSum += block.Solvetime

	// handle rounds
	idx.cycle.MissedRounds += block.Round
	if block.Round > idx.cycle.RoundMax {
		idx.cycle.WorstBakedBlock = block.Height
	}
	if idx.cycle.StartHeight == block.Height {
		idx.cycle.RoundMin = block.Round
		idx.cycle.RoundMax = block.Round
	} else {
		idx.cycle.RoundMin = util.Min(idx.cycle.RoundMin, block.Round)
		idx.cycle.RoundMax = util.Max(idx.cycle.RoundMax, block.Round)
	}

	// for endorsements look at parent block
	if block.Parent != nil {
		numEndorsers := p.EndorsersPerBlock + p.ConsensusCommitteeSize
		idx.cycle.MissedEndorsements += numEndorsers - block.Parent.NSlotsEndorsed
		if idx.cycle.EndorsementsMin > block.Parent.NSlotsEndorsed {
			idx.cycle.WorstEndorsedBlock = block.Parent.Height
		}
		if idx.cycle.StartHeight == block.Height {
			idx.cycle.EndorsementsMin = block.Parent.NSlotsEndorsed
			idx.cycle.EndorsementsMax = block.Parent.NSlotsEndorsed
		} else {
			idx.cycle.EndorsementsMin = util.Min(idx.cycle.EndorsementsMin, block.Parent.NSlotsEndorsed)
			idx.cycle.EndorsementsMax = util.Max(idx.cycle.EndorsementsMax, block.Parent.NSlotsEndorsed)
		}
	}

	// add penalty and seed ops
	for _, v := range block.Ops {
		switch v.Type {
		case model.OpTypeDoubleBaking:
			idx.cycle.Num2Baking++
		case model.OpTypeDoubleEndorsement, model.OpTypeDoublePreendorsement:
			idx.cycle.Num2Endorsement++
		case model.OpTypeNonceRevelation:
			idx.cycle.NumSeeds++
		case model.OpTypeBake,
			model.OpTypeBonus,
			model.OpTypeEndorsement,
			model.OpTypePreendorsement:
			idx.unique[v.SenderId] = struct{}{}
		}
	}
	idx.cycle.UniqueBakers = len(idx.unique)

	// store
	var err error
	if idx.cycle.RowId == 0 {
		err = idx.table.Insert(ctx, idx.cycle)
	} else {
		err = idx.table.Update(ctx, idx.cycle)
	}
	return err
}

func (idx *CycleIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	return nil
}

func (idx *CycleIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *CycleIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *CycleIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *CycleIndex) initCycle(block *model.Block) {
	p := block.Params
	var issuance rpc.Issuance
	for _, v := range block.TZ.Issuance {
		if v.Cycle == block.Cycle {
			issuance = v
			break
		}
	}
	// issuance is missing on genesis, so we fill in at block 1
	if block.Height == 1 {
		issuance.BakingReward = p.BakingRewardFixedPortion
		issuance.BakingBonus = p.BakingRewardBonusPerSlot
		issuance.BakingReward = p.BlockReward
		issuance.AttestingReward = p.EndorsingRewardPerSlot
		issuance.SeedNonceTip = p.SeedNonceRevelationTip
		if p.Version >= 12 {
			issuance.VdfTip = p.SeedNonceRevelationTip
			issuance.LBSubsidy = p.LiquidityBakingSubsidy
		}
	}
	start, end := p.CycleStartHeight(block.Cycle), p.CycleEndHeight(block.Cycle)
	idx.cycle = &model.Cycle{
		Cycle:                    block.Cycle,
		StartHeight:              start,
		EndHeight:                end,
		SnapshotHeight:           -1,
		SnapshotIndex:            -1,
		BlockReward:              issuance.BakingReward,
		BlockBonusPerSlot:        issuance.BakingBonus,
		MaxBlockReward:           issuance.BakingReward + issuance.BakingBonus*int64(p.ConsensusCommitteeSize-p.ConsensusThreshold),
		EndorsementRewardPerSlot: issuance.AttestingReward,
		NonceRevelationReward:    issuance.SeedNonceTip,
		VdfRevelationReward:      issuance.VdfTip,
		LBSubsidy:                issuance.LBSubsidy,
	}
	if idx.unique == nil {
		idx.unique = make(map[model.AccountID]struct{})
	} else {
		for n := range idx.unique {
			delete(idx.unique, n)
		}
	}
}

func (idx *CycleIndex) prepareCycle(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// try load cycle from DB
	c, err := idx.loadCycle(ctx, block.Cycle)
	if err != nil {
		return err
	}
	idx.cycle = c
	idx.unique = make(map[model.AccountID]struct{})

	if builder.IsLightMode() {
		// fill unique bakers from block table
		blocks, err := builder.Table(model.BlockTableKey)
		if err != nil {
			return err
		}
		var b model.Block
		err = pack.NewQuery("etl.scan_blocks").
			WithTable(blocks).
			AndEqual("cycle", block.Cycle).
			WithFields("baker_id", "proposer_id").
			Stream(ctx, func(row pack.Row) error {
				if err := row.Decode(&b); err != nil {
					return err
				}
				idx.unique[b.BakerId] = struct{}{}
				idx.unique[b.ProposerId] = struct{}{}
				return nil
			})
		if err != nil {
			return err
		}
	} else {
		// fill unique bakers from rights table
		rights, err := builder.Table(model.RightsTableKey)
		if err != nil {
			return err
		}
		var r model.Right
		err = pack.NewQuery("etl.scan_rights").
			WithTable(rights).
			AndEqual("cycle", block.Cycle).
			WithFields("account_id", "blocks_baked", "blocks_endorsed").
			Stream(ctx, func(row pack.Row) error {
				if err := row.Decode(&r); err != nil {
					return err
				}
				if r.Endorsed.Count() > 0 || r.Baked.Count() > 0 {
					idx.unique[r.AccountId] = struct{}{}
				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *CycleIndex) loadCycle(ctx context.Context, i int64) (*model.Cycle, error) {
	var c model.Cycle
	err := pack.NewQuery("etl.load_cycle").
		WithTable(idx.table).
		AndEqual("cycle", i).
		Execute(ctx, &c)
	if err != nil {
		return nil, err
	}
	if c.RowId == 0 {
		return nil, model.ErrNoCycle
	}
	return &c, nil
}

func (idx *CycleIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
