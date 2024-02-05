// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
	"blockwatch.cc/tzindex/rpc"
)

const IncomeIndexKey = "income"

type IncomeIndex struct {
	db    *pack.DB
	table *pack.Table
	cache map[model.AccountID]*model.Income
	cycle int64
}

var _ model.BlockIndexer = (*IncomeIndex)(nil)

func NewIncomeIndex() *IncomeIndex {
	return &IncomeIndex{cache: make(map[model.AccountID]*model.Income)}
}

func (idx *IncomeIndex) DB() *pack.DB {
	return idx.db
}

func (idx *IncomeIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *IncomeIndex) Key() string {
	return IncomeIndexKey
}

func (idx *IncomeIndex) Name() string {
	return IncomeIndexKey + " index"
}

func (idx *IncomeIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Income{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	return err
}

func (idx *IncomeIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Income{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *IncomeIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *IncomeIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %s", idx.Name(), err)
		}
		idx.table = nil
	}
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *IncomeIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// ignore genesis block
	if block.Height == 0 {
		return nil
	}

	// bootstrap first cycles on first block using all genesis bakers as snapshot proxy
	// block 1 contains all initial rights, this number is fixed at crawler.go
	if block.Height == 1 {
		if err := idx.bootstrapIncome(ctx, block, builder); err != nil {
			log.Error(err)
		}
		return nil
	}

	// update expected income/deposits and luck on cycle start when params are known
	if block.TZ.IsCycleStart() {
		// this only happens during mainnet bootstrap
		if err := idx.updateCycleIncome(ctx, block, builder); err != nil {
			log.Error(err)
		}
		// reset cache at start of cycle
		for n := range idx.cache {
			delete(idx.cache, n)
		}
		idx.cycle = block.Cycle
	}

	// skip when no new rights are defined
	if len(block.TZ.Baking) > 0 || len(block.TZ.Endorsing) > 0 {
		if err := idx.createCycleIncome(ctx, block, builder); err != nil {
			log.Error(err)
		}
	}

	// update income from flows and rights
	if err := idx.updateBlockIncome(ctx, block, builder, false); err != nil {
		log.Error(err)
	}

	// update burn from nonce revelations, if any
	if err := idx.updateNonceRevelations(ctx, block, builder, false); err != nil {
		log.Error(err)
	}

	return nil
}

func (idx *IncomeIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// rollback current income
	if err := idx.updateBlockIncome(ctx, block, builder, true); err != nil {
		log.Error(err)
	}

	// update burn from nonce revelations, if any
	if err := idx.updateNonceRevelations(ctx, block, builder, true); err != nil {
		log.Error(err)
	}

	// new rights are fetched in cycles
	// if block.Params.IsCycleStart(block.Height) {
	if block.TZ.IsCycleStart() {
		if err := idx.DeleteCycle(ctx, block.Cycle+block.Params.PreservedCycles); err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (idx *IncomeIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *IncomeIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting income for cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *IncomeIndex) bootstrapIncome(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// on bootstrap use the initial params from block 1
	p := block.Params

	// get sorted list of bakers
	bkrs := make([]*model.Baker, 0)
	for _, v := range builder.Bakers() {
		bkrs = append(bkrs, v)
	}
	sort.Slice(bkrs, func(i, j int) bool { return bkrs[i].AccountId < bkrs[j].AccountId })

	// spillover endorsement rights
	spillover := make([]rpc.EndorsingRight, 0)

	var cycleEndBlock = p.BlocksPerCycle - 1

	for c := range block.TZ.Baking {
		log.Debugf("income: bootstrap for cycle %d with %d bakers", c, len(bkrs))
		bake := block.TZ.Baking[c]
		endorse := block.TZ.Endorsing[c]
		var totalStake int64

		// new income data for each cycle
		incomeMap := make(map[model.AccountID]*model.Income)
		for _, v := range bkrs {
			cycle := int64(c)
			power := v.BakingPower(p, 0)
			incomeMap[v.AccountId] = &model.Income{
				Cycle:          cycle,
				StartHeight:    p.CycleStartHeight(cycle),
				EndHeight:      p.CycleEndHeight(cycle),
				AccountId:      v.AccountId,
				Balance:        v.Balance(),
				StakingBalance: v.StakingBalance(),
				Delegated:      v.DelegatedBalance,
				NDelegations:   v.ActiveDelegations,
				NStakers:       v.ActiveStakers,
				LuckPct:        10000,
			}
			totalStake += power
		}

		// pre-calculate reward amounts
		blockReward, endorseReward := p.BlockReward, p.EndorsementReward

		// assign baking rights
		for _, v := range bake {
			if v.Round > 0 || v.Level > cycleEndBlock {
				continue
			}
			acc, ok := builder.AccountByAddress(v.Address())
			if !ok {
				return fmt.Errorf("income: missing bootstrap baker %s at height %d",
					v.Address(), v.Level)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for baker %q %d at height %d",
					v.Address(), acc.RowId, v.Level)
			}
			ic.NBakingRights++
			ic.ExpectedIncome += blockReward
		}

		// assign from endorsement rights in protos before Ithaca
		for _, v := range endorse {
			// store spillover rights
			if v.Level >= cycleEndBlock {
				spillover = append(spillover, v)
				continue
			}
			acc, ok := builder.AccountByAddress(v.Address())
			if !ok {
				return fmt.Errorf("income: missing bootstrap endorser %s at height %d",
					v.Address(), v.Level)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for endorser %s %d at height %d",
					v.Address(), acc.RowId, v.Level)
			}
			ic.NEndorsingRights += int64(v.Power)
			if p.Version < 12 {
				ic.ExpectedIncome += endorseReward * int64(v.Power)
			}
		}

		// handle spill-over endorsement rights from previous cycle's last block
		for _, v := range spillover {
			acc, ok := builder.AccountByAddress(v.Address())
			if !ok {
				return fmt.Errorf("income: missing bootstrap endorser %s at height %d",
					v.Address(), v.Level)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for endorser %s %d at height %d",
					v.Address(), acc.RowId, v.Level)
			}
			ic.NEndorsingRights += int64(v.Power)
			if p.Version < 12 {
				ic.ExpectedIncome += endorseReward * int64(v.Power)
			}
		}
		// prepare for next cycle
		spillover = spillover[:0]
		cycleEndBlock += p.BlocksPerCycle

		if p.Version >= 12 {
			// Endorsement rewards are not distributed at each block anymore but at the end
			// of a cycle instead. This is not based on the number of included endorsements.
			// Instead, the reward is proportional to a baker's stake.
			//
			// Rewards formula:
			//   `20 XTZ * nb_blocks_per_cycle * baker own stake / total chain stake`
			//
			// However, rewards are only distributed if the baker has had a sufficient
			// activity in a cycle meaning: at least 2/3 of a baker's endorsements were
			// included in blocks in the last cycle.
			info := block.TZ.SnapInfo
			eRewards := tezos.NewZ(p.EndorsingRewardPerSlot * int64(p.ConsensusCommitteeSize) * p.BlocksPerCycle)
			stake := info.BakerStake
			if totalStake != info.TotalStake.Value() {
				log.Warnf("income: index total stake=%d != node total stake=%d",
					totalStake,
					info.TotalStake.Value(),
				)
				totalStake = info.TotalStake.Value()
			}

			for _, bkr := range bkrs {
				income, ok := incomeMap[bkr.AccountId]
				if !ok {
					continue
				}
				for i := 0; i < len(stake); i++ {
					// find baker in list
					if stake[i].Baker.Equal(bkr.Address) {
						income.StakingBalance = stake[i].ActiveStake.Value()
						// protect against int64 overflow
						income.ExpectedIncome += tezos.NewZ(income.StakingBalance).
							Mul(eRewards).
							Div64(info.TotalStake.Value()).
							Int64()
						stake = append(stake[:i], stake[i+1:]...)
						break
					}
				}
			}
		}

		// calculate luck and append for insert
		inc := make([]*model.Income, 0, len(incomeMap))
		for _, v := range incomeMap {
			v.UpdateLuck(totalStake, p)
			inc = append(inc, v)
		}
		// sort by account id
		sort.Slice(inc, func(i, j int) bool { return inc[i].AccountId < inc[j].AccountId })

		// cast into insertion slice
		ins := make([]pack.Item, len(inc))
		for i, v := range inc {
			ins[i] = v
		}
		if err := idx.table.Insert(ctx, ins); err != nil {
			return err
		}
	}
	return nil
}

// use to update cycles 0..14 expected income and deposits because ramp-up constants
// are only available at start of cycle and not when the income rows are created
//
// also used to update income after upgrade to v006 an v012 for future cycles due
// to changes in rights distributions and reward allocation
func (idx *IncomeIndex) updateCycleIncome(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	p := block.Params
	if !p.IsMainnet() {
		return nil
	}

	// check pre-conditon and pick cycles to update
	if block.Cycle > 2*(p.PreservedCycles+2) {
		// no update required
		return nil
	}
	// during ramp-up cycles
	updateCycles := []int64{block.Cycle}
	blockReward, endorseReward := p.BlockReward, p.EndorsementReward
	var totalStake int64

	for _, v := range updateCycles {
		// log.Infof("Income: update for cycle %d ", v)
		incomes := make([]*model.Income, 0)
		err := idx.table.Stream(ctx,
			pack.NewQuery("etl.update").
				WithTable(idx.table).
				AndEqual("cycle", v),
			func(r pack.Row) error {
				in := &model.Income{}
				if err := r.Decode(in); err != nil {
					return err
				}
				in.ExpectedIncome = blockReward * in.NBakingRights
				in.ExpectedIncome += endorseReward * in.NEndorsingRights
				totalStake += in.StakingBalance
				incomes = append(incomes, in)
				return nil
			})
		if err != nil {
			return err
		}

		// update luck and convert type
		upd := make([]pack.Item, len(incomes))
		for i, v := range incomes {
			v.UpdateLuck(totalStake, p)
			upd[i] = v
		}
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
	}
	return nil
}

func (idx *IncomeIndex) createCycleIncome(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	p := block.Params
	sn := block.TZ.Snapshot
	incomeMap := make(map[model.AccountID]*model.Income)
	var totalStake int64

	if sn == nil {
		return fmt.Errorf("income: missing snapshot info at block %d", block.Height)
	}

	if sn.Base < 0 {
		// log.Infof("income: created for cycle C_%d from current state", sn.Cycle)

		// build income from current balance values because there is no snapshot yet
		bkrs := make([]*model.Baker, 0)
		for _, v := range builder.Bakers() {
			// skip inactive bakers and bakers with too low staking balance
			if !v.IsActive || v.BakingPower(p, 0) < p.MinimalStake {
				continue
			}
			bkrs = append(bkrs, v)
		}
		sort.Slice(bkrs, func(i, j int) bool { return bkrs[i].AccountId < bkrs[j].AccountId })

		for _, v := range bkrs {
			stake := v.StakingBalance()
			log.Debugf("income: created income for baker %s %d stake %d in cycle %d without snapshot",
				v, v.AccountId, stake, sn.Cycle)
			incomeMap[v.AccountId] = &model.Income{
				Cycle:          sn.Cycle,
				StartHeight:    p.CycleStartHeight(sn.Cycle),
				EndHeight:      p.CycleEndHeight(sn.Cycle),
				AccountId:      v.AccountId,
				Balance:        v.Balance(),
				Delegated:      v.DelegatedBalance,
				OwnStake:       v.FrozenStake(),
				StakingBalance: stake,
				NDelegations:   v.ActiveDelegations,
				NStakers:       v.ActiveStakers,
				LuckPct:        10000,
			}
			totalStake += stake
		}

	} else {
		// log.Infof("income: created for cycle C_%d from snapshot C_%d/%d", sn.Cycle, sn.Base, sn.Index)

		// build income from snapshot
		snap, err := builder.Table(model.SnapshotTableKey)
		if err != nil {
			return err
		}
		s := &model.Snapshot{}
		err = pack.NewQuery("etl.create").
			WithTable(snap).
			WithoutCache().
			AndEqual("cycle", sn.Base).  // source snapshot cycle
			AndEqual("index", sn.Index). // selected index
			AndEqual("is_baker", true).  // bakers only (active+inactive)
			Stream(ctx, func(r pack.Row) error {
				if err := r.Decode(s); err != nil {
					return err
				}
				if s.StakingBalance >= p.MinimalStake {
					incomeMap[s.AccountId] = &model.Income{
						Cycle:          sn.Cycle, // the future cycle
						StartHeight:    p.CycleStartHeight(sn.Cycle),
						EndHeight:      p.CycleEndHeight(sn.Cycle),
						AccountId:      s.AccountId,
						Balance:        s.Balance,
						Delegated:      s.Delegated,
						OwnStake:       s.OwnStake,
						StakingBalance: s.StakingBalance,
						NDelegations:   s.NDelegations,
						NStakers:       s.NStakers,
						LuckPct:        10000,
					}
					totalStake += s.StakingBalance
				}
				return nil
			})
		if err != nil {
			return err
		}

		// log.Debugf("income: created records for %d bakers in cycle %d from snapshot [%d/%d]",
		// 	len(incomeMap), sn.Cycle, sn.Base, sn.Index)
	}

	// assign from rights
	for _, v := range block.TZ.Baking[0] {
		if v.Round > 0 {
			continue
		}
		b, ok := builder.BakerByAddress(v.Address())
		if !ok {
			log.Warnf("income: missing baker %s with rights in cycle %d", v.Address(), block.Cycle)
			continue
		}
		ic, ok := incomeMap[b.AccountId]
		if !ok {
			log.Warnf("income: create income record for baker %s %d with rights for cycle %d without snapshot",
				b, b.AccountId, sn.Cycle)
			ic = &model.Income{
				Cycle:          sn.Cycle,
				StartHeight:    p.CycleStartHeight(sn.Cycle),
				EndHeight:      p.CycleEndHeight(sn.Cycle),
				AccountId:      b.AccountId,
				Balance:        b.Balance(),
				Delegated:      b.DelegatedBalance,
				OwnStake:       b.FrozenStake(),
				StakingBalance: b.StakingBalance(),
				NDelegations:   b.ActiveDelegations,
				NStakers:       b.ActiveStakers,
				LuckPct:        10000,
			}
			incomeMap[b.AccountId] = ic
			totalStake += ic.StakingBalance
		}
		ic.NBakingRights++
		ic.ExpectedIncome += p.BlockReward // Ithaca+: block reward + max bonus
	}

	// Note: expectations about endorsement rewards for the last block in a cycle:
	// endorsement income for a cycle is shifted by 1 (the last block in a cycle
	// is endorsed in the next cycle and this also shifts income opportunities.
	// We allocate such income to the next cycle.
	//
	// Keep in mind that the previous cycle could have more bakers than the current
	// (due to deactivations) but a spillover endorsement right would still require
	// us to insert an income record for this baker.
	endorseEndBlock := p.CycleEndHeight(sn.Cycle)
	for _, v := range block.TZ.Endorsing[0] {
		if v.Level >= endorseEndBlock {
			// log.Infof("Skipping end of cycle height %d > %d", v.Level, endorseEndBlock)
			continue
		}
		b, ok := builder.BakerByAddress(v.Address())
		if !ok {
			log.Warnf("income: missing endorser %s with rights in cycle %d", v.Address(), sn.Cycle)
			continue
		}
		ic, ok := incomeMap[b.AccountId]
		if !ok {
			log.Warnf("income: create income record for endorser %s %d with rights for cycle %d without snapshot",
				b, b.AccountId, sn.Cycle)
			ic = &model.Income{
				Cycle:          sn.Cycle,
				StartHeight:    p.CycleStartHeight(sn.Cycle),
				EndHeight:      p.CycleEndHeight(sn.Cycle),
				AccountId:      b.AccountId,
				Balance:        b.Balance(),
				Delegated:      b.DelegatedBalance,
				OwnStake:       b.FrozenStake(),
				StakingBalance: b.StakingBalance(),
				NDelegations:   b.ActiveDelegations,
				NStakers:       b.ActiveStakers,
				LuckPct:        10000,
			}
			incomeMap[b.AccountId] = ic
			totalStake += ic.StakingBalance
		}
		power := int64(v.Power)
		ic.NEndorsingRights += power
		if p.Version < 12 {
			ic.ExpectedIncome += p.EndorsementReward * power
		}
	}

	// Assign spill-over income from previous cycle's last block endorsing rights,
	// baker may be missing when deactivated or closing
	for _, v := range block.TZ.PrevEndorsing {
		if !v.Address().IsValid() {
			log.Warnf("income: empty endorser in last block rights %#v", block.TZ.PrevEndorsing)
			continue
		}
		b, ok := builder.BakerByAddress(v.Address())
		if !ok {
			log.Warnf("income: missing endorser %s with last block rights in previous cycle %d", v.Address(), sn.Cycle-1)
			continue
		}
		ic, ok := incomeMap[b.AccountId]
		if !ok {
			log.Debugf("income: create for endorser %s %d with rights for last block in cycle %d without snapshot",
				b, b.AccountId, sn.Cycle-1)
			ic = &model.Income{
				Cycle:          sn.Cycle,
				StartHeight:    p.CycleStartHeight(sn.Cycle),
				EndHeight:      p.CycleEndHeight(sn.Cycle),
				AccountId:      b.AccountId,
				Balance:        b.Balance(),
				Delegated:      b.DelegatedBalance,
				OwnStake:       b.FrozenStake(),
				StakingBalance: b.StakingBalance(),
				NDelegations:   b.ActiveDelegations,
				NStakers:       b.ActiveStakers,
				LuckPct:        10000,
			}
			incomeMap[b.AccountId] = ic
		}
		power := int64(v.Power)
		ic.NEndorsingRights += power
		if p.Version < 12 {
			ic.ExpectedIncome += p.EndorsementReward * power
		}
	}

	if p.Version >= 12 {
		// Endorsement rewards are not distributed at each block anymore but at the end
		// of a cycle instead. This is not based on the number of included endorsements.
		// Instead, the reward is proportional to a baker's stake.
		//
		// Rewards formula:
		//   `20 XTZ * nb_blocks_per_cycle * baker own stake / total chain stake`
		//
		// However, rewards are only distributed if the baker has had a sufficient
		// activity in a cycle meaning: at least 2/3 of a baker's endorsements were
		// included in blocks in the last cycle.
		info := block.TZ.SnapInfo
		eRewards := tezos.NewZ(p.EndorsingRewardPerSlot * int64(p.ConsensusCommitteeSize) * p.BlocksPerCycle)
		totalStake := tezos.NewZ(info.TotalStake.Value())
		stake := info.BakerStake
		for _, bkr := range builder.Bakers() {
			income, ok := incomeMap[bkr.AccountId]
			if !ok {
				continue
			}
			for i := 0; i < len(stake); i++ {
				// find baker in list
				if stake[i].Baker.Equal(bkr.Address) {
					income.StakingBalance = stake[i].ActiveStake.Value()
					// protect against int64 overflow
					income.ExpectedIncome += tezos.NewZ(income.StakingBalance).
						Mul(eRewards).
						Div(totalStake).
						Int64()
					stake = append(stake[:i], stake[i+1:]...)
					break
				}
			}
		}
	}

	// prepare income records
	inc := make([]*model.Income, 0, len(incomeMap))
	for _, v := range incomeMap {
		// set 100% performance when no rights are assigned (this will not update later)
		if v.NBakingRights+v.NEndorsingRights == 0 {
			v.ContributionPct = 10000
			v.PerformancePct = 10000
		}
		// calculate luck
		v.UpdateLuck(totalStake, p)
		inc = append(inc, v)
	}

	// sort by account id
	sort.Slice(inc, func(i, j int) bool { return inc[i].AccountId < inc[j].AccountId })

	// cast into insertion slice
	ins := make([]pack.Item, len(inc))
	for i, v := range inc {
		ins[i] = v
	}
	return idx.table.Insert(ctx, ins)
}

func (idx *IncomeIndex) updateBlockIncome(ctx context.Context, block *model.Block, builder model.BlockBuilder, isRollback bool) error {
	var err error
	p := block.Params
	incomeMap := make(map[model.AccountID]*model.Income)
	var mul int64 = 1
	if isRollback {
		mul = -1
	}

	// replay rights effects
	if block.OfflineBaker > 0 {
		in, ok := incomeMap[block.OfflineBaker]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, block.OfflineBaker)
			if err == nil {
				incomeMap[in.AccountId] = in
			} else {
				// should not happen
				// log error (likely from just deactivated bakers or
				// bakers without rights in cycle)
				log.Debugf("income: unknown absent baker %d in block %d c%d",
					block.OfflineBaker, block.Height, block.Cycle)
			}
		}
		if in != nil {
			in.NBlocksNotBaked += mul
		}
	}
	// we count missing endorsements in cycle start block to next cycle
	for _, v := range block.OfflineEndorsers {
		in, ok := incomeMap[v]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, v)
			if err == nil {
				incomeMap[in.AccountId] = in
			} else {
				// should not happen
				// ignore error (likely from just deactivated bakers or
				// bakers without rights in cycle)
				log.Debugf("income: unknown absent endorser %d in block %d c%d",
					v, block.Height, block.Cycle)
			}
		}
		if in != nil {
			in.NBlocksNotEndorsed += mul
		}
	}

	// replay operation effects
	for _, op := range block.Ops {
		switch op.Type {
		case model.OpTypeDeposit:
			// the very first mainnet cycle 468 on Ithaca created a deposit
			// operation at the first cycle block (!)
			if p.Version < 12 || !block.TZ.IsCycleStart() {
				continue
			}
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown baker %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			bkr, ok := builder.BakerById(in.AccountId)
			if !ok {
				return fmt.Errorf("income: missing baker %d in %s op %d in block %d c%d",
					op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
			}
			// in.TotalDeposits = bkr.FrozenDeposits
			in.OwnStake = bkr.FrozenDeposits

		case model.OpTypeBake:
			// handle fees pre/post Ithaca from sum of fees paid in block
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown baker %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.TotalIncome += (op.Fee + op.Reward) * mul
			in.FeesIncome += op.Fee * mul
			in.BakingIncome += op.Reward * mul
			// old protocols made a deposit every block, Oxford+ auto-deposits a share
			in.OwnStake += op.Deposit * mul
			in.NBlocksBaked += mul
			in.NBlocksProposed += mul

		case model.OpTypeBonus:
			// post-Ithaca baking bonus
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown baker %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.TotalIncome += op.Reward * mul
			in.BakingIncome += op.Reward * mul
			in.NBlocksProposed += mul
			// Oxford+ auto-deposits a share
			in.OwnStake += op.Deposit * mul

		case model.OpTypeReward:
			// post-Ithaca endorser reward paid or burned at end of cycle
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown baker %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			if op.Burned > 0 {
				in.TotalLoss += op.Burned * mul
				in.EndorsingLoss += op.Burned * mul
			} else {
				in.TotalIncome += op.Reward * mul
				in.EndorsingIncome += op.Reward * mul
			}

		case model.OpTypeNonceRevelation:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown seeder %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.NSeedsRevealed += mul
			in.TotalIncome += op.Reward * mul
			in.SeedIncome += op.Reward * mul

		case model.OpTypeEndorsement:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown endorser %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}

			power, _ := strconv.Atoi(op.Data)
			in.NSlotsEndorsed += mul * int64(power)
			in.NBlocksEndorsed += mul

			// pre-Ithaca, endorsements mint rewards and lock deposits
			in.TotalIncome += op.Reward * mul
			in.EndorsingIncome += op.Reward * mul
			in.OwnStake += op.Deposit * mul
			// in.TotalDeposits += op.Deposit * mul

		case model.OpTypeDoubleBaking,
			model.OpTypeDoubleEndorsement,
			model.OpTypeDoublePreendorsement:
			// sender == accuser
			// receiver == offender

			// credit accuser
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown 2B/2E/2PE accuser %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.AccusationIncome += op.Volume * mul
			in.TotalIncome += op.Volume * mul

			// debit offender
			in, ok = incomeMap[op.ReceiverId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.ReceiverId)
				if err != nil {
					return fmt.Errorf("income: unknown 2B/2E/2PE accuser %d in %s op %d in block %d c%d",
						op.ReceiverId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.AccusationLoss += op.Burned * mul
			in.TotalLoss += op.Burned * mul

		case model.OpTypeStakeSlash:
			// sender == accuser
			// receiver == offender

			// credit accuser
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown 2B/2E/2PE accuser %d in %s op %d in block %d c%d",
						op.SenderId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.AccusationIncome += op.Reward * mul
			in.TotalIncome += op.Reward * mul

			// debit offender
			in, ok = incomeMap[op.ReceiverId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.ReceiverId)
				if err != nil {
					return fmt.Errorf("income: unknown 2B/2E/2PE accuser %d in %s op %d in block %d c%d",
						op.ReceiverId, op.Type, op.Id(), block.Height, block.Cycle)
				}
				incomeMap[in.AccountId] = in
			}
			in.AccusationLoss += op.Deposit * mul
			in.TotalLoss += op.Deposit * mul
		}
	}

	// use flows from double-x to account specific loss categories
	for _, f := range block.Flows {
		if f.Type != model.FlowTypePenalty {
			continue
		}
		// debit offender
		in, ok := incomeMap[f.AccountId]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, f.AccountId)
			if err != nil {
				return fmt.Errorf("income: unknown 2bake/2endorse offender %d", f.AccountId)
			}
			incomeMap[f.AccountId] = in
		}
		// sum individual losses
		switch f.Kind {
		case model.FlowKindDeposits:
			in.LostAccusationDeposits += f.AmountOut * mul
		case model.FlowKindRewards:
			in.LostAccusationRewards += f.AmountOut * mul
		case model.FlowKindFees:
			in.LostAccusationFees += f.AmountOut * mul
		case model.FlowKindStake:
			in.LostAccusationDeposits += f.AmountOut * mul
		}
	}

	if len(incomeMap) == 0 {
		return nil
	}

	upd := make([]pack.Item, 0, len(incomeMap))
	for _, v := range incomeMap {
		var reliability int64
		bkr, ok := builder.BakerById(v.AccountId)
		if ok {
			reliability = bkr.Reliability
		}
		v.UpdatePerformance(reliability)
		upd = append(upd, v)
	}
	return idx.table.Update(ctx, upd)
}

func (idx *IncomeIndex) updateNonceRevelations(ctx context.Context, block *model.Block, builder model.BlockBuilder, isRollback bool) error {
	cycle := block.Cycle - 1
	if cycle < 0 {
		return nil
	}
	var err error
	incomeMap := make(map[model.AccountID]*model.Income)
	var mul int64 = 1
	if isRollback {
		mul = -1
	}

	for _, f := range block.Flows {
		// skip irrelevant flows
		if f.Type != model.FlowTypeNonceRevelation || !f.IsBurned {
			continue
		}
		// find and update the income row
		in, ok := incomeMap[f.AccountId]
		if !ok {
			in, err = idx.loadIncome(ctx, cycle, f.AccountId)
			if err != nil {
				return fmt.Errorf("income: unknown seed nonce baker %d", f.AccountId)
			}
			incomeMap[in.AccountId] = in
		}
		in.TotalLoss += f.AmountOut * mul
		switch f.Kind {
		case model.FlowKindRewards:
			in.SeedLoss += f.AmountOut * mul
			in.LostSeedRewards += f.AmountOut * mul
		case model.FlowKindFees:
			in.SeedLoss += block.Fee * mul
			in.LostSeedFees += block.Fee * mul
		case model.FlowKindBalance:
			// Ithaca losses
			in.SeedLoss += f.AmountOut * mul
			in.LostSeedRewards += f.AmountOut * mul
		}
	}

	if len(incomeMap) == 0 {
		return nil
	}

	upd := make([]pack.Item, 0, len(incomeMap))
	for _, v := range incomeMap {
		var reliability int64
		bkr, ok := builder.BakerById(v.AccountId)
		if ok {
			reliability = bkr.Reliability
		}
		v.UpdatePerformance(reliability)
		upd = append(upd, v)
	}
	return idx.table.Update(ctx, upd)
}

func (idx *IncomeIndex) loadIncome(ctx context.Context, cycle int64, id model.AccountID) (*model.Income, error) {
	if cycle < 0 && id <= 0 {
		return nil, model.ErrNoIncome
	}
	// try load from cache
	in, ok := idx.cache[id]
	if ok && in.Cycle == cycle {
		return in, nil
	}
	// load from table
	in = &model.Income{}
	err := pack.NewQuery("etl.search").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		AndEqual("account_id", id).
		Execute(ctx, in)
	if err != nil || in.RowId == 0 {
		return nil, model.ErrNoIncome
	}
	if idx.cache != nil && cycle == idx.cycle {
		idx.cache[id] = in
	}
	return in, nil
}

func (idx *IncomeIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *IncomeIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
