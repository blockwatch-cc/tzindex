// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

const (
	IncomePackSizeLog2    = 15 // =32k packs ~ 3M unpacked
	IncomeJournalSizeLog2 = 16 // =64k entries for busy blockchains
	IncomeCacheSize       = 2  // minimum
	IncomeFillLevel       = 90
	IncomeIndexKey        = "income"
	IncomeTableKey        = "income"
)

var (
	ErrNoIncomeEntry = errors.New("income not indexed")
)

type IncomeIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
	cache map[model.AccountID]*model.Income
	cycle int64
}

var _ model.BlockIndexer = (*IncomeIndex)(nil)

func NewIncomeIndex(opts pack.Options) *IncomeIndex {
	return &IncomeIndex{opts: opts}
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
	fields, err := pack.Fields(model.Income{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		IncomeTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, IncomePackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, IncomeJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, IncomeCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, IncomeFillLevel),
		})
	if err != nil {
		return err
	}
	return nil
}

func (idx *IncomeIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		IncomeTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, IncomeJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, IncomeCacheSize),
		})
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
	if block.Params.IsCycleStart(block.Height) {
		// this only happens during mainnet bootstrap
		if err := idx.updateCycleIncome(ctx, block, builder); err != nil {
			log.Error(err)
			// return err
			return nil
		}
		// reset cache at start of cycle
		idx.cache = make(map[model.AccountID]*model.Income)
		idx.cycle = block.Cycle
	}

	// skip when no new rights are defined
	if len(block.TZ.Baking) > 0 || len(block.TZ.Endorsing) > 0 {
		if err := idx.createCycleIncome(ctx, block, builder); err != nil {
			log.Error(err)
			// return err
		}
	}

	// update income from flows and rights
	if err := idx.updateBlockIncome(ctx, block, builder, false); err != nil {
		log.Error(err)
		// return err
		return nil
	}

	// update burn from nonce revelations, if any
	if err := idx.updateNonceRevelations(ctx, block, builder, false); err != nil {
		log.Error(err)
		// return err
		return nil
	}

	return nil
}

func (idx *IncomeIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// rollback current income
	if err := idx.updateBlockIncome(ctx, block, builder, true); err != nil {
		log.Error(err)
		// return err
		return nil
	}

	// update burn from nonce revelations, if any
	if err := idx.updateNonceRevelations(ctx, block, builder, true); err != nil {
		log.Error(err)
		// return err
		return nil
	}

	// new rights are fetched in cycles
	if block.Params.IsCycleStart(block.Height) {
		if err := idx.DeleteCycle(ctx, block.Cycle+block.Params.PreservedCycles); err != nil {
			log.Error(err)
			// return err
			return nil
		}
	}
	return nil
}

func (idx *IncomeIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *IncomeIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting income for cycle %d", cycle)
	_, err := pack.NewQuery("etl.income.delete", idx.table).
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

	for cycle := int64(0); cycle < p.PreservedCycles+1; cycle++ {
		// log.Infof("Income: bootstrap for cycle %d for %d bakers", cycle, len(accs))

		// new income data for each cycle
		var totalRolls int64
		incomeMap := make(map[model.AccountID]*model.Income)
		for _, v := range bkrs {
			rolls := v.StakingBalance() / p.TokensPerRoll
			totalRolls += rolls
			incomeMap[v.AccountId] = &model.Income{
				Cycle:        cycle,
				AccountId:    v.AccountId,
				Rolls:        rolls,
				Balance:      v.Balance(),
				Delegated:    v.DelegatedBalance,
				NDelegations: v.ActiveDelegations,
				LuckPct:      10000,
			}
		}

		// log.Debugf("New bootstrap income for cycle %d from no snapshot with %d bakers",
		// 	cycle, len(incomeMap))

		// pre-calculate deposit and reward amounts
		blockDeposit, endorseDeposit := p.BlockSecurityDeposit, p.EndorsementSecurityDeposit
		if cycle < p.SecurityDepositRampUpCycles-1 {
			blockDeposit = blockDeposit * cycle / p.SecurityDepositRampUpCycles
			endorseDeposit = endorseDeposit * cycle / p.SecurityDepositRampUpCycles
		}
		blockReward, endorseReward := p.BlockReward, p.EndorsementReward
		if cycle < p.NoRewardCycles {
			blockReward, endorseReward = 0, 0
		}

		// assign baking rights
		cycleEndBlock := p.CycleEndHeight(cycle)
		for _, v := range block.TZ.Baking[int(cycle)] {
			if v.Round > 0 || v.Level > cycleEndBlock {
				continue
			}
			acc, ok := builder.AccountByAddress(v.Address())
			if !ok {
				return fmt.Errorf("income: missing bootstrap baker %s", v.Address())
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for baker %s %d", v.Address(), acc.RowId)
			}
			ic.NBakingRights++
			ic.ExpectedIncome += blockReward
		}

		// assign from endorsement rights in protos before Ithaca
		for _, v := range block.TZ.Endorsing[int(cycle)] {
			// store spillove rrights
			if v.Level >= cycleEndBlock {
				spillover = append(spillover, v)
				continue
			}
			acc, ok := builder.AccountByAddress(v.Address())
			if !ok {
				return fmt.Errorf("income: missing bootstrap endorser %s", v.Address())
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for endorser %s %d", v.Address(), acc.RowId)
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
				return fmt.Errorf("income: missing bootstrap endorser %s", v.Address())
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for endorser %s %d", v.Address(), acc.RowId)
			}
			ic.NEndorsingRights += int64(v.Power)
			if p.Version < 12 {
				ic.ExpectedIncome += endorseReward * int64(v.Power)
			}
		}
		spillover = spillover[:0]

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
			endorseRewardCycle := big.NewInt(p.EndorsingRewardPerSlot * int64(p.ConsensusCommitteeSize) * p.BlocksPerCycle)
			totalStake := big.NewInt(info.TotalStake)
			for _, bkr := range bkrs {
				income, ok := incomeMap[bkr.AccountId]
				if !ok {
					continue
				}
				stake := info.BakerStake
				for i := 0; i < len(stake); i++ {
					// find baker in list
					if stake[i].Baker.Equal(bkr.Address) {
						income.Rolls = stake[i].ActiveStake / p.TokensPerRoll
						income.ActiveStake = stake[i].ActiveStake
						// protect against int64 overflow
						activeStake := big.NewInt(income.ActiveStake)
						activeStake.Mul(activeStake, endorseRewardCycle)
						activeStake.Div(activeStake, totalStake)
						income.ExpectedIncome += activeStake.Int64()
						stake = append(stake[:i], stake[i+1:]...)
						break
					}
				}
			}
		}

		// calculate luck and append for insert
		inc := make([]*model.Income, 0, len(incomeMap))
		for _, v := range incomeMap {
			v.UpdateLuck(totalRolls, p)
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
func (idx *IncomeIndex) updateCycleIncome(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	p := block.Params
	if !p.IsMainnet() {
		return nil
	}

	// check pre-conditon and pick cycles to update
	var updateCycles []int64
	switch true {
	case block.Cycle <= 2*(p.PreservedCycles+2):
		// during ramp-up cycles
		// log.Debugf("Updating expected income for cycle %d during ramp-up.", block.Cycle)
		updateCycles = []int64{block.Cycle}

	default:
		// no update required on
		return nil
	}

	blockReward, endorseReward := p.BlockReward, p.EndorsementReward

	for _, v := range updateCycles {
		// log.Infof("Income: update for cycle %d ", v)
		incomes := make([]*model.Income, 0)
		var totalRolls int64
		err := idx.table.Stream(ctx,
			pack.NewQuery("etl.income.update", idx.table).AndEqual("cycle", v),
			func(r pack.Row) error {
				in := &model.Income{}
				if err := r.Decode(in); err != nil {
					return err
				}
				in.ExpectedIncome = blockReward * in.NBakingRights
				in.ExpectedIncome += endorseReward * in.NEndorsingRights
				totalRolls += in.Rolls
				incomes = append(incomes, in)
				return nil
			})
		if err != nil {
			return err
		}

		// update luck and convert type
		upd := make([]pack.Item, len(incomes))
		for i, v := range incomes {
			v.UpdateLuck(totalRolls, p)
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
	var totalRolls int64

	if sn.Base < 0 {
		// build income from current balance values because there is no snapshot yet
		bkrs := make([]*model.Baker, 0)
		for _, v := range builder.Bakers() {
			bkrs = append(bkrs, v)
		}
		sort.Slice(bkrs, func(i, j int) bool { return bkrs[i].AccountId < bkrs[j].AccountId })

		for _, v := range bkrs {
			// skip inactive bakers and bakers with no roll
			rolls := v.StakingBalance() / p.TokensPerRoll
			if !v.IsActive || rolls == 0 {
				continue
			}
			log.Debugf("income: created income record for baker %s %d for cycle %d without snapshot", v, v.AccountId, sn.Cycle)
			incomeMap[v.AccountId] = &model.Income{
				Cycle:        sn.Cycle,
				AccountId:    v.AccountId,
				Rolls:        rolls,
				Balance:      v.Balance(),
				Delegated:    v.DelegatedBalance,
				NDelegations: v.ActiveDelegations,
				LuckPct:      10000,
			}
			totalRolls += rolls
		}

	} else {
		// build income from snapshot
		snap, err := builder.Table(SnapshotTableKey)
		if err != nil {
			return err
		}
		s := &model.Snapshot{}
		err = pack.NewQuery("snapshot.create_income", snap).
			WithoutCache().
			AndEqual("cycle", sn.Base).  // source snapshot cycle
			AndEqual("index", sn.Index). // selected index
			AndEqual("is_baker", true).  // bakers only (active+inactive)
			Stream(ctx, func(r pack.Row) error {
				if err := r.Decode(s); err != nil {
					return err
				}
				if s.Rolls > 0 {
					incomeMap[s.AccountId] = &model.Income{
						Cycle:        sn.Cycle, // the future cycle
						AccountId:    s.AccountId,
						Rolls:        s.Rolls,
						Balance:      s.Balance,
						Delegated:    s.Delegated,
						NDelegations: s.NDelegations,
						LuckPct:      10000,
					}
					totalRolls += s.Rolls
				}
				return nil
			})
		if err != nil {
			return err
		}

		log.Debugf("income: created records for %d bakers in cycle %d from snapshot [%d/%d]",
			len(incomeMap), sn.Cycle, sn.Base, sn.Index)
	}

	// assign from rights
	for _, v := range block.TZ.Baking[0] {
		if v.Round > 0 {
			continue
		}
		bkr, ok := builder.BakerByAddress(v.Address())
		if !ok {
			log.Errorf("income: missing baker %s with rights in cycle %d", v.Address(), block.Cycle)
			continue
		}
		ic, ok := incomeMap[bkr.AccountId]
		if !ok {
			log.Debugf("income: create income record for baker %s %d with rights for cycle %d without snapshot", bkr, bkr.AccountId, sn.Cycle)
			ic = &model.Income{
				Cycle:        sn.Cycle,
				AccountId:    bkr.AccountId,
				Rolls:        bkr.StakingBalance() / p.TokensPerRoll,
				Balance:      bkr.Balance(),
				Delegated:    bkr.DelegatedBalance,
				NDelegations: bkr.ActiveDelegations,
				LuckPct:      10000,
			}
			incomeMap[bkr.AccountId] = ic
			totalRolls += ic.Rolls
		}
		ic.NBakingRights++
		ic.ExpectedIncome += p.BlockReward
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
		bkr, ok := builder.BakerByAddress(v.Address())
		if !ok {
			log.Errorf("income: missing endorser %s with rights in cycle %d", v.Address(), sn.Cycle)
			continue
		}
		ic, ok := incomeMap[bkr.AccountId]
		if !ok {
			log.Debugf("income: create income record for endorser %s %d with rights for cycle %d without snapshot", bkr, bkr.AccountId, sn.Cycle)
			ic = &model.Income{
				Cycle:        sn.Cycle,
				AccountId:    bkr.AccountId,
				Rolls:        bkr.StakingBalance() / p.TokensPerRoll,
				Balance:      bkr.Balance(),
				Delegated:    bkr.DelegatedBalance,
				NDelegations: bkr.ActiveDelegations,
				LuckPct:      10000,
			}
			incomeMap[bkr.AccountId] = ic
			totalRolls += ic.Rolls
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
			log.Errorf("income: empty endorser in last block rights %#v", block.TZ.PrevEndorsing)
			continue
		}
		bkr, ok := builder.BakerByAddress(v.Address())
		if !ok {
			log.Errorf("income: missing endorser %s with last block rights in previous cycle %d", v.Address(), sn.Cycle-1)
			continue
		}
		ic, ok := incomeMap[bkr.AccountId]
		if !ok {
			log.Debugf("income: create income record for endorser %s %d with rights for last block in cycle %d without snapshot", bkr, bkr.AccountId, sn.Cycle-1)
			ic = &model.Income{
				Cycle:        sn.Cycle,
				AccountId:    bkr.AccountId,
				Rolls:        bkr.StakingBalance() / p.TokensPerRoll,
				Balance:      bkr.Balance(),
				Delegated:    bkr.DelegatedBalance,
				NDelegations: bkr.ActiveDelegations,
				LuckPct:      10000,
			}
			incomeMap[bkr.AccountId] = ic
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
		endorseRewardCycle := big.NewInt(p.EndorsingRewardPerSlot * int64(p.ConsensusCommitteeSize) * p.BlocksPerCycle)
		totalStake := big.NewInt(info.TotalStake)
		totalRolls = 0
		for _, bkr := range builder.Bakers() {
			income, ok := incomeMap[bkr.AccountId]
			if !ok {
				continue
			}
			stake := info.BakerStake
			for i := 0; i < len(stake); i++ {
				// find baker in list
				if stake[i].Baker.Equal(bkr.Address) {
					income.Rolls = stake[i].ActiveStake / p.TokensPerRoll
					income.ActiveStake = stake[i].ActiveStake
					// protect against int64 overflow
					exp := big.NewInt(income.ActiveStake)
					exp.Mul(exp, endorseRewardCycle)
					exp.Div(exp, totalStake)
					income.ExpectedIncome += exp.Int64()
					stake = append(stake[:i], stake[i+1:]...)
					totalRolls += income.Rolls
					break
				}
			}
		}
	}

	// prepare income records for insert
	inc := make([]*model.Income, 0, len(incomeMap))
	for _, v := range incomeMap {
		// set 100% performance when no rights are assigned (this will not update later)
		if v.NBakingRights+v.NEndorsingRights == 0 {
			v.ContributionPct = 10000
			v.PerformancePct = 10000
		}
		// calculate luck and append for insert (we're still using rolls here although
		// its deprecated in Ithaca, but earlier protocols require it and its a fair
		// estimate)
		v.UpdateLuck(totalRolls, p)
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

	// - post-Ithaca deposits count from frozen deposit balance in accounts
	// - deposits are processed at end of cycle
	// - don't run this function during protocol upgrade code (flows == 0)
	if p.IsCycleEnd(block.Height) && len(block.Flows) > 0 && p.Version >= 12 {
		upd := make([]pack.Item, 0)
		for _, bkr := range builder.Bakers() {
			if bkr.StakingBalance() < p.TokensPerRoll {
				continue
			}
			// load next cycle's income
			in, err := idx.loadIncome(ctx, block.Cycle+1, bkr.AccountId)
			if err != nil {
				// deposit is paid right after activation, but before rights/income
				// exists - we expect multiple deposit events happening on a fresh
				// baker before he get rights, but income records are only created for
				// bakers with rights
				if err != ErrNoIncomeEntry {
					return fmt.Errorf("income: handling deposit for baker %d at height %d in cycle %d: %w",
						bkr.AccountId, block.Height, block.Cycle+1, err)
				}
				continue
			}
			in.TotalDeposits = bkr.FrozenDeposits
			upd = append(upd, in)
		}
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
	}

	// replay rights effects
	if block.AbsentBaker > 0 {
		in, ok := incomeMap[block.AbsentBaker]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, block.AbsentBaker)
			if err != nil {
				// should not happen
				// log error (likely from just deactivated bakers or
				// bakers without rights in cycle)
				log.Errorf("income: unknown absent baker %d in block %d c%d",
					block.AbsentBaker, block.Height, block.Cycle)
				// return fmt.Errorf("income: unknown absent baker %d in block %d c%d",
				// 	block.AbsentBaker, block.Height, block.Cycle)
			} else {
				incomeMap[in.AccountId] = in
			}
		}
		if in != nil {
			in.NBlocksNotBaked++
		}
	}
	// we count missing endorsements in cycle start block to next cycle
	for _, v := range block.AbsentEndorsers {
		in, ok := incomeMap[v]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, v)
			if err != nil {
				// ignore error (likely from just deactivated bakers or
				// bakers without rights in cycle)
				log.Debugf("income: unknown absent endorser %d in block %d c%d",
					v, block.Height, block.Cycle)
				// return fmt.Errorf("income: unknown absent endorser %d in block %d c%d",
				// 	v, block.Height, block.Cycle)
			} else {
				incomeMap[in.AccountId] = in
			}
		}
		if in != nil {
			in.NBlocksNotEndorsed++
		}
	}

	// replay operation effects
	// Note: ignore post-Itahca deposit ops here because they affect the next cycle
	// we already handle this case more efficiently above
	for _, op := range block.Ops {
		switch op.Type {
		case model.OpTypeDeposit:
			// the very first mainnet cycle 468 on Ithaca created a deposit
			// operation at the first cycle block (!)
			if p.Version < 12 || !p.IsCycleStart(block.Height) {
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
			in.TotalDeposits = bkr.FrozenDeposits

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
			in.TotalDeposits += op.Deposit * mul
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
			in.TotalDeposits += op.Deposit * mul

		case model.OpTypeDoubleBaking, model.OpTypeDoubleEndorsement, model.OpTypeDoublePreendorsement:
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
		}
	}

	// use flows from double-x to account specific loss categories
	for _, f := range block.Flows {
		if f.Operation != model.FlowTypePenalty {
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
		switch f.Category {
		case model.FlowCategoryDeposits:
			in.LostAccusationDeposits += f.AmountOut * mul
		case model.FlowCategoryRewards:
			in.LostAccusationRewards += f.AmountOut * mul
		case model.FlowCategoryFees:
			in.LostAccusationFees += f.AmountOut * mul
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
		if f.Operation != model.FlowTypeNonceRevelation || !f.IsBurned {
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
		switch f.Category {
		case model.FlowCategoryRewards:
			in.SeedLoss += f.AmountOut * mul
			in.LostSeedRewards += f.AmountOut * mul
		case model.FlowCategoryFees:
			in.SeedLoss += block.Fee * mul
			in.LostSeedFees += block.Fee * mul
		case model.FlowCategoryBalance:
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
		return nil, ErrNoIncomeEntry
	}
	// try load from cache
	in, ok := idx.cache[id]
	if ok && in.Cycle == cycle {
		return in, nil
	}
	// load from table
	in = &model.Income{}
	err := pack.NewQuery("etl.income.search", idx.table).
		AndEqual("cycle", cycle).
		AndEqual("account_id", id).
		Execute(ctx, in)
	if err != nil || in.RowId == 0 {
		return nil, ErrNoIncomeEntry
	}
	if idx.cache != nil && cycle == idx.cycle {
		idx.cache[id] = in
	}
	return in, nil
}

func (idx *IncomeIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
