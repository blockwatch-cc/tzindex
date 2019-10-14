// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package index

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strconv"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	. "blockwatch.cc/tzindex/etl/model"
)

const (
	IncomePackSizeLog2    = 15 // =32k packs
	IncomeJournalSizeLog2 = 17 // =128k entries
	IncomeCacheSize       = 2  // minimum
	IncomeFillLevel       = 100
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
}

var _ BlockIndexer = (*IncomeIndex)(nil)

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
	fields, err := pack.Fields(Income{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
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

func (idx *IncomeIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %v", idx.Name(), err)
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

func (idx *IncomeIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// update income from flows and rights
	if err := idx.UpdateBlockIncome(ctx, block, builder, false); err != nil {
		return err
	}

	// skip when no new rights are defined
	if len(block.TZ.Baking) == 0 || len(block.TZ.Endorsing) == 0 {
		return nil
	}

	// bootstrap first 7 cycles on first block using all foundation bakers as sanpshot proxy
	if block.Height == 1 {
		return idx.BootstrapIncome(ctx, block, builder)
	}

	return idx.CreateCycleIncome(ctx, block, builder)
}

func (idx *IncomeIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// rollback current income
	if err := idx.UpdateBlockIncome(ctx, block, builder, true); err != nil {
		return err
	}

	// new rights are fetched in cycles
	if block.Params.IsCycleStart(block.Height) {
		return idx.DeleteCycle(ctx, block.Height)
	}
	return nil
}

func (idx *IncomeIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *IncomeIndex) BootstrapIncome(ctx context.Context, block *Block, builder BlockBuilder) error {
	// on bootstrap use the initial params from block 1
	p := block.Params

	// get sorted list of foundation bakers
	accs := make([]*Account, 0, 8)
	for _, v := range builder.Delegates() {
		accs = append(accs, v)
	}
	sort.Slice(accs, func(i, j int) bool { return accs[i].RowId < accs[j].RowId })

	for cycle := int64(0); cycle < p.PreservedCycles+2; cycle++ {
		// new income data for each cycle
		var totalRolls int64
		incomeMap := make(map[AccountID]*Income)
		for _, v := range accs {
			rolls := v.StakingBalance() / p.TokensPerRoll
			totalRolls += rolls
			incomeMap[v.RowId] = &Income{
				Cycle:        cycle,
				AccountId:    v.RowId,
				Rolls:        rolls,
				Balance:      v.Balance(),
				Delegated:    v.DelegatedBalance,
				NDelegations: v.ActiveDelegations,
				LuckPercent:  100.0,
			}
		}

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

		for _, v := range block.TZ.Baking {
			if p.CycleFromHeight(v.Level) != cycle || v.Priority > 0 {
				continue
			}
			acc, ok := builder.AccountByAddress(v.Delegate)
			if !ok {
				return fmt.Errorf("income: missing bootstrap baker %s", v.Delegate)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for baker %s %d", v.Delegate, acc.RowId)
			}
			ic.NBakingRights++
			ic.ExpectedIncome += blockReward
			ic.ExpectedBonds += blockDeposit
		}

		// set correct expectations about endorsement rewards for the last block in a cycle:
		// endorsement income for a cycle is left-shifted by 1 (the last block in a cycle
		// is endorsed in the next cycle and this shifts income from rights into this cycle too)
		endorseStartBlock := p.CycleEndHeight(cycle - 1)
		endorseEndBlock := p.CycleEndHeight(cycle) - 1
		for _, v := range block.TZ.Endorsing {
			if v.Level < endorseStartBlock || v.Level > endorseEndBlock {
				continue
			}
			acc, ok := builder.AccountByAddress(v.Delegate)
			if !ok {
				return fmt.Errorf("income: missing bootstrap endorser %s", v.Delegate)
			}
			ic, ok := incomeMap[acc.RowId]
			if !ok {
				return fmt.Errorf("income: missing bootstrap income data for endorser %s %d", v.Delegate, acc.RowId)
			}
			ic.NEndorsingRights += int64(len(v.Slots))
			ic.ExpectedIncome += endorseReward * int64(len(v.Slots))
			ic.ExpectedBonds += endorseDeposit * int64(len(v.Slots))
		}

		// calculate luck and append for insert
		inc := make([]*Income, 0, len(incomeMap))
		for _, v := range incomeMap {
			// fraction of all rolls
			rollsShare := float64(v.Rolls) / float64(totalRolls)

			// full blocks
			fairBakingShare := int64(math.Round(rollsShare * float64(p.BlocksPerCycle)))

			// full endorsements
			fairEndorsingShare := int64(math.Round(rollsShare * float64(p.BlocksPerCycle) * float64(p.EndorsersPerBlock)))

			// full income as a multiple of blocks and endorsements
			fairIncome := fairBakingShare * blockReward
			fairIncome += fairEndorsingShare * endorseReward

			// diff between expected and fair (positive when higher, negative when lower)
			v.Luck = v.ExpectedIncome - fairIncome

			// absolute luck as expected vs fair income where 100% is the ideal case
			// =100%: fair == expected (luck == 0)
			// <100%: fair > expected (luck < 0)
			// >100%: fair < expected (luck > 0)
			if fairIncome > 0 {
				v.LuckPercent = (1.0 + float64(v.Luck)/float64(fairIncome)) * 100.0
			}

			// collect for insert
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

func (idx *IncomeIndex) CreateCycleIncome(ctx context.Context, block *Block, builder BlockBuilder) error {
	sn := block.TZ.Snapshot
	incomeMap := make(map[AccountID]*Income)

	// build income from snapshot
	snap, err := builder.Table(SnapshotIndexKey)
	if err != nil {
		return err
	}
	// FIXME: params should come from the future cycle;
	// FIXEM: preserved cycles, deposits and rewards change on protocol upgrade
	p := block.Params
	s := &Snapshot{}
	var totalRolls int64
	err = snap.Stream(ctx, pack.Query{
		Name:    "snapshot.income_list",
		NoCache: true,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: snap.Fields().Find("c"), // cycle
				Mode:  pack.FilterModeEqual,
				Value: sn.Cycle - (p.PreservedCycles + 2), // adjust to source snapshot cycle
			},
			pack.Condition{
				Field: snap.Fields().Find("i"), // index
				Mode:  pack.FilterModeEqual,
				Value: sn.RollSnapshot, // the selected index
			},
			pack.Condition{
				Field: snap.Fields().Find("v"), // (previously) active delegates only
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		}}, func(r pack.Row) error {
		if err := r.Decode(s); err != nil {
			return err
		}
		incomeMap[s.AccountId] = &Income{
			Cycle:        sn.Cycle, // the future cycle
			AccountId:    s.AccountId,
			Rolls:        s.Rolls,
			Balance:      s.Balance,
			Delegated:    s.Delegated,
			NDelegations: s.NDelegations,
			LuckPercent:  100.0,
		}
		totalRolls += s.Rolls
		return nil
	})
	if err != nil {
		return err
	}

	// pre-calculate deposit and reward amounts
	blockDeposit, endorseDeposit := p.BlockSecurityDeposit, p.EndorsementSecurityDeposit
	if sn.Cycle < p.SecurityDepositRampUpCycles-1 {
		blockDeposit = blockDeposit * sn.Cycle / p.SecurityDepositRampUpCycles
		endorseDeposit = endorseDeposit * sn.Cycle / p.SecurityDepositRampUpCycles
	}
	blockReward, endorseReward := p.BlockReward, p.EndorsementReward
	if sn.Cycle < p.NoRewardCycles {
		blockReward, endorseReward = 0, 0
	}

	// assign from rights
	for _, v := range block.TZ.Baking {
		if v.Priority > 0 {
			continue
		}
		acc, ok := builder.AccountByAddress(v.Delegate)
		if !ok {
			return fmt.Errorf("income: missing baker %s", v.Delegate)
		}
		ic, ok := incomeMap[acc.RowId]
		if !ok {
			return fmt.Errorf("income: missing income data for baker %s %d at %d[%d]", v.Delegate, acc.RowId, s.Cycle, s.Index)
		}
		ic.NBakingRights++
		ic.ExpectedIncome += blockReward
		ic.ExpectedBonds += blockDeposit
	}

	// set correct expectations about endorsement rewards for the last block in a cycle:
	// endorsement income for a cycle is left-shifted by 1 (the last block in a cycle
	// is endorsed in the next cycle and this shifts income from rights into this cycle too)
	endorseStartBlock := p.CycleEndHeight(sn.Cycle - 1)
	endorseEndBlock := p.CycleEndHeight(sn.Cycle) - 1

	for _, v := range block.TZ.Endorsing {
		if v.Level > endorseEndBlock {
			// log.Infof("Skipping end of cycle height %d > %d", v.Level, endorseEndBlock)
			continue
		}
		acc, ok := builder.AccountByAddress(v.Delegate)
		if !ok {
			return fmt.Errorf("income: missing endorser %s", v.Delegate)
		}
		ic, ok := incomeMap[acc.RowId]
		if !ok {
			return fmt.Errorf("income: missing income data for endorser %s %d at %d[%d]", v.Delegate, acc.RowId, s.Cycle, s.Index)
		}
		ic.NEndorsingRights += int64(len(v.Slots))
		ic.ExpectedIncome += endorseReward * int64(len(v.Slots))
		ic.ExpectedBonds += endorseDeposit * int64(len(v.Slots))
	}
	// load endorse rights for last block of previous cycle
	rights, err := builder.Table(RightsIndexKey)
	if err != nil {
		return err
	}
	right := Right{}
	err = rights.Stream(ctx, pack.Query{
		Name:   "etl.rights.search",
		Fields: rights.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: rights.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: endorseStartBlock,
			},
			pack.Condition{
				Field: rights.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.RightTypeEndorsing),
			},
		},
	}, func(r pack.Row) error {
		if err := r.Decode(&right); err != nil {
			return err
		}
		// the previous cycle could have more delegates which still get some trailing
		// endorsement rewards here even though they may not have any more rights
		ic, ok := incomeMap[right.AccountId]
		if !ok {
			// load prev data
			ic, err = idx.loadIncome(ctx, right.Cycle, right.AccountId)
			if err != nil {
				return fmt.Errorf("income: missing income data for prev cycle endorser %d at %d[%d]", right.AccountId, s.Cycle, s.Index)
			}
			// copy to new income struct
			ic = &Income{
				Cycle:        sn.Cycle, // the future cycle
				AccountId:    right.AccountId,
				Rolls:        ic.Rolls,
				Balance:      ic.Balance,
				Delegated:    ic.Delegated,
				NDelegations: ic.NDelegations,
				LuckPercent:  100.0,
			}
			incomeMap[ic.AccountId] = ic
		}
		ic.NEndorsingRights++
		ic.ExpectedIncome += endorseReward
		ic.ExpectedBonds += endorseDeposit
		return nil
	})
	if err != nil {
		return err
	}

	// calculate luck and append for insert
	inc := make([]*Income, 0, len(incomeMap))
	for _, v := range incomeMap {
		// fraction of all rolls
		rollsShare := float64(v.Rolls) / float64(totalRolls)

		// full blocks
		fairBakingShare := int64(math.Round(rollsShare * float64(p.BlocksPerCycle)))

		// full endorsements
		fairEndorsingShare := int64(math.Round(rollsShare * float64(p.BlocksPerCycle) * float64(p.EndorsersPerBlock)))

		// full income as a multiple of blocks and endorsements
		fairIncome := fairBakingShare * blockReward
		fairIncome += fairEndorsingShare * endorseReward

		// diff between expected and fair (positive when higher, negative when lower)
		v.Luck = v.ExpectedIncome - fairIncome

		// absolute luck as expected vs fair income where 100% is the ideal case
		// =100%: fair == expected (luck == 0)
		// <100%: fair > expected (luck < 0)
		// >100%: fair < expected (luck > 0)
		if fairIncome > 0 {
			v.LuckPercent = (1.0 + float64(v.Luck)/float64(fairIncome)) * 100.0
		}

		// collect for insert
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

func (idx *IncomeIndex) UpdateBlockIncome(ctx context.Context, block *Block, builder BlockBuilder, isRollback bool) error {
	var err error
	p := block.Params
	incomeMap := make(map[AccountID]*Income)
	var mul int64 = 1
	if isRollback {
		mul = -1
	}

	// handle flows from (baking, endorsing, seed nonce, double baking, double endorsement)
	for _, f := range block.Flows {
		// all income is frozen, so ignore any other flow right away
		if !f.IsFrozen {
			continue
		}
		// fetch baker from map
		in, ok := incomeMap[f.AccountId]
		if !ok {
			in, err = idx.loadIncome(ctx, block.Cycle, f.AccountId)
			if err != nil {
				return fmt.Errorf("income: unknown baker %d", f.AccountId)
			}
			incomeMap[in.AccountId] = in
		}

		switch f.Operation {
		case FlowTypeBaking:
			switch f.Category {
			case FlowCategoryDeposits:
				in.TotalBonds += f.AmountIn * mul

			case FlowCategoryRewards:
				in.TotalIncome += f.AmountIn * mul
				in.BakingIncome += f.AmountIn * mul
				if block.Priority > 0 {
					// the real baker stole this income
					in.StolenBakingIncome += f.AmountIn * mul

					// the original prio 0 baker lost it
					for _, v := range builder.Rights(chain.RightTypeBaking) {
						if v.Priority > 0 {
							continue
						}
						loser, ok := incomeMap[v.AccountId]
						if !ok {
							loser, err = idx.loadIncome(ctx, block.Cycle, v.AccountId)
							if err != nil {
								return fmt.Errorf("income: unknown losing baker %d", v.AccountId)
							}
							incomeMap[loser.AccountId] = loser
						}
						loser.LostBakingIncome += f.AmountIn * mul
					}
				}
			}
		case FlowTypeEndorsement:
			switch f.Category {
			case FlowCategoryDeposits:
				in.TotalBonds += f.AmountIn * mul
			case FlowCategoryRewards:
				in.TotalIncome += f.AmountIn * mul
				in.EndorsingIncome += f.AmountIn * mul
			}

		case FlowTypeNonceRevelation:
			if f.Category == FlowCategoryRewards {
				in.TotalIncome += f.AmountIn * mul
				in.SeedIncome += f.AmountIn * mul
			}

		case FlowTypeDenounciation:
			// there's only one flow type here, so we cannot split 2bake and 2endorse
			// will be handled using ops

		default:
			// fee flows from all kinds of operations
			if f.Category == FlowCategoryFees {
				in.FeesIncome += f.AmountIn * mul
			}
		}
	}

	// update bake counters separate from flow
	if block.BakerId > 0 {
		baker, ok := incomeMap[block.BakerId]
		if !ok {
			baker, err = idx.loadIncome(ctx, block.Cycle, block.BakerId)
			if err != nil {
				return fmt.Errorf("income: unknown baker %d", block.BakerId)
			}
			incomeMap[baker.AccountId] = baker
		}
		baker.NBlocksBaked += mul
		if block.Priority > 0 {
			// the real baker stole this block
			baker.NBlocksStolen += mul

			// the original prio 0 baker lost it
			for _, v := range builder.Rights(chain.RightTypeBaking) {
				if v.Priority > 0 {
					continue
				}
				loser, ok := incomeMap[v.AccountId]
				if !ok {
					loser, err = idx.loadIncome(ctx, block.Cycle, v.AccountId)
					if err != nil {
						return fmt.Errorf("income: unknown losing baker %d", v.AccountId)
					}
					incomeMap[loser.AccountId] = loser
				}
				loser.NBlocksLost += mul
			}
		}
	}

	// for counters and denounciations we parse operations
	for _, op := range block.Ops {
		switch op.Type {
		case chain.OpTypeSeedNonceRevelation:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown seeder %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			in.NSeedsRevealed += mul

		case chain.OpTypeEndorsement:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown endorser %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			slots, _ := strconv.ParseUint(op.Data, 10, 32)
			in.NSlotsEndorsed += mul * int64(bits.OnesCount32(uint32(slots)))

		case chain.OpTypeDoubleBakingEvidence:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown 2bake accuser %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			in.DoubleBakingIncome += op.Reward * mul

			// debit receiver
			in, ok = incomeMap[op.ReceiverId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.ReceiverId)
				if err != nil {
					return fmt.Errorf("income: unknown 2bake offender %d", op.ReceiverId)
				}
				incomeMap[in.AccountId] = in
			}
			in.SlashedIncome += op.Volume * mul

		case chain.OpTypeDoubleEndorsementEvidence:
			// credit sender
			in, ok := incomeMap[op.SenderId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.SenderId)
				if err != nil {
					return fmt.Errorf("income: unknown 2endorse accuser %d", op.SenderId)
				}
				incomeMap[in.AccountId] = in
			}
			in.DoubleEndorsingIncome += op.Reward * mul

			// debit receiver
			in, ok = incomeMap[op.ReceiverId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, op.ReceiverId)
				if err != nil {
					return fmt.Errorf("income: unknown 2endorse offender %d", op.ReceiverId)
				}
				incomeMap[in.AccountId] = in
			}
			in.SlashedIncome += op.Volume * mul
		}
	}

	// missed endorsements require an idea about how much an endorsement is worth
	endorseReward := p.EndorsementReward
	if block.Cycle < p.NoRewardCycles {
		endorseReward = 0
	}

	// handle missed endorsements
	if block.Parent != nil && block.Parent.SlotsEndorsed != math.MaxUint32 {
		for _, v := range builder.Rights(chain.RightTypeEndorsing) {
			if !v.IsMissed {
				continue
			}
			in, ok := incomeMap[v.AccountId]
			if !ok {
				in, err = idx.loadIncome(ctx, block.Cycle, v.AccountId)
				if err != nil {
					return fmt.Errorf("income: unknown missed endorser %d", v.AccountId)
				}
				incomeMap[in.AccountId] = in
			}
			in.MissedEndorsingIncome += endorseReward * mul
			in.NSlotsMissed += mul
		}
	}

	if len(incomeMap) == 0 {
		return nil
	}

	upd := make([]pack.Item, 0, len(incomeMap))
	for _, v := range incomeMap {
		// absolute efficiency as expected vs actual income where 100% is the ideal case
		// =100%: total == expected
		// <100%: total > expected
		// >100%: total < expected
		totalGain := v.TotalIncome - v.ExpectedIncome
		if v.ExpectedIncome > 0 {
			v.EfficiencyPercent = (1.0 + float64(totalGain)/float64(v.ExpectedIncome)) * 100
		} else {
			// base calculation on rights
			totalRights := v.NBakingRights + v.NEndorsingRights
			totalWork := v.NBlocksBaked + v.NSlotsEndorsed
			totalGain := totalWork - totalRights
			if totalRights > 0 {
				v.EfficiencyPercent = (1.0 + float64(totalGain)/float64(totalRights)) * 100
			}
		}
		upd = append(upd, v)
	}
	return idx.table.Update(ctx, upd)
}

func (idx *IncomeIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	log.Debugf("Rollback deleting income for cycle %d", cycle)
	q := pack.Query{
		Name: "etl.income.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("c"), // cycle (!)
			Mode:  pack.FilterModeEqual,
			Value: cycle,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}

func (idx *IncomeIndex) loadIncome(ctx context.Context, cycle int64, id AccountID) (*Income, error) {
	if cycle < 0 && id <= 0 {
		return nil, ErrNoIncomeEntry
	}
	in := &Income{}
	err := idx.table.Stream(ctx, pack.Query{
		Name: "etl.income.search",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.table.Fields().Find("c"), // cycle (!)
				Mode:  pack.FilterModeEqual,
				Value: cycle,
			}, pack.Condition{
				Field: idx.table.Fields().Find("A"), // account
				Mode:  pack.FilterModeEqual,
				Value: id.Value(),
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(in)
	})
	if err != nil || in.RowId == 0 {
		return nil, ErrNoIncomeEntry
	}
	return in, nil
}
