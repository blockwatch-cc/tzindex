// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

// Note: as if this was not already complicated enough, protocol migration happens
// one block BEFORE cycle end, so the last block in a cycle already runs on the new
// protocol with new rights and new endorsements. This also means that endorsements
// rights for the update block (the one we're processing right now) have changed.
//
func (b *Builder) MigrateGranada(ctx context.Context, oldparams, params *tezos.Params) error {
	// nothing to do in light mode
	if b.idx.lightMode {
		return nil
	}

	// we need to update rights and income
	income, err := b.idx.Table(index.IncomeTableKey)
	if err != nil {
		return err
	}
	rights, err := b.idx.Table(index.RightsTableKey)
	if err != nil {
		return err
	}

	// builder cache contains rights for n+5 at this point, so we only need to
	// fetch and rebuild cycles n .. n+4 (plus 2 extra blocks for endorsing)
	startCycle, endCycle := params.StartCycle, params.StartCycle+params.PreservedCycles-1
	log.Infof("Migrate v%03d: updating rights and baker income for cycles %d..%d", params.Version, startCycle, endCycle)

	// 1
	//
	// delete all rights starting from current block
	// - all endorsing rights (because they change with new proto)
	// - baking rights for future cycles (in the hope baking for current
	//   last 2 blocks of cycle will not change)
	log.Infof("Migrate v%03d: removing deprecated rights", params.Version)
	n, err := pack.NewQuery("etl.rights.delete", rights).
		AndEqual("type", tezos.RightTypeEndorsing).
		AndGte("height", b.block.Height).
		Delete(ctx)
	if err != nil {
		return fmt.Errorf("migrate: deleting endorsing rights starting at block %d: %v", b.block.Height, err)
	}
	log.Infof("Migrate v%03d: dropped %d obsolete endorsing rights", params.Version, n)
	n, err = pack.NewQuery("etl.rights.delete", rights).
		AndEqual("type", tezos.RightTypeBaking).
		AndGte("cycle", startCycle).
		Delete(ctx)
	if err != nil {
		return fmt.Errorf("migrate: deleting baking rights starting cycle %d: %v", startCycle, err)
	}
	log.Infof("Migrate v%03d: dropped %d obsolete baking rights", params.Version, n)
	if err := rights.Flush(ctx); err != nil {
		return fmt.Errorf("migrate: flushing rights after clear: %v", err)
	}

	// 2
	//
	// reload endorsement rights for this block and next, assuming protocol
	// activates 1 block before cycle end; we don't update income expectations
	// for the last 2 blocks because income reflects the correct state before and
	// without protocol upgrade, anything that bakers win or lose is due to
	// protocol upgrade
	//
	b.endorsing = b.endorsing[:0]
	overflowRights := make([]model.Right, 0)
	for height, end := b.block.Height-1, oldparams.CycleEndHeight(b.block.Cycle); height <= end; height++ {
		log.Infof("Migrate v%03d: pulling new rights for block %d", params.Version, height)
		er, err := b.rpc.GetEndorsingRightsHeight(ctx, height)
		if err != nil {
			return fmt.Errorf("migrate: loading new endorsing rights for block %d: %v", height, err)
		}
		log.Infof("Migrate v%03d: inserting %d endorse rights for block %d", params.Version, len(er), height)
		for _, v := range er {
			acc, ok := b.AccountByAddress(v.Address())
			if !ok {
				acc, err = b.idx.LookupAccount(ctx, v.Address())
				if err != nil {
					return fmt.Errorf("migrate: missing endorser account %s: %v", v.Address(), err)
				}
			}
			bits := vec.NewBitSet(params.EndorsersPerBlock)
			for _, idx := range v.Slots {
				bits.Set(idx)
			}
			right := &model.Right{
				Type:      tezos.RightTypeEndorsing,
				Height:    v.Level,
				Cycle:     b.block.Cycle,
				AccountId: acc.RowId,
				Slots:     bits.Bytes(),
			}
			// insert directly to set row id
			if err := rights.Insert(ctx, right); err != nil {
				return fmt.Errorf("migrate: inserting rights: %v", err)
			}
			if height < b.block.Height {
				b.endorsing = append(b.endorsing, *right)
			} else if height == end {
				overflowRights = append(overflowRights, *right)
			}
		}
	}

	// 3
	//
	// fetch and insert new rights for cycle .. cycle + preserved_cycles - 1
	// Note: rights for n+5 are already loaded and will be applied as usual during
	// regular ConnectBlock calls
	//
	ins := make([]pack.Item, 0)
	for cycle := startCycle; cycle <= endCycle; cycle++ {
		log.Infof("Migrate v%03d: processing cycle %d", params.Version, cycle)

		// 3.1 fetch new rights
		br, err := b.rpc.GetBakingRightsCycle(ctx, b.block.Height, cycle)
		if err != nil {
			return fmt.Errorf("migrate: fetching baking rights for cycle %d: %v", cycle, err)
		}
		if len(br) == 0 {
			return fmt.Errorf("migrate: empty baking rights for cycle %d", cycle)
		}
		er, err := b.rpc.GetEndorsingRightsCycle(ctx, b.block.Height, cycle)
		if err != nil {
			return fmt.Errorf("migrate: fetching endorsing rights for cycle %d: %v", cycle, err)
		}
		if len(er) == 0 {
			return fmt.Errorf("migrate: empty endorsing rights for cycle %d", cycle)
		}
		log.Infof("Migrate v%03d: fetched %d + %d new rights for cycle %d", params.Version, len(br), len(er), cycle)

		// 3.2
		// process all baking rights for this cycle; we may lack a few
		// loaded baker accounts since more/other rights exist now, load on the
		// fly if missing
		ins = ins[:0]
		for _, v := range br {
			acc, ok := b.AccountByAddress(v.Address())
			if !ok {
				acc, err = b.idx.LookupAccount(ctx, v.Address())
				if err != nil {
					return fmt.Errorf("migrate: missing baker account %s: %v", v.Address(), err)
				}
			}
			ins = append(ins, &model.Right{
				Type:      tezos.RightTypeBaking,
				Height:    v.Level,
				Cycle:     params.CycleFromHeight(v.Level),
				Priority:  v.Priority,
				AccountId: acc.RowId,
			})
		}

		// 3.3. sort by height and priority
		sort.Slice(ins, func(i, j int) bool {
			return ins[i].(*model.Right).Height < ins[j].(*model.Right).Height ||
				(ins[i].(*model.Right).Height == ins[j].(*model.Right).Height && ins[i].(*model.Right).Priority < ins[j].(*model.Right).Priority)
		})

		// 3.4 add endorsing rights, all slots go into a single rights entry
		for _, v := range er {
			acc, ok := b.AccountByAddress(v.Address())
			if !ok {
				acc, err = b.idx.LookupAccount(ctx, v.Address())
				if err != nil {
					return fmt.Errorf("migrate: missing endorser account %s: %v", v.Address(), err)
				}
			}
			bits := vec.NewBitSet(params.EndorsersPerBlock)
			for _, idx := range v.Slots {
				bits.Set(idx)
			}
			right := &model.Right{
				Type:      tezos.RightTypeEndorsing,
				Height:    v.Level,
				Cycle:     params.CycleFromHeight(v.Level),
				AccountId: acc.RowId,
				Slots:     bits.Bytes(),
			}
			ins = append(ins, right)
		}

		// 3.5 insert full cycle of rights
		if err := rights.Insert(ctx, ins); err != nil {
			return fmt.Errorf("migrate: inserting rights for cycle %d: %v", cycle, err)
		}

		log.Infof("Migrate v%03d: updating baker income", params.Version)

		// 3.6 reset and update baker income (no new bakers, all are known)
		incomeMap := make(map[model.AccountID]*model.Income)
		var totalRolls int64
		err = pack.NewQuery("etl.income.scan", income).
			AndEqual("cycle", cycle).
			Stream(ctx, func(r pack.Row) error {
				in := &model.Income{}
				if err := r.Decode(in); err != nil {
					return err
				}
				totalRolls += in.Rolls
				// reset on load
				in.LuckPct = 10000
				in.NEndorsingRights = 0
				in.NBakingRights = 0
				in.ExpectedIncome = 0
				in.ExpectedBonds = 0
				in.ContributionPct = 0
				in.PerformancePct = 0
				incomeMap[in.AccountId] = in
				return nil
			})
		if err != nil {
			return fmt.Errorf("migrate: loading income for cycle %d: %v", cycle, err)
		}

		// use new Granada constants
		blockDeposit, endorseDeposit := params.BlockSecurityDeposit, params.EndorsementSecurityDeposit
		blockReward, endorseReward := params.BlockReward, params.EndorsementReward

		// 3.7 walk overflow endorsing rights
		for _, right := range overflowRights {
			in, ok := incomeMap[right.AccountId]
			if !ok {
				// FIXME: complex edge case where a baker was deactivated
				// but has spill-over endorsing rights from last cycle,
				// ignore and log this case
				log.Warnf("migrate: missing endorser %d in income map for overflow from cycle %d", right.AccountId, cycle-1)
				continue
			}
			n := int64(vec.NewBitSetFromBytes(right.Slots, params.EndorsersPerBlock).Count())
			in.NEndorsingRights += n
			in.ExpectedIncome += endorseReward * n
			in.ExpectedBonds += endorseDeposit * n
		}
		overflowRights = overflowRights[:0]

		// 3.8 walk processed rights and apply to income
		for _, v := range ins {
			right := v.(*model.Right)
			if right.Priority > 0 {
				continue
			}
			in, ok := incomeMap[right.AccountId]
			if !ok {
				return fmt.Errorf("migrate: missing baker %d in income map for cycle %d", right.AccountId, cycle)
			}
			switch right.Type {
			case tezos.RightTypeBaking:
				in.NBakingRights++
				in.ExpectedIncome += blockReward
				in.ExpectedBonds += blockDeposit
			case tezos.RightTypeEndorsing:
				// skip last block of each cycle, keep for next cycle
				if params.IsCycleEnd(right.Height) {
					overflowRights = append(overflowRights, *right)
					continue
				}
				n := int64(vec.NewBitSetFromBytes(right.Slots, params.EndorsersPerBlock).Count())
				in.NEndorsingRights += n
				in.ExpectedIncome += endorseReward * n
				in.ExpectedBonds += endorseDeposit * n
			}
		}

		// 3.9 convert income map to update list
		upd := make([]pack.Item, 0, len(incomeMap))
		for _, in := range incomeMap {
			// set 100% performance when baker has no rights assigned
			if in.NBakingRights+in.NEndorsingRights == 0 {
				in.ContributionPct = 10000
				in.PerformancePct = 10000
			}
			// calculate luck and append for insert
			in.UpdateLuck(totalRolls, params)
			upd = append(upd, in)
		}

		log.Infof("Migrate v%03d: updating %d income records", params.Version, len(upd))

		// 3.10 update income table
		if err := income.Update(ctx, upd); err != nil {
			return fmt.Errorf("migrate: updating income for %d bakers: %v", len(upd), err)
		}
	}

	log.Infof("Migrate v%03d: complete", params.Version)

	// rebuild rights cache
	b.idx.updateRights(ctx, b.block.Height)

	return nil
}
