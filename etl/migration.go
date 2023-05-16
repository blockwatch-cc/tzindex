// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) MigrateProtocol(ctx context.Context, prevparams, nextparams *rpc.Params) error {
	if b.block.Height <= 1 || prevparams.Version == nextparams.Version {
		return nil
	}

	switch {
	case nextparams.Protocol.Equal(tezos.ProtoV002):
		// origination bugfix
		return b.FixOriginationBug(ctx, nextparams)

	case nextparams.Protocol.Equal(tezos.PtAthens):
		// adds invoice
		return b.MigrateAthens(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.PsBabyM1):
		// adds invoice
		// runs manager airdrop
		// migrates delegator contracts
		return b.MigrateBabylon(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.PsCARTHA):
		// new rewards
		return b.MigrateCarthage(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.PtGRANAD):
		// Granada changes cycle length and rights
		// - remove and reload future rights
		// - remove and build future income data
		return b.MigrateGranada(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.Psithaca):
		// Ithaca changes all rights
		// - remove and reload future rights
		// - remove and rebuild future income data
		return b.MigrateIthaca(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.PtLimaPt):
		// Lima changes all rights
		// - remove and reload future rights
		// - remove and rebuild future income data
		return b.MigrateLima(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.PtMumbai):
		// Mumbai changes cycle length and rights
		// - remove and reload future rights
		// - remove and rebuild future income data
		return b.MigrateMumbai(ctx, prevparams, nextparams)
	}

	return nil
}

// Pre-Babylon Bigmaps do not have a bigmap id in alloc.
//
// big_map_diffs in proto < v005 lack id and action. Also allocs are not explicit.
// In order to satisfy further processing logic we patch in an alloc when we see a
// new contract using a bigmap.
// Contracts before v005 can only own a single bigmap which makes life a bit easier.
// Note: on zeronet big_map is a regular map due to protocol bug
func (b *Builder) PatchBigmapEvents(ctx context.Context, diff micheline.BigmapEvents, addr tezos.Address, script *micheline.Script) (micheline.BigmapEvents, error) {
	// do nothing on post-Athens bigmaps
	if b.block.Params.Version > 4 {
		return diff, nil
	}

	// without diff, check if script contains a bigmap alloc
	if diff == nil {
		if script == nil {
			return nil, nil
		}
		// technically, the bigmap type must be top-level or part of a pair
		// https://gitlab.com/tezos/tezos/merge_requests/617
		if _, ok := script.Code.Storage.FindOpCodes(micheline.T_BIG_MAP); !ok {
			return nil, nil
		}
	}

	// log.Infof("Patching bigmap for account %d at height %d", accId, b.block.Height)

	// either script is set (origination) or we lookup the contract (transaction)
	if script == nil {
		// load contract
		contract, err := b.idx.LookupContract(ctx, addr)
		if err != nil {
			return nil, err
		}
		// unpack script
		script = micheline.NewScript()
		if err := script.UnmarshalBinary(contract.Script); err != nil {
			return nil, fmt.Errorf("unmarshal script: %v", err)
		}
	}

	// bitmap id allocation on mainnet is not origination order!
	// looks random, so we hard-code ids here
	staticAthensBigmapIds := map[string]int64{
		"KT1LvAUw8xXH2X4WQRKUYvSiDuXkh15kNC1B": 0,
		"KT1WRUe3csC1jiThN9KUtaji2bd412upfn1E": 1,
		"KT1UDc2ZUoAAvv8amw2DqVuQK1fKjb1HjxR4": 2,
		"KT1R3uoZ6W1ZxEwzqtv75Ro7DhVY6UAcxuK2": 3,
		"KT1VG2WtYdSWz5E7chTeAdDPZNy2MpP8pTfL": 4,
		"KT1CvzXrz19fnHKuWedFY3WqmVAB7kMTPLLS": 5,
		"KT1FbkiY8Y1gSh4x9QVzfvtcUrXEQAx7wYnf": 6,
		"KT1SAaFjYUD5KFYidYxPzpnf6HgFs4oAJuTz": 7,
		"KT1A1N85VE2Mi3zuDvKidWNy6P6Fj4iRz2rA": 8,
		"KT1UvfyLytrt71jh63YV4Yex5SmbNXpWHxtg": 9,
		"KT1REHQ183LzfoVoqiDR87mCrt7CLUH1MbcV": 10,
	}
	id, ok := staticAthensBigmapIds[addr.String()]
	if !ok {
		return nil, fmt.Errorf("bigmap patch unknown contract %s", addr)
	}

	// check if bigmap is allocated
	var needAlloc bool
	if _, err := b.idx.LookupBigmapAlloc(ctx, id); err != nil {
		if err != model.ErrNoBigmap {
			return nil, err
		}
		needAlloc = true
	}

	// inject a synthetic alloc to satisfy processing logic
	if needAlloc {
		// find bigmap type definition
		maps, ok := script.Code.Storage.FindOpCodes(micheline.T_BIG_MAP)
		if ok {
			// create alloc for new bigmaps
			alloc := micheline.BigmapEvent{
				Action:    micheline.DiffActionAlloc,
				Id:        id,              // alloc new id
				KeyType:   maps[0].Args[0], // (Left) == key_type
				ValueType: maps[0].Args[1], // (Right) == value_type
			}
			// prepend
			diff = append(micheline.BigmapEvents{alloc}, diff...)
			// log.Infof("Alloc bigmap %d for account %s at height %d", id, addr, b.block.Height)

			// set id on all items
			for i := range diff {
				diff[i].Id = id
			}
		} else {
			return nil, fmt.Errorf("missing bigmap type def for contract/account %d", addr)
		}
	} else {
		// patch id for existing bigmap
		// log.Infof("Patching bigmap: patch id to %d", foundid)
		for i := range diff {
			diff[i].Id = id
		}
	}

	return diff, nil
}

func (b *Builder) RebuildFutureRightsAndIncome(ctx context.Context, params *rpc.Params) error {
	// we need to update rights and income indexes
	income, err := b.idx.Index(index.IncomeIndexKey)
	if err != nil {
		return err
	}
	rights, err := b.idx.Index(index.RightsIndexKey)
	if err != nil {
		return err
	}

	// empty fetched rights, if any
	b.block.TZ.Baking = nil
	b.block.TZ.Endorsing = nil
	b.block.TZ.PrevEndorsing = nil
	// b.block.TZ.Snapshot = nil // don't clear snapshot data (!)
	// b.block.TZ.SnapInfo = nil // don't clear snapshot data (!)

	// builder cache contains rights for n+5 at this point, so we only need to
	// fetch and rebuild cycles n .. n+4 (we ignore the last 2 extra blocks for
	// endorsing)
	startCycle, endCycle := params.StartCycle, params.StartCycle+params.PreservedCycles
	if startCycle == 0 {
		startCycle, endCycle = b.block.Cycle, b.block.Cycle+params.PreservedCycles
	}
	log.Infof("Migrate v%03d: updating rights and baker income for cycles %d..%d", params.Version, startCycle, endCycle)

	// 1 delete all future cycle rights
	log.Infof("Migrate v%03d: removing deprecated future rights", params.Version)
	for cycle := startCycle; cycle <= endCycle; cycle++ {
		_ = rights.DeleteCycle(ctx, cycle)
		_ = income.DeleteCycle(ctx, cycle)
	}
	if err := rights.Flush(ctx); err != nil {
		return fmt.Errorf("migrate: flushing rights after clear: %v", err)
	}
	if err := income.Flush(ctx); err != nil {
		return fmt.Errorf("migrate: flushing income after clear: %v", err)
	}

	// 2
	//
	// fetch and insert new rights for cycle .. cycle + preserved_cycles - 1
	// Note: some new bakers may appear, so check and update builder caches
	//
	for cycle := startCycle; cycle <= endCycle; cycle++ {
		log.Infof("Migrate v%03d: processing cycle %d", params.Version, cycle)

		// 2.1 fetch new rights
		bundle := &rpc.Bundle{
			Block:  b.block.TZ.Block,
			Params: params,
		}
		err := b.rpc.FetchRightsByCycle(ctx, b.block.Height, cycle, bundle)
		if err != nil {
			return fmt.Errorf("migrate: %v", err)
		}
		log.Infof("Migrate v%03d: fetched %d + %d new rights for cycle %d",
			params.Version, len(bundle.Baking[0]), len(bundle.Endorsing[0]), cycle)

		// strip pre-cycle rights if current block is not start of cycle
		if !b.block.TZ.IsCycleStart() {
			bundle.PrevEndorsing = nil
		}

		// 2.2 analyze rights for new bakers (optional)
		for _, v := range bundle.Baking[0] {
			_, ok := b.AccountByAddress(v.Address())
			if !ok {
				log.Errorf("migrate: missing baker account %s with rights", v.Address())
			}
		}
		for _, v := range bundle.Endorsing[0] {
			_, ok := b.AccountByAddress(v.Address())
			if !ok {
				log.Errorf("migrate: missing endorser account %s with rights", v.Address())
			}
		}

		// 2.3 construct an empty block to insert rights into indexers
		block := &model.Block{
			Height: b.block.Height,
			Params: params,
			TZ:     bundle,
		}

		if err := rights.ConnectBlock(ctx, block, b); err != nil {
			return fmt.Errorf("migrate: insert rights for cycle %d: %v", cycle, err)
		}

		if err := income.ConnectBlock(ctx, block, b); err != nil {
			return fmt.Errorf("migrate: insert income for cycle %d: %v", cycle, err)
		}
	}

	if err := rights.Flush(ctx); err != nil {
		return fmt.Errorf("migrate: flushing rights after upgrade: %v", err)
	}
	if err := income.Flush(ctx); err != nil {
		return fmt.Errorf("migrate: flushing income after upgrade: %v", err)
	}

	return nil
}
