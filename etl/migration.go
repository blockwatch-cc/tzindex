// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/index"
)

func (b *Builder) MigrateProtocol(ctx context.Context, prevparams, nextparams *tezos.Params) error {
	if b.block.Height <= 1 || prevparams.Version == nextparams.Version {
		return nil
	}

	switch {
	case nextparams.Protocol.Equal(tezos.ProtoV002):
		// origination bugfix
		return b.FixOriginationBug(ctx, nextparams)

	case nextparams.Protocol.Equal(tezos.ProtoV004):
		// adds invoice
		return b.MigrateAthens(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.ProtoV005_2):
		// adds invoice
		// runs manager airdrop
		// migrates delegator contracts
		return b.MigrateBabylon(ctx, prevparams, nextparams)

	case nextparams.Protocol.Equal(tezos.ProtoV012_2):
		// Ithaca changes all rights
		// - remove and reload future rights
		// - remove and rebuild future income data
		return b.MigrateIthaca(ctx, prevparams, nextparams)
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
func (b *Builder) PatchBigmapDiff(ctx context.Context, diff micheline.BigmapDiff, addr tezos.Address, script *micheline.Script) (micheline.BigmapDiff, error) {
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
			return nil, fmt.Errorf("unmarshal script: %w", err)
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
		if err != index.ErrNoBigmapAlloc {
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
			alloc := micheline.BigmapDiffElem{
				Action:    micheline.DiffActionAlloc,
				Id:        id,              // alloc new id
				KeyType:   maps[0].Args[0], // (Left) == key_type
				ValueType: maps[0].Args[1], // (Right) == value_type
			}
			// prepend
			diff = append([]micheline.BigmapDiffElem{alloc}, diff...)
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
