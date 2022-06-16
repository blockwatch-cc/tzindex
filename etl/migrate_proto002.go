// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// v002 fixed an 'origination bug'
//
// Changelog published on Slack at 20-07-2018 15:45:31 (block 26,579 cycle 6)
// https://log.tezos.link/index.php?date=20-07-2018
//
// - Fixed a bug in delegations, where contracts could delegate to unregistered
//   delegates. This will be enforced from now on, and the existing unregistered
//   delegates will be automatically registered (except for two empty addresses).
//
// Note: we register self-delegation on origination bakers right away, but track them
// explicitly (account.baker_id == 0 and account.is_baker = true). During migration
// we check if an active delegation exists and upgrade status to full baker.
//
// Correctly registered bakers (via self delegation) are not affected since their
// baker_id is set.
//
func (b *Builder) FixOriginationBug(ctx context.Context, params *tezos.Params) error {
	var count int
	var err error

	// set id and reset grace period for all bakers with delegations
	count = 0
	for _, bkr := range b.bakerMap {
		if bkr.ActiveDelegations > 0 {
			if bkr.Account.BakerId == 0 {
				bkr.Account.BakerId = bkr.Account.RowId
				b.AppendMagicBakerRegistrationOp(ctx, bkr, 0)
			}
			bkr.InitGracePeriod(b.block.Cycle, b.block.Params)
			count++
		}
	}
	log.Infof("Migrate v%03d: updated %d active bakers grace period", params.Version, count)

	// unregister bakers when not properly registered and no more delegations exist
	drop := make([]*model.Baker, 0)
	for _, bkr := range b.bakerMap {
		// keep bakers if they have delegations
		if bkr.ActiveDelegations > 0 {
			continue
		}
		// keep valid registered bakers
		if bkr.Account.IsBaker && bkr.Account.BakerId > 0 {
			continue
		}
		// drop everyone else
		drop = append(drop, bkr)
	}

	// remove baker records
	delIds := make([]uint64, 0)
	for _, v := range drop {
		log.Debugf("Migrate v%03d: deregistering baker %s", params.Version, v)
		delIds = append(delIds, v.RowId.Value())
		b.UnregisterBaker(v)
	}

	bakers, err := b.idx.Table(index.BakerTableKey)
	if err == nil {
		_ = bakers.DeleteIds(ctx, delIds)
	}
	log.Infof("Migrate v%03d: dropped %d non-bakers", params.Version, len(drop))

	// for all remaining bakers set baker id to account id
	for _, bkr := range b.bakerMap {
		bkr.Account.BakerId = bkr.Account.RowId
		bkr.Account.IsDirty = true
	}

	// cross-check against a list of all active bakers known to the protocol
	if b.validate {
		realbakers, err := b.rpc.ListActiveDelegates(ctx, rpc.BlockLevel(b.block.Height))
		if err != nil {
			return fmt.Errorf("listing bakers: %v", err)
		}
		missing := make(map[string]struct{})
		illegal := make(map[string]struct{})
		for _, v := range realbakers {
			hash := b.accCache.AddressHashKey(v)
			if _, ok := b.bakerHashMap[hash]; !ok {
				missing[v.String()] = struct{}{}
			}
		}
		for _, v := range b.bakerMap {
			a := v.Address
			var found bool
			for _, vv := range realbakers {
				if vv.Equal(a) {
					found = true
					break
				}
			}
			if !found {
				illegal[a.String()] = struct{}{}
			}
		}
		log.Infof("Audit: %d missing, %d illegal bakers", len(missing), len(illegal))
		for n, _ := range missing {
			log.Infof("Audit: missing baker %s", n)
		}
		for n, _ := range illegal {
			log.Infof("Audit: illegal baker %s", n)
		}
	}

	return nil
}
