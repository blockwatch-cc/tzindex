// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) MigrateOxford(ctx context.Context, oldparams, params *rpc.Params) error {
	// nothing to do when chain starts with this proto
	if b.block.Height <= 2 {
		return nil
	}

	// migrate all bakers from frozen deposits to stake variables
	var count int
	for _, v := range b.bakerMap {
		if v.FrozenDeposits == 0 {
			continue
		}
		v.TotalStake = v.FrozenDeposits
		v.TotalShares = v.FrozenDeposits
		v.FrozenDeposits = 0
		v.StakingEdge = 1000000000 // = 100%
		v.StakingLimit = 0         // = 0
		v.Account.IsStaked = true
		v.Account.StakedBalance = v.TotalStake
		v.Account.StakeShares = v.TotalShares
		v.IsDirty = true
		v.Account.IsDirty = true

		// update current supply
		b.block.Supply.FrozenDeposits -= v.TotalStake

		log.Infof("Migrate v%03d: %s frozen stake %d", params.Version, v.Account, v.TotalStake)

		count++
	}
	log.Infof("Migrate v%03d: updated %d active bakers to staking", params.Version, count)

	// validate frozen deposits are zeroed
	if b.block.Supply.FrozenDeposits > 0 {
		return fmt.Errorf("Non-zero total frozen deposits %d after stake migration",
			b.block.Supply.FrozenDeposits)
	}

	// on mainnet remove invalid bigmap entries (ticket stuff apparently)
	// we do this by injecting a migration op with bigmap remove events
	if params.IsMainnet() {
		acc, err := b.idx.LookupAccount(ctx, oxfordBigmapAddr)
		if err != nil {
			return fmt.Errorf("loading bigmap contract %s: %w", oxfordBigmapAddr, err)
		}
		// insert into cache
		b.accMap[acc.RowId] = acc
		b.accHashMap[b.accCache.AccountHashKey(acc)] = acc

		// load contract
		cc, err := b.LoadContractByAccountId(ctx, acc.RowId)
		if err != nil {
			return fmt.Errorf("loading contract %s: %w", oxfordBigmapAddr, err)
		}
		b.conMap[acc.RowId] = cc
		b.conCache.Add(cc)

		// create removal events
		events := make(micheline.BigmapEvents, len(oxfordBigmapKeys))
		for i, k := range oxfordBigmapKeys {
			events[i] = micheline.BigmapEvent{
				Action:  micheline.DiffActionRemove,
				Id:      oxfordBigmapId,
				KeyHash: k,
				Key:     micheline.Unit, // we don't know
			}
		}

		// create migration op (will be processed during indexing)
		if err := b.AppendBigmapMigrationOp(ctx, acc, cc, 0, events); err != nil {
			return fmt.Errorf("creating bigmap migration op: %w", err)
		}
	}

	log.Infof("Migrate v%03d: complete", params.Version)
	return nil
}

var (
	oxfordBigmapId   int64 = 5696
	oxfordBigmapAddr       = tezos.MustParseAddress("KT1CnygLoKfJA66499U9ZQkL6ykUfzgruGfM")
	oxfordBigmapKeys       = parseBigmapKeys([]string{
		"exprtXBtxJxCDEDETueKAFLL7r7vZtNEo1MHajpHba1djtGKqJzWd3",
		"exprtbuRhaGDS942BgZ1qFdD7HAKeBjPEqzRxgLQyWQ6HWxcaiLC2c",
		"exprtePxSLgrhJmTPZEePyFBmESLhaBUN1WodvLYy9xYhEYE6dKPLe",
		"exprtx9GaYz5Fy5ytiuYgSfJqeYqkxGgobust8U6dpCLaeZUMiitmg",
		"expru28t4XoyB61WuRQnExk3Kq8ssGv1ejgdo9XHxpTXoQjXTGw1Dg",
		"expru2fZALknjB4vJjmQBPkrs3dJZ5ytuzfmE9A7ScUk5opJiZQyiJ",
		"expru2riAFKURjHJ1vNpvsZGGw6z4wtTvbstXVuwQPj1MaTqKPeQ6z",
		"expruHoZDr8ioVhaAs495crYTprAYyC87CruEJ6HaS7diYV6qLARqQ",
		"expruMie2gfy5smMd81NtcvvWm4jD7ThUebw9hpF3N3apKVtxkVG9M",
		"expruc3QW7cdxrGurDJQa6k9QqMZjGkRDJahy2XNtBt9WQzC1yavJK",
		"exprud86wYL7inFCVHkF1Jcz8uMXVY7dnbzxVupyyknZjtDVmwoQTJ",
		"exprufYzeBTGn9733Ga8xEEmU4SsrSyDrzEip8V8hTBAG253T5zZQx",
		"exprum9tuHNvisMa3c372AFmCa27rmkbCGrhzMSprrxgJjzXhrKAag",
		"expruokt7oQ6dDHRvL4sURKUzfwJirR8FPHvpXwjgUD4KHhPWhDGbv",
		"expruom5ds2hVgjdTB877Fx3ZuWT5WUnw1H6kUZavVHcJFbCkcgo3x",
		"exprv2DPd1pV3GVSN2CgW7PPrAQUTuZAdeJphwToQrTNrxiJcWzvtX",
		"exprv65Czv5TnKyEWgBHjDztkCkc1FAVEPxZ3V3ocgvGjfXwjPLo8M",
		"exprv6S2KAvqAC18jDLYjaj1w9oc4ESdDGJkUZ63EpkqSTAz88cSYB",
		"exprvNg3VDBnhtTHvc75krAEYzz6vUMr3iU5jtLdxs83FbgTbZ9nFT",
		"exprvS7wNDHYKYZ19nj3ZUo7AAVMCDpTK3NNERFhqe5SJGCBL4pwFA",
	})
)

func parseBigmapKeys(s []string) []tezos.ExprHash {
	keys := make([]tezos.ExprHash, len(s))
	for i, v := range s {
		keys[i] = tezos.MustParseExprHash(v)
	}
	return keys
}

func (b *Builder) MigrateAdaptiveIssuance(ctx context.Context, params *rpc.Params) error {
	// nothing to do in light mode or when chain starts with this option
	if b.idx.lightMode || b.block.Height <= 2 {
		return nil
	}

	// fetch and build rights + income for future 5 cycles
	if err := b.RebuildFutureRightsAndIncome(ctx, params); err != nil {
		return err
	}

	log.Infof("Migrate v%03d AI: complete", params.Version)
	return nil
}

// temp fix for light-mode migration issue
func (b *Builder) FixOxfordMigration(ctx context.Context) error {
	if !b.idx.lightMode {
		return nil
	}
	var needFix bool
	for _, v := range b.bakerMap {
		if v.FrozenDeposits > 0 {
			needFix = true
			break
		}
	}
	if !needFix {
		return nil
	}
	var count int
	for _, v := range b.bakerMap {
		if v.FrozenDeposits == 0 {
			continue
		}
		v.TotalStake += v.FrozenDeposits
		v.TotalShares = v.TotalStake
		v.FrozenDeposits = 0
		v.StakingEdge = 1000000000 // = 100%
		v.StakingLimit = 0         // = 0
		v.Account.IsStaked = true
		v.Account.StakedBalance = v.TotalStake
		v.Account.StakeShares = v.TotalShares
		v.IsDirty = true
		v.Account.IsDirty = true

		// update current supply
		b.block.Supply.FrozenDeposits -= v.TotalStake

		log.Infof("Fix Oxford stake: %s frozen stake %d", v.Account, v.TotalStake)

		count++
	}
	log.Infof("Fix Oxford stake: updated %d active bakers to staking", count)

	return nil
}
