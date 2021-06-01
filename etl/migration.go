// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/index"
	. "blockwatch.cc/tzindex/etl/model"
)

func (b *Builder) BuildGenesisBlock(ctx context.Context) (*Block, error) {
	gen := b.block.TZ.Block.Header.Content.Parameters
	if gen == nil {
		return nil, fmt.Errorf("missing genesis protocol_parameters")
	}
	log.Info("Building genesis dataset.")

	// register new protocol (will save as new deployment)
	b.block.Params.StartHeight = b.block.Height
	b.idx.ConnectProtocol(ctx, b.block.Params)

	accounts := make([]pack.Item, 0)
	contracts := make([]pack.Item, 0)

	opCounter := 1
	flowCounter := 1

	// collect supply statistics
	b.block.Supply.Height = b.block.Height
	b.block.Supply.Cycle = b.block.Cycle
	b.block.Supply.Timestamp = b.block.Timestamp

	// process foundation bakers and early backer accounts (activate right away)
	for i, v := range gen.Accounts {
		// we use hard coded row ids for registrations
		acc := NewAccount(v.Addr)
		acc.RowId = AccountID(len(accounts) + 1)
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.SpendableBalance = v.Value
		acc.IsFunded = true
		acc.IsActivated = true
		acc.IsSpendable = true
		acc.IsDelegatable = true

		// revealed accounts are registered as active bakers (i.e. foundation bakers)
		if v.Key.IsValid() {
			acc.IsRevealed = true
			acc.Pubkey = v.Key.Bytes()
			acc.IsDelegate = true
			acc.IsActiveDelegate = true
			acc.DelegateSince = b.block.Height
			acc.DelegateId = acc.RowId
			b.block.NDelegation++
			b.RegisterDelegate(acc, true)
			b.AppendMagicBakerRegistrationOp(ctx, acc, i)

			// update supply counters
			b.block.Supply.ActiveStaking += v.Value

			// log.Debugf("1 BOOT REG SELF %d %s -> %d bal=%d",
			// 	acc.RowId, acc, acc.ActiveDelegations, acc.Balance())
		} else {
			b.accCache.Add(acc)
			b.accMap[acc.RowId] = acc
			b.accHashMap[b.accCache.AccountHashKey(acc)] = acc
		}

		// update block counters
		b.block.NewImplicitAccounts++
		b.block.FundedAccounts++
		b.block.NewAccounts++
		b.block.SeenAccounts++
		b.block.ActivatedSupply += v.Value
		b.block.Supply.Activated += v.Value

		// register activation flows (will not be applied, just saved!)
		f := NewFlow(b.block, acc, nil, opCounter, OPL_PROTOCOL_UPGRADE, flowCounter, 0, 0)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeActivation
		f.AmountIn = acc.SpendableBalance
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// register implicit activation ops
		op := NewImplicitOp(b.block, 0, tezos.OpTypeActivateAccount, opCounter, OPL_PROTOCOL_UPGRADE, opCounter)
		op.SenderId = acc.RowId
		op.Counter = int64(opCounter)
		op.Volume = acc.SpendableBalance
		b.block.Ops = append(b.block.Ops, op)
		opCounter++

		// prepare for insert
		accounts = append(accounts, acc)

		log.Debug(newLogClosure(func() string {
			var as, vs, ds, rs string
			if acc.IsActivated {
				as = " [activated]"
			}
			if acc.IsDelegate {
				ds = " [delegated]"
			}
			if acc.IsRevealed {
				rs = " [revealed]"
			}
			return fmt.Sprintf("Registered %d %s %.6f%s%s%s%s", acc.RowId, acc,
				b.block.Params.ConvertValue(acc.Balance()), as, ds, rs, vs)
		}))
	}

	// process KT1 vesting contracts
	for _, v := range gen.Contracts {
		// we use hard coded row ids for registrations
		acc := NewAccount(v.Addr)
		acc.RowId = AccountID(len(accounts) + 1)
		acc.CreatorId = acc.RowId // satisfy invariant
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.IsContract = true
		acc.IsFunded = true
		acc.SpendableBalance = v.Value

		// update block counters
		b.block.NewAccounts++
		b.block.SeenAccounts++
		b.block.NewContracts++
		b.block.FundedAccounts++
		b.block.ActivatedSupply += v.Value

		// update supply counters
		b.block.Supply.Activated += v.Value
		b.block.Supply.ActiveStaking += v.Value
		b.block.Supply.ActiveDelegated += v.Value

		// register activation flows (will not be applied, just saved!)
		f := NewFlow(b.block, acc, nil, opCounter, OPL_PROTOCOL_UPGRADE, flowCounter, 0, 0)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeActivation
		f.AmountIn = acc.Balance()
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// link to and update delegate
		dlg, _ := b.AccountByAddress(v.Delegate)
		acc.IsDelegated = true
		acc.DelegateId = dlg.RowId
		acc.DelegatedSince = b.block.Height
		dlg.TotalDelegations++
		dlg.ActiveDelegations++
		dlg.DelegatedBalance += acc.Balance()
		// log.Debugf("1 BOOT ADD delegation %d %s -> %d (%d %s) bal=%d",
		// 	dlg.RowId, dlg, dlg.ActiveDelegations, acc.RowId, acc, acc.Balance())
		// register delegation flows (will not be applied, just saved!)
		f = NewFlow(b.block, dlg, acc, opCounter, OPL_PROTOCOL_UPGRADE, flowCounter, 0, 0)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeDelegation
		f.AmountIn = acc.Balance()
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// register implicit delegation ops
		op := NewImplicitOp(b.block, 0, tezos.OpTypeDelegation, opCounter, OPL_PROTOCOL_UPGRADE, opCounter)
		op.SenderId = acc.RowId
		op.DelegateId = dlg.RowId
		op.Counter = int64(opCounter)
		op.Volume = acc.Balance()
		b.block.Ops = append(b.block.Ops, op)
		opCounter++

		// put in cache
		b.accCache.Add(acc)
		b.accMap[acc.RowId] = acc
		b.accHashMap[b.accCache.AccountHashKey(acc)] = acc

		// prepare for insert
		accounts = append(accounts, acc)

		// save as contract (not spendable, not delegatebale, no fee, no gas, no limits)
		oop := &rpc.OriginationOp{
			Script:   &v.Script,
			Metadata: &rpc.OriginationOpMetadata{}, // empty is OK
		}
		contracts = append(contracts, NewContract(acc, oop, op))

		log.Debug(newLogClosure(func() string {
			var as, vs, ds, rs string
			if acc.IsActivated {
				as = " [activated]"
			}
			if acc.IsDelegate {
				ds = " [delegated]"
			}
			if acc.IsRevealed {
				rs = " [revealed]"
			}
			return fmt.Sprintf("Registered %d %s %.6f%s%s%s%s", acc.RowId, acc,
				b.block.Params.ConvertValue(acc.Balance()), as, ds, rs, vs)
		}))
	}

	// process fundraiser accounts that must be activated by users
	for _, v := range gen.Commitments {
		// we use hard coded row ids for registrations
		acc := NewAccount(v.Addr)
		acc.RowId = AccountID(len(accounts) + 1)
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.UnclaimedBalance = v.Value
		acc.IsSpendable = true
		acc.IsDelegatable = true

		// update block counters
		b.block.NewImplicitAccounts++
		b.block.NewAccounts++
		b.block.SeenAccounts++

		// count unclaimed supply
		b.block.Supply.Unclaimed += acc.UnclaimedBalance

		// prepare for insert
		accounts = append(accounts, acc)
	}

	// insert accounts to create rows (later the indexer will update all accounts again,
	// but we need to properly init the table row_id counter here)
	table, err := b.idx.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	if err := table.Insert(ctx, accounts); err != nil {
		return nil, err
	}
	table, err = b.idx.Table(index.ContractTableKey)
	if err != nil {
		return nil, err
	}
	if err := table.Insert(ctx, contracts); err != nil {
		return nil, err
	}

	// init chain counters from block
	// set initial unclaimed accounts to number of blinded accounts
	b.block.Chain.Update(b.block, b.dlgMap)
	b.block.Chain.UnclaimedAccounts = int64(len(gen.Commitments))

	// update supply counters
	b.block.Supply.Staking = b.block.Supply.ActiveStaking
	b.block.Supply.Delegated = b.block.Supply.ActiveDelegated
	b.block.Supply.Total = b.block.Supply.Activated + b.block.Supply.Unclaimed
	b.block.Supply.Circulating = b.block.Supply.Activated

	if genesisSupply := gen.Supply(); b.block.Supply.Total != genesisSupply {
		return nil, fmt.Errorf("Genesis supply mismatch exp=%d got=%d (active=%d unclaimed=%d)",
			genesisSupply, b.block.Supply.Total,
			b.block.Supply.Activated, b.block.Supply.Unclaimed)
	}

	return b.block, nil
}

func (b *Builder) MigrateProtocol(ctx context.Context, prevparams, nextparams *tezos.Params) error {
	if b.block.Height <= 1 || prevparams.Version == nextparams.Version {
		return nil
	}

	// origination bug
	if prevparams.HasOriginationBug && !nextparams.HasOriginationBug {
		if err := b.FixOriginationBug(ctx, nextparams); err != nil {
			return err
		}
	}

	// babylon airdrop
	if nextparams.Protocol.Equal(tezos.ProtoV005_2) && nextparams.ChainId.Equal(tezos.Mainnet) {
		// airdrop 1 mutez to managers
		n, err := b.RunBabylonAirdrop(ctx, nextparams)
		if err != nil {
			return err
		}
		// upgrade KT1 contracts without code
		if err := b.RunBabylonUpgrade(ctx, nextparams, n); err != nil {
			return err
		}
	}

	return nil
}

// Bakers implicitly activated by a bug in v001 (called magic delegates by us)
//
// Sadly, there is no documentation about broken baker activation rules.
// Core devs are unable or unwilling to answer questions regarding this issue.
// Our previous implementation resulted in many false positives which in turn
// resulted in excess total rolls and in turn in wrong luck and payout
// share calculations.
//
var v001MagicDelegates = []tezos.Address{
	tezos.MustParseAddress("tz1T7NFTcJQULn4GVoEEcod8v5f7fRJwF2JJ"),
	tezos.MustParseAddress("tz1LGdjnU54XLWBTb3jeqoYdxwdzfbtLDikY"),
	tezos.MustParseAddress("tz1aMdq4MVVcNAinaLbBbz1RaiEYDAj32gLM"),
	tezos.MustParseAddress("tz1fahTqRiZ88aozjxt593aqEyGhXzPMPqp6"),
	tezos.MustParseAddress("tz1XkALkYAQ2KFA8NqK2tJ8HBtRXqeSX14aS"),
	tezos.MustParseAddress("tz1e6ousJm7xbVUudCCrijZbaMfk2sjv3C91"),
	tezos.MustParseAddress("tz1b3SaPHFSw51r92ARcV5mGyYbSSsdFd5Gz"),
	tezos.MustParseAddress("tz1eU1Xb57o9cgHymUYJ45VYm1uLvwBFQA9N"),
	tezos.MustParseAddress("tz1PtbtUpKHF9KrCZjeKDzk1xuzGFkmXdYMk"),
	tezos.MustParseAddress("tz1Kra1CK3zxgpfb6fHDMuant4kAxgakmPxV"),
	tezos.MustParseAddress("tz1e17nNMNs9wfkUuPivXg2XWaMkvcqdyWje"),
	tezos.MustParseAddress("tz1Mvef6LM2sqmGWHyxq5wWRRfkA3nCeNqUB"),
	tezos.MustParseAddress("tz1NLTAX47PdGCdz3F46qoj9kokEx2dBnoo8"),
	// needed for snapshot on cycle 4 and 5, but has no rolls in cycle 2
	// since v002 migration happens in cycle 6 we need to add it here,
	// otherwise income table entries are missing
	tezos.MustParseAddress("tz1UcuaXouNppYnbJr3JWGV31Fa2fnzesmJ4"),
}

// Bakers in the magic list that are deactivaed by v002 for whatever
// fucked up reason.
var v001IllegalDelegates = []tezos.Address{
	tezos.MustParseAddress("tz1aMdq4MVVcNAinaLbBbz1RaiEYDAj32gLM"),
	tezos.MustParseAddress("tz1e6ousJm7xbVUudCCrijZbaMfk2sjv3C91"),
	tezos.MustParseAddress("tz1eU1Xb57o9cgHymUYJ45VYm1uLvwBFQA9N"),
	tezos.MustParseAddress("tz1Kra1CK3zxgpfb6fHDMuant4kAxgakmPxV"),
	tezos.MustParseAddress("tz1Mvef6LM2sqmGWHyxq5wWRRfkA3nCeNqUB"),
}

// hash table for lookup
var v001MagicDelegateFilter = tezos.NewAddressFilter(v001MagicDelegates...)

// activated by v002 migration (list compiled from research about roll
// distributions and babylon airdrop)
var v002MagicDelegates = []tezos.Address{
	tezos.MustParseAddress("tz1a8jxLZv6M8cDjmNYwAXweQiKUnXgFWVQN"),
	tezos.MustParseAddress("tz1ajpiR5wkPXghYDdT4tizu3BG8iy4WJLz4"),
	tezos.MustParseAddress("tz1aR3E7CGyceYDa2BHBde8AL1RvWbS1ZgYJ"),
	tezos.MustParseAddress("tz1azWX5Ux5Hizb3qj1vHF5LZwwCMFA8b4mZ"),
	tezos.MustParseAddress("tz1bcx82twLzuDHRM9i7B4Jjnp7NMAUSMQFN"),
	tezos.MustParseAddress("tz1bg9WkHYxigQ7J4n2sufKWcPn955UrF3Kb"),
	tezos.MustParseAddress("tz1bh296rrEb5yRtpg87j3TVfcsFL4NxoDdj"),
	tezos.MustParseAddress("tz1bkhnnvrtmwcryKzHGbKp48yS2qNMRDehA"),
	tezos.MustParseAddress("tz1bQofEmH6iF5DwNzfPkuMkSoPJfgFYFJYd"),
	tezos.MustParseAddress("tz1bVXGLBa8qhHaymZ3yEwgHjiAE7MDom13K"),
	tezos.MustParseAddress("tz1bxz1kjYmnE4fsWFQKyU2NWzyiTG2YjhGV"),
	tezos.MustParseAddress("tz1cP3XjgyQ4xY3kCJbxXLbq2QzkeMFUFBoh"),
	tezos.MustParseAddress("tz1cQM6iWcptjU68FGfy1b7TNLr6aKUTQbTT"),
	tezos.MustParseAddress("tz1cs4Q98YbsUfNpch7ijQHtEgMqvdzTvnhW"),
	tezos.MustParseAddress("tz1dFhaP5bWLgBswYtBxpTFEXec7mmzBskNw"),
	tezos.MustParseAddress("tz1dhMmmUA1k3AoF2thLk9rvfd8yDxXxEGun"),
	tezos.MustParseAddress("tz1djECaHtJXhYP1kbK4KgJ2EHpgCVjvANnQ"),
	tezos.MustParseAddress("tz1duEr8qA9y2PUkRYnA7qE2nwmUpunANcQg"),
	tezos.MustParseAddress("tz1dWokQy9hhBCj1bZnJVjjfc61JxW2qCG92"),
	tezos.MustParseAddress("tz1e1BgVt3DZgA1AuTMTRGS2cgS2vGP3hMRE"),
	tezos.MustParseAddress("tz1e5NtW8mi6F6U8DfKaMwSeRaiPjrxKxT3V"),
	tezos.MustParseAddress("tz1e9jBy9dEGER2dKrtzcWtCpDfbbLNPTQab"),
	tezos.MustParseAddress("tz1ei6WjcQWCttFQtpqw4zaZrpb3XJUVfGem"),
	tezos.MustParseAddress("tz1eMKUnpTfS7ypTH9SRTRXBW4RBE8EEszsD"),
	tezos.MustParseAddress("tz1eNUaSdwY7RJfb3aVXFwPc3tiG6HeCADnq"),
	tezos.MustParseAddress("tz1eRPe6QWnsB6mp8wqbNBDB4VuufS5bcv5e"),
	tezos.MustParseAddress("tz1ewpKn61gGEyvvpgWSTTsZvWVGA2t7fK7i"),
	tezos.MustParseAddress("tz1f4U4NUdnMgP8rkPHvUBVznZsgUG636nhz"),
	tezos.MustParseAddress("tz1fc7jqJ4YuJx9Diyb8b4iiWAto34p7pqRT"),
	tezos.MustParseAddress("tz1ffqW9CQ6aCD8zwcq5CLs8Gth335LWAEDJ"),
	tezos.MustParseAddress("tz1fntgFVaRT3jxaMyHaxVua7w2TaNcPKeZP"),
	tezos.MustParseAddress("tz1foqx9ArpckkTvwbPiV4kjoYsxnbQdSE3o"),
	tezos.MustParseAddress("tz1fR6dVH7fS58y2EdDGtM24ZcBuwDnaiTBA"),
	tezos.MustParseAddress("tz1fuPAGNKQVktnvVHiGR6RNwf2TXSTwZn9T"),
	tezos.MustParseAddress("tz1g7ZuJf8m1G2PUXhwQDB9AEXyPp2zNK6GB"),
	tezos.MustParseAddress("tz1g9e5poiqG2V2SC7aya93MTKJt6pbyWrEk"),
	tezos.MustParseAddress("tz1g9EpsbjJBjC7h5cd1crpnxY7FqzGWFpLw"),
	tezos.MustParseAddress("tz1gAiP5zzKdh56evj1bxrXw27moCuPdAX5W"),
	tezos.MustParseAddress("tz1gFjEVbJjEmCWUa274oX6yjRxkrf4mgPU2"),
	tezos.MustParseAddress("tz1gftALWAg7Ui7Tb5tkdbw1g97BRHUQZevA"),
	tezos.MustParseAddress("tz1gJvShTiuxoaZtjcwMv3LHcGU2QFqx5dsE"),
	tezos.MustParseAddress("tz1gkWnVtzqzavL8PJNsDTVYyP8mLhdwqF45"),
	tezos.MustParseAddress("tz1gthtquS9XUKQnzV72AQKqMjpNJEMgoRJU"),
	tezos.MustParseAddress("tz1gwHTJH5UPRyF1fq2uGYyL5ZtCYxe8oyW7"),
	tezos.MustParseAddress("tz1hbQBiAccFQCWhxetrmXceRWxUW2noVoLU"),
	tezos.MustParseAddress("tz1hE2bwMvNAJJuSnTLjxfLCdLbkuZwRumsW"),
	tezos.MustParseAddress("tz1hoFUMWpvRWy4fMUgLGZjwe3i5xxtN1Qci"),
	tezos.MustParseAddress("tz1hqDpNW9hVHautPpe5n2umNcrdMKZkjpkX"),
	tezos.MustParseAddress("tz1i3fUf3HdAmHAYrFkiBiYDbX2xoLuQQcDP"),
	tezos.MustParseAddress("tz1iDNPdZiKzLDQYQMwn4opK5gK5b7S9rXnE"),
	tezos.MustParseAddress("tz1iGjJkxZjHEh9t7XSJf1fURbGPYjBvLB5z"),
	tezos.MustParseAddress("tz1ihTyGCkQUPCf2QGz5vxxYMTwLmbnjT6WP"),
	tezos.MustParseAddress("tz1iUKcomroMTdQvhMkuY6TDxAwoMb2M6Ryx"),
	tezos.MustParseAddress("tz1KgWEWyAFqGD2i5iKA3E64ABkaybWr3TBG"),
	tezos.MustParseAddress("tz1Knoe8doKmD8b364hh94hSZZ6Au46uBLgU"),
	tezos.MustParseAddress("tz1Kvszu74tzrfjZRYW9d1r7ePK81rHxsZUB"),
	tezos.MustParseAddress("tz1KxJeKFKZj2AGzdefoCxgWRYySewxqptcu"),
	tezos.MustParseAddress("tz1L6a3SsVqzvcxESxzqvEJpAcU8Hs4SSHEF"),
	tezos.MustParseAddress("tz1LHFqnoQnmqTQd79DvxdRMXfVFTSk8XeUt"),
	tezos.MustParseAddress("tz1LkWA74w264oHvmuQUVEFM2c7w19EMDsv9"),
	tezos.MustParseAddress("tz1LmJsZuRyxswNV4YghF3q5fmLLxrKST3gp"),
	tezos.MustParseAddress("tz1LpfwGyyfYCGfsUqjXmo1ZaCNBtCm1HJ3e"),
	tezos.MustParseAddress("tz1LQBPXnV2rWd9pL5dhZZ9iNZCx6D6wJexj"),
	tezos.MustParseAddress("tz1LrFegiq14oByxgcS7vGFnorj9uYBed6bD"),
	tezos.MustParseAddress("tz1LS6oGf95DV7c2mSZ17C6RsuoEiD9EwGWc"),
	tezos.MustParseAddress("tz1LUWkTyB62ZFpvn8ZrqbaVDPekXzcVMuFd"),
	tezos.MustParseAddress("tz1LVHUSTmfNHn1NpDa8Mz8vq1Sh5CCMXX4V"),
	tezos.MustParseAddress("tz1MGrrhm1vabnAJRBEQxVHYcEC1adqiziRs"),
	tezos.MustParseAddress("tz1MK15cQnc6snngNWw7YfjowkCZw2JNNmbU"),
	tezos.MustParseAddress("tz1MPzCt4xgE74D1fwHFrmjabb1sZvgQNSDF"),
	tezos.MustParseAddress("tz1MRHkVE9zxbAgho7uNuqAcmct17d3Ej9VS"),
	tezos.MustParseAddress("tz1MYMR3dySgoe14L4nEPycFFBwD9dnmSdHm"),
	tezos.MustParseAddress("tz1Mz7ZZu5Rgg2LamJmu2dzozZ2KZ8Jb2rLP"),
	tezos.MustParseAddress("tz1NaujomKqcKKacopVcQtqh32DTNaLAdcNb"),
	tezos.MustParseAddress("tz1NC7TTSyNwB5N7bQWXmafvJbCVrPKGNPcS"),
	tezos.MustParseAddress("tz1NEV1TPAeF68AiyLBUG7CPBFNJ1txVYqu1"),
	tezos.MustParseAddress("tz1NgGYS3RiesowW19n9TZpd4gnrHH1Ckkhn"),
	tezos.MustParseAddress("tz1NHGYDZj1EkNo2ZqE5nCxTJosGw4PBedch"),
	tezos.MustParseAddress("tz1NLQyBAjbgG9tk1rcVgXL2ArwBoH9jJxKo"),
	tezos.MustParseAddress("tz1NqYMDAR4dDxsgxV4WDcVNGfXAKakpdTeR"),
	tezos.MustParseAddress("tz1Nthwqk6zjHei1tEGdj228Awt7VsN86c6b"),
	tezos.MustParseAddress("tz1NuXPd1qePQeMzsMTZQAqy8a8DSkqYUVcb"),
	tezos.MustParseAddress("tz1P4CZSLSmD6VVUm9dqNFpy9eV3ZU1LwwbQ"),
	tezos.MustParseAddress("tz1P6GGbfN6EGVpgYHHbFpMAkBmGjhAqsEWJ"),
	tezos.MustParseAddress("tz1P6nfhyAx8uUapcZSuFmYtBzv4RmwF6qvg"),
	tezos.MustParseAddress("tz1PAcQy7L3EqKLaYZjpJ7sUNRXWe4NNnmEc"),
	tezos.MustParseAddress("tz1PCPMQ7WC62WqGxgHB1G48wVUCmvTbmoAE"),
	tezos.MustParseAddress("tz1Pk341z4zeN8rRTX1HwWXMfbzSsn6dwEYo"),
	tezos.MustParseAddress("tz1PPVuUuJR258nGtdHEsUSmBHHsvFeLrRTW"),
	tezos.MustParseAddress("tz1PS6NW7jeVrQEik1F8pguKR8tKQZbiT8fC"),
	tezos.MustParseAddress("tz1PygG8dRGV5vev2DALRAqmdYAqReTD8987"),
	tezos.MustParseAddress("tz1Q3fqvAJmijgABnHbbNm1ou81rvFcmBipM"),
	tezos.MustParseAddress("tz1QJVCDbrGkfEjcdWD1eXy71fXYtbNg93Gp"),
	tezos.MustParseAddress("tz1Qk2Q8Ju3YCSqPv9QxCEafSYZM1ZwTTcCn"),
	tezos.MustParseAddress("tz1QRKeabUMA4dExyk1y12v1MwqibWoczoZU"),
	tezos.MustParseAddress("tz1QRz9FBkKwtmP6nv6WhHVbnbGkFG5mNjwS"),
	tezos.MustParseAddress("tz1Qsa82diwpvMbsyi3t57KVyV6dGZX5zkSg"),
	tezos.MustParseAddress("tz1QWLv49qn15Vq7cCR2LnzWNrD8HtkwAeNd"),
	tezos.MustParseAddress("tz1R4MPhiReS2ujzj9RzVuvmrAZiTx1s1URX"),
	tezos.MustParseAddress("tz1RCpatyxtpTEzXYqQjsz6r2VrhMeF3pCY6"),
	tezos.MustParseAddress("tz1Rctu7qNj3RyAyz7kdyJjYkbYxeTpNFQRF"),
	tezos.MustParseAddress("tz1Rf4CBpave59kipUeSwUSvNjacnVPSpsoP"),
	tezos.MustParseAddress("tz1RQMjZjF2hg4ySfMCuZH5hAzNLziqTkazH"),
	tezos.MustParseAddress("tz1RQRJtR9xBKCPk6XBxVZo6Z5bjASYrDRtN"),
	tezos.MustParseAddress("tz1S3ucpKQrtkp8Bz7mw4LJ1zPVqmWufC5aS"),
	tezos.MustParseAddress("tz1S8ocaHL58fSrneqJeF6Ure4LSjarPcDDx"),
	tezos.MustParseAddress("tz1SQ3fSVjscp2vjmVSiyWQL9Yapt3y6FZHJ"),
	tezos.MustParseAddress("tz1SYSLhuc8woqw68isT2zFvkRgksyJReMTm"),
	tezos.MustParseAddress("tz1Szcfqv3iTVSsTb11X8YCCnxRsFP6uK3v5"),
	tezos.MustParseAddress("tz1TJY3ouYwqdcyPQFWU9DEy5q4Y5qEusPqY"),
	tezos.MustParseAddress("tz1TKzBHiEh1KrcYckcSiNWRRKjfowKw2GH3"),
	tezos.MustParseAddress("tz1TuY6PkTDL6LKL3jFjMBPst728uhGdQ6c6"),
	tezos.MustParseAddress("tz1TWQmJTfosQPFGUXjbXUzV6Tj23s8zbXUs"),
	tezos.MustParseAddress("tz1TwzoBefS8PEbe91h3eTkYsA4QAQEBMcVL"),
	tezos.MustParseAddress("tz1Ua95YukXAmcMbfUv67gEhxiJx1n9djMiU"),
	tezos.MustParseAddress("tz1UHQ7YYDaxSV4dY8boJRhUfmU7jKprEsZw"),
	tezos.MustParseAddress("tz1UrBsKAUybPbqZHKaNp8ru4F8NcW2e1inG"),
	tezos.MustParseAddress("tz1UVB4Yt8raLZq8AH9k386aqr7CG7qSMMjU"),
	tezos.MustParseAddress("tz1VayoLunKK13JkS6ZpLfHvB193VaZLnU3N"),
	tezos.MustParseAddress("tz1VDn5stQhZzeyPiMMNGwRpXZC9MP9AnAt6"),
	tezos.MustParseAddress("tz1VDRt5NL44SEECAW7Qft8nSCjhDWvhYPrb"),
	tezos.MustParseAddress("tz1VdUYXimk7JCvZawMtsZgd6gwj8XdpQHF1"),
	tezos.MustParseAddress("tz1VJDAEFypQPtU3t23ZFiVxPQDV8zamkvgZ"),
	tezos.MustParseAddress("tz1VpBoHR8MHD33kbzN5VHM5b6dBtp35LoZp"),
	tezos.MustParseAddress("tz1VqbLLmk7WVxStGh22wMQwpnspahivg7QT"),
	tezos.MustParseAddress("tz1VQuud7J1kmBCrhcKYsYHU1FX5nkFjtLpu"),
	tezos.MustParseAddress("tz1VuiTbm9gJa1HoYQqZxvggdBaB9DB4mM8z"),
	tezos.MustParseAddress("tz1VUunMWp6tfK7T7QQQTBcsrnp713CmCDYi"),
	tezos.MustParseAddress("tz1VuWhc9ZvbXgmnwAcYdYAkZQscuVZyrdba"),
	tezos.MustParseAddress("tz1W7roMZucBCjh8QgwwgJsjEazW2YgA7sJ5"),
	tezos.MustParseAddress("tz1WAvs7bK5EYscH3xBf1LbJCoMMXBpjgK5F"),
	tezos.MustParseAddress("tz1WeuWTkfMaViHypSX7joYjWX8NApHHC2sq"),
	tezos.MustParseAddress("tz1WtkJEkKjHX3bMDYwJoDVC4gPksNPUa3v7"),
	tezos.MustParseAddress("tz1WtUtJcKEEp8ixYDEqkHEUYLyS2U4qWpzJ"),
	tezos.MustParseAddress("tz1WUVANirUcv3rSNNWxcv8GwMUAnH9mDnjn"),
	tezos.MustParseAddress("tz1X1T8PQWFoVzRS8WLDTYs9HWUVNTe3FNv5"),
	tezos.MustParseAddress("tz1X4C6KvSAkavFAexxCJNpdyYtP8bftRcoe"),
	tezos.MustParseAddress("tz1XB7RRogXyqoDPVcRLd9LS2kJoQRGT4Eje"),
	tezos.MustParseAddress("tz1Xbr2W9JAjfSa8P2LZut3kAUGwfN5Gb1B8"),
	tezos.MustParseAddress("tz1XkRTJT7gn41VczW8dx1KQjPFxWYVei8Cs"),
	tezos.MustParseAddress("tz1XWPzj88rcMfFAbwe9MPYQQ3wJi64HVWCp"),
	tezos.MustParseAddress("tz1XymQfBfSJMDoeCAMmseR5SiHKMXCWMaNy"),
	tezos.MustParseAddress("tz1XzPWgr4vYMKXJ64MwpfD2EWXKvnpcmFgi"),
	tezos.MustParseAddress("tz1YJPm9wxbGrX9gC8ExdqFQqtGCVMRhoJ43"),
	tezos.MustParseAddress("tz1YKEF6GHFkkHxmpn11ongpYtmA3tCL7ZXv"),
	tezos.MustParseAddress("tz1YptCde7YGdZt5Hyefi84LkQ6g23rPUzuh"),
	tezos.MustParseAddress("tz1YRDGSE2DDyLdVDn6tXm2gGqTSF5FXTGhB"),
	tezos.MustParseAddress("tz1Yua1wMkSaggN3pkFF157jfXTELAVExuxM"),
	tezos.MustParseAddress("tz1YVWh2g8Lne3RrJukx7bESXKWzryiXvyyV"),
	tezos.MustParseAddress("tz1Z2YY9D5piNsiPwe9KrvqBam4vqhvyLboD"),
	tezos.MustParseAddress("tz1ZSr8MfNZsFQJ2Gt67rJfNFeJks2P7cgwr"),
	tezos.MustParseAddress("tz1NRxCpNaQuYpTvqCTq6Qns6gm25ApRwg4Q"),
	tezos.MustParseAddress("tz1hG79KtHTkfJuCedpayNhuDwbFnbQL6tCG"),
	tezos.MustParseAddress("tz1U8xtmSRzu9RWP62YQMSev6Q3XG1KhHqRz"),
	tezos.MustParseAddress("tz1TW1Ncg3BPHLbmy83R9c6xoGE6WaTQE5bL"),
	tezos.MustParseAddress("tz1U8xtmSRzu9RWP62YQMSev6Q3XG1KhHqRz"),
	tezos.MustParseAddress("tz1MXj88Yeq7jW3Y6GpeV8GcPqySB4Y2grZR"),

	// non-roll-owners, but still bakers with delegators
	// why didn't we find them earlier, why didn't it fail in old versions?
	// - builder contains an extra check and loads missing delegates
	// - they were put into the delegate map (but without activation)
	// - that way, op processing finds and uses them
	// - however, they were never treated as real delegates
	// - now some database bug prevents them from being found somehow
	// - it would actually be best to disable the extra builder search,
	//   identify and list all magic delegates here, then look for a
	//   common identifying pattern again to get rid of this list
	tezos.MustParseAddress("tz1a2jLpMzJPdML19GxmsMturYnPfhRQrLe7"),
	tezos.MustParseAddress("tz1a2vSs3fC9haoJkrb75WDMCaBpAxkKiTWt"),
	tezos.MustParseAddress("tz1a3hwYmJK2ugELXQcQ4562rADjznjqu9Ak"),
	tezos.MustParseAddress("tz1a5a6ZpiDgAug9D1aEL7iYQeBg6k9q8yVu"),
	tezos.MustParseAddress("tz1a9GEwG5VuZXpdN9v2nRPg3gt3APDui9vC"),
	tezos.MustParseAddress("tz1abCp2gK3i5vyrwQ1LrzSYBkqxG7mQWgSV"),
	tezos.MustParseAddress("tz1aBFP8VEvjBNwRCuwmhkYiYG3HpWKkLqXo"),
	tezos.MustParseAddress("tz1adNdPMFZza44worgqSxmuiETT8BoTyjtw"),
	tezos.MustParseAddress("tz1adW9WgurbtU2jsvUMRNsQf3wTd84V4HAK"),
	tezos.MustParseAddress("tz1aEcdoBpGQYXAhDpxXRXj5SdYTHFTSXDjq"),
	tezos.MustParseAddress("tz1aEvwbBptpWJiVkLYCccgmvpxn5dK1ZMjH"),
	tezos.MustParseAddress("tz1agLNpQPDKhWQaVsTSMfRRJqEMEuBtkng8"),
	tezos.MustParseAddress("tz1agVi8BDCpAoBEp6pK1QxzcLUzuKcup8H2"),
	tezos.MustParseAddress("tz1aHZmYFNB7q33Nx72GBtezMZkF88ArvEMo"),
	tezos.MustParseAddress("tz1aj8mRZAzzDmqdAJb9Ksg94eJoXnHNwZbW"),
	tezos.MustParseAddress("tz1ajMyVnDuSBigCJNHTKjGpYidoUXkguKsg"),
	tezos.MustParseAddress("tz1aJwMHZvXhkdxg3UXm6VoASxr6mXqgvTyf"),
	tezos.MustParseAddress("tz1aKPGR3up2mMVPuDm89LtZuRr2a6gYxHSo"),
	tezos.MustParseAddress("tz1akuKasLr8R64qVPV2QRmQfRXvLTxfGJjo"),
	tezos.MustParseAddress("tz1amLKMWZjkJwP3zURaCAd4Qk2QJzpoWVfS"),
	tezos.MustParseAddress("tz1aQQyfwf7hXYwcQZZdGVzShJ31YvTXksnG"),
	tezos.MustParseAddress("tz1asEnC3kHaU6yNgXxgpGLHzKZ5r1agEazY"),
	tezos.MustParseAddress("tz1aTqxPRU3TgaY3t9MnggEvNTSzFLk3xYaw"),
	tezos.MustParseAddress("tz1aUFJzDGrrDZd6McBFejhcpQeMAy7pzUks"),
	tezos.MustParseAddress("tz1aV8f5WuzNg5qCdigghec7QpCoXhTtQZQP"),
	tezos.MustParseAddress("tz1avbrTxLRnfNFoAzop5RgDiJuPFeR59Np3"),
	tezos.MustParseAddress("tz1aVeGFBP6wTescgyHvm1uk41Hv8c3Gr1Y6"),
	tezos.MustParseAddress("tz1axasq86ttMpQdfXYtW626VKXHkSjdDMgM"),
	tezos.MustParseAddress("tz1aXdm4oTmbKXvGw5prURFfCxTg2tKRGCEo"),
	tezos.MustParseAddress("tz1aYWhrYB2xo4RYnT6uuYyfechD4xtHavRs"),
	tezos.MustParseAddress("tz1azq9MGJuwKuRzBZarFyYYEt5SAfcmWiHx"),
	tezos.MustParseAddress("tz1b5ASzFkdzH7pWXN3s7UmK93HGDjKPgyZY"),
	tezos.MustParseAddress("tz1b68NSrP6GXuRbHdiaFEMEVaGubCQiVLX7"),
	tezos.MustParseAddress("tz1b7c2uv4A2SB9ZZCiRUNmdxG4wAFHn1dB9"),
	tezos.MustParseAddress("tz1bcREs71f9tLZCxJCBjqzbWG3WibdAA7xF"),
	tezos.MustParseAddress("tz1bCXqUEh3Wgkwkz5UnRz6YqFyi8JkN2okd"),
	tezos.MustParseAddress("tz1bDq6c1KQpoWzHHm86WQmxYBxk9DZkz9nx"),
	tezos.MustParseAddress("tz1bDwfFpE4LnGQDEFox9MzmmBtPHQMgjnBw"),
	tezos.MustParseAddress("tz1bE6skhGJkrP5R8uVdj2SZXUYqGU21EtF9"),
	tezos.MustParseAddress("tz1be7HQwyHiBxNYBFk5tXdo8PEc8rSgFmME"),
	tezos.MustParseAddress("tz1beFG6xQBuX7MRjGYdo4oADm68pUyJvrmf"),
	tezos.MustParseAddress("tz1bFeMtHGPTdF6vExzLdMfa3d7RGWfeMv5J"),
	tezos.MustParseAddress("tz1bftavVp5jo6EvEoFJTGt1dYdmcuWZ2eRg"),
	tezos.MustParseAddress("tz1bgBJZfyjRgouLVrddbZmvbac9Q215myuv"),
	tezos.MustParseAddress("tz1bgezwz9gaf8ihS9MrBNcA9SJp99fyo3HY"),
	tezos.MustParseAddress("tz1bgouaGFU9gewujSFWmYBoeaTr2V9PDZbS"),
	tezos.MustParseAddress("tz1bHCZ5N8m6bKu4tfQScht9v1Hb28iiY9Kg"),
	tezos.MustParseAddress("tz1bhTyacCVJYn3XRoYxkRPUNTp8mYkefawx"),
	tezos.MustParseAddress("tz1bJrJjSh4KWGmWvXyvnAxtaxCvSNHY7ZY5"),
	tezos.MustParseAddress("tz1bkn3kGwEFjXJ13CDpsvBGFC85tP36tRkP"),
	tezos.MustParseAddress("tz1bLLjCuER4uW8JvimoBu1SprsebdNkNu6g"),
	tezos.MustParseAddress("tz1bmY2HicjVdR2P793Lm2N6L9ioEzsq3Ti6"),
	tezos.MustParseAddress("tz1bpBG492N7B13uWixoiFL5QeyR7oSHm81N"),
	tezos.MustParseAddress("tz1bpufqoa5i3wzj7pir7oSuKsdu27ocT4KJ"),
	tezos.MustParseAddress("tz1bq23FtkgkkSJAqHCksrRD7Mgf8vEQgBz6"),
	tezos.MustParseAddress("tz1bsVipUoKwLUXR1SmGzgTDAUXaEpctSzPU"),
	tezos.MustParseAddress("tz1bTxSTu9sxQ48EApkZQK9AHnxsGbhcbHV3"),
	tezos.MustParseAddress("tz1buEWYAXpyyv6JHKqzqZSSzFCvmckX9GMJ"),
	tezos.MustParseAddress("tz1bwFSPH1Z2RwsJKtmte3BMjPemYTgtsvfg"),
	tezos.MustParseAddress("tz1bxpyUCadFZGUptnjWBNP7KHoDqKwczgWK"),
	tezos.MustParseAddress("tz1bz4mMssWhQHJXyHLXL9iJa7KPnABcSQF4"),
	tezos.MustParseAddress("tz1bZcdFWNWrcEqfpavgkS962r7jhwXucFuh"),
	tezos.MustParseAddress("tz1bzLYJWmCDkBakBuU8chLAhNMspUMV8aL4"),
	tezos.MustParseAddress("tz1c3VerTV42YJfHhy3jZaqBCZy6xb9bNKYc"),
	tezos.MustParseAddress("tz1c6Nyp896QAJ4BntmCLnUGn2ts7g6vnNCi"),
	tezos.MustParseAddress("tz1caRjGfzmkeRbqpNbY9Kiu7nq7aoDXKu2s"),
	tezos.MustParseAddress("tz1cbBXhoZiwF4jWz5qvjYnsmtUfuYYGy9JZ"),
	tezos.MustParseAddress("tz1cBjwSe9ZLC29H8FTa8phYbnnktWVrw12y"),
	tezos.MustParseAddress("tz1cBWmyTgDVcpz38qvkdtRxmP8moobuwNTg"),
	tezos.MustParseAddress("tz1cBzbETGUbGYYZtJrSKd6do8kStgKbjKBe"),
	tezos.MustParseAddress("tz1cCbKMhGG7E3wk7DULejkRqo7rdqtKj46N"),
	tezos.MustParseAddress("tz1cCQkY4HdRkgmz6385VoyCF6RcAc7UPmtH"),
	tezos.MustParseAddress("tz1cCvTZY49oqtEbGGRQw36GiyPU861Rp4JN"),
	tezos.MustParseAddress("tz1cD1zyJTTNReUFuEhMwP1tJpNB2UHJ5bgj"),
	tezos.MustParseAddress("tz1cDJ4hyF8h9SPRZb1eGHAEmGoqNt9ZdgXb"),
	tezos.MustParseAddress("tz1cdRyfq21HduYAmV17NnBpRovJiQqg3NaN"),
	tezos.MustParseAddress("tz1cFH4zprbaiekjarqkvvCmDBxquyaS3j3e"),
	tezos.MustParseAddress("tz1cHkMAwwRoT9QjHVBJmCExsHBXJxQbgXho"),
	tezos.MustParseAddress("tz1ciaBXDUBgdNBH4gMqTM8ABqcQFeB3kGEo"),
	tezos.MustParseAddress("tz1cjPrhDUi5ikK9rqpBSxvkPQczft5XFdHY"),
	tezos.MustParseAddress("tz1cKJypDecMnCptUfYrxQBPSmfL5doyNFjj"),
	tezos.MustParseAddress("tz1ckvWZBjV1XKFxjnVB13bJVo3ppzmcZdh7"),
	tezos.MustParseAddress("tz1cmdS2mhe8aZCEWmvsTEFZrmxg9mzm7mJy"),
	tezos.MustParseAddress("tz1cNvfhSfYxsYBLA1MGeQJ3ZoJzvGckjsYQ"),
	tezos.MustParseAddress("tz1cQP9SeVbbLj9H4WbXAdcro6wmTVsVduCN"),
	tezos.MustParseAddress("tz1cqrjGcRwvxvUMP2JxR36MzNM8Go8aoa1p"),
	tezos.MustParseAddress("tz1cQupwwkgYG4caHC13DZuAP4bk2KowCA4u"),
	tezos.MustParseAddress("tz1crDkzwYkL6DCwejrEbgM5gZiWY2gC7uaZ"),
	tezos.MustParseAddress("tz1cUN39AMQ5t177FMYK2GUQGobLVGRfZbk4"),
	tezos.MustParseAddress("tz1cw1txkVU4dcd6y6A7zuiAGaWdeJDQKNaF"),
	tezos.MustParseAddress("tz1cx7eBYbHRnVecFhGM9S3NzfSDC1zswqfH"),
	tezos.MustParseAddress("tz1cyJLWofkruBkyD5dGPKffsSUzo5UuvY6m"),
	tezos.MustParseAddress("tz1d2VSieovSd2EFrQjx35rEMtRLmA9icppX"),
	tezos.MustParseAddress("tz1d31s2LnSyBbe4hAbcNoj4RNt1HSKwH7sG"),
	tezos.MustParseAddress("tz1d6MpSRCSRpwwAXoV78q42oj689QiovjUw"),
	tezos.MustParseAddress("tz1dAHtDMFa2nV6rcVp91sYtAXrKKiHugSqd"),
	tezos.MustParseAddress("tz1dan5jPeN6PukoLLpWmXmmAFGJ9ZK5ZrDz"),
	tezos.MustParseAddress("tz1dBGvW6cYAtSJNHDaLZaunW9C2rokhXaFP"),
	tezos.MustParseAddress("tz1dDFpqRftgspUCfwbgF98zEeQ4pWUZoqnr"),
	tezos.MustParseAddress("tz1deBLqhV1WPN1rtBi5ia1p6bfMJgABReRy"),
	tezos.MustParseAddress("tz1dHrTpkQG56FDZZJ9zybhtWCNS8rC6VTod"),
	tezos.MustParseAddress("tz1dhtGBmxpH4K2msp3eXFPdaTSc83efzmdr"),
	tezos.MustParseAddress("tz1dmQyHsUxohAFdbYPRQ32cd4soGAyDN1zP"),
	tezos.MustParseAddress("tz1dRzM2Rj3JCgc9ic7139tVQUgcJDLRxnXb"),
	tezos.MustParseAddress("tz1ds6Rj4hYUBnY52JyN3vbDzFVcDKfCbK41"),
	tezos.MustParseAddress("tz1dShEyDCT1LbhQdqzBFrSPKqZH9uGtEm7E"),
	tezos.MustParseAddress("tz1dSQY5Ho48i9Cd1oDVKgazrWboz3tix1RC"),
	tezos.MustParseAddress("tz1dTUMJxkrQ72eXQXQiphoMBDmuf6Q1HKYp"),
	tezos.MustParseAddress("tz1dVdciQVsJnNpnAnqjwTkM1akbHavJFBpo"),
	tezos.MustParseAddress("tz1dwhkPfR7sqtTrJCr61MESXtp8aPtH8CCD"),
	tezos.MustParseAddress("tz1dy1D5obUTfuYDuWY91bjNGffQ8d3BMyom"),
	tezos.MustParseAddress("tz1e2wLYUbcz31nKh733B3RhMxVQRoDqaXjA"),
	tezos.MustParseAddress("tz1e3ykSyGSLM2Kj3Cobk4hJaTwaFib51vKT"),
	tezos.MustParseAddress("tz1e5ftnH5vUX92rBHYu6KKn6nec5AMsSBGm"),
	tezos.MustParseAddress("tz1e5JECJ4hzHfjx8xo6S6wvxBCTNtS37sQn"),
	tezos.MustParseAddress("tz1e7nvCAA4ta4i1hJ35SWFFVgh3TtTabW7n"),
	tezos.MustParseAddress("tz1e8T7pwfvPtmQWpekxz1H9LRJic2j5kRbm"),
	tezos.MustParseAddress("tz1e8T8wTaHcS6sWHoaVF8vqvYP4Bk2jUSJ2"),
	tezos.MustParseAddress("tz1e9BY36N8h1irnEEU2LrbwCoPxjEkMiBvA"),
	tezos.MustParseAddress("tz1edXQqgF3PL3pCyiJpB7eZRoHaJqZX9RUL"),
	tezos.MustParseAddress("tz1eea5rUxvVJNDnXRp3UpbqtDuPymuGDzwj"),
	tezos.MustParseAddress("tz1eFLv5ttCtKA6wKcd9knuo1HMpVWmC8sNi"),
	tezos.MustParseAddress("tz1egqW2zVtpXh31WyQXpSeYqPD1T1RgeqxA"),
	tezos.MustParseAddress("tz1ehcUpD13GRDxKuttRaCu86h2VHmyRb3mM"),
	tezos.MustParseAddress("tz1eka4iGGiQcUd9m7vA3bWKd1HS2VY6TcLg"),
	tezos.MustParseAddress("tz1ekHDiDG6kABLQ4Q3pKjGSx6dwtNaVmTTM"),
	tezos.MustParseAddress("tz1emqssX3dP8xM9oxwoTXfxGKeASMAw3CWW"),
	tezos.MustParseAddress("tz1en4Bhcn56iT43sKq8cZr1bvorR6nd7BxY"),
	tezos.MustParseAddress("tz1eoPa7bSx25BfqF7NQq59CYVLm3rcnkREB"),
	tezos.MustParseAddress("tz1ePopTZhobwzob9gsAGa2w1ybEiDXFscay"),
	tezos.MustParseAddress("tz1eTQUfTprQTvduwkcroTj2e2GNuWwDTk1j"),
	tezos.MustParseAddress("tz1eujQyxRWs8L9rVPKNNzCivViU8KN9iw1W"),
	tezos.MustParseAddress("tz1eurWVAE9RYiRSzK7PvxkoyyE9T79Jc5mC"),
	tezos.MustParseAddress("tz1eXYMJ5Wx5Rmxx6khB8wrXQWqPh3Zgtb66"),
	tezos.MustParseAddress("tz1eYe2MuMxmKPX7jrWnAafYNsnBRXBg1p2t"),
	tezos.MustParseAddress("tz1eYsVh8yK5gFfG89cSGtiwCL3RpqszPej3"),
	tezos.MustParseAddress("tz1eYTXrCFr2eu9Y9Qiq5Zn3WjcuqEgdP8Ek"),
	tezos.MustParseAddress("tz1eZE6D9Y1NnA34gdbLMemTthvjQoR1E8B6"),
	tezos.MustParseAddress("tz1eZNTrL16PHLi9RMjdig76ykBcjRz1oHvq"),
	tezos.MustParseAddress("tz1eZom2e8KYXVehnuZiLFy98jMhA2bm6VEk"),
	tezos.MustParseAddress("tz1eZQegEytMaDur8CAcvCWAAFBHrfk22VvZ"),
	tezos.MustParseAddress("tz1ezr3qTG8f1ZENtjHou6PLhp2vAFrhXR3J"),
	tezos.MustParseAddress("tz1eZrqRN9AJwRfx3Wv8aX5nDhJDB6tuqzyD"),
	tezos.MustParseAddress("tz1eZwDSFVksh1VGhJsgLoAtEE4GsE6D3FeF"),
	tezos.MustParseAddress("tz1fDceP8H5J3uYCRcKfHp44ZduGfxoijB67"),
	tezos.MustParseAddress("tz1fEwKprNt9KcrfxeRzNc5UFABzgNLXz8Q8"),
	tezos.MustParseAddress("tz1fhjkBk2RU67VtNKjEZPKXFS4CxSVB9jtx"),
	tezos.MustParseAddress("tz1fjABR1hVXvHfvnyRXK8ZRHeRkAvUepWpT"),
	tezos.MustParseAddress("tz1fjowi1ZR2hjKcgtRifnXUeSFkPEGfpoSQ"),
	tezos.MustParseAddress("tz1fKB6VCmx2aLvTZ5kwrb5VmFiuwM11hNEf"),
	tezos.MustParseAddress("tz1fLcqxaYspcrsYTRwEdoaKFZ69BCpiRtJo"),
	tezos.MustParseAddress("tz1fLKR9QZCkwEj1Rw2ibi6PF1k2BnoLQqx3"),
	tezos.MustParseAddress("tz1fnnD8PnGHxTAjjmHAnoDNYysL61rRuH7H"),
	tezos.MustParseAddress("tz1foEmzUkWS3S7jM4S84idRddN5X1cXXhSA"),
	tezos.MustParseAddress("tz1frLZkFUBAH3pRSjR817w7QXT4KTEM132r"),
	tezos.MustParseAddress("tz1fTcLkfr13QnHHoHWqDKhPh5beNBYVieLR"),
	tezos.MustParseAddress("tz1fTQJ3xbiezWFx9y38SktZEYrRQLuAQ27t"),
	tezos.MustParseAddress("tz1fTuH6R546o3EXotmsbSwYwJGX23RwabEg"),
	tezos.MustParseAddress("tz1ftYwdE8Ebtbge33f1qdNhGwwE96g4oUFt"),
	tezos.MustParseAddress("tz1fuW7iVUWAwmzamWDBit9MtG9dHvE4zYZD"),
	tezos.MustParseAddress("tz1fUXScbdgh7R3RRLqbTpPXQqTeWRp9WoHF"),
	tezos.MustParseAddress("tz1fvdcfnLrAfZneNhgfy4BbLWpUagHjDWb7"),
	tezos.MustParseAddress("tz1fxgLoAVmrkKmFraLZJRNo3h6uKhXv5TdF"),
	tezos.MustParseAddress("tz1g1eDUZSbNAj6F4pYpZAwxB9DLVXdDYjYH"),
	tezos.MustParseAddress("tz1g2JZe9MuLq2tFYjwoURYtqgMMh1MMD6CT"),
	tezos.MustParseAddress("tz1g8XQnPN1xz9AKG1sDQG2D9STtc9az6zHk"),
	tezos.MustParseAddress("tz1g9j74Scc3FCrNK867rK3Z1skCgCooc83m"),
	tezos.MustParseAddress("tz1g9ndP2GQQ363SgvRFWHcXAAu2JbHMSe4G"),
	tezos.MustParseAddress("tz1gaL7D1mW6CemQLLNiF76iFcoiwNbfchVZ"),
	tezos.MustParseAddress("tz1gBtHdpYCQJBpit8K6knryk471TmC2cDCv"),
	tezos.MustParseAddress("tz1gdEjpGJ7P9XFPiDyMcixwJiCXpDTh7GEW"),
	tezos.MustParseAddress("tz1gfHNnhBns5oxeY2o49KJ599HdVrS7hMRT"),
	tezos.MustParseAddress("tz1gFLTi9vB7BwPAt1G3gGc8V4wYDLpXQUcx"),
	tezos.MustParseAddress("tz1gfYqmaJwQAvGRim7JCMU9qFaxtMpCEBJf"),
	tezos.MustParseAddress("tz1gg9d3RLar9iurRny5iovv2Gi9qRi1k6vR"),
	tezos.MustParseAddress("tz1gGoMibAH8q3Zq7WVrVfx7pe7M3VyFHkkC"),
	tezos.MustParseAddress("tz1gGSv1CYSxRMihw7Ec9tNnqaWvePvBxLJc"),
	tezos.MustParseAddress("tz1gHfH6YJN2LpDVbutQ864XTgwqRBgnbcKq"),
	tezos.MustParseAddress("tz1gJHpi7bbujxEqfBx3LW8nRMrawCwesGHB"),
	tezos.MustParseAddress("tz1gjQop8hy9GAcM2LvodsaPWLN9paXC1ekc"),
	tezos.MustParseAddress("tz1gJSAYoQ1LA1twgGaG4q4NTgeezf8GyREx"),
	tezos.MustParseAddress("tz1gPL5SSq23c9sX8jynWC1RuyCJRJR2VJMP"),
	tezos.MustParseAddress("tz1gqNkTWDDtw5JgEhbTbTMDuQVncXt3n6ti"),
	tezos.MustParseAddress("tz1gQSJ1NffxNp93iZNB29mbB4KwR61w8vrz"),
	tezos.MustParseAddress("tz1gqudR8Xexx4M2mSZDJrTEwgfaYCigT4sW"),
	tezos.MustParseAddress("tz1gryYvdpKitAf9Tm1KCWYpTmzivKmWwqay"),
	tezos.MustParseAddress("tz1gSnE9LF6xqD76LjzKPneEFXBuKmCDEFti"),
	tezos.MustParseAddress("tz1gtn7bBTmHWFKkq84skiU6kEXLooJfzzz7"),
	tezos.MustParseAddress("tz1gtWeqsrXbzhsCb4WnNodKgKaJaqM7Hgtc"),
	tezos.MustParseAddress("tz1gwvo47JLz17K6ZwvYLFMGeZ1jN38dRap1"),
	tezos.MustParseAddress("tz1gXEjp46bniYTobne5fzF8FW8HHxtExQ7p"),
	tezos.MustParseAddress("tz1gxsVpsxCAQHEh7nSYpDcu4EdK2DeywJyP"),
	tezos.MustParseAddress("tz1gyeSsuTop5HSFLydoC2nccEihNc5pDPDd"),
	tezos.MustParseAddress("tz1h1MTzu6cPhusxTVuEAqAgbCdCP44D8iZK"),
	tezos.MustParseAddress("tz1h29hUTKApAgZ3CGBzgdfkmPDbypbK7kkB"),
	tezos.MustParseAddress("tz1h2gSPWgpVfFT7LnM8X9UuAFDccP7EPAZc"),
	tezos.MustParseAddress("tz1h6PSs2y9xBCjm1qFf5zSy9zgtNHzjdnRV"),
	tezos.MustParseAddress("tz1h73i4WseQQxGa127BaLLSDzFuvxD8qiez"),
	tezos.MustParseAddress("tz1h7gFaiF2SKB7G45BVjXhUhcBVZZ7ubGsh"),
	tezos.MustParseAddress("tz1h7MkPXFKQW3sFiTKosLtGrSRociw6RMA9"),
	tezos.MustParseAddress("tz1h7RQuHbUoH4FCu78v2BnWdDVhGnbzBSov"),
	tezos.MustParseAddress("tz1h8BZ6BqikE77S7Y3Lh7Xs4xcHeqk3jest"),
	tezos.MustParseAddress("tz1haPzfc4wUwV8RvzKTt1dc8KCZ8MSdMBCx"),
	tezos.MustParseAddress("tz1hefi2WuApmxYKbPNqjwgX5KnqEzYUicdB"),
	tezos.MustParseAddress("tz1hesFCPtLCgRpKtBUZdqL5rPPXj5fuUZGi"),
	tezos.MustParseAddress("tz1hFbnxNVPk26vbBYeSo7D2ctS8TjV6QDPm"),
	tezos.MustParseAddress("tz1hfYSZyEEHiN6MiAninZkma1wa8pW15LL7"),
	tezos.MustParseAddress("tz1hgiUAvvsioQCtYqgTZjHyyppVrd3ycVMK"),
	tezos.MustParseAddress("tz1hjhYG2wHnwMxguNmYwm59XmdQNKNnuPVx"),
	tezos.MustParseAddress("tz1hJVbBsk79yFSGgR5WZsAaj42u2QuFpk7s"),
	tezos.MustParseAddress("tz1hKgZqgbwN4H5scSVKgTkz4Zd8ExaMrUh8"),
	tezos.MustParseAddress("tz1hkzwjcdYevoj7RwVjM9LYmQsAT3DuJp5b"),
	tezos.MustParseAddress("tz1hLssfuaVNKFGCjmcaUEzWFSvDttKgGg27"),
	tezos.MustParseAddress("tz1hMHZgAXVzFbr4xd1zZpVRXpG7AkEP5YSJ"),
	tezos.MustParseAddress("tz1hoeFVkmShwaUgm3Q52J3VCDpUAyYjxZES"),
	tezos.MustParseAddress("tz1hoYsvjLykAXcRobTcFDiszB4oKRfP8Ymw"),
	tezos.MustParseAddress("tz1hpVB6w1AQ6Y4qYYcHH4KbGdjvoh7J1CDR"),
	tezos.MustParseAddress("tz1hRvBAFMck8YAafSJub47JxG93dipqKGuY"),
	tezos.MustParseAddress("tz1ht8VSoiZBFWgGATtdZkfY4WU17r2KqD5D"),
	tezos.MustParseAddress("tz1hTFnUuA7KczWNskW1pbAUWSHwvE2UuKcA"),
	tezos.MustParseAddress("tz1htgWi3rF69Cey348QcCQUaucs3zg1zrhc"),
	tezos.MustParseAddress("tz1hXbugQDouUtiH7SndMcDK1UEicv6wbnHE"),
	tezos.MustParseAddress("tz1hZEUTWrDBW1UibP59V9CwHofma6fxceV7"),
	tezos.MustParseAddress("tz1iBvy8aujWLAfDpS7mbmdURwYzUfvxsHmr"),
	tezos.MustParseAddress("tz1iCjmbKixq1FMmeZSD3htaYrQtY8xoRNWh"),
	tezos.MustParseAddress("tz1iDAPx3Bn59FKkgS75x56FQrdXjbF5JoD6"),
	tezos.MustParseAddress("tz1iDRmPTfhbjXCYmN3tScRFxZwExL6bMr82"),
	tezos.MustParseAddress("tz1iERarcdd6hQurSzGXnUyGJmDkAe6nXGV6"),
	tezos.MustParseAddress("tz1iffzhKwqgfkSpR63zntVUJDDj2BijZKUd"),
	tezos.MustParseAddress("tz1ighnz6De1EhE6cUzQizfQ1WBZ28csod4w"),
	tezos.MustParseAddress("tz1iGW48dZuiViyz8TvWzvuex19WMDmeceKn"),
	tezos.MustParseAddress("tz1iHooUHG7xuFW8HZRUeMwTWWL3Kfoce87u"),
	tezos.MustParseAddress("tz1iKqmdfv9Zwab8zxdnj1dog8ztuQ3dEkkt"),
	tezos.MustParseAddress("tz1iLZcFsTb7unodySh3cKxPGW4hmVQMVaEF"),
	tezos.MustParseAddress("tz1iov7deeKVWF4CdFqc1diFqm73pF1eEAis"),
	tezos.MustParseAddress("tz1iqVE5cA4nJtJKQsX129gdCeCS41FEEPiT"),
	tezos.MustParseAddress("tz1iQxfNx7ksX22xuczRDPE3ogU6qVFoDXfK"),
	tezos.MustParseAddress("tz1isKQqzWQWdEziWR1C6ga9YyQJnTEYqxS1"),
	tezos.MustParseAddress("tz1itfxq8XkWsiY25VNEr4Yytu2wZm2DPsrE"),
	tezos.MustParseAddress("tz1ivJxvTNR8ojfFC5n2NZXQ5CTzUMn4AnQy"),
	tezos.MustParseAddress("tz1iZF2fDSpSB97PsbQ6rH6nWPtdZAb7VMHd"),
	tezos.MustParseAddress("tz1iZVB8uVYDmT9kwx818dXjA4fAjtf8QVrw"),
	tezos.MustParseAddress("tz1KgX7FU4jcEeHA5mbQA3wxgFmGoHii35r5"),
	tezos.MustParseAddress("tz1KhUcJa3CqcxScw9ofv8JKC4eE8J1d1KEv"),
	tezos.MustParseAddress("tz1KjdKQDqpEEjRrnWjM97GK4eGWFoiYgf6J"),
	tezos.MustParseAddress("tz1KjyU96eNehuMSKDzKPDkV8saRU79c3tB1"),
	tezos.MustParseAddress("tz1KnkZdbzewkqZh52TbEuE1t9zg1ABgCVXm"),
	tezos.MustParseAddress("tz1KoCP2ZFWHdMV2QEqDq7G1xAc2uq4u4BYy"),
	tezos.MustParseAddress("tz1KpQLaz9M9v8kX8Dem6adtqjdDDJrVjHk4"),
	tezos.MustParseAddress("tz1Kr9WpB91rmpQayZ26P4XqkY4qykztPSwH"),
	tezos.MustParseAddress("tz1KsDkdoWa7CfrB1hj4YAofmeEKJAzQqN42"),
	tezos.MustParseAddress("tz1L2RejeDLNV8NHTPfUTwJ7GknesSRwJUPX"),
	tezos.MustParseAddress("tz1L4ECEyefqKou9WroUjb2qBkXqGU7YA9jJ"),
	tezos.MustParseAddress("tz1L4MkiaMYoNBSnzLeNWVrVQJnkUdoJSAo7"),
	tezos.MustParseAddress("tz1L5R4X9b6GGHp1K5Jyj83fEP1Khuzsqzjv"),
	tezos.MustParseAddress("tz1L7NJH3ZdzE3GvbGjJagaGTCd7p3CXMMF3"),
	tezos.MustParseAddress("tz1L9v2HBuhcyJhye5Zs2fhhbmQZSzzAmbY7"),
	tezos.MustParseAddress("tz1LBzJAvGSgktYg3N3TVoBhGGNZS6odKL2h"),
	tezos.MustParseAddress("tz1LCgsz5qQA1FCDvNfTokVwFdpn4zu88sio"),
	tezos.MustParseAddress("tz1LeNtUxqHxWnfTSMe6ByF7Rsi7yk4xTWvU"),
	tezos.MustParseAddress("tz1LEQVXDkeVcMiH2Z1axvGoKGvTXx9StYt7"),
	tezos.MustParseAddress("tz1LEVurozodZpUfdGjLPDuX9QjdW2T543Vc"),
	tezos.MustParseAddress("tz1LGCxqcqUwsJhusVrkxDj4Joz15MipC6wf"),
	tezos.MustParseAddress("tz1LHuArfU9La5uZsgM6tSmGZkWxqy6qfqfh"),
	tezos.MustParseAddress("tz1LiFhPNp7rjFGZRVzm8yCtVqvNEAUBDoK7"),
	tezos.MustParseAddress("tz1LjkAKTg3xNh4LEKd7Yf5ddSjFPn77thbx"),
	tezos.MustParseAddress("tz1LjTPWDAR5Rt2Rcm2at2vV11sidrqprpW5"),
	tezos.MustParseAddress("tz1Lk2T1rREsCGA5tFbKZJw1QBQPHkqtooKm"),
	tezos.MustParseAddress("tz1LLJcgLioM7mFbnY7eqtKYr8Zigb3dyiYm"),
	tezos.MustParseAddress("tz1LMzJxgKA3S4Ce6AmxyeLsaDMns3wJPfZ2"),
	tezos.MustParseAddress("tz1LNaamtNYE48TkQmCTDiigxvthAsfuN7Z4"),
	tezos.MustParseAddress("tz1LNPtv6xPcSBpMZ7Ym8mjR33HeoPWPDkFi"),
	tezos.MustParseAddress("tz1LP7k9juM8msmHkxEQDkKhfDHrdEs4Zmot"),
	tezos.MustParseAddress("tz1Lr9KHjBuvYnVS3jAUWHbWycGbKsPH4itd"),
	tezos.MustParseAddress("tz1Lruu2jxqsRjXkfqzvSWRPYmWp8RuqmTow"),
	tezos.MustParseAddress("tz1LtTS9pGADZ2S1FaCmX6FaQ4iQFnvnaxV2"),
	tezos.MustParseAddress("tz1LUbNjqSUY6romwDJ4HsNZk5ULtiXvBpdn"),
	tezos.MustParseAddress("tz1LuoEwFNGzo1P1VeDy1LEm1nWii5dnbgKG"),
	tezos.MustParseAddress("tz1LVE3BnDEnnpFqrsjxggiEJcZzmBWaz1mF"),
	tezos.MustParseAddress("tz1LX4HF5v238Mf5iQJwgUuYdY71x2LbtPxH"),
	tezos.MustParseAddress("tz1LxgDGvGESh4dGEY2fmDVRag931mJ7fMqy"),
	tezos.MustParseAddress("tz1LXuT37HLhLWZuCBYubzi4LFTRqz1AiBWU"),
	tezos.MustParseAddress("tz1LyCRppt1FWZNiR8NZUKuUQ9Mj4zp6p97t"),
	tezos.MustParseAddress("tz1LYcxmuAGpYUypZC5Urm3TJckeBBETBC23"),
	tezos.MustParseAddress("tz1LyEctxUubWimfGyhCb1U5FN8rNASLMLMn"),
	tezos.MustParseAddress("tz1LYXE7uDu9Xg3oKfoLdauJS9VEu34fexLn"),
	tezos.MustParseAddress("tz1LZ3WKsVb1Xx3FR4Qg595AVA3ESeKvdg4w"),
	tezos.MustParseAddress("tz1M86eLSVfwjZLmSrcQbzYVy6L5K4MySwge"),
	tezos.MustParseAddress("tz1M98nRXqgLaMjZ7FtY1hEpo8RpipFBR8z7"),
	tezos.MustParseAddress("tz1M9zMsB17qx4xRzRXNDDGqsWpXUFrPKFmq"),
	tezos.MustParseAddress("tz1MarZNTxSDEEUNnqkqzLPgDggVbLcj2Ljc"),
	tezos.MustParseAddress("tz1MaWm74HYVKL1a4DyiATZ47wiiHX8QAXiL"),
	tezos.MustParseAddress("tz1McCjheyKdXW8gMZ6VnCxiUVAgrib8BW5N"),
	tezos.MustParseAddress("tz1MCgL54FuyjWzJB9fxRkBAuMSQMUu1V66W"),
	tezos.MustParseAddress("tz1MdBQtL2RUyX8NTxNg8brLKEu84WBi3Mak"),
	tezos.MustParseAddress("tz1MDeMaEwyCYnrfSeomtomJTLXuqdCEmJYF"),
	tezos.MustParseAddress("tz1MDiVMstADXk4JnvDP7mPLYEwmpMy6tx2f"),
	tezos.MustParseAddress("tz1MdsdFvx63grHpeHaD8ZN479GqvmVaenQY"),
	tezos.MustParseAddress("tz1MgdSYvCUQgSN5Np6h7yQVsE4La2RhfqMJ"),
	tezos.MustParseAddress("tz1Mh95qQUt5VRU2HVafDneJ3sY4oSow1hZe"),
	tezos.MustParseAddress("tz1MKsi3Fv1GTnRt2zF2P887GsCZQ5QJaJ4U"),
	tezos.MustParseAddress("tz1Mm5xERyJaqj7p98sLp1odGb1hSPCcDxVv"),
	tezos.MustParseAddress("tz1MMsgQhs7NdtnV726LoELdwPaxiJZBXQVo"),
	tezos.MustParseAddress("tz1MNf6AqLqkGpa4RWhxvtPxc5piBBJk9qid"),
	tezos.MustParseAddress("tz1MNLam2dZ845d2KSTBrkjgQx6HMm426YRk"),
	tezos.MustParseAddress("tz1MoT1U4wc4ueAQkbHrG65jC2BzCEuCWnSB"),
	tezos.MustParseAddress("tz1Mp8bLVGt7xfYNCPLJE966W67j6A4pnqr2"),
	tezos.MustParseAddress("tz1MpE7qiryVBuhr5SsRr4MnbqNG2usJKSB9"),
	tezos.MustParseAddress("tz1Mq4zwA6KrRKmixpLgZaNH5KtHVoDf7ZGm"),
	tezos.MustParseAddress("tz1MsrPDgwa8bM3MxxmcJAUMhojzdBaS8ZN8"),
	tezos.MustParseAddress("tz1MSyefycH8u3fdnBbUrDiqJGScxFutRFHq"),
	tezos.MustParseAddress("tz1MtBCscxTehEQoPousNVGkUZVAfv9m65R6"),
	tezos.MustParseAddress("tz1MtugxEcHBPgfHmzKKEuSuwzoGbgdACuH7"),
	tezos.MustParseAddress("tz1MVXJbPKc6y9nQQDSQMn4qAN8e64Eua1Tw"),
	tezos.MustParseAddress("tz1MxAjNdFYDqqZcEwoGv2DSpgnubVswzLBW"),
	tezos.MustParseAddress("tz1MxemasrFS15ueET1srBgJWvFpwbcbXyLN"),
	tezos.MustParseAddress("tz1MXqxwdwmz951srj1cQQEMqkFaHN5BXQBc"),
	tezos.MustParseAddress("tz1N2gDB4VbJmAEGmEuJTs2DSczjUSdkaYys"),
	tezos.MustParseAddress("tz1N6CSdWbn6eHibEE9WyGxVVVkdH2sTw4EG"),
	tezos.MustParseAddress("tz1NaBgVjZTT9BQAwEGi8PfVz69nLrdMGcRf"),
	tezos.MustParseAddress("tz1NajNRPaMF4Zi6pvssrkw2w3abyuFsJB4V"),
	tezos.MustParseAddress("tz1NBsuJvHZvUaKkSyhmLtsZ9U4x8EF7hvrQ"),
	tezos.MustParseAddress("tz1NeTwPm6TkmNypUGiJR5QUswTfZ6daExSN"),
	tezos.MustParseAddress("tz1NgsaXviQq7AWJKfHKG8PGyAye4WMCi3Nt"),
	tezos.MustParseAddress("tz1NhG9MPhmxdJGfxkxDetn8dX5RpSAgF8of"),
	tezos.MustParseAddress("tz1Njb1Lkou5zhy5e3p7vk24ChSFCEY61ykJ"),
	tezos.MustParseAddress("tz1NNFRC4mQfJDwqbcB7sEeajse5BrpGSkUn"),
	tezos.MustParseAddress("tz1NNS8S6N12AUyYL4rnXM1snCrBP3Ckd2WF"),
	tezos.MustParseAddress("tz1NPEqpHhkrNdiYfK4P4qyx6K9UufiHSwJ5"),
	tezos.MustParseAddress("tz1NpvDkvYYU2onRzfNF5eJT6oGsgLEGcx7b"),
	tezos.MustParseAddress("tz1NSoqe4njfPX2GZvgAHa5VRwoAtcFUi92M"),
	tezos.MustParseAddress("tz1NtVsC4dB71TYuMVXd2uAxo8sjWyjnbUL7"),
	tezos.MustParseAddress("tz1Nwc2qpv8wwaYf2E6KJjmTu3BNcvKddxT1"),
	tezos.MustParseAddress("tz1NXwHEWfT9yZ7RdUeDwnd1V4uDEkyrKukU"),
	tezos.MustParseAddress("tz1NYrDz2vTSqgUGm52CXGzR1zAZzg1PB8NV"),
	tezos.MustParseAddress("tz1NyrmmJZ3Dz9xtgBcTrcTqPre4t7sTRDEf"),
	tezos.MustParseAddress("tz1P469ZeBUcbsdmprNqRZCh4X5CDHp7sdG8"),
	tezos.MustParseAddress("tz1P5uTXSU4vCVnBfXz7AVN54ZUTxoWn8VpY"),
	tezos.MustParseAddress("tz1PA3MCpS4kTy9pAFmSA2kiGCA1ohwewKME"),
	tezos.MustParseAddress("tz1Pa5zXipj8SDCzjZMPicHn8G7M6QUU6FmA"),
	tezos.MustParseAddress("tz1PaAJwUochM4VXpQaFeu1UuBZdjtZtDz2p"),
	tezos.MustParseAddress("tz1PaD39QjuhbvGPpLRjvW74UKYJtjS88xNq"),
	tezos.MustParseAddress("tz1PAV2vuBEUswUrAnccmgCU4vzfFHXL4BH3"),
	tezos.MustParseAddress("tz1PcYXxrcy8Bb3UqPEdcb5xXtq3Y7apX2Wi"),
	tezos.MustParseAddress("tz1PeDeH8CnwLKw6B8fZ9o5Lg2m3apLhP9cZ"),
	tezos.MustParseAddress("tz1PeJFFuYgenYuq5PMvRbXkXTsBTQG3NTrL"),
	tezos.MustParseAddress("tz1Pf1Vzq3uKkwpb2JqNjYEp4xKMkFXE4qwb"),
	tezos.MustParseAddress("tz1PfjgnwwaZoDPfAZnxcVCZrjNMQA8661w2"),
	tezos.MustParseAddress("tz1PiwpneHKVjGpgA126rUu2q7tr7LordS93"),
	tezos.MustParseAddress("tz1PjDCmyK2oDRfRDfu77UtMsNvWqLyE7qid"),
	tezos.MustParseAddress("tz1PKknQrZHPV383Rqpz9zB8xr3XwS93ULSC"),
	tezos.MustParseAddress("tz1PLHf4Yn6FoAU3Ev86GZTho23LEGYLB2uG"),
	tezos.MustParseAddress("tz1PMDGt4CXqyeHBMgpnw4dNujrax2pgTYvM"),
	tezos.MustParseAddress("tz1PmieHywiF9PvZ5Lpcibum5RrsRfR6V113"),
	tezos.MustParseAddress("tz1PN2iJSxHHaLQXfCVEvhGGfrsLJppCZb46"),
	tezos.MustParseAddress("tz1PpyPBDbzTAdmxByiU9mcnmv8PXsstoHLb"),
	tezos.MustParseAddress("tz1PqboHxfAzbWuzoKZbSZnVC1HxTUieedQP"),
	tezos.MustParseAddress("tz1PRbJ5xjz2W7GNFDY53PSWnwjNEib4mtYr"),
	tezos.MustParseAddress("tz1PuqnZdKRihHQRdDwRxg9At64xj4PDJAed"),
	tezos.MustParseAddress("tz1Pv6ncZKoq9ZK6cWQvxBDKwXi41q7Wc7t5"),
	tezos.MustParseAddress("tz1Pvd7EsYKq1eAwNczkDJkZTyg4Ho7n6Zrz"),
	tezos.MustParseAddress("tz1PWH9PdYJUUWLMbBA17SyP8nDLWuUMNHxd"),
	tezos.MustParseAddress("tz1PX7X8zXvCMPEjB92GwMdTVaNcDvjoHXWf"),
	tezos.MustParseAddress("tz1PxbHYUZ6tHMd2KKpbSA36EfLyjEZjX6rd"),
	tezos.MustParseAddress("tz1PXZjHzSdp9bMg7qMKVR9dh5qo1pwqJ2nd"),
	tezos.MustParseAddress("tz1PYeFpKggpJw9oX2NXRFtX9ReBUnu3uFKY"),
	tezos.MustParseAddress("tz1PytTBVbXQ3psCVX9i9b8YWEeuZjgFoRjf"),
	tezos.MustParseAddress("tz1Q5dLbT1TxC6TxwpyPxPXHEz81CRCzos5j"),
	tezos.MustParseAddress("tz1Q8iYpvjGft9aYwUq5AvKCEVKmKNa6F4MC"),
	tezos.MustParseAddress("tz1Q9iBo7C6fAQgsA1NMrNJLNqGKrhmVfLU3"),
	tezos.MustParseAddress("tz1QBMXJ8JxCwXEMnHr8oL1mWnCUnmiJPLSf"),
	tezos.MustParseAddress("tz1QDt7CBwXPxxYsTqhVQxhowk5w6ksKUGfo"),
	tezos.MustParseAddress("tz1Qe7YUe2iu4mwHGxcu7Sm2x1GMbH8F6gjd"),
	tezos.MustParseAddress("tz1QFbAXY7VgG4HVmontBbhW5W1FAq3a9ugf"),
	tezos.MustParseAddress("tz1QiGMN1JME8cuAZ1Jw7ZDLmWJF9ygFQqnF"),
	tezos.MustParseAddress("tz1QkfLMUh4bMngiJexAC44XpCuREAHT5azq"),
	tezos.MustParseAddress("tz1QKHptXS2gNBMpX75RQtzh8xWxQjemUcoK"),
	tezos.MustParseAddress("tz1QKkb2kLRU45PD5bTwMRC4QkrbXxHQ3wGT"),
	tezos.MustParseAddress("tz1QKYHzern9ZgvHyFa9vDyCptDcsnDoFYGq"),
	tezos.MustParseAddress("tz1Qo2cg1RdBRsPdYcCgvJaUSydSU2xT3R1p"),
	tezos.MustParseAddress("tz1Qpq23uVmjkGWULuSiicgs4nHy5LVZGrdu"),
	tezos.MustParseAddress("tz1QQA5QdqimdM4nMM24atbqNWJz7huBcaRz"),
	tezos.MustParseAddress("tz1QR4MWWPYH86Xzz62NSPMWbrXx4BQbcq8T"),
	tezos.MustParseAddress("tz1QRzamawV183Rw3XRNCey2rPdMYdXXQtG3"),
	tezos.MustParseAddress("tz1QSBq6z4bzzg8A5Ea3c1vNeTabxzr1H22b"),
	tezos.MustParseAddress("tz1QTv6Qqi1d9aqf4oNLoMfyKwis52Rddms8"),
	tezos.MustParseAddress("tz1Queo5aWrPBYF1qo4Uu1MEaVEiJRcX47v4"),
	tezos.MustParseAddress("tz1QVxPaSa8DvdUxAHnfKcK5vRseyC5mETz3"),
	tezos.MustParseAddress("tz1Qx7gAWfnXxSBR2SRm5AkjY3GXrTs4JbRJ"),
	tezos.MustParseAddress("tz1QyfPj5PCaW3pMSVaQgFeNj3gYn8aydmhv"),
	tezos.MustParseAddress("tz1QyiKhefi5vVFyPdYsVZD8BJzb2ZLC56U3"),
	tezos.MustParseAddress("tz1QYzHbHkU8vAqpLUz7BoyT4TEjV2PegMjS"),
	tezos.MustParseAddress("tz1R2bW58Vxicxz3WT32q7XDtfihqSGJx6GG"),
	tezos.MustParseAddress("tz1R3k1BbJDs2z1Mn1jddtFeFmtbPjrd6DGr"),
	tezos.MustParseAddress("tz1R3y1VGtE228gV3xJV1KAANHygyk1xxjU3"),
	tezos.MustParseAddress("tz1R7hJqWF2c8iHZNkPF96svFJM2zhhwgZdf"),
	tezos.MustParseAddress("tz1RaDPEqVxUbrUqzjpxu1mtK3LJPRr5kQJ4"),
	tezos.MustParseAddress("tz1RB3N9nivgS33NH1gegaTayKPBTEsSa5aC"),
	tezos.MustParseAddress("tz1RBj7Xzd7xfNcz55jaCN1BCdJzvPQvcyHd"),
	tezos.MustParseAddress("tz1RfK1o6jWKEnEY3ekuRUH5tz91RgXi5W8L"),
	tezos.MustParseAddress("tz1Rkdx6hDc4c4R2BMhDwMDEc5tdkq898CZ6"),
	tezos.MustParseAddress("tz1RMksuFdDPFjm7EarGSbBcuxXNQSgcArU6"),
	tezos.MustParseAddress("tz1Rp8HrzhLf2Ne6UGizQiyNdQk7mpcihuLb"),
	tezos.MustParseAddress("tz1RpKGUno6QpwmSwUo4pMMrvRxN7qsZ163h"),
	tezos.MustParseAddress("tz1RPM6U5c6njb1HZJGSpNciMYMZpjSf7ERF"),
	tezos.MustParseAddress("tz1RQQg9dRnJ5DNjEcD1Td6MoZazEnQYvkua"),
	tezos.MustParseAddress("tz1RrfsT58qk2Ez5FYPsyWNdUrjLqqZjEqFP"),
	tezos.MustParseAddress("tz1RtJciYyd1PdVjrai1vxGUuLXX9orMRB5y"),
	tezos.MustParseAddress("tz1Rv9D6gTGBmHxBvUvvfeoSaqhzTRqencf7"),
	tezos.MustParseAddress("tz1RyKanJTZGGytEDAFknJaPx1wCyg6hMrXa"),
	tezos.MustParseAddress("tz1RyPnAujsaqHCE9GzY9XCUPouwpj3uqdTt"),
	tezos.MustParseAddress("tz1S2oxGoeiRErSwmf5GYssRaV7Lu31kaBZL"),
	tezos.MustParseAddress("tz1S44EMmMAfVXKirfW1jjBTzb9m6an2JLq8"),
	tezos.MustParseAddress("tz1S4QG7or8tqevFGC1yPvq7n8FocAw3p6EF"),
	tezos.MustParseAddress("tz1S8bMF6XkWAdgfZ2AYuWcPoWh5QNTNLnHN"),
	tezos.MustParseAddress("tz1SBZGukv9PyJ9dHvD8p4NUkYW59HzN4XEs"),
	tezos.MustParseAddress("tz1SDmH7kP7iJjd58UABgaBQpuL7wxWdCxmQ"),
	tezos.MustParseAddress("tz1SgkyrnXQV7RWJJYNS9nxoJb2nNc9xXjo9"),
	tezos.MustParseAddress("tz1SGViakkcGsdXkesueAMgC8c5uosf8tGQ7"),
	tezos.MustParseAddress("tz1SHR6xVFCtWWJTZNAGmvvKP16TFGjk6dw4"),
	tezos.MustParseAddress("tz1SJNipszdBnQYAGgpiegPhgrdpTXTC2rmF"),
	tezos.MustParseAddress("tz1SLYJNbezSm2fyN7FJWUh9HPgrB6gsxq8h"),
	tezos.MustParseAddress("tz1SMNkFnsebd2SGNfxRSJrPrSHsLtNonEGx"),
	tezos.MustParseAddress("tz1SnAWHtjMAsQmsxdCEEuttYxUxyokssBuv"),
	tezos.MustParseAddress("tz1SpWJPgQEj9S5qMCSFMg9RGg1dgYdpyWkR"),
	tezos.MustParseAddress("tz1SpZ4DdQQRXmP7W57RCjmbokaktjrnhWAa"),
	tezos.MustParseAddress("tz1SqnRsbi6cPKy61wSJv1cBm8478aNRWLP8"),
	tezos.MustParseAddress("tz1SQNyk6vmynFuNKX41gszUyzWxFbfeqGjM"),
	tezos.MustParseAddress("tz1SqXqtMQup2MbNbApvUjA2vCce22g9U77h"),
	tezos.MustParseAddress("tz1SR2uSFs8h35jRmfnXM4pSYTyJzBNN8Pc6"),
	tezos.MustParseAddress("tz1SrGhCHecrjthACrYUQGbvuFM5be7bLjW4"),
	tezos.MustParseAddress("tz1SribpNhgRZdt87cNk8aUTNKXuBcm8ZadV"),
	tezos.MustParseAddress("tz1STkHAS9A4s5XR4qjvAXoMKjX1V7E6t5e1"),
	tezos.MustParseAddress("tz1StYLyvQigHRzz1LWKpo6e8T5LGDGkg8Ff"),
	tezos.MustParseAddress("tz1SUeQuHNcJj88MHqEyeZmtKBZc7cmQ4dgS"),
	tezos.MustParseAddress("tz1Sut4xMoRfMtPu2DJNaP567h8XDqbkv6mg"),
	tezos.MustParseAddress("tz1SUx9N9Diuo5dxBTu7uVvhyS3GMyW8xrCo"),
	tezos.MustParseAddress("tz1SvUPX9xpxnpvzNnqjbThkJrUkfWXeymb9"),
	tezos.MustParseAddress("tz1SwDSFXiNXkbs5GZjtynxwWYAamwGtxjpV"),
	tezos.MustParseAddress("tz1SXgtms7rRn5EEfyKWnoxEGtCrkwsvRE9H"),
	tezos.MustParseAddress("tz1SyDthLaWk11nGEHTgY7x1WTYftehXTxR5"),
	tezos.MustParseAddress("tz1SYn5VyQk1J3skPL2Gvy3Dm6TtdH8CWV4z"),
	tezos.MustParseAddress("tz1Sz6cAUZNyRHPGY42PvjBu7HMak5uB2A57"),
	tezos.MustParseAddress("tz1SZQcUscU1HqKTQYUVjmdTTdzpGF8AgYfg"),
	tezos.MustParseAddress("tz1T1SmKX1atFaALLniKhFyFKtqoktHvZMgs"),
	tezos.MustParseAddress("tz1T2ziQbpX2M1ayr91fUzV5K6rB9gjNSaf5"),
	tezos.MustParseAddress("tz1T5gMKS68EFCqhVWoYFmazhKSCQ39WdXcV"),
	tezos.MustParseAddress("tz1T6NMVbyFniRyqctgubeNk4wPRL1gAMWPK"),
	tezos.MustParseAddress("tz1T8N72GtqGJZ1ZbbiXg1SaGeE9ugyE8e7z"),
	tezos.MustParseAddress("tz1T9VJoDctkvL6aBXgqKsceovcuKEJtthgd"),
	tezos.MustParseAddress("tz1TahLqcTLWs1dCtmtaWJTQfXcJZPehfNTf"),
	tezos.MustParseAddress("tz1Taq5yZmytyiom7fcEGaHxmpwX1ZTYaoPK"),
	tezos.MustParseAddress("tz1TbCe6L6vCj2YFMCbgEs6YEdn2vmji8KVu"),
	tezos.MustParseAddress("tz1TcgvvzDD4hwHQHdPNGw6ZW9wkomwxaQkP"),
	tezos.MustParseAddress("tz1TEqPMr8f71HykDgb1KF3WYH5yHEDUN1pr"),
	tezos.MustParseAddress("tz1Tf6KFHJfELnZufERnwYq7hpA2gAAZ3Riy"),
	tezos.MustParseAddress("tz1Tf6UKNUphi9VcLaMsPDyiNd3j61ovepwT"),
	tezos.MustParseAddress("tz1TFamfvjkEnp25wpZrC7f1BgmX4GHEmybc"),
	tezos.MustParseAddress("tz1Tfp3gttKwisAo3jXSLcPGFErrpjzs1dgv"),
	tezos.MustParseAddress("tz1Tfp4AnrG9UnWcYxd7VJ2gdS32K9WayUjU"),
	tezos.MustParseAddress("tz1THBsB8mmXJeWcAtCWoqdUxDXYG7f2CtNN"),
	tezos.MustParseAddress("tz1THnHytiV6t9U6YDs5f6PWFw4ZDN3oY7i6"),
	tezos.MustParseAddress("tz1THyKikQPi2cCTsFVEWafTGiAXE6trVprt"),
	tezos.MustParseAddress("tz1TKR1jQSkk5YAK2ajXmyxfrCjjpJTQiXLH"),
	tezos.MustParseAddress("tz1TLBrZgMsP9o3cJGPziroCcLRwyduNzaks"),
	tezos.MustParseAddress("tz1TLoqSs3ECTg5ZiifBWdH9mBGWsyrX2Von"),
	tezos.MustParseAddress("tz1TMbMFK3AnkwH3UzmxWQLXnziYjqWKaEx4"),
	tezos.MustParseAddress("tz1TmPQXsiFobBEgH6zhBpB1c1RXzCYE7Smo"),
	tezos.MustParseAddress("tz1Tn9JrBv9paXav8G2tBGHDazANzTuzKQxd"),
	tezos.MustParseAddress("tz1TnDZTVWTtaE79VDRvED6XT3BD6auDi1G9"),
	tezos.MustParseAddress("tz1TNFpH6ERuLJVLMqtztTbu5KA2VKocc9Jk"),
	tezos.MustParseAddress("tz1Tp8Rk4a3Mb7EyS9fhEVEvofcRURDCaSCv"),
	tezos.MustParseAddress("tz1TpdDMHgYYZd9D4W6gEmBXyusqaKqkXQLz"),
	tezos.MustParseAddress("tz1TSSiMLDU2FxtruvcnURkqw6yazC7eVo15"),
	tezos.MustParseAddress("tz1TtZiFYbfTP34VNxw66iVmoVYSTActxy4F"),
	tezos.MustParseAddress("tz1TUkBBEnaXGgxqE7bcJrMgJsLixa8qYFzJ"),
	tezos.MustParseAddress("tz1TvjGhC8RSA2J4fruechkLKfReBQUrWu2E"),
	tezos.MustParseAddress("tz1TvZ1qLcz1VKLNP5ZqVi9A4zbBwd5B1SU2"),
	tezos.MustParseAddress("tz1TWCie12zhzmeWDZQaZYyiVycUKpsP3yRm"),
	tezos.MustParseAddress("tz1TWcJiqNHLkTiMRqAgUDmAzw1oV9z471ZB"),
	tezos.MustParseAddress("tz1TX2RUrqVgTX1kLycfGRhW784W3kEWuKfZ"),
	tezos.MustParseAddress("tz1TXK9kpXEbALxNgdbhZhJi3FzrRVxrugUK"),
	tezos.MustParseAddress("tz1U2F9b6wS1JviVNdButz299v2xSr4Rnn18"),
	tezos.MustParseAddress("tz1U41GUcgoDBwMAFQNnKTR6a2XJDyqMBojj"),
	tezos.MustParseAddress("tz1U4Dp5TjEM3f1wPAxdQG4792ayu29FXMvr"),
	tezos.MustParseAddress("tz1U6d5H5xW2wKLumkH6QpWRBA1C8JYytwAZ"),
	tezos.MustParseAddress("tz1U7jmfxuAngkne2jDnkbsAXWhhtc5UK4A2"),
	tezos.MustParseAddress("tz1U8eKCjWt5BSqcmxkA9KMi4sGp1Q79Roj1"),
	tezos.MustParseAddress("tz1U9nUeAJjmwF3W7Wyzscw541k2tmdvdgfv"),
	tezos.MustParseAddress("tz1UC1uY4PA5HfoB2XJyUzhvsfcfpHtitcV9"),
	tezos.MustParseAddress("tz1UdYYDcotrGvuL4m1dgq5Z5woEhuFqm5WR"),
	tezos.MustParseAddress("tz1UEuxS1DUXt7QWHodmFg1EvLjzE9NtMYvG"),
	tezos.MustParseAddress("tz1UJ8EvZu4H9czXyujWj68mmmxZTiWbDS7R"),
	tezos.MustParseAddress("tz1UKuHkVFfKGv3QPkaiB8pwkvGvtywDxF4T"),
	tezos.MustParseAddress("tz1ULGqzomKKDk6efcsCLQS2n736QcB8jq8y"),
	tezos.MustParseAddress("tz1UMf6EsXu5WpfkJbT4qb8TB9poXRdYNuCe"),
	tezos.MustParseAddress("tz1UMwLRRZv8W2ZE5ZM64iGdqfBeAC7rQ8FE"),
	tezos.MustParseAddress("tz1UNF1DrQm76YMW7PQ6v7ETon9aQ7vEzT8f"),
	tezos.MustParseAddress("tz1UnN2Zb9xatgKFCvdUG1bBmLiLNUMMcMkM"),
	tezos.MustParseAddress("tz1UnqwFYMj4JwWxnqJWRDoMug8LgTUmKmen"),
	tezos.MustParseAddress("tz1UpQ6tQHWSnbM9uPhLw3tmA54j7Zazcy9e"),
	tezos.MustParseAddress("tz1UPwzUSExjSW5GAL8rzuLCNfd1zuoaNcnb"),
	tezos.MustParseAddress("tz1UQU4xV9UGr7NT9yy9kqRCcJwFeKZ64w1m"),
	tezos.MustParseAddress("tz1UrBt36uSmuSk4LCc6GZdhDRxBX4v8XoZe"),
	tezos.MustParseAddress("tz1URTFWLQrwmqH99YgyUoTrrJD7LcD8M5Dk"),
	tezos.MustParseAddress("tz1URWCCaWtKDSdXVBg4DytqvBbvqQu1UAjq"),
	tezos.MustParseAddress("tz1USaW6YxNi3VJeD3bW6rEQEvn39wanDCNJ"),
	tezos.MustParseAddress("tz1UT1FQLDs3Vx1wWPtGe3XKkm7cUtSXNeQz"),
	tezos.MustParseAddress("tz1UTrZhn1DvAJqepDEqhEVk8DfzPjq3VN6j"),
	tezos.MustParseAddress("tz1Uui8oAGqskNVDSfgK6UHnFuZgbcQJibHU"),
	tezos.MustParseAddress("tz1UV1hmTgAvjax4nyoSidqnn3LFuJaewZkU"),
	tezos.MustParseAddress("tz1UVWCrXMTh6LC3Xuhyru6U6JZKvZHSuyJF"),
	tezos.MustParseAddress("tz1UvwpH6nHceXziioX8R7DovrWkq9ocsvFh"),
	tezos.MustParseAddress("tz1UXQW5KLa6rTBxsKbxxwvzhh6HSC48Tmhr"),
	tezos.MustParseAddress("tz1UyRNftMYJBQaJDaAofPuQTSNVhdL5irQi"),
	tezos.MustParseAddress("tz1UzMHvm7hP6C8U8C6hRgtM524gcKQuLGdQ"),
	tezos.MustParseAddress("tz1V3erS4KnSywZEGmM86fGtSto6wzpcj48J"),
	tezos.MustParseAddress("tz1V9X7gyGrCTTdXHQXmbUxVnHyRaTBJbrit"),
	tezos.MustParseAddress("tz1VBdGrAU18fc3SwzDnM6UA9LuG2nXTMYBa"),
	tezos.MustParseAddress("tz1VbH22jvMmz6vdYf9xCAiFba985hY4Q29y"),
	tezos.MustParseAddress("tz1VBLkTqFYjDQEGb6Am6BzNL7otaXoy59BR"),
	tezos.MustParseAddress("tz1VFbybDxdux5MwSi17z7k3etKr29YQuZrY"),
	tezos.MustParseAddress("tz1VgNqH18epbfm27Zsv9Ta2Lv1N3gq5JGNv"),
	tezos.MustParseAddress("tz1VgQMgavg59pU1k8B9Y2yJcg5Rh9YcUQew"),
	tezos.MustParseAddress("tz1VkwQer6xFSBNmcSid7ucm6om8rVKNZNXS"),
	tezos.MustParseAddress("tz1VLJCewEBemMZP9eE23NFuTQbHPsKeT8Be"),
	tezos.MustParseAddress("tz1VmqpVUHzhRi4QHZHCiNn1Uug7BNyzS2hR"),
	tezos.MustParseAddress("tz1VPUYyjMwinuBRjT3uU2CFaVrhHm7yPcT4"),
	tezos.MustParseAddress("tz1VRiuEURsUijEAwBGag9o7enaXi3QbReqR"),
	tezos.MustParseAddress("tz1VS246hSK8P1oVeXPHUcYzaB8PVkouJ4eN"),
	tezos.MustParseAddress("tz1VsvV2TSVxPK2BhmDTFMgeMv2aXvVhxZGY"),
	tezos.MustParseAddress("tz1VtpbF6GoayBK6H11VRDi5SsVMNGV3WieJ"),
	tezos.MustParseAddress("tz1VTySCY5mjSNqid8r8nW3qrfRDeq1H8hY4"),
	tezos.MustParseAddress("tz1VvZHeMfowvpuNKNDJNTjmPkX65sjmVEi2"),
	tezos.MustParseAddress("tz1VxiXUFjaEaEtpEEMLyNrrye1ec2Ecvyjf"),
	tezos.MustParseAddress("tz1VXyqagpu2vHeetRPzqgfZTBBWg1zr4DGu"),
	tezos.MustParseAddress("tz1VyDBWBGdw9ZCAH9sCeHEEEJQ8CSNAaTp4"),
	tezos.MustParseAddress("tz1VYEFCcH8q4jUjNc9n7E7XsYNTYCfJZf53"),
	tezos.MustParseAddress("tz1VYrztF15m2eqRwz71ecz6JQUXxZpsiAoX"),
	tezos.MustParseAddress("tz1VyvHvf9ntnErAAZfGZQXFKJGpqQySaGmX"),
	tezos.MustParseAddress("tz1W3d5YNRTjpSPzC1kaNsjVrw1GnQjPru8a"),
	tezos.MustParseAddress("tz1W6UGHY8qEgrEyVEvBNhkk1TM4PG6mTUxo"),
	tezos.MustParseAddress("tz1W7MVVM5vJdJ8yFuYgEN1kgqq4y9hDgEnF"),
	tezos.MustParseAddress("tz1WABk2JRE6vnRD568XfQnSoa9fjjka7az2"),
	tezos.MustParseAddress("tz1WBaiS5EvwsXUso7exVCN4oij1DGDKFbcZ"),
	tezos.MustParseAddress("tz1WBRjFFTUYWcHTUrxJyiMRQtxYBkLotm8s"),
	tezos.MustParseAddress("tz1WckFbzX6snEWTv8RhBMJiEe4DWGpS363k"),
	tezos.MustParseAddress("tz1Wco6WXheMQoUsXmBx1n5dxHZLE5TJNfj5"),
	tezos.MustParseAddress("tz1WctHsXmn2JVbASVvqWMWxmpSnCEkYfCvn"),
	tezos.MustParseAddress("tz1WeBQWYXa4HXoy2kUr8q1zJyUXQdkEoEgW"),
	tezos.MustParseAddress("tz1WgRxtFsoXvEfDXVtcGkT5fKpE5jTptEN4"),
	tezos.MustParseAddress("tz1WjNEe354sxJ9Qsbj1GymQVHLX1P5YitSE"),
	tezos.MustParseAddress("tz1WLJDszx9PdUkXwDm54oZQZtUEe7NZmwgD"),
	tezos.MustParseAddress("tz1WmraLRG7bjyhJL9pQSR7yW2jeG6nfffZr"),
	tezos.MustParseAddress("tz1WP1Ag63zcsmvzN6grdqNRey1So9TQaDRn"),
	tezos.MustParseAddress("tz1WqiYFHizF9LXbWQDjH6sgqwqbnCYaCzM9"),
	tezos.MustParseAddress("tz1WQj4nzKBgRUpTTxoSr7mddwH7DP3afYx7"),
	tezos.MustParseAddress("tz1Ws53oTb23dhKSrgZt7uoq4XxbNtWPZg2n"),
	tezos.MustParseAddress("tz1WSUic9ahahMZa7Tb8ym5jxmgPTkMzefuE"),
	tezos.MustParseAddress("tz1WsuPxT2TSjuo32FUqrwdVqeWPJqCXihso"),
	tezos.MustParseAddress("tz1WuMqpJQE3XBZQhpehLZCSyb52zzqcnhNU"),
	tezos.MustParseAddress("tz1WVJ1S1pa8EAKqPTPVXtXvtWeEhsska74x"),
	tezos.MustParseAddress("tz1Wy12nnHBBkRja7mXVGuA6G8q9pgDycd6d"),
	tezos.MustParseAddress("tz1WY7jBr7BR3MgybSCnjc8YNEBn5mbnUtfJ"),
	tezos.MustParseAddress("tz1WyiE6WwgNAQBTyVkhQNnaGE9Hm6oY2vvT"),
	tezos.MustParseAddress("tz1X1VtSroLRjE3FPzHqy7txhL8wd6vopVh6"),
	tezos.MustParseAddress("tz1X5pRkRx2JA3Uj6aDfdv3YM7n6pDnY4Wnp"),
	tezos.MustParseAddress("tz1X9mRvaue8LkhzVvTM8zCjBRVHyS7hDsiU"),
	tezos.MustParseAddress("tz1XBjHzzwVBGjiHDq2wr2mYuexrSLJ62Aa9"),
	tezos.MustParseAddress("tz1XbLYF5Bc2hJxppjzjVa4jjfeEefHCQM5X"),
	tezos.MustParseAddress("tz1XBRRvoCWDRgCHkrhEvUoprhPyuT3BnN74"),
	tezos.MustParseAddress("tz1XdSdkPGU2kMRFRp7GZCwPYTdPjgCuDzZv"),
	tezos.MustParseAddress("tz1XdUpiuh8hAGZkb7w9RXY2PJK3Ra9sB3VV"),
	tezos.MustParseAddress("tz1XfBC8JhPKTcbWTYeNcUeutWrFVPbHcFW5"),
	tezos.MustParseAddress("tz1XgPdSchXyqmvXMCddCzmWqDVZubLeWzVA"),
	tezos.MustParseAddress("tz1XmCsYfJu1fUKr4UZBvL1ipiiyN5gWKEox"),
	tezos.MustParseAddress("tz1XnuKMpEg7YwgHPo8QN7AAdjaJMAGbUftg"),
	tezos.MustParseAddress("tz1XnyMeWaGPTFDYsSo9ksffWYoQFJMQspfT"),
	tezos.MustParseAddress("tz1XpYwN7iX4SteatssaeA8BdcxPZJwBavKg"),
	tezos.MustParseAddress("tz1XQ371AWDsBzPM5iLYFjMZ88byvFhVK6zq"),
	tezos.MustParseAddress("tz1XQhjxQqJ2VTV7RAtEtkLYne5mmtkgzGGf"),
	tezos.MustParseAddress("tz1XRAFccwX1PrfFkWxTrttS9ke7SRD7HbZZ"),
	tezos.MustParseAddress("tz1XrkTP3ve26obErxNBBSczQwmC8FHu1jNP"),
	tezos.MustParseAddress("tz1XU91xvWE6Ut68TFEpqYcdoRB4NZ81PwEB"),
	tezos.MustParseAddress("tz1XWg37T4nYVGyNAToKucS1Kj7TeCZN92FX"),
	tezos.MustParseAddress("tz1Xxb52b8phTzR5zxvBDjipYENoskDa2YqT"),
	tezos.MustParseAddress("tz1XxSud1tRHtJS8RA8Hxt21UWo5qf6DZgug"),
	tezos.MustParseAddress("tz1Y1oAkABNQ9rajqirs9HrCwdx5XDZsDqVj"),
	tezos.MustParseAddress("tz1Y1tm9s5gVG3BgcwbArVcYUiPEs9MCg9md"),
	tezos.MustParseAddress("tz1Y3cfsvdc3UcxY8Pim6DmT9JahoPdTNc9k"),
	tezos.MustParseAddress("tz1Y3wAb9CDY2tKivo3ud5PaRjWMR2rsfXpq"),
	tezos.MustParseAddress("tz1Y46H43YgbpYSZTLQhW4niAAVVb8sw4kjR"),
	tezos.MustParseAddress("tz1Y7ktad7ReaEXzBa48KY1sqc5oAm512WHo"),
	tezos.MustParseAddress("tz1Ya5JVU9DGwt3DeQJ9SmKKPdAqFjJ4JBn4"),
	tezos.MustParseAddress("tz1YBB63zekBCr8NvmpyJC9m8GLB4Ttb2G4c"),
	tezos.MustParseAddress("tz1YBngprWZLQaMoYnPinXtVHNFL43ixBBw4"),
	tezos.MustParseAddress("tz1YCPRUpiucx2P1DzknSRYPWbG12JwC5y2P"),
	tezos.MustParseAddress("tz1YcRfmH4JsdvLzzijq1mh4FXjoHmh2ANEQ"),
	tezos.MustParseAddress("tz1YcRYg6Tdg5kdGn6swPVp6U9DDYNVC45q2"),
	tezos.MustParseAddress("tz1YDoQJUKmwaxGj5QeBkTG7kokT9t5iSncp"),
	tezos.MustParseAddress("tz1YDQRTsGPdB5F5PfLkciftn6VWijnFE8CZ"),
	tezos.MustParseAddress("tz1YerBjTTi82b2eb8dLxqqspcED8RMb96LL"),
	tezos.MustParseAddress("tz1YEvmhwjhHNdYDj211gJnHvbpWx5nbocPJ"),
	tezos.MustParseAddress("tz1YFhrXAhUfmUy8ZMuEQUL9Fh77mB52L4c1"),
	tezos.MustParseAddress("tz1Yk2f5Vq4xHNUTn6qjuF2uPs57evAtcvjG"),
	tezos.MustParseAddress("tz1YmxzfdUbVXurwcckJopDuWtNHK1D3SnnV"),
	tezos.MustParseAddress("tz1YQVSoW2fRMHUyNyts8dqCDnkemwY193uY"),
	tezos.MustParseAddress("tz1YryRwGgfZrA9Dcr4fpAf34tCsbtj8PSVo"),
	tezos.MustParseAddress("tz1YS9LY8Wsy2qBWCrHVjRQ7hoieDtt9bzzx"),
	tezos.MustParseAddress("tz1YT6C5FK4GLQZnYCHdD5gkuANcryxH1VX1"),
	tezos.MustParseAddress("tz1YtUhjZKhBAxLUC7YVxzgqHu9YdgWyGZN5"),
	tezos.MustParseAddress("tz1YUCYb47aNJqW58seBPFAkTuLeRnnxtWjC"),
	tezos.MustParseAddress("tz1YUPGMu4yB1XvfwgiYzRz2z35eC8tnyuZQ"),
	tezos.MustParseAddress("tz1YWd2Mx9qJSXN3SyJP6yMg2b27rdtVaJm1"),
	tezos.MustParseAddress("tz1YWJWjyx99Aiy5tdF74cJYmjDLbkehKxq6"),
	tezos.MustParseAddress("tz1YWRTzDaBU8xN9jNmr7yb285bgboDSNycV"),
	tezos.MustParseAddress("tz1Yx11JAZZCCBpoL95UAkHwMyFx9QeuX8f7"),
	tezos.MustParseAddress("tz1Yy3ZGorHh2eqv3fHyE8S86Y7KN1fRiscM"),
	tezos.MustParseAddress("tz1YZe3WMcU9Qrd3SAWmaBPMVMyBUDXe5AQc"),
	tezos.MustParseAddress("tz1YZuSR9KV1CyqjJAxBM39VYcgi8JCXTdQZ"),
	tezos.MustParseAddress("tz1Z1amVeVeVMuR9WrAEDGcNhe7HegAFnR2d"),
	tezos.MustParseAddress("tz1Z1mdVVzApiyWRAE12NQ8VgzSt7V2ZFFDp"),
	tezos.MustParseAddress("tz1Z1p8hbeMyhcoAwvNWGyGkdU1frcHQEUqn"),
	tezos.MustParseAddress("tz1Z5MbSaTxaHwQQN8JtH2dzBoJwEsrrG2hM"),
	tezos.MustParseAddress("tz1Z79nNUkdj8RpYoVUNkrjpY2KuHhtWes5D"),
	tezos.MustParseAddress("tz1Z8Y6AW24xZADCteXMzXsHUbfggQcfYoaU"),
	tezos.MustParseAddress("tz1ZaoFp2scgiGQo5BcuTTiubtXTWV3R6S81"),
	tezos.MustParseAddress("tz1ZD76PRJCcZTzmufAGnXezHV21G2sJothd"),
	tezos.MustParseAddress("tz1ZDn82ZJ2QcNkMuuGJN4HcjAsP1KLw5KYk"),
	tezos.MustParseAddress("tz1ZENDjBG3ZJoibTsBbgpWVKTNUjziLyjyQ"),
	tezos.MustParseAddress("tz1ZF3dWQghuEEprwdXGk2EoHtuYk6EZjWDS"),
	tezos.MustParseAddress("tz1ZfZxXCTRCBKPHUNqCgMkKh1s9wDw2GpuU"),
	tezos.MustParseAddress("tz1ZgXMewtKHcUzUqbSPFPFAm2yqUSDA4fsW"),
	tezos.MustParseAddress("tz1ZHn3VicRyHvuHmPHxs9o19AN2pxLyqAGU"),
	tezos.MustParseAddress("tz1Zmj7mV7Vms5k96uWZJb7meeE676AY5edK"),
	tezos.MustParseAddress("tz1ZowVoi76ochPmerwq26n9beKMfyJiBW4G"),
	tezos.MustParseAddress("tz1Zp8bwAUV25VLftnernQxy3dXpYVtjrpvo"),
	tezos.MustParseAddress("tz1ZpLYyD5eLKichQFqS5ExmyA3XvdoPFNMB"),
	tezos.MustParseAddress("tz1Zq3Vy4qJgZuRa2W3ZYsiQ3J6rsxAxk6KK"),
	tezos.MustParseAddress("tz1ZQFHYTd7iQDcfowvypwevSXGdeh3PWspj"),
	tezos.MustParseAddress("tz1ZqKkr1n4QswfPFMvXL6yJvAVPPkfr2ndm"),
	tezos.MustParseAddress("tz1ZRufn5DrKThjS34nu998VJtTi5QXtdUWH"),
	tezos.MustParseAddress("tz1ZtTEpJSVCyhJxZqRDHX25ToZmHxPunvdS"),
	tezos.MustParseAddress("tz1ZV4d6xzmWdiAM9puWqPRav6fL4w1kF6AU"),
	tezos.MustParseAddress("tz1ZW2LPDauTSKZu83gaYVecFkykwHJ55AvZ"),
	tezos.MustParseAddress("tz1ZXqXLurHc8Wd9oYcpURXdvG7vQY8sno5A"),
}

// v002 fixed an 'origination bug'
//
// Changelog published on Slack at 20-07-2018 15:45:31 (block 26,579 cycle 6)
// https://log.tezos.link/index.php?date=20-07-2018
//
// - Fixed a bug in delegations, where contracts could delegate to unregistered
//   delegates. This will be enforced from now on, and the existing unregistered
//   delegates will be automatically registered (except for two empty addresses).
//
// Note: this description is not fully correct (the correct answer is unknown) but
// it provides us with a sufficiently accurate estimate that overestimates the
// number of delegates, but is safe to move forward
//
// We usually register all delegates as soon as they send an op to include them into
// snapshots. In protocols that have params.HasOriginationBug set we do this as soon
// as the origination is sent to a non registered delegate. That's why here we
// re-register such delegates to update their grace period and set a proper delegate id
// which does not happen during origination on purpose. That way we can discern such
// delegates from correctly registered delegates by checking
//
//   IsDelegate == true && DelegateId == 0
//
func (b *Builder) FixOriginationBug(ctx context.Context, params *tezos.Params) error {
	// only run on mainnet
	if !params.IsMainnet() {
		return nil
	}

	var count int
	var err error

	for i, addr := range v002MagicDelegates {
		dlg, ok := b.AccountByAddress(addr)
		if !ok {
			dlg, err = b.idx.LookupAccount(ctx, addr)
			if err != nil {
				return fmt.Errorf("Upgrade v%03d: missing account %s", params.Version, addr)
			}
		}

		// skip properly registered bakers
		if dlg.DelegateId > 0 {
			continue
		}

		// activate magic bakers
		dlg.DelegateId = dlg.RowId
		b.RegisterDelegate(dlg, true)
		count++

		// inject an implicit baker registration
		b.AppendMagicBakerRegistrationOp(ctx, dlg, i)
	}
	log.Infof("Migrate v%03d: registered %d extra bakers", params.Version, count)

	// reset grace period for v001 magic bakers
	count = 0
	for _, addr := range v001MagicDelegates {
		dlg, ok := b.AccountByAddress(addr)
		if !ok {
			dlg, err = b.idx.LookupAccount(ctx, addr)
			if err != nil {
				return fmt.Errorf("Migrate v%03d: missing baker account %s", params.Version, addr)
			}
		}

		// bump grace period if smaller than cycle + preserved + 2
		// - tz1b3SaPHFSw51r92ARcV5mGyYbSSsdFd5Gz has 14 (stays at 14)
		// - tz1fahTqRiZ88aozjxt593aqEyGhXzPMPqp6 has 17 (reinit to 6 + 11)
		// - tz1UcuaXouNppYnbJr3JWGV31Fa2fnzesmJ4 has 17 (reinit to 6 + 11)
		if dlg.GracePeriod <= b.block.Cycle+b.block.Params.PreservedCycles+2 {
			dlg.InitGracePeriod(b.block.Cycle, b.block.Params)
			count++
		}
	}
	log.Infof("Migrate v%03d: updated %d extra bakers", params.Version, count)

	// unregister non-baker accounts
	drop := make([]*Account, 0)
	for _, dlg := range b.dlgMap {
		if dlg.DelegateId > 0 {
			continue
		}
		drop = append(drop, dlg)
	}

	// demote a few illegal bakers (with rolls & rights!!!) - you know ;)
	for _, ill := range v001IllegalDelegates {
		hash := b.accCache.AddressHashKey(ill)
		dlg, ok := b.dlgHashMap[hash]
		if !ok {
			continue
		}
		drop = append(drop, dlg)
	}

	for _, v := range drop {
		log.Debugf("Migrate v002: deregistering baker %s", v)
		b.UnregisterDelegate(v)
	}

	log.Infof("Migrate v%03d: dropped %d non-bakers", params.Version, len(drop))

	// get a list of all active delegates
	if b.validate {
		delegates, err := b.rpc.ListActiveDelegates(ctx, b.block.Height)
		if err != nil {
			return fmt.Errorf("listing delegates: %v", err)
		}
		missing := make(map[string]struct{})
		illegal := make(map[string]struct{})
		for _, v := range delegates {
			hash := b.accCache.AddressHashKey(v)
			if _, ok := b.dlgHashMap[hash]; !ok {
				missing[v.String()] = struct{}{}
			}
		}
		for _, v := range b.dlgMap {
			a := v.Address()
			var found bool
			for _, vv := range delegates {
				if vv.Equal(a) {
					found = true
					break
				}
			}
			if !found {
				illegal[a.String()] = struct{}{}
			}
		}
		log.Infof("Validated %d missing, %d illegal bakers", len(missing), len(illegal))
		for n, _ := range missing {
			log.Infof("Missing %s", n)
		}
		for n, _ := range illegal {
			log.Infof("Illegal %s", n)
		}
	}

	return nil
}

// v005 airdrops 1 mutez to unfunded manager accounts to avoid origination burn
func (b *Builder) RunBabylonAirdrop(ctx context.Context, params *tezos.Params) (int, error) {
	// collect all eligible addresses and inject airdrop flows
	table, err := b.idx.Table(index.AccountTableKey)
	if err != nil {
		return 0, err
	}

	// The rules are:
	// - process all originated accounts (KT1)
	// - if it has code and is spendable allocate the manager contract (implicit account)
	// - if it has code and is delegatble allocate the manager contract (implicit account)
	// - if it has no code (delegation KT1) allocate the manager contract (implicit account)
	// - (extra side condition) implicit account is not registered as delegate
	//
	// The above three cases are the cases where the manager contract (implicit account) is
	// able to interact through the KT1 that it manages. For example, if the originated
	// account has code but is neither spendable nor delegatable then the manager contract
	// cannot act on behalf of the originated contract.

	// find eligible KT1 contracts where we need to check the manager
	managers := make([]uint64, 0)
	contract := &Account{}
	err = pack.NewQuery("etl.addr.babylon_airdrop_eligible", table).
		AndEqual("address_type", tezos.AddressTypeContract).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(contract); err != nil {
				return err
			}
			// skip all excluded contracts that do not match the rules above
			if contract.IsContract {
				if !contract.IsSpendable && !contract.IsDelegatable {
					return nil
				}
			}
			if id := contract.CreatorId.Value(); id > 0 {
				managers = append(managers, id)
			}
			return nil
		})
	if err != nil {
		return 0, err
	}
	// log.Infof("Upgrade: found %d eligible managers", len(vec.Uint64.Unique(managers)))

	// find unfunded managers who are not reqistered as delegates
	var count int
	err = pack.NewQuery("etl.addr.babylon_airdrop", table).
		AndEqual("is_funded", false).
		AndEqual("is_delegate", false).
		AndIn("I", vec.UniqueUint64Slice(managers)). // make list unique
		Stream(ctx, func(r pack.Row) error {
			acc := AllocAccount()
			if err := r.Decode(acc); err != nil {
				acc.Free()
				return err
			}
			// airdrop 1 mutez
			if err := b.AppendAirdropOp(ctx, acc, 1, count); err != nil {
				return err
			}
			count++
			// log.Debugf("%04d airdrop: %s %f", count, acc, params.ConvertValue(1))
			// add account to builder map if not exist
			if _, ok := b.accMap[acc.RowId]; !ok {
				b.accMap[acc.RowId] = acc
			} else {
				acc.Free()
			}
			return nil
		})
	if err != nil {
		return 0, err
	}
	log.Infof("Upgrade to v%03d: executed %d airdrops", params.Version, count)
	return count, nil
}

func (b *Builder) RunBabylonUpgrade(ctx context.Context, params *tezos.Params, n int) error {
	// collect all eligible addresses and inject airdrop flows
	table, err := b.idx.Table(index.AccountTableKey)
	if err != nil {
		return err
	}
	// find eligible delegator KT1 accounts that are not yet contracts
	// Note: these are KT1 accounts distinct from the tz1/2/3 airdrop
	// accounts above
	var count int
	err = pack.NewQuery("etl.account.babylon_upgrade", table).
		WithoutCache().
		AndEqual("address_type", tezos.AddressTypeContract).
		AndEqual("is_contract", true).
		Stream(ctx, func(r pack.Row) error {
			acc := &Account{}
			if err := r.Decode(acc); err != nil {
				return err
			}
			// not all such KT1's are eligible for upgrade, just ones
			// that are either spendable or delegatable
			if !acc.NeedsBabylonUpgrade(params) {
				return nil
			}
			// upgrade note:
			// - updates account model
			// - adds new contracts with code to the contract table!
			// - does not touch existing smart contracts with code
			// - does not change parameters for existing operations
			// log.Debugf("upgrade: %s to smart contract", acc)
			acc.UpgradeToBabylon(params)
			acc.LastSeen = b.block.Height

			// add account to builder map, account index will write back to db
			b.accMap[acc.RowId] = acc

			// build manager.tz contract
			contract, err := NewManagerTzContract(acc, b.block.Height)
			if err != nil {
				return err
			}

			// add contract to builder map, contract index will insert to db
			b.conMap[acc.RowId] = contract

			// create migration op
			if err := b.AppendContractMigrationOp(ctx, acc, contract, n+count); err != nil {
				return err
			}
			count++
			return nil
		})
	if err != nil {
		return err
	}
	log.Infof("Upgrade to v%03d: migrated %d manager.tz delegators", params.Version, count)

	// find eligible smart KT1 contracts to upgrade
	var smart int
	acc := &Account{}
	err = pack.NewQuery("etl.contract.babylon_upgrade", table).
		WithoutCache().
		AndEqual("address_type", tezos.AddressTypeContract).
		AndEqual("is_contract", true).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(acc); err != nil {
				return err
			}
			con, err := b.LoadContractByAccountId(ctx, acc.RowId)
			if err != nil {
				return err
			}

			// not all such KT1's are eligible for upgrade, just ones
			// that are either spendable or delegatable
			if !con.NeedsBabylonUpgrade(params) {
				return nil
			}

			// upgrade note:
			// - updates account model
			// - adds new contracts with code to the contract table!
			// - does not touch existing smart contracts with code
			// - does not change parameters for existing operations
			// log.Debugf("upgrade: %s to smart contract", acc)
			if err := con.UpgradeToBabylon(params, acc); err != nil {
				return err
			}
			con.LastSeen = b.block.Height

			// add account to builder map, account index will write back to db
			acc.LastSeen = b.block.Height
			acc.IsDirty = true
			b.accMap[acc.RowId] = acc

			// create migration op
			if err := b.AppendContractMigrationOp(ctx, acc, con, n+count+smart); err != nil {
				return err
			}
			smart++
			return nil
		})
	if err != nil {
		return err
	}
	log.Infof("Upgrade to v%03d: migrated %d smart contracts", params.Version, smart)
	return nil
}

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
	if _, _, err := b.idx.LookupBigmap(ctx, id, false); err != nil {
		if err != index.ErrNoBigmapEntry {
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
