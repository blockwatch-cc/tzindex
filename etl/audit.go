// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"strconv"

	logpkg "github.com/echa/log"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) AuditState(ctx context.Context, offset int64) error {
	// skip audit at migration blocks (next protocol changes)
	if b.block.TZ.Block.IsProtocolUpgrade() {
		log.Info("Skipping audit at migration block since effects are not yet published in block receipts.")
		return nil
	}

	// every cycle check all stored accounts
	// only run full check when not in rollback mode
	if b.block.TZ.IsCycleStart() && offset == 0 {
		return b.AuditAccountDatabase(ctx, true)
		// } else {
		// 	return nil
	}

	block := rpc.BlockLevel(b.block.Height - offset)
	isCycleEnd := b.block.TZ.IsCycleEnd()

	plog := logpkg.NewProgressLogger(log).SetAction("Verified").SetEvent("account")

	logError := func(format string, args ...any) {
		log.Errorf("Audit %d "+format, append([]any{block}, args...)...)
	}
	logWarn := func(format string, args ...any) {
		log.Warnf("Audit %d "+format, append([]any{block}, args...)...)
	}

	// exclusivity
	for id := range b.bakerMap {
		if id == 0 {
			logWarn("baker %s with null id is_baker=%t in maps", b.bakerMap[id], b.bakerMap[id].Account.IsBaker)
		}
		if acc, ok := b.accMap[id]; ok {
			logWarn("baker %s [%d] is_baker=%t in multiple builder maps", acc, id, acc.IsBaker)
		}
	}
	for id := range b.accMap {
		if id == 0 {
			logWarn("normal account %s with null id is_baker=%t in maps", b.accMap[id], b.accMap[id].IsBaker)
		}
		if acc, ok := b.bakerMap[id]; ok {
			logWarn("normal account %s [%d] is_baker=%t in multiple builder maps", acc, id, acc.Account.IsBaker)
		}
	}

	// regular accounts
	var failed int
aloop:
	for _, acc := range b.accMap {
		// stop when cancelled
		select {
		case <-ctx.Done():
			break aloop
		default:
		}
		if !acc.IsDirty {
			continue
		}
		// skip non-activated accounts and rollups
		switch acc.Type {
		case tezos.AddressTypeBlinded, tezos.AddressTypeTxRollup, tezos.AddressTypeSmartRollup:
			continue aloop
		}

		// run a sanity check
		if acc.IsRevealed && acc.Type != tezos.AddressTypeContract {
			if !acc.Pubkey.IsValid() {
				logError("invalid pubkey: acc=%s key=%s", acc, acc.Pubkey.String())
			}
			if acc.Address != acc.Pubkey.Address() {
				logError("key mismatch: acc=%s type=%s bad-key=%s", acc, acc.Pubkey.Type, acc.Pubkey)
			}
		}
		// check balance against node rpc
		state, err := b.rpc.GetContract(ctx, acc.Address, block)
		if err != nil {
			// skip 404 errors on non-funded accounts (they may not exist,
			// but are indexed because they may have appeared in failed operations)
			if httpErr, ok := err.(rpc.HTTPStatus); !ok || httpErr.StatusCode() != 404 || acc.SpendableBalance > 0 {
				return fmt.Errorf("Audit: fetching balance for %s: %v", acc, err)
			}
			continue
		}
		// use vesting balance too, but without rollup bond
		if state.Balance != acc.SpendableBalance {
			logError("balance mismatch for %s: index=%d node=%d", acc, acc.SpendableBalance, state.Balance)
			failed++
		}
		plog.Log(1)
	}

	// baker accounts
	plog.Flush()
	plog.SetEvent("baker")
bloop:
	for _, bkr := range b.bakerMap {
		// stop when cancelled
		select {
		case <-ctx.Done():
			break bloop
		default:
		}
		if !bkr.IsDirty && !bkr.Account.IsDirty {
			continue
		}
		acc := bkr.Account

		// run sanity checks
		if !acc.IsBaker {
			logError("baker %s with missing baker flag", acc)
			continue
		}
		if acc.BakerId == 0 {
			if b.block.Params.Version < 2 {
				continue
			}
			logError("baker %s with zero baker id link", acc)
			failed++
		} else if acc.BakerId != acc.RowId {
			logError("baker %s with non-self baker id link", acc)
			failed++
		}
		if !acc.IsRevealed {
			logError("baker %s with unrevealed key", acc)
			failed++
		}
		if !acc.Pubkey.IsValid() {
			logError("invalid pubkey: acc=%s key=%s", acc, acc.Pubkey)
			failed++
		}
		// Note: consensus key may differ from address
		if acc.Address != acc.Pubkey.Address() {
			logError("baker account key mismatch: acc=%s type=%s bad-key=%s", acc, acc.Pubkey.Type, acc.Pubkey)
			failed++
		}
		// check balance against node rpc
		bal, err := b.rpc.GetContract(ctx, acc.Address, block)
		if err != nil {
			logWarn("fetching balance for %s: %v", acc, err)
			continue
		}
		// only use spendable balance here!
		if bal.Balance != acc.SpendableBalance {
			logError("baker balance mismatch for %s: index=%d node=%d", acc, acc.SpendableBalance, bal.Balance)
			failed++
		}
		// check baker status against node rpc
		state, err := b.rpc.GetBakerInfo(ctx, acc.Address, block)
		if err != nil && bkr.IsActive {
			logWarn("fetching baker state for %s: %v", acc, err)
			continue
		}
		// the indexer deactivates bakers at start of cycle, when we run this check at the
		// last block of a cycle where the node has already deactivated the baker, we
		// get a wrong result, just skip this check at cycle end
		if !isCycleEnd && bkr.IsActive && state.Deactivated {
			logError("baker active state mismatch for %s: index=%t node=%t", bkr, bkr.IsActive, !state.Deactivated)
			failed++
		}
		if state.GracePeriod != bkr.GracePeriod {
			logError("baker grace period mismatch for %s: index=%d node=%d", bkr, bkr.GracePeriod, state.GracePeriod)
			failed++
		}
		// node counts empty KT1 as delegators (!)
		if l := int64(len(state.DelegatedContracts)); l < bkr.ActiveDelegations {
			logError("baker delegation count mismatch for %s: index=%d node=%d (note: node still counts empty accounts as active delegators)", bkr, bkr.ActiveDelegations, l)
			failed++
		}
		// mix pre/post Ithaca values
		if f := state.FrozenBalance + state.CurrentFrozenDeposits; f != bkr.FrozenStake() {
			logError("baker frozen balance mismatch for %s: index=%d node=%d", bkr, bkr.FrozenStake(), f)
			failed++
		}
		if state.StakingBalance != bkr.StakingBalance() {
			logError("baker staking balance mismatch for %s: index=%d node=%d", bkr, bkr.StakingBalance(), state.StakingBalance)
			failed++
		}
		if state.DelegatedBalance != bkr.DelegatedBalance {
			logError("baker delegated balance mismatch for %s: index=%d node=%d", bkr, bkr.DelegatedBalance, state.DelegatedBalance)
			failed++
		}
		// state.FrozenDepositsLimit disappears in oxford
		if state.FrozenDepositsLimit > 0 && bkr.DepositsLimit > -1 {
			if state.FrozenDepositsLimit != bkr.DepositsLimit {
				logError("baker deposits limit mismatch for %s: index=%d node=%d", bkr, bkr.DepositsLimit, state.FrozenDepositsLimit)
				failed++
			}
		}

		// TODO: oxford check
		// state.TotalDelegatedStake

		plog.Log(1)
	}
	plog.Flush()

	if failed > 0 {
		return fmt.Errorf("Account audit failed")
	}
	return nil
}

func (b *Builder) AuditAccountDatabase(ctx context.Context, nofail bool) error {
	block := rpc.BlockLevel(b.block.Height)
	log.Infof("Auditing account database with %d bakers and %d total accounts at cycle %d block %d",
		len(b.bakerMap), b.block.Chain.TotalAccounts, b.block.Cycle, block)

	var count, failed int
	plog := logpkg.NewProgressLogger(log).SetAction("Verified").SetEvent("baker")

	logError := func(format string, args ...any) {
		log.Errorf("Audit %d "+format, append([]any{block}, args...)...)
	}
	logWarn := func(format string, args ...any) {
		log.Warnf("Audit %d "+format, append([]any{block}, args...)...)
	}

	// baker accounts
bloop:
	for _, bkr := range b.bakerMap {
		// stop when cancelled
		select {
		case <-ctx.Done():
			break bloop
		default:
		}

		acc := bkr.Account
		// run sanity checks
		if !acc.IsBaker {
			logError("baker %s with missing baker flag", acc)
			failed++
			continue
		}
		if acc.BakerId == 0 {
			if b.block.Params.Version < 2 {
				continue
			}
			logError("baker %s with zero baker id link", acc)
			failed++
		} else if acc.BakerId != acc.RowId {
			logError("baker %s with non-self baker id link", acc)
			failed++
		}
		if !acc.IsRevealed {
			logError("baker %s with unrevealed key", acc)
			failed++
		}
		if !acc.Pubkey.IsValid() {
			logError("invalid baker pubkey: acc=%s key=%s", acc, acc.Pubkey)
			failed++
		}
		if acc.Address != acc.Pubkey.Address() {
			logError("baker key mismatch: acc=%s type=%s bad-key=%s", acc, acc.Pubkey.Type, acc.Pubkey)
			failed++
		}
		// check balance against node rpc
		bal, err := b.rpc.GetContract(ctx, acc.Address, rpc.BlockLevel(b.block.Height))
		if err != nil {
			logError("fetching balance for %s: %v", acc, err)
			failed++
			continue
		}
		// only use spendable balance here!
		if bal.Balance != acc.SpendableBalance {
			logError("baker balance mismatch for %s: index=%d node=%d", acc, acc.SpendableBalance, bal.Balance)
			failed++
		}
		// check baker status against node rpc
		state, err := b.rpc.GetBakerInfo(ctx, acc.Address, block)
		if err != nil {
			if bkr.IsActive {
				logError("fetching baker state for %s: %v", acc, err)
				failed++
			} else {
				logWarn("fetching baker state for %s: %v", acc, err)
			}
			continue
		}
		if bkr.IsActive != !state.Deactivated {
			logError("baker active state mismatch for %s: index=%t node=%t", bkr, bkr.IsActive, !state.Deactivated)
			failed++
		}
		if state.GracePeriod != bkr.GracePeriod {
			logError("baker grace period mismatch for %s: index=%d node=%d", bkr, bkr.GracePeriod, state.GracePeriod)
			failed++
		}
		// node counts empty KT1 as delegators (!)
		if l := int64(len(state.DelegatedContracts)); l < bkr.ActiveDelegations {
			logError("baker delegation count mismatch for %s: index=%d node=%d (note: node still counts empty accounts as active delegators)", bkr, bkr.ActiveDelegations, l)
			failed++
		}
		// mix pre/post Ithaca values
		if f := state.FrozenBalance + state.CurrentFrozenDeposits; f != bkr.FrozenStake() {
			logError("baker frozen balance mismatch for %s: index=%d node=%d", bkr, bkr.FrozenStake(), f)
			failed++
		}
		if state.StakingBalance != bkr.StakingBalance() {
			logError("baker staking balance mismatch for %s: index=%d node=%d", bkr, bkr.StakingBalance(), state.StakingBalance)
			failed++
		}
		if state.DelegatedBalance != bkr.DelegatedBalance {
			logError("baker delegated balance mismatch for %s: index=%d node=%d", bkr, bkr.DelegatedBalance, state.DelegatedBalance)
			failed++
		}
		if bkr.DepositsLimit > -1 {
			if state.FrozenDepositsLimit != bkr.DepositsLimit {
				logError("baker deposits limit mismatch for %s: index=%d node=%d", bkr, bkr.DepositsLimit, state.FrozenDepositsLimit)
				failed++
			}
		}
		count++

		pct := float64(count) * 100 / float64(len(b.bakerMap))
		plog.Log(1, strconv.FormatFloat(pct, 'f', 2, 64)+"%% complete")
	}

	// all accounts
	plog.Flush()
	plog.SetEvent("account")
	count = 0

	// Note: we're at the end of a block here and the account database has not been updated
	seen := make(map[uint64]*model.Account)
	table, _ := b.idx.Table(model.AccountTableKey)
	err := pack.NewQuery("audit.accounts").
		WithTable(table).
		WithoutCache().
		WithFields("I", "H", "t", "k", "s").
		Stream(ctx, func(r pack.Row) error {
			acc := model.AllocAccount()
			if err := r.Decode(acc); err != nil {
				return err
			}
			// skip non-activated accounts
			if acc.Type == tezos.AddressTypeBlinded {
				acc.Free()
				return nil
			}
			// check duplicates in DB
			hash := b.accCache.AccountHashKey(acc)
			if n, ok := seen[hash]; ok && acc.Address == n.Address {
				log.Errorf("Duplicate account %s in database at id %d", acc.Address, acc.RowId)
			} else {
				seen[hash] = acc
			}
			return nil
		})
	if err != nil {
		return err
	}

	total := float64(len(seen))
aloop:
	for _, acc := range seen {
		// stop when cancelled
		select {
		case <-ctx.Done():
			break aloop
		default:
		}

		// skip non-activated accounts and rollups
		var skip bool
		switch acc.Type {
		case tezos.AddressTypeBlinded, tezos.AddressTypeTxRollup, tezos.AddressTypeSmartRollup:
			skip = true
		}
		if skip {
			continue
		}

		// key and balance to check
		// without frozen rollup bond!
		key := acc.Pubkey
		indexedBalance := acc.SpendableBalance

		// for dirty accounts, use the builder value because the block update
		// has not been written yet
		if dacc, ok := b.AccountById(acc.RowId); ok && dacc.IsDirty {
			key = dacc.Pubkey
			// without frozen rollup bond!
			indexedBalance = dacc.SpendableBalance
		}

		// check key matches address
		if acc.IsRevealed && acc.Type != tezos.AddressTypeContract {
			if key.IsValid() && acc.Address != key.Address() {
				if nofail {
					log.Errorf("pubkey mismatch: acc=%s type=%s bad-key=%s", acc.Address, key.Type, key)
					failed++
				} else {
					return fmt.Errorf("pubkey mismatch: acc=%s type=%s bad-key=%s", acc.Address, key.Type, key)
				}
			}
		}

		// check balance against node rpc
		state, err := b.rpc.GetContract(ctx, acc.Address, block)
		if err != nil {
			// skip 404 errors on non-funded accounts (they may not exist,
			// but are indexed because they may have appeared in failed operations)
			if httpErr, ok := err.(rpc.HTTPStatus); !ok || httpErr.StatusCode() != 404 || acc.SpendableBalance > 0 {
				if nofail {
					log.Errorf("fetching balance for %s: %s", acc.Address, err)
					failed++
				} else {
					return fmt.Errorf("fetching balance for %s: %v", acc.Address, err)
				}
			}
		} else if state.Balance != indexedBalance {
			if nofail {
				log.Errorf("balance mismatch for %s: index=%d node=%d", acc.Address, acc.SpendableBalance, state.Balance)
				failed++
			} else {
				return fmt.Errorf("balance mismatch for %s: index=%d node=%d", acc.Address, acc.SpendableBalance, state.Balance)
			}
		}

		// free and remove from seen map
		acc.Free()
		delete(seen, b.accCache.AccountHashKey(acc))

		count++
		pct := float64(count) * 100 / total
		plog.Log(1, strconv.FormatFloat(pct, 'f', 2, 64)+"%% complete")

	}
	stats := table.Stats()[0]
	cstats := b.accCache.Stats()
	log.Infof("%d accounts, %d cached (%s), %d packs, %d keys revealed, %d checks failed",
		stats.TupleCount, cstats.Size, util.ByteSize(cstats.Bytes), stats.PacksCount, count, failed)
	if failed > 0 {
		return fmt.Errorf("Account database check failed")
	}
	plog.Flush()
	return nil
}

func (b *Builder) DumpState() {
	// log.Infof("Builder dump at block %d", b.block.Height)
	// for n, v := range b.accHashMap { // map[uint64]*Account
	// 	log.Infof("HashMap %x -> %s %s %t", n, v, v.Key(), v.IsRevealed)
	// }
	// for n, v := range b.accMap { //   map[AccountID]*Account
	// 	log.Infof("AccMap %d -> %s %s %t", n, v, v.Key(), v.IsRevealed)
	// }
	// for n, v := range b.dlgHashMap { // map[uint64]*Account
	// 	log.Infof("DlgHashMap %x -> %s %s %t", n, v, v.Key(), v.IsRevealed)
	// }
	// for n, v := range b.dlgMap { //    map[AccountID]*Account
	// 	log.Infof("DlgMap %d -> %s %s %t", n, v, v.Key(), v.IsRevealed)
	// }
	// for n, v := range b.conMap { //   map[AccountID]*Contract
	// 	log.Infof("ContractMap %d -> %s", n, v)
	// }
	// if f, err := os.OpenFile("cache-dump.json", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644); err == nil {
	// 	enc := json.NewEncoder(f)
	// 	err := b.accCache.Walk(func(key uint64, acc *model.Account) error {
	// 		if err := enc.Encode(acc); err != nil {
	// 			return err
	// 		}
	// 		f.Write([]byte{','})
	// 		f.Write([]byte{'\n'})
	// 		return nil
	// 	})
	// 	f.Close()
	// 	if err != nil {
	// 		log.Errorf("Cache dump error: %s", err)
	// 	}
	// } else {
	// 	log.Errorf("Cache dump failed: %s", err)
	// }
}
