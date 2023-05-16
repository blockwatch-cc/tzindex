// Copyright (c) 2020-2022 Blockwatch Data Inc.
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

	isCycleEnd := b.block.TZ.IsCycleEnd()

	plog := logpkg.NewProgressLogger(log).SetAction("Verified").SetEvent("account")

	// exclusivity
	for id := range b.bakerMap {
		if id == 0 {
			log.Warnf("Audit: Baker account %s with null id is_baker=%t in maps", b.bakerMap[id], b.bakerMap[id].Account.IsBaker)
		}
		if acc, ok := b.accMap[id]; ok {
			log.Warnf("Audit: Baker account %s [%d] is_baker=%t in multiple builder maps", acc, id, acc.IsBaker)
		}
	}
	for id := range b.accMap {
		if id == 0 {
			log.Warnf("Audit: Normal account %s with null id is_baker=%t in maps", b.accMap[id], b.accMap[id].IsBaker)
		}
		if acc, ok := b.bakerMap[id]; ok {
			log.Warnf("Audit: Normal account %s [%d] is_baker=%t in multiple builder maps", acc, id, acc.Account.IsBaker)
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
		var skip bool
		switch acc.Type {
		case tezos.AddressTypeBlinded, tezos.AddressTypeTxRollup, tezos.AddressTypeSmartRollup:
			skip = true
		}
		if skip {
			continue
		}
		// run a sanity check
		if acc.IsRevealed && acc.Type != tezos.AddressTypeContract {
			if !acc.Pubkey.IsValid() {
				log.Errorf("Audit: invalid pubkey: acc=%s key=%s", acc, acc.Pubkey.String())
			}
			if acc.Address != acc.Pubkey.Address() {
				log.Errorf("Audit: key mismatch: acc=%s type=%s bad-key=%s", acc, acc.Pubkey.Type, acc.Pubkey)
			}
		}
		// check balance against node rpc
		state, err := b.rpc.GetContract(ctx, acc.Address, rpc.BlockLevel(b.block.Height-offset))
		if err != nil {
			// skip 404 errors on non-funded accounts (they may not exist,
			// but are indexed because they may have appeared in failed operations)
			if httpErr, ok := err.(rpc.HTTPStatus); !ok || httpErr.StatusCode() != 404 || acc.SpendableBalance > 0 {
				return fmt.Errorf("Audit: fetching balance for %s: %v", acc, err)
			}
			continue
		}
		// use vesting balance too
		if state.Balance != acc.SpendableBalance {
			log.Errorf("Audit: balance mismatch for %s: index=%d node=%d", acc, acc.SpendableBalance, state.Balance)
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
			log.Errorf("Audit: baker %s with missing baker flag", acc)
			continue
		}
		if acc.BakerId == 0 {
			if b.block.Params.Version < 2 {
				continue
			}
			return fmt.Errorf("Audit: baker %s with zero baker id link", acc)
		} else if acc.BakerId != acc.RowId {
			return fmt.Errorf("Audit: baker %s with non-self baker id link", acc)
		}
		if !acc.IsRevealed {
			log.Errorf("Audit: baker %s with unrevealed key", acc)
			continue
		}
		if !acc.Pubkey.IsValid() {
			log.Errorf("Audit: invalid pubkey: acc=%s key=%s", acc, acc.Pubkey)
		}
		if acc.Address != acc.Pubkey.Address() {
			log.Errorf("Audit: baker key mismatch: acc=%s type=%s bad-key=%s", acc, acc.Pubkey.Type, acc.Pubkey)
		}
		// check balance against node rpc
		bal, err := b.rpc.GetContract(ctx, acc.Address, rpc.BlockLevel(b.block.Height-offset))
		if err != nil {
			return fmt.Errorf("Audit: fetching balance for %s: %v", acc, err)
		}
		// only use spendable balance here!
		if bal.Balance != acc.SpendableBalance {
			log.Errorf("Audit: baker balance mismatch for %s: index=%d node=%d", acc, acc.SpendableBalance, bal.Balance)
			failed++
		}
		// check baker status against node rpc
		state, err := b.rpc.GetDelegate(ctx, acc.Address, rpc.BlockLevel(b.block.Height-offset))
		if err != nil {
			return fmt.Errorf("Audit: fetching baker state for %s: %v", acc, err)
		}
		// the indexer deactivates bakers at start of cycle, when we run this check at the
		// last block of a cycle where the node has already deactivated the baker, we
		// get a wrong result, just skip this check at cycle end
		if !isCycleEnd && bkr.IsActive && state.Deactivated {
			log.Errorf("Audit: baker active state mismatch for %s: index=%t node=%t", bkr, bkr.IsActive, !state.Deactivated)
			failed++
		}
		if state.GracePeriod != bkr.GracePeriod {
			log.Warnf("Audit: baker grace period mismatch for %s: index=%d node=%d", bkr, bkr.GracePeriod, state.GracePeriod)
		}
		// node counts empty KT1 as delegators (!)
		if l := int64(len(state.DelegatedContracts)); l < bkr.ActiveDelegations {
			log.Errorf("Audit: baker delegation count mismatch for %s: index=%d node=%d (note: node still counts empty accounts as active delegators)", bkr, bkr.ActiveDelegations, l)
			failed++
		}
		// mix pre/post Ithaca values
		if f := state.FrozenBalance + state.FrozenDeposits; f != bkr.FrozenBalance() {
			log.Errorf("Audit: baker frozen balance mismatch for %s: index=%d node=%d", bkr, bkr.FrozenBalance(), f)
			failed++
		}
		if state.StakingBalance != bkr.StakingBalance() {
			log.Errorf("Audit: baker staking balance mismatch for %s: index=%d node=%d", bkr, bkr.StakingBalance(), state.StakingBalance)
			failed++
		}
		if state.DelegatedBalance != bkr.DelegatedBalance {
			log.Errorf("Audit: baker delegated balance mismatch for %s: index=%d node=%d", bkr, bkr.DelegatedBalance, state.DelegatedBalance)
			failed++
		}
		if bkr.DepositsLimit > -1 {
			if state.FrozenDepositsLimit != bkr.DepositsLimit {
				log.Errorf("Audit: baker deposits limit mismatch for %s: index=%d node=%d", bkr, bkr.DepositsLimit, state.FrozenDepositsLimit)
				failed++
			}
		}

		plog.Log(1)
	}

	// every cycle check all stored accounts
	if !b.block.TZ.IsCycleStart() {
		if failed > 0 {
			return fmt.Errorf("Account audit failed")
		}
		return nil
	}

	// only run full check when not in rollback mode
	if offset == 0 {
		if err := b.AuditAccountDatabase(ctx, true); err != nil {
			return err
		}
	}

	if failed > 0 {
		return fmt.Errorf("Account audit failed")
	}
	plog.Flush()
	return nil
}

func (b *Builder) AuditAccountDatabase(ctx context.Context, nofail bool) error {
	log.Infof("Auditing account database with %d bakers and %d total accounts at cycle %d block %d",
		len(b.bakerMap), b.block.Chain.TotalAccounts, b.block.Cycle, b.block.Height)

	var count, failed int
	plog := logpkg.NewProgressLogger(log).SetAction("Verified").SetEvent("baker")

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
			log.Errorf("Audit: baker %s with missing baker flag", acc)
			failed++
			continue
		}
		if acc.BakerId == 0 {
			if b.block.Params.Version < 2 {
				continue
			}
			log.Errorf("Audit: baker %s with zero baker id link", acc)
			failed++
		} else if acc.BakerId != acc.RowId {
			log.Errorf("Audit: baker %s with non-self baker id link", acc)
			failed++
		}
		if !acc.IsRevealed {
			log.Errorf("Audit: baker %s with unrevealed key", acc)
			failed++
		}
		if !acc.Pubkey.IsValid() {
			log.Errorf("Audit: invalid baker pubkey: acc=%s key=%s", acc, acc.Pubkey)
			failed++
		}
		if acc.Address != acc.Pubkey.Address() {
			log.Errorf("Audit: baker key mismatch: acc=%s type=%s bad-key=%s", acc, acc.Pubkey.Type, acc.Pubkey)
			failed++
		}
		// check balance against node rpc
		bal, err := b.rpc.GetContract(ctx, acc.Address, rpc.BlockLevel(b.block.Height))
		if err != nil {
			log.Errorf("Audit: fetching balance for %s: %v", acc, err)
			failed++
			continue
		}
		// only use spendable balance here!
		if bal.Balance != acc.SpendableBalance {
			log.Errorf("Audit: baker balance mismatch for %s: index=%d node=%d", acc, acc.SpendableBalance, bal.Balance)
			failed++
		}
		// check baker status against node rpc
		state, err := b.rpc.GetDelegate(ctx, acc.Address, rpc.BlockLevel(b.block.Height))
		if err != nil {
			log.Errorf("Audit: fetching baker state for %s: %v", acc, err)
			continue
		}
		if bkr.IsActive && state.Deactivated {
			log.Errorf("Audit: baker active state mismatch for %s: index=%t node=%t", bkr, bkr.IsActive, !state.Deactivated)
			failed++
		}
		if state.GracePeriod != bkr.GracePeriod {
			log.Warnf("Audit: baker grace period mismatch for %s: index=%d node=%d", bkr, bkr.GracePeriod, state.GracePeriod)
		}
		// node counts empty KT1 as delegators (!)
		if l := int64(len(state.DelegatedContracts)); l < bkr.ActiveDelegations {
			log.Errorf("Audit: baker delegation count mismatch for %s: index=%d node=%d (note: node still counts empty accounts as active delegators)", bkr, bkr.ActiveDelegations, l)
			failed++
		}
		// mix pre/post Ithaca values
		if f := state.FrozenBalance + state.FrozenDeposits; f != bkr.FrozenBalance() {
			log.Errorf("Audit: baker frozen balance mismatch for %s: index=%d node=%d", bkr, bkr.FrozenBalance(), f)
			failed++
		}
		if state.StakingBalance != bkr.StakingBalance() {
			log.Errorf("Audit: baker staking balance mismatch for %s: index=%d node=%d", bkr, bkr.StakingBalance(), state.StakingBalance)
			failed++
		}
		if state.DelegatedBalance != bkr.DelegatedBalance {
			log.Errorf("Audit: baker delegated balance mismatch for %s: index=%d node=%d", bkr, bkr.DelegatedBalance, state.DelegatedBalance)
			failed++
		}
		if bkr.DepositsLimit > -1 {
			if state.FrozenDepositsLimit != bkr.DepositsLimit {
				log.Errorf("Audit: baker deposits limit mismatch for %s: index=%d node=%d", bkr, bkr.DepositsLimit, state.FrozenDepositsLimit)
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
		key := acc.Pubkey
		indexedBalance := acc.Balance()

		// for dirty accounts, use the builder value because the block update
		// has not been written yet
		if dacc, ok := b.AccountById(acc.RowId); ok && dacc.IsDirty {
			key = dacc.Pubkey
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
		state, err := b.rpc.GetContract(ctx, acc.Address, rpc.BlockLevel(b.block.Height))
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
