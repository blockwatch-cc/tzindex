// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	logpkg "github.com/echa/log"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) CheckState(ctx context.Context) error {
	plog := logpkg.NewProgressLogger(log).SetAction("Verified").SetEvent("account")

	// exclusivity
	for id := range b.dlgMap {
		if id == 0 {
			log.Warnf("Baker account %s with null id is_baker=%t in maps", b.dlgMap[id], b.dlgMap[id].IsDelegate)
		}
		if acc, ok := b.accMap[id]; ok {
			log.Warnf("Baker account %s [%d] is_baker=%t in multiple builder maps", acc, id, acc.IsDelegate)
		}
	}
	for id := range b.accMap {
		if id == 0 {
			log.Warnf("Normal account %s with null id is_baker=%t in maps", b.accMap[id], b.accMap[id].IsDelegate)
		}
		if acc, ok := b.dlgMap[id]; ok {
			log.Warnf("Normal account %s [%d] is_baker=%t in multiple builder maps", acc, id, acc.IsDelegate)
		}
	}

	// regular accounts
	var failed int
	for _, acc := range b.accMap {
		if !acc.IsDirty {
			continue
		}
		// skip non-activated accounts
		if acc.Type == chain.AddressTypeBlinded {
			return nil
		}
		// run a sanity check
		if acc.IsRevealed && acc.Type != chain.AddressTypeContract {
			if have, want := len(acc.PubkeyHash), acc.PubkeyType.Len(); want != have {
				log.Errorf("key type check failed: acc=%s type=%s want-len=%d have-len=%d",
					acc, acc.PubkeyType.String(), want, have)
			}
			key := chain.NewKey(acc.PubkeyType.KeyType(), acc.PubkeyHash)
			addr := acc.Address()
			if !addr.IsEqual(key.Address()) {
				log.Errorf("key mismatch: acc=%s type=%s bad-key=%s", addr, key.Type, key)
			}
		}
		// check balance against node rpc
		addr := acc.Address()
		bal, err := b.rpc.GetContractBalanceHeight(ctx, addr, b.block.Height)
		if err != nil {
			// skip 404 errors on non-funded accounts (they may not exist,
			// but are indexed because they may have appeared in failed operations)
			if httpErr, ok := err.(rpc.HTTPStatus); !ok || httpErr.StatusCode() != 404 || acc.SpendableBalance > 0 {
				return fmt.Errorf("fetching balance for %s: %v", addr, err)
			}
		}
		// use vesting balance too
		if bal != acc.Balance() {
			log.Errorf("balance mismatch for %s: index=%d node=%d", addr, acc.Balance(), bal)
			failed++
		}
		plog.Log(1)
	}

	// delegate accounts
	for _, acc := range b.dlgMap {
		if !acc.IsDirty {
			continue
		}
		// skip non-activated accounts
		if acc.Type == chain.AddressTypeBlinded {
			return nil
		}
		// run a sanity check
		if !acc.IsRevealed {
			log.Errorf("baker %s with unrevealed key", acc)
		}
		if have, want := len(acc.PubkeyHash), acc.PubkeyType.KeyType().Len(); want != have {
			log.Errorf("baker key type check failed: acc=%s type=%s want-len=%d have-len=%d",
				acc, acc.PubkeyType.KeyType(), want, have)
		}
		key := chain.NewKey(acc.PubkeyType.KeyType(), acc.PubkeyHash)
		addr := acc.Address()
		if !addr.IsEqual(key.Address()) {
			log.Errorf("baker key mismatch: acc=%s type=%s bad-key=%s", addr, key.Type, key)
		}
		// check balance against node rpc
		bal, err := b.rpc.GetContractBalanceHeight(ctx, addr, b.block.Height)
		if err != nil {
			return fmt.Errorf("fetching balance for %s: %v", addr, err)
		}
		// only use spendable balance here!
		if bal != acc.SpendableBalance {
			log.Errorf("balance mismatch for %s: index=%d node=%d", addr, acc.SpendableBalance, bal)
			failed++
		}
		plog.Log(1)
	}

	// every cycle check all stored accounts
	if !b.block.Params.IsCycleStart(b.block.Height) {
		if failed > 0 {
			return fmt.Errorf("Account balance check failed")
		}
		return nil
	}

	log.Infof("Checking account database at cycle %d block %d", b.block.Cycle, b.block.Height)
	if err := b.CheckAccountDatabase(ctx, true); err != nil {
		return err
	}

	if failed > 0 {
		return fmt.Errorf("Account balance check failed")
	}
	return nil
}

func (b *Builder) CheckAccountDatabase(ctx context.Context, nofail bool) error {
	plog := logpkg.NewProgressLogger(log).SetAction("Verified").SetEvent("account")
	// Note: we're at the end of a block here, but the database has not been
	// updated yet. That's why we must exclude all dirty accounts
	table, _ := b.idx.Table(index.AccountTableKey)
	acc := &model.Account{}
	seen := make(map[uint64]chain.Address)
	var count, failed int
	err := table.Stream(ctx, pack.Query{
		Name:    "validate.accounts",
		NoCache: true,
		Fields:  table.Fields().Select("I", "H", "t", "k", "s"),
	}, func(r pack.Row) error {
		// stop when cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := r.Decode(acc); err != nil {
			return err
		}
		// skip non-activated accounts
		if acc.Type == chain.AddressTypeBlinded {
			return nil
		}

		// check duplicates in DB
		hash := accountHashKey(acc)
		addr := acc.Address()
		if n, ok := seen[hash]; ok && addr.IsEqual(n) {
			log.Errorf("Duplicate account %s in database at id %d", addr, acc.RowId)
		}

		// key and balance to check
		key := chain.NewKey(acc.PubkeyType.KeyType(), acc.PubkeyHash)
		indexedBalance := acc.Balance()
		if acc.IsActiveDelegate {
			indexedBalance = acc.SpendableBalance
		}

		// for dirty accounts, use the builder value because the block update
		// has not been written yet
		if dacc, ok := b.AccountById(acc.RowId); ok && dacc.IsDirty {
			key = chain.NewKey(acc.PubkeyType.KeyType(), acc.PubkeyHash)
			if dacc.IsActiveDelegate {
				indexedBalance = dacc.SpendableBalance
			} else {
				indexedBalance = dacc.Balance()
			}
		}

		// check key matches address
		if acc.IsRevealed && acc.Type != chain.AddressTypeContract {
			if key.IsValid() && !addr.IsEqual(key.Address()) {
				if nofail {
					log.Errorf("pubkey mismatch: acc=%s type=%s bad-key=%s", addr, key.Type, key)
					failed++
				} else {
					return fmt.Errorf("pubkey mismatch: acc=%s type=%s bad-key=%s", addr, key.Type, key)
				}
			}
		}

		// check balance against node rpc
		bal, err := b.rpc.GetContractBalanceHeight(ctx, addr, b.block.Height)
		if err != nil {
			// skip 404 errors on non-funded accounts (they may not exist,
			// but are indexed because they may have appeared in failed operations)
			if httpErr, ok := err.(rpc.HTTPStatus); !ok || httpErr.StatusCode() != 404 || acc.SpendableBalance > 0 {
				if nofail {
					log.Errorf("fetching balance for %s: %v", addr, err)
					failed++
				} else {
					return fmt.Errorf("fetching balance for %s: %v", addr, err)
				}
			}
		}
		if bal != indexedBalance {
			if nofail {
				log.Errorf("balance mismatch for %s: index=%d node=%d", addr, acc.SpendableBalance, bal)
				failed++
			} else {
				return fmt.Errorf("balance mismatch for %s: index=%d node=%d", addr, acc.SpendableBalance, bal)
			}
		}

		count++
		plog.Log(1)
		return nil
	})
	if err != nil {
		return err
	}
	stats := table.Stats()
	log.Infof("%d accounts, %d packs, %d keys revealed, %d checks failed",
		stats.TupleCount, stats.PacksCount, count, failed)
	if failed > 0 {
		return fmt.Errorf("Account database check failed")
	}
	return nil
}
