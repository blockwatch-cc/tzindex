// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) BuildGenesisBlock(ctx context.Context) (*model.Block, error) {
	gen := b.block.TZ.Block.Header.Content.Parameters
	if gen == nil {
		return nil, fmt.Errorf("missing genesis protocol_parameters")
	}
	log.Info("Building genesis dataset.")

	// register new protocol (will save as new deployment)
	b.block.Params.StartHeight = b.block.Height
	if err := b.idx.ConnectProtocol(ctx, b.block.Params, b.parent.Params); err != nil {
		return nil, err
	}

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
		acc := model.NewAccount(v.Addr)
		acc.RowId = model.AccountID(len(accounts) + 1)
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.SpendableBalance = v.Value
		acc.IsFunded = true
		acc.IsActivated = true
		acc.IsDirty = true

		// revealed accounts are registered as active bakers (i.e. foundation bakers)
		if v.Key.IsValid() {
			acc.IsRevealed = true
			acc.Pubkey = v.Key
			acc.IsBaker = true
			acc.BakerId = acc.RowId
			bkr := b.RegisterBaker(acc, true)
			if err := b.AppendMagicBakerRegistrationOp(ctx, bkr, i); err != nil {
				return nil, err
			}

			// update supply counters
			b.block.Supply.ActiveStaking += v.Value

			log.Debugf("1 BOOT REG SELF %d %s -> %d bal=%d",
				acc.RowId, acc, bkr.ActiveDelegations, acc.Balance())
		}

		// update block counters
		b.block.FundedAccounts++
		b.block.NewAccounts++
		b.block.SeenAccounts++
		b.block.NEvents++
		b.block.ActivatedSupply += v.Value
		b.block.MintedSupply += v.Value
		b.block.Supply.Activated += v.Value

		// register activation flows (will not be applied, just saved!)
		id := model.OpRef{
			Kind: model.OpTypeActivation,
			N:    opCounter,
			L:    model.OPL_PROTOCOL_UPGRADE,
			P:    flowCounter,
		}
		f := model.NewFlow(b.block, acc, acc, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeActivation
		f.AmountIn = acc.SpendableBalance
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// register implicit activation ops
		op := model.NewEventOp(b.block, acc.RowId, id)
		op.SenderId = acc.RowId
		op.Counter = int64(opCounter)
		op.Volume = acc.SpendableBalance
		b.block.Ops = append(b.block.Ops, op)
		opCounter++

		// prepare for insert
		accounts = append(accounts, acc)

		log.Debug(newLogClosure(func() string {
			var sfx string
			if acc.IsActivated {
				sfx += " [activated]"
			}
			if acc.IsBaker {
				sfx += " [baker]"
			}
			if acc.IsRevealed {
				sfx += " [revealed]"
			}
			return fmt.Sprintf("Registered %d %s %.6f%s", acc.RowId, acc,
				b.block.Params.ConvertValue(acc.Balance()), sfx)
		}))
	}

	// process KT1 vesting contracts
	for _, v := range gen.Contracts {
		// we use hard coded row ids for registrations
		acc := model.NewAccount(v.Addr)
		acc.RowId = model.AccountID(len(accounts) + 1)
		acc.CreatorId = acc.RowId // satisfy invariant
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.SpendableBalance = v.Value
		acc.IsContract = true
		acc.IsFunded = true
		acc.IsDirty = true

		// update block counters
		b.block.NewAccounts++
		b.block.SeenAccounts++
		b.block.NewContracts++
		b.block.FundedAccounts++
		b.block.NEvents++
		b.block.ActivatedSupply += v.Value
		b.block.MintedSupply += v.Value

		// update supply counters
		b.block.Supply.Activated += v.Value
		b.block.Supply.ActiveStaking += v.Value
		b.block.Supply.ActiveDelegated += v.Value

		// register activation flows (will not be applied, just saved!)
		id := model.OpRef{
			Kind: model.OpTypeOrigination,
			N:    opCounter,
			L:    model.OPL_PROTOCOL_UPGRADE,
			P:    flowCounter,
		}
		f := model.NewFlow(b.block, acc, nil, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeActivation
		f.AmountIn = acc.Balance()
		b.block.Flows = append(b.block.Flows, f)

		// register implicit origination ops
		op := model.NewEventOp(b.block, 0, id)
		op.ReceiverId = acc.RowId
		op.Counter = int64(opCounter)
		op.Volume = acc.Balance()
		op.IsContract = true
		b.block.Ops = append(b.block.Ops, op)
		opCounter++

		// prepare for insert
		accounts = append(accounts, acc)

		// save as contract (not spendable, not delegatebale, no fee, no gas, no limits)
		var not bool
		oop := &rpc.Origination{
			Script:      &v.Script,
			Spendable:   &not,
			Delegatable: &not,
			Manager: rpc.Manager{
				Generic: rpc.Generic{
					Metadata: &rpc.OperationMetadata{},
				},
			},
		}
		con := model.NewContract(acc, oop, op, nil, b.block.Params)
		op.IsStorageUpdate = true
		op.Storage = con.Storage
		op.StorageHash = con.StorageHash
		op.Contract = con
		contracts = append(contracts, con)
		b.conMap[op.ReceiverId] = con

		// link to and update baker
		bkr, _ := b.BakerByAddress(v.Delegate)
		acc.IsDelegated = true
		acc.BakerId = bkr.AccountId
		acc.DelegatedSince = b.block.Height
		bkr.TotalDelegations++
		bkr.ActiveDelegations++
		bkr.DelegatedBalance += acc.Balance()
		log.Debugf("1 BOOT ADD delegation %s %d bal=%d -> %s %d delegations=%d",
			acc, acc.RowId, acc.Balance(), bkr, bkr.AccountId, bkr.ActiveDelegations)

		id = model.OpRef{
			Kind: model.OpTypeDelegation,
			N:    opCounter,
			L:    model.OPL_PROTOCOL_UPGRADE,
			P:    flowCounter,
		}
		// register delegation flows (will not be applied, just saved!)
		f = model.NewFlow(b.block, bkr.Account, acc, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeDelegation
		f.AmountIn = acc.Balance()
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// register implicit delegation ops
		op = model.NewEventOp(b.block, 0, id)
		op.SenderId = acc.RowId
		op.BakerId = bkr.AccountId
		op.Counter = int64(opCounter)
		op.Volume = acc.Balance()
		b.block.Ops = append(b.block.Ops, op)
		b.block.NEvents++
		opCounter++

		log.Debug(newLogClosure(func() string {
			var sfx string
			if acc.IsActivated {
				sfx += " [activated]"
			}
			if acc.IsBaker {
				sfx += " [baker]"
			}
			if acc.IsContract {
				sfx += " [contract]"
			}
			if acc.IsRevealed {
				sfx += " [revealed]"
			}
			return fmt.Sprintf("Registered %d %s %.6f%s", acc.RowId, acc,
				b.block.Params.ConvertValue(acc.Balance()), sfx)
		}))
	}

	// process fundraiser accounts that must be activated by users
	for _, v := range gen.Commitments {
		// we use hard coded row ids for registrations
		acc := model.NewAccount(v.Addr)
		acc.RowId = model.AccountID(len(accounts) + 1)
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.UnclaimedBalance = v.Value

		// update block counters
		b.block.NewAccounts++
		b.block.SeenAccounts++

		// count unclaimed supply
		b.block.Supply.Unclaimed += acc.UnclaimedBalance
		b.block.MintedSupply += acc.UnclaimedBalance

		// prepare for insert
		accounts = append(accounts, acc)
	}

	// insert accounts to create rows (later the indexer will update all accounts again,
	// but we need to properly init the table row_id counter here)
	table, err := b.idx.Table(model.AccountTableKey)
	if err != nil {
		return nil, err
	}
	if err := table.Insert(ctx, accounts); err != nil {
		return nil, err
	}
	table, err = b.idx.Table(model.ContractTableKey)
	if err != nil {
		return nil, err
	}
	if err := table.Insert(ctx, contracts); err != nil {
		return nil, err
	}

	// add all non-baker accounts to builder to be picked up by indexers
	for _, v := range accounts {
		acc := v.(*model.Account)
		if acc.IsBaker || acc.SpendableBalance == 0 {
			continue
		}
		b.accMap[acc.RowId] = acc
		b.accHashMap[b.accCache.AccountHashKey(acc)] = acc
	}

	// init chain counters from block
	// set initial unclaimed accounts to number of blinded accounts
	b.block.Chain.Update(b.block, b.accMap, b.bakerMap)
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
