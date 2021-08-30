// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func (b *Builder) BuildGenesisBlock(ctx context.Context) (*model.Block, error) {
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
		acc := model.NewAccount(v.Addr)
		acc.RowId = model.AccountID(len(accounts) + 1)
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
			//  acc.RowId, acc, acc.ActiveDelegations, acc.Balance())
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
		f := model.NewFlow(b.block, acc, nil, opCounter, OPL_PROTOCOL_UPGRADE, flowCounter, 0, 0)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeActivation
		f.AmountIn = acc.SpendableBalance
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// register implicit activation ops
		op := model.NewImplicitOp(b.block, 0, tezos.OpTypeActivateAccount, opCounter, OPL_PROTOCOL_UPGRADE, opCounter)
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
		acc := model.NewAccount(v.Addr)
		acc.RowId = model.AccountID(len(accounts) + 1)
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
		f := model.NewFlow(b.block, acc, nil, opCounter, OPL_PROTOCOL_UPGRADE, flowCounter, 0, 0)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeActivation
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
		//  dlg.RowId, dlg, dlg.ActiveDelegations, acc.RowId, acc, acc.Balance())
		// register delegation flows (will not be applied, just saved!)
		f = model.NewFlow(b.block, dlg, acc, opCounter, OPL_PROTOCOL_UPGRADE, flowCounter, 0, 0)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeDelegation
		f.AmountIn = acc.Balance()
		b.block.Flows = append(b.block.Flows, f)
		flowCounter++

		// register implicit delegation ops
		op := model.NewImplicitOp(b.block, 0, tezos.OpTypeDelegation, opCounter, OPL_PROTOCOL_UPGRADE, opCounter)
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
		contracts = append(contracts, model.NewContract(acc, oop, op))

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
		acc := model.NewAccount(v.Addr)
		acc.RowId = model.AccountID(len(accounts) + 1)
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
