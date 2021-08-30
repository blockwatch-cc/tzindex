// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

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
	contract := &model.Account{}
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
			acc := model.AllocAccount()
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
		AndEqual("is_contract", false).
		Stream(ctx, func(r pack.Row) error {
			acc := &model.Account{}
			if err := r.Decode(acc); err != nil {
				return err
			}
			// not all such KT1's are eligible for upgrade, just ones
			// that are either spendable or delegatable
			if !NeedsBabylonUpgradeAccount(acc, params) {
				return nil
			}
			// upgrade note:
			// - updates account model
			// - adds new contracts with code to the contract table!
			// - does not touch existing smart contracts with code
			// - does not change parameters for existing operations
			// log.Debugf("upgrade: %s to smart contract", acc)
			UpgradeToBabylonAccount(acc, params)
			acc.LastSeen = b.block.Height

			// add account to builder map, account index will write back to db
			b.accMap[acc.RowId] = acc

			// build manager.tz contract
			contract, err := model.NewManagerTzContract(acc, b.block.Height)
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
	acc := &model.Account{}
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
			if !NeedsBabylonUpgradeContract(con, params) {
				return nil
			}

			// upgrade note:
			// - updates account model
			// - adds new contracts with code to the contract table!
			// - does not touch existing smart contracts with code
			// - does not change parameters for existing operations
			// log.Debugf("upgrade: %s to smart contract", acc)
			if err := UpgradeToBabylon(con, params, acc); err != nil {
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

func NeedsBabylonUpgradeAccount(a *model.Account, p *tezos.Params) bool {
	isEligible := a.Type == tezos.AddressTypeContract && !a.IsContract
	isEligible = isEligible && (a.IsSpendable || (!a.IsSpendable && a.IsDelegatable))
	return isEligible && p.Version >= 5
}

func UpgradeToBabylonAccount(a *model.Account, p *tezos.Params) {
	if !NeedsBabylonUpgradeAccount(a, p) {
		return
	}
	a.IsContract = true
	a.IsDirty = true
}

// upgrade smart contracts from before babylon
// - patch code and storage
// - only applies to mainnet contracts originated before babylon
// - don't upgrade when query height < babylon to return old params/storage format
func NeedsBabylonUpgradeContract(c *model.Contract, p *tezos.Params) bool {
	// babylon activation
	isEligible := p.IsPostBabylon() && p.IsPreBabylonHeight(c.FirstSeen)
	// contract upgrade criteria
	isEligible = isEligible && (c.IsSpendable || (!c.IsSpendable && c.IsDelegatable))
	return isEligible
}

func UpgradeToBabylon(c *model.Contract, p *tezos.Params, a *model.Account) error {
	if !NeedsBabylonUpgradeContract(c, p) {
		return nil
	}

	// extend call stats array by moving existing stats
	offs := 1
	if !c.IsSpendable && c.IsDelegatable {
		offs++
	}
	newcs := make([]byte, offs*4+len(c.CallStats))
	copy(newcs[offs*4:], c.CallStats)
	c.CallStats = newcs

	// unmarshal script
	script := micheline.NewScript()
	err := script.UnmarshalBinary(c.Script)
	if err != nil {
		return err
	}

	// need manager (creator)
	mgrHash := a.Address.Bytes()

	// migrate script
	switch true {
	case c.IsSpendable:
		script.MigrateToBabylonAddDo(mgrHash)
		c.InterfaceHash = script.InterfaceHash()
		c.CodeHash = script.CodeHash()
	case !c.IsSpendable && c.IsDelegatable:
		script.MigrateToBabylonSetDelegate(mgrHash)
		c.InterfaceHash = script.InterfaceHash()
		c.CodeHash = script.CodeHash()
	}
	c.Features = script.Features()
	c.Interfaces = script.Interfaces()

	// marshal script
	c.Script, err = script.MarshalBinary()
	if err != nil {
		return err
	}

	// unmarshal initial storage
	storage := micheline.Prim{}
	if err := storage.UnmarshalBinary(c.Storage); err != nil {
		return err
	}
	storage = storage.MigrateToBabylonStorage(mgrHash)
	c.Storage, err = storage.MarshalBinary()
	if err != nil {
		return err
	}

	c.IsDirty = true
	return nil
}
