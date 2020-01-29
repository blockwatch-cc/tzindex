// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cespare/xxhash"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
	"blockwatch.cc/tzindex/rpc"
)

func hashKey(typ chain.AddressType, h []byte) uint64 {
	var buf [21]byte
	buf[0] = byte(typ)
	copy(buf[1:], h)
	return xxhash.Sum64(buf[:])
}

func accountHashKey(a *Account) uint64 {
	return hashKey(a.Type, a.Hash)
}

func addressHashKey(a chain.Address) uint64 {
	return hashKey(a.Type, a.Hash)
}

type Builder struct {
	idx        *Indexer               // storage reference
	accHashMap map[uint64]*Account    // hash(acc_hash) -> *Account (both known and new accounts)
	accMap     map[AccountID]*Account // id -> *Account (both known and new accounts)
	dlgHashMap map[uint64]*Account    // delegates by hash
	dlgMap     map[AccountID]*Account // delegates by id

	// build state
	block     *Block
	parent    *Block
	baking    []Right
	endorsing []Right
	branches  map[string]*Block
}

func NewBuilder(idx *Indexer) *Builder {
	return &Builder{
		idx:        idx,
		accHashMap: make(map[uint64]*Account),
		accMap:     make(map[AccountID]*Account),
		dlgMap:     make(map[AccountID]*Account),
		dlgHashMap: make(map[uint64]*Account),
		baking:     make([]Right, 0, 64),
		endorsing:  make([]Right, 0, 32),
		branches:   make(map[string]*Block, 128), // more than max of 64
	}
}

func (b *Builder) RegisterDelegate(acc *Account) {
	acc.IsDelegate = true
	acc.LastSeen = b.block.Height
	acc.DelegateSince = b.block.Height
	acc.InitGracePeriod(b.block.Cycle, b.block.Params)
	acc.IsDirty = true
	b.ActivateDelegate(acc)
	hashkey := accountHashKey(acc)
	b.dlgMap[acc.RowId] = acc
	b.dlgHashMap[hashkey] = acc
	delete(b.accMap, acc.RowId)
	delete(b.accHashMap, hashkey)
}

// only called ofrom rollback and bug fix code
func (b *Builder) UnregisterDelegate(acc *Account) {
	acc.DelegateId = 0
	acc.IsDelegate = false
	acc.IsActiveDelegate = false
	acc.DelegateSince = 0
	acc.TotalDelegations = 0
	acc.ActiveDelegations = 0
	acc.GracePeriod = 0
	acc.IsDirty = true
	// move from delegate map to accounts
	hashkey := accountHashKey(acc)
	b.accMap[acc.RowId] = acc
	b.accHashMap[hashkey] = acc
	delete(b.dlgMap, acc.RowId)
	delete(b.dlgHashMap, hashkey)
}

func (b *Builder) ActivateDelegate(acc *Account) {
	acc.IsActiveDelegate = true
	acc.IsDirty = true
}

func (b *Builder) DeactivateDelegate(acc *Account) {
	acc.IsActiveDelegate = false
	acc.IsDirty = true
}

func (b *Builder) AccountByAddress(addr chain.Address) (*Account, bool) {
	key := addressHashKey(addr)
	// lookup delegate accounts first
	acc, ok := b.dlgHashMap[key]
	if ok && acc.Type == addr.Type && bytes.Compare(acc.Hash, addr.Hash) == 0 {
		return acc, true
	}
	// lookup regular accounts second
	acc, ok = b.accHashMap[key]
	if ok && acc.Type == addr.Type && bytes.Compare(acc.Hash, addr.Hash) == 0 {
		return acc, true
	}
	return nil, false
}

func (b *Builder) AccountById(id AccountID) (*Account, bool) {
	// lookup delegate accounts first
	if acc, ok := b.dlgMap[id]; ok {
		return acc, ok
	}
	// lookup regular accounts second
	acc, ok := b.accMap[id]
	return acc, ok
}

func (b *Builder) BranchByHash(h chain.BlockHash) (*Block, bool) {
	branch, ok := b.branches[h.String()]
	return branch, ok
}

func (b *Builder) Accounts() map[AccountID]*Account {
	return b.accMap
}

func (b *Builder) Delegates() map[AccountID]*Account {
	return b.dlgMap
}

func (b *Builder) Rights(typ chain.RightType) []Right {
	switch typ {
	case chain.RightTypeBaking:
		return b.baking
	case chain.RightTypeEndorsing:
		return b.endorsing
	default:
		return nil
	}
}

func (b *Builder) Table(key string) (*pack.Table, error) {
	return b.idx.Table(key)
}

func (b *Builder) Init(ctx context.Context, tip *ChainTip, c *rpc.Client) error {
	if tip.BestHeight < 0 {
		return nil
	}

	// load parent block, rpc bundle (need deactivated list) and chain state from tables
	var err error
	b.parent, err = b.idx.BlockByHeight(ctx, tip.BestHeight)
	if err != nil {
		return err
	}
	if err := b.parent.FetchRPC(ctx, c); err != nil {
		return err
	}
	b.parent.Chain, err = b.idx.ChainByHeight(ctx, tip.BestHeight)
	if err != nil {
		return err
	}
	b.parent.Supply, err = b.idx.SupplyByHeight(ctx, tip.BestHeight)
	if err != nil {
		return err
	}
	b.parent.Params, err = b.idx.ParamsByDeployment(b.parent.Version)
	if err != nil {
		return err
	}

	// to make our crawler happy, we also expose the last block on load
	b.block = b.parent

	// load all registered delegates; Note: if we ever want to change this
	// the cache strategy needs to be reworked because delegates are kept
	// out of the cache
	if dlgs, err := b.idx.ListAllDelegates(ctx); err != nil {
		return err
	} else {
		log.Debugf("Loaded %d total delegates", len(dlgs))
		for _, acc := range dlgs {
			b.dlgMap[acc.RowId] = acc
			b.dlgHashMap[accountHashKey(acc)] = acc
		}
	}

	return nil
}

func (b *Builder) Build(ctx context.Context, tz *Bundle) (*Block, error) {
	// 1  create a new block structure to house extracted data
	var err error
	if b.block, err = NewBlock(tz, b.parent); err != nil {
		return nil, fmt.Errorf("build stage 1: %v", err)
	}

	// build genesis accounts at height 1 and return
	if b.block.TZ.Block.Header.Content != nil {
		return b.BuildGenesisBlock(ctx)
	}

	// 2  lookup accounts or create new accounts
	if err := b.InitAccounts(ctx); err != nil {
		return nil, fmt.Errorf("build stage 2: %v", err)
	}

	// 3  unpack operations and update accounts
	if err := b.Decorate(ctx, false); err != nil {
		return nil, fmt.Errorf("build stage 3: %v", err)
	}

	// 4  update block stats when all data is resolved
	if err := b.UpdateStats(ctx); err != nil {
		return nil, fmt.Errorf("build stage 4: %v", err)
	}

	return b.block, nil
}

func (b *Builder) Clean() {
	// add unique addrs to cache
	for _, acc := range b.accMap {
		if acc == nil {
			continue
		}
		// reset flags
		acc.IsDirty = false
		acc.IsNew = false
		acc.WasFunded = false

		// keep delegates out of cache
		if acc.IsDelegate && !acc.MustDelete {
			continue
		}
	}

	for _, acc := range b.dlgMap {
		// reset flags
		acc.IsDirty = false
		acc.IsNew = false
		acc.WasFunded = false
	}

	// clear build state
	b.accHashMap = make(map[uint64]*Account)
	b.accMap = make(map[AccountID]*Account)
	b.baking = b.baking[:0]
	b.endorsing = b.endorsing[:0]

	// free previous parent block
	if b.parent != nil {
		b.parent.Free()
		b.parent = nil
	}

	// clear branches (keep most recent 64 blocks only)
	for n, v := range b.branches {
		if v.Height < b.block.Height-64 {
			v.Free()
			delete(b.branches, n)
		}
	}

	// keep current block as parent
	b.parent = b.block
	b.block = nil
}

// remove state on error
func (b *Builder) Purge() {
	// clear build state
	b.accHashMap = make(map[uint64]*Account)
	b.accMap = make(map[AccountID]*Account)

	// clear delegate state
	b.dlgHashMap = make(map[uint64]*Account)
	b.dlgMap = make(map[AccountID]*Account)

	// clear branches (keep most recent 64 blocks only)
	for _, v := range b.branches {
		v.Free()
	}
	b.branches = make(map[string]*Block, 128)

	// free previous parent block
	if b.parent != nil {
		b.parent.Free()
		b.parent = nil
	}

	if b.block != nil {
		b.block.Free()
		b.block = nil
	}

	b.baking = b.baking[:0]
	b.endorsing = b.endorsing[:0]
}

// during reorg rpc and model blocks are already loaded, parent data is ignored
func (b *Builder) BuildReorg(ctx context.Context, tz *Bundle, parent *Block) (*Block, error) {
	// use parent as builder parent
	b.parent = parent

	// build reorg block
	var err error
	if b.block, err = NewBlock(tz, parent); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 1: %v", tz.Block.Header.Level, err)
	}

	// 2  lookup accounts
	if err := b.InitAccounts(ctx); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 2: %v", b.block.Height, err)
	}

	// 3  unpack operations and update accounts
	if err := b.Decorate(ctx, true); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 3: %v", b.block.Height, err)
	}

	// 4  stats
	if err := b.RollbackStats(ctx); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 4: %v", b.block.Height, err)
	}

	return b.block, nil
}

func (b *Builder) CleanReorg() {
	// add unique addrs to cache
	for _, acc := range b.accMap {
		if acc == nil {
			continue
		}
		// reset flags
		acc.IsDirty = false
		acc.IsNew = false
		acc.WasFunded = false
	}

	// clear build state
	b.accHashMap = make(map[uint64]*Account)
	b.accMap = make(map[AccountID]*Account)
	b.baking = b.baking[:0]
	b.endorsing = b.endorsing[:0]

	for _, acc := range b.dlgMap {
		// reset flags
		acc.IsDirty = false
		acc.IsNew = false
		acc.WasFunded = false
	}

	// don't clear branches during reorg because we'll need them later

	// release, but do not free parent block (it may be fork block which we'll need
	// for forward reorg and later)
	b.parent = nil

	// free current block (no longer needed during rollback)
	if b.block != nil {
		b.block.Free()
	}
	b.block = nil
}

// Note on hash collisions
//
// Account hashes without a type may not be unique, hence we treat both as pair.
// Our pack hash index works on a single field only, so we may see some collisions
// when retrieving data from the accounts table.
//
// The code below takes care of this fact and matches by both type+hash. In the
// worst case we fetch an undesired address more, but we'll never use it.
//
func (b *Builder) InitAccounts(ctx context.Context) error {
	// collect unique accounts/addresses
	addresses := make(util.StringList, 0)

	// guard against unknown deactivated delegates
	for _, v := range b.block.TZ.Block.Metadata.BalanceUpdates {
		var addr chain.Address
		switch v.BalanceUpdateKind() {
		case "contract":
			addr = v.(*rpc.ContractBalanceUpdate).Contract

		case "freezer":
			addr = v.(*rpc.FreezerBalanceUpdate).Delegate
		}
		if _, ok := b.AccountByAddress(addr); !ok && addr.IsValid() {
			addresses.AddUnique(addr.String())
		}
	}

	// add unknown deactivated delegates (from parent block: we're deactivating
	// AFTER a block has been fully indexed at the start of the next block)
	if b.parent != nil {
		for _, v := range b.parent.TZ.Block.Metadata.Deactivated {
			if _, ok := b.AccountByAddress(v); !ok {
				addresses.AddUnique(v.String())
			}
		}
	}

	// collect from ops
	var op_n int
	for _, oll := range b.block.TZ.Block.Operations {
		for _, oh := range oll {
			// init branches
			br := oh.Branch.String()
			if _, ok := b.branches[br]; !ok {
				branch, err := b.idx.BlockByHash(ctx, oh.Branch)
				if err != nil {
					return fmt.Errorf("op [%d:%d]: invalid branch %s: %v", oh.Branch, err)
				}
				b.branches[br] = branch
			}
			// parse operations
			for op_c, op := range oh.Contents {
				switch kind := op.OpKind(); kind {
				case chain.OpTypeActivateAccount:
					// need to search for blinded key
					aop := op.(*rpc.AccountActivationOp)
					bkey, err := chain.BlindAddress(aop.Pkh, aop.Secret)
					if err != nil {
						return fmt.Errorf("activation op [%d:%d]: blinded address creation failed: %v",
							op_n, op_c, err)
					}
					addresses.AddUnique(bkey.String())

				case chain.OpTypeBallot:
					// deactivated delegates can still cast votes
					addr := op.(*rpc.BallotOp).Source
					if _, ok := b.AccountByAddress(addr); !ok {
						addresses.AddUnique(addr.String())
					}

				case chain.OpTypeDelegation:
					del := op.(*rpc.DelegationOp)
					addresses.AddUnique(del.Source.String())

					// deactive delegates may not be in map
					if del.Delegate.IsValid() {
						if _, ok := b.AccountByAddress(del.Delegate); !ok {
							addresses.AddUnique(del.Delegate.String())
						}
					}

				case chain.OpTypeDoubleBakingEvidence:
					// empty

				case chain.OpTypeDoubleEndorsementEvidence:
					// empty

				case chain.OpTypeEndorsement:
					// deactive delegates may not be in map
					end := op.(*rpc.EndorsementOp)
					if _, ok := b.AccountByAddress(end.Metadata.Delegate); !ok {
						addresses.AddUnique(end.Metadata.Delegate.String())
					}

				case chain.OpTypeOrigination:
					orig := op.(*rpc.OriginationOp)
					addresses.AddUnique(orig.Source.String())
					if orig.ManagerPubkey.IsValid() {
						addresses.AddUnique(orig.ManagerPubkey.String())
					}
					if orig.ManagerPubkey2.IsValid() {
						addresses.AddUnique(orig.ManagerPubkey2.String())
					}
					for _, v := range orig.Metadata.Result.OriginatedContracts {
						addresses.AddUnique(v.String())
					}
					if orig.Delegate != nil {
						if _, ok := b.AccountByAddress(*orig.Delegate); !ok {
							addresses.AddUnique(orig.Delegate.String())
						}
					}

				case chain.OpTypeProposals:
					// deactivated delegates can still send proposals
					addr := op.(*rpc.ProposalsOp).Source
					if _, ok := b.AccountByAddress(addr); !ok {
						addresses.AddUnique(addr.String())
					}

				case chain.OpTypeReveal:
					addresses.AddUnique(op.(*rpc.RevelationOp).Source.String())

				case chain.OpTypeSeedNonceRevelation:
					// not necessary because this is done by the baker

				case chain.OpTypeTransaction:
					tx := op.(*rpc.TransactionOp)
					addresses.AddUnique(tx.Source.String())
					addresses.AddUnique(tx.Destination.String())
					for _, res := range tx.Metadata.InternalResults {
						if res.Destination != nil {
							addresses.AddUnique(res.Destination.String())
						}
						if res.Delegate != nil {
							addresses.AddUnique(res.Delegate.String())
						}
						for _, v := range res.Result.OriginatedContracts {
							addresses.AddUnique(v.String())
						}
					}
				}
			}
			op_n++
		}
	}

	// collect unknown/unloaded delegates for lookup or creation
	unknownDelegateIds := make([]uint64, 0)

	// fetch baking and endorsing rights for this block
	table, err := b.idx.Table(index.RightsTableKey)
	if err != nil {
		return err
	}
	q := pack.Query{
		Name:   "etl.rights.search",
		Fields: table.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: b.block.Height,
			},
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.RightTypeBaking),
			},
		},
	}
	right := Right{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(&right); err != nil {
			return err
		}
		b.baking = append(b.baking, right)
		return nil
	})
	if err != nil {
		return err
	}
	// endorsements are for block-1
	q.Conditions[0].Value = b.block.Height - 1
	q.Conditions[1].Value = int64(chain.RightTypeEndorsing)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(&right); err != nil {
			return err
		}
		b.endorsing = append(b.endorsing, right)
		return nil
	})
	if err != nil {
		return err
	}

	// collect from rights: on genesis and when deactivated, delegates are not in map
	for _, r := range b.baking {
		if _, ok := b.AccountById(r.AccountId); !ok {
			unknownDelegateIds = append(unknownDelegateIds, r.AccountId.Value())
		}
	}
	for _, r := range b.endorsing {
		if _, ok := b.AccountById(r.AccountId); !ok {
			unknownDelegateIds = append(unknownDelegateIds, r.AccountId.Value())
		}
	}

	// collect from future cycle rights when available
	for _, r := range b.block.TZ.Baking {
		if _, ok := b.AccountByAddress(r.Delegate); !ok {
			addresses.AddUnique(r.Delegate.String())
		}
	}
	for _, r := range b.block.TZ.Endorsing {
		if _, ok := b.AccountByAddress(r.Delegate); !ok {
			addresses.AddUnique(r.Delegate.String())
		}
	}

	// collect from invoices
	if b.parent != nil {
		parentProtocol := b.parent.TZ.Block.Metadata.Protocol
		blockProtocol := b.block.TZ.Block.Metadata.Protocol
		if !parentProtocol.IsEqual(blockProtocol) {
			for n, _ := range b.block.Params.Invoices {
				addresses.AddUnique(n)
			}
		}
	}

	// delegation to inactive delegates is not explicitly forbidden, so
	// we have to check if any inactive (or deactivated) delegate is still
	// referenced

	// search cached accounts and build map
	hashes := make([][]byte, 0)
	for _, v := range addresses {
		if len(v) == 0 {
			continue
		}
		addr, err := chain.ParseAddress(v)
		if err != nil {
			return fmt.Errorf("addr decode for '%s' failed: %v", v, err)
		}

		// skip delegates
		hashKey := addressHashKey(addr)
		if _, ok := b.dlgHashMap[hashKey]; ok {
			continue
		}
		// skip duplicate addresses
		if _, ok := b.accHashMap[hashKey]; ok {
			continue
		}
		// create tentative new account and schedule for lookup
		acc := NewAccount(addr)
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		hashes = append(hashes, addr.Hash)

		// store in map, will be overwritten when resolved from db
		// or kept as new address when this is the first time we see it
		b.accHashMap[hashKey] = acc
	}

	// lookup addr by hashes (non-existent addrs are expected to not resolve)
	table, err = b.idx.Table(index.AccountTableKey)
	if err != nil {
		return err
	}
	if len(hashes) > 0 {
		q := pack.Query{
			Name:   "etl.addr_hash.search",
			Fields: table.Fields(),
			Conditions: pack.ConditionList{pack.Condition{
				Field: table.Fields().Find("H"),
				Mode:  pack.FilterModeIn,
				Value: hashes,
			}},
		}
		err = table.Stream(ctx, q, func(r pack.Row) error {
			acc := AllocAccount()
			if err := r.Decode(acc); err != nil {
				return err
			}

			// skip delegates (should have not been looked up in the first place)
			if acc.IsDelegate {
				acc.Free()
				return nil
			}

			// collect unknown delegates when referenced
			if acc.DelegateId > 0 {
				if _, ok := b.AccountById(acc.DelegateId); !ok {
					// if _, ok := b.dlgMap[acc.DelegateId]; !ok {
					unknownDelegateIds = append(unknownDelegateIds, acc.DelegateId.Value())
				}
			}

			hashKey := accountHashKey(acc)

			// sanity check for hash collisions (unlikely when we use type+hash)
			if a, ok := b.accHashMap[hashKey]; ok {
				if bytes.Compare(a.Hash, acc.Hash) != 0 {
					return fmt.Errorf("Hash collision: account %s (%d) h=%x and %s (%d) h=%x have same hash %d",
						a, a.RowId, a.Hash, acc, acc.RowId, acc.Hash, hashKey)
				}
			}

			// return temp addrs to pool (Note: don't free addrs loaded from cache!)
			if tmp, ok := b.accHashMap[hashKey]; ok && tmp.RowId == 0 {
				tmp.Free()
			}
			// this overwrites temp address in map
			b.accHashMap[hashKey] = acc
			b.accMap[acc.RowId] = acc

			return nil
		})
		if err != nil {
			return err
		}
	}

	// lookup inactive delegates
	if len(unknownDelegateIds) > 0 {
		// creates a new slice
		unknownDelegateIds = vec.UniqueUint64Slice(unknownDelegateIds)
		err := table.StreamLookup(ctx, unknownDelegateIds, func(r pack.Row) error {
			acc := AllocAccount()
			if err := r.Decode(acc); err != nil {
				acc.Free()
				return err
			}
			// ignore when its an active delegate (then it's in the delegate maps)
			if acc.IsActiveDelegate {
				acc.Free()
				return nil
			}
			// handle like a regular account
			hashKey := accountHashKey(acc)
			b.dlgHashMap[hashKey] = acc
			b.dlgMap[acc.RowId] = acc
			return nil
		})
		if err != nil {
			return err
		}
	}

	// collect new addrs and bulk insert to generate ids
	// Note: due to random map walk in Go, address id allocation will be
	//       non-deterministic, also deletion of addresses on reorgs makes
	//       it non-deterministic, so we assume this is OK here; however
	//       address id's are not interchangable between two versions of the
	//       accounts table. Keep this in mind for downstream use!
	newacc := make([]pack.Item, 0)
	for _, v := range b.accHashMap {
		if v.IsNew {
			newacc = append(newacc, v)
		}
	}

	// bulk insert to generate ids
	if len(newacc) > 0 {
		err := table.Insert(ctx, newacc)
		if err != nil {
			return err
		}
		// and add new addresses under their new ids into the id map
		for _, v := range newacc {
			acc := v.(*Account)
			b.accMap[acc.RowId] = acc
		}
	}

	return nil
}

func (b *Builder) Decorate(ctx context.Context, rollback bool) error {
	// handle upgrades and end of cycle events right before processing the next block
	if b.parent != nil {
		// first step: handle deactivated delegates from parent block
		// this is idempotent
		if b.block.Params.IsCycleStart(b.block.Height) && b.block.Height > 0 {
			// deactivate based on grace period
			var count int
			for _, dlg := range b.dlgMap {
				if dlg.IsActiveDelegate && dlg.GracePeriod < b.block.Cycle {
					count++
					if rollback {
						b.ActivateDelegate(dlg)
					} else {
						b.DeactivateDelegate(dlg)
					}
				}
			}
			if count > 0 {
				if rollback {
					log.Debugf("Rollback-reactivated %d delegates at %d cycle %d", count, b.block.Height-1, b.block.Cycle-1)
				} else {
					log.Debugf("Deactivated %d delegates at %d cycle %d", count, b.block.Height-1, b.block.Cycle-1)
				}
			}

			// cross check if we have missed a deactivation and fail
			for _, v := range b.parent.TZ.Block.Metadata.Deactivated {
				acc, ok := b.AccountByAddress(v)
				if !ok {
					return fmt.Errorf("deactivate: missing account %s", v)
				}
				if rollback {
					if !acc.IsActiveDelegate {
						return fmt.Errorf("deactivate: found non-reactivated delegate %s", v)
					}
				} else {
					if acc.IsActiveDelegate {
						return fmt.Errorf("deactivate: found non-deactivated delegate %s", v)
					}
				}
			}
		}

		// check for new protocol
		parentProtocol := b.parent.TZ.Block.Metadata.Protocol
		blockProtocol := b.block.TZ.Block.Metadata.Protocol
		if !parentProtocol.IsEqual(blockProtocol) {
			if !rollback {
				// register new protocol (will save as new deployment)
				log.Infof("New protocol %s detected at %d", blockProtocol, b.block.Height)
				b.block.Params.StartHeight = b.block.Height
				if err := b.idx.ConnectProtocol(ctx, b.block.Params); err != nil {
					return err
				}
			}

			prevparams, err := b.idx.ParamsByProtocol(parentProtocol)
			if err != nil {
				return err
			}
			nextparams, err := b.idx.ParamsByProtocol(blockProtocol)
			if err != nil {
				return err
			}

			// special actions on protocol upgrades

			// fix bugs by updating state
			if !rollback {
				err := b.FixUpgradeBugs(ctx, prevparams, nextparams)
				if err != nil {
					return err
				}
			}

			// process invoices at start of new protocol (i.e. add new accounts and flows)
			if rollback {
				err = b.RollbackInvoices(ctx)
			} else {
				err = b.ApplyInvoices(ctx)
			}
			if err != nil {
				return err
			}
		} else if b.block.Params != nil && b.block.Params.IsCycleStart(b.block.Height) {
			// update params at start of cycle (to capture early ramp up data)
			b.idx.reg.Register(b.block.Params)
		}
	}

	// identify the baker account id (should be in delegate map)
	// Note: blocks 0 and 1 don't have a baker, blocks 0-2 have no endorsements
	if b.block.TZ.Block.Metadata.Baker.IsValid() {
		baker, ok := b.AccountByAddress(b.block.TZ.Block.Metadata.Baker)
		if !ok {
			return fmt.Errorf("missing baker account %s", b.block.TZ.Block.Metadata.Baker)
		}
		b.block.Baker = baker
		b.block.BakerId = baker.RowId
		if baker.IsActiveDelegate {
			// extend grace period
			baker.UpdateGracePeriod(b.block.Cycle, b.block.Params)
		} else {
			// reset inactivity
			baker.IsActiveDelegate = true
			baker.InitGracePeriod(b.block.Cycle, b.block.Params)
		}
		if !rollback {
			if b.block.Priority == 0 {
				b.block.Baker.BlocksBaked++
				b.block.Baker.IsDirty = true
			} else {
				b.block.Baker.BlocksBaked++
				b.block.Baker.BlocksStolen++
				b.block.Baker.IsDirty = true
				// identify lower prio bakers from rights table and update BlocksMissed
				// assuming the rights list is sorted by priority
				for i := 0; i < b.block.Priority; i++ {
					id := b.baking[i].AccountId
					missed, ok := b.AccountById(id)
					if !ok {
						return fmt.Errorf("missing baker account %d", id)
					}
					missed.BlocksMissed++
					missed.IsDirty = true
				}
			}
		} else {
			if b.block.Priority == 0 {
				b.block.Baker.BlocksBaked--
				b.block.Baker.IsDirty = true
			} else {
				b.block.Baker.BlocksBaked--
				b.block.Baker.BlocksStolen--
				b.block.Baker.IsDirty = true
				// identify lower prio bakers from rights table and update BlocksMissed
				// assuming the rights list is sorted by priority
				for i := 0; i < b.block.Priority; i++ {
					id := b.baking[i].AccountId
					missed, ok := b.AccountById(id)
					if !ok {
						return fmt.Errorf("missing baker account %d", id)
					}
					missed.BlocksMissed--
					missed.IsDirty = true
				}
			}
		}

		// collect data from header balance updates, note this is no explicit 'operation'
		// in Tezos (i.e. it has no op hash and will not be stored in the op table)
		// also handles baker payments and unfreeze of payouts
		_, err := b.NewBakerFlows()
		if err != nil {
			return err
		}
	}

	// process operations
	// - create new op and flow objects
	// - init/update accounts
	// - sum op volume, fees, rewards, deposits
	// - attach extra data if defined
	var op_n int
	for _, ol := range b.block.TZ.Block.Operations {
		for _, oh := range ol {
			// rpc.OperationHeader
			for op_c, o := range oh.Contents {
				switch kind := o.OpKind(); kind {
				case chain.OpTypeActivateAccount:
					if err := b.NewActivationOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeBallot:
					if err := b.NewBallotOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeDelegation:
					if err := b.NewDelegationOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeDoubleBakingEvidence:
					if err := b.NewDoubleBakingOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeDoubleEndorsementEvidence:
					if err := b.NewDoubleEndorsingOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeEndorsement:
					if err := b.NewEndorsementOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeOrigination:
					if err := b.NewOriginationOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeProposals:
					if err := b.NewProposalsOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeReveal:
					if err := b.NewRevealOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeSeedNonceRevelation:
					if err := b.NewSeedNonceOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeTransaction:
					if err := b.NewTransactionOp(ctx, oh, op_n, op_c, rollback); err != nil {
						return err
					}
				}
			}
			op_n++
		}
	}

	return nil
}

func (b *Builder) ApplyInvoices(ctx context.Context) error {
	for n, v := range b.block.Params.Invoices {
		addr, err := chain.ParseAddress(n)
		if err != nil {
			return fmt.Errorf("decoding invoice address %s: %v", n, err)
		}
		acc, ok := b.AccountByAddress(addr)
		if !ok {
			return fmt.Errorf("missing invoice account %s: %v", addr, err)
		}
		acc.IsDirty = true
		acc.LastIn = b.block.Height
		acc.LastSeen = b.block.Height
		b.block.Flows = append(b.block.Flows, b.NewInvoiceFlow(acc, v))
		log.Debugf("invoice: %s %f", acc, b.block.Params.ConvertValue(v))
	}
	return nil
}

func (b *Builder) RollbackInvoices(ctx context.Context) error {
	for n, v := range b.block.Params.Invoices {
		addr, err := chain.ParseAddress(n)
		if err != nil {
			return fmt.Errorf("decoding invoice address %s: %v", n, err)
		}
		acc, ok := b.AccountByAddress(addr)
		if !ok {
			return fmt.Errorf("rollback invoice: unknown invoice account %s", n, err)
		}
		b.block.Flows = append(b.block.Flows, b.NewInvoiceFlow(acc, v))
		if acc.FirstSeen == b.block.Height {
			acc.MustDelete = true
		}
	}
	return nil
}

// update counters, totals and sums from flows and ops
func (b *Builder) UpdateStats(ctx context.Context) error {
	// init pre-funded state
	for _, acc := range b.accMap {
		acc.WasFunded = (acc.FrozenBalance() + acc.SpendableBalance) > 0
	}
	for _, acc := range b.dlgMap {
		acc.WasFunded = (acc.FrozenBalance() + acc.SpendableBalance) > 0
	}

	// apply pure in-flows
	for _, f := range b.block.Flows {
		// skip any out flow
		if f.AmountOut > 0 {
			continue
		}
		acc, ok := b.AccountById(f.AccountId)
		if !ok {
			return fmt.Errorf("flow update [%s:%s]: missing account id %d",
				f.Category, f.Operation, f.AccountId)
		}
		if err := acc.UpdateBalance(f); err != nil {
			return err
		}
	}

	// apply out-flows and in/out-flows
	for _, f := range b.block.Flows {
		// skip any pure in flow
		if f.AmountOut == 0 {
			continue
		}
		acc, ok := b.AccountById(f.AccountId)
		if !ok {
			return fmt.Errorf("flow update [%s:%s]: missing account id %d",
				f.Category, f.Operation, f.AccountId)
		}
		if err := acc.UpdateBalance(f); err != nil {
			return err
		}
	}

	// update delegation counters when delegator balance flips funded state
	for _, acc := range b.accMap {
		// skip new accounts and accounts without activity this block
		if acc.IsNew || acc.LastSeen != b.block.Height {
			continue
		}
		// skip undelegated accounts and self-delegates
		if !acc.IsDelegated || acc.DelegateId == acc.RowId {
			continue
		}
		dlg, _ := b.AccountById(acc.DelegateId)
		dlg.IsDirty = true
		if !acc.IsFunded && acc.WasFunded {
			// remove active delegation
			dlg.ActiveDelegations--
		} else if acc.IsFunded && !acc.WasFunded {
			// re-add active delegation
			dlg.ActiveDelegations++
		}
	}

	// count endorsement slots missed
	erights := make(map[AccountID]int)
	var count int
	for _, r := range b.endorsing {
		acc, ok := b.AccountById(r.AccountId)
		if !ok {
			return fmt.Errorf("missing endorsement delegate %d: %#v", r.AccountId, r)
		}
		num, _ := erights[acc.RowId]
		erights[acc.RowId] = num + 1
		count++
	}

	for _, op := range b.block.Ops {
		if op.Type != chain.OpTypeEndorsement {
			continue
		}
		o, _ := b.block.GetRPCOp(op.OpN, op.OpC)
		eop := o.(*rpc.EndorsementOp)
		acc, _ := b.AccountByAddress(eop.Metadata.Delegate)
		num, _ := erights[acc.RowId]
		erights[acc.RowId] = num - len(eop.Metadata.Slots)
	}

	for id, n := range erights {
		acc, _ := b.AccountById(id)
		if n > 0 {
			// missed endorsements
			acc.SlotsMissed += n
			acc.IsDirty = true
		}
		if n < 0 {
			// stolen endorsements (should not happen)
			log.Warnf("%d stolen endorsement(s) in block %d %s delegate %d %s",
				-n, b.block.Height, b.block.Hash, id, acc)
		}
	}

	// update supplies and totals
	b.block.Update(b.accMap, b.dlgMap)
	b.block.Chain.Update(b.block, b.dlgMap)
	b.block.Supply.Update(b.block, b.dlgMap)
	return nil
}

func (b *Builder) RollbackStats(ctx context.Context) error {
	// init pre-funded state (at the end of block processing using current state)
	for _, acc := range b.accMap {
		acc.WasFunded = (acc.FrozenBalance() + acc.SpendableBalance) > 0
	}
	for _, acc := range b.dlgMap {
		acc.WasFunded = (acc.FrozenBalance() + acc.SpendableBalance) > 0
	}

	// reverse apply out-flows and in/out-flows
	for _, f := range b.block.Flows {
		// skip any non-out flow
		if f.AmountOut == 0 {
			continue
		}
		acc, ok := b.AccountById(f.AccountId)
		if !ok {
			return fmt.Errorf("flow rollback [%s:%s]: missing account id %d",
				f.Category, f.Operation, f.AccountId)
		}
		if err := acc.RollbackBalance(f); err != nil {
			return err
		}
	}

	// reverse apply pure in-flows
	for _, f := range b.block.Flows {
		// skip any out flow
		if f.AmountOut > 0 {
			continue
		}
		acc, ok := b.AccountById(f.AccountId)
		if !ok {
			return fmt.Errorf("flow rollback [%s:%s]: missing account id %d",
				f.Category, f.Operation, f.AccountId)
		}
		if err := acc.RollbackBalance(f); err != nil {
			return err
		}
	}

	// revert delegation count updates
	for _, acc := range b.accMap {
		// skip new accounts and accounts without activity this block
		if acc.IsNew || acc.LastSeen != b.block.Height {
			continue
		}
		if acc.IsDelegated && acc.DelegateId != acc.RowId {
			dlg, _ := b.AccountById(acc.DelegateId)
			dlg.IsDirty = true
			if !acc.IsFunded && acc.WasFunded {
				// remove active delegation
				dlg.ActiveDelegations--
			}
			if acc.IsFunded && !acc.WasFunded {
				// re-add active delegation
				dlg.ActiveDelegations++
			}
		}
	}

	// update supplies and totals
	b.block.Rollback(b.accMap, b.dlgMap)
	b.block.Chain.Rollback(b.block)
	b.block.Supply.Rollback(b.block)
	return nil
}

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

	// process foundation bakers and early backer accounts (activate right away)
	for _, v := range gen.Accounts {
		// we use hard coded row ids for registrations
		acc := NewAccount(v.Addr)
		acc.RowId = AccountID(len(accounts) + 1)
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.SpendableBalance = v.Value
		acc.IsFunded = true
		acc.IsActivated = true

		// revealed accounts are registered as active delegates (foundation bakers)
		if v.Key.IsValid() {
			acc.IsRevealed = true
			acc.PubkeyHash = v.Key.Data
			acc.PubkeyType = v.Key.Type.HashType()
			acc.IsDelegate = true
			acc.IsActiveDelegate = true
			acc.DelegateSince = b.block.Height
			acc.DelegateId = acc.RowId
			b.block.NDelegation++
			b.RegisterDelegate(acc)
			log.Debugf("1 BOOT REG SELF %d %s -> %d bal=%d",
				acc.RowId, acc, acc.ActiveDelegations, acc.Balance())
		} else {
			b.accMap[acc.RowId] = acc
			b.accHashMap[accountHashKey(acc)] = acc
		}

		// update block counters
		b.block.NewImplicitAccounts++
		b.block.FundedAccounts++
		b.block.NewAccounts++
		b.block.SeenAccounts++
		b.block.ActivatedSupply += v.Value

		// register activation flows (will not be applied, just saved!)
		f := NewFlow(b.block, acc, acc)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeActivation
		f.AmountIn = acc.SpendableBalance
		b.block.Flows = append(b.block.Flows, f)

		// prepare for insert
		accounts = append(accounts, acc)

		log.Debug(newLogClosure(func() string {
			var as, vs, ds, rs string
			if acc.IsActivated {
				as = " [activated]"
			}
			if acc.IsVesting {
				vs = " [vesting]"
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
		acc.FirstSeen = b.block.Height
		acc.LastSeen = b.block.Height
		acc.IsVesting = true
		acc.IsContract = true
		acc.IsFunded = true
		acc.UnclaimedBalance = v.Value

		// update block counters
		b.block.NewAccounts++
		b.block.SeenAccounts++
		b.block.NewContracts++
		b.block.FundedAccounts++
		b.block.Supply.Unvested += acc.UnclaimedBalance

		// link to and update delegate
		if v.Delegate.IsValid() {
			dlg, _ := b.AccountByAddress(v.Delegate)
			acc.IsDelegated = true
			acc.DelegateId = dlg.RowId
			acc.DelegatedSince = b.block.Height
			dlg.TotalDelegations++
			dlg.ActiveDelegations++
			dlg.DelegatedBalance += acc.Balance() // this includes unvested
			log.Debugf("1 BOOT ADD delegation %d %s -> %d (%d %s) bal=%d",
				dlg.RowId, dlg, dlg.ActiveDelegations, acc.RowId, acc, acc.Balance())
			// register delegation flows (will not be applied, just saved!)
			f := NewFlow(b.block, dlg, acc)
			f.Category = FlowCategoryDelegation
			f.Operation = FlowTypeDelegation
			f.AmountIn = acc.Balance()
			b.block.Flows = append(b.block.Flows, f)
		}

		b.accMap[acc.RowId] = acc
		b.accHashMap[accountHashKey(acc)] = acc

		// prepare for insert
		accounts = append(accounts, acc)

		// save as contract (not spendable, not delegatebale, no fee, gas, limits)
		cc := AllocContract()
		cc.Hash = acc.Hash
		cc.AccountId = acc.RowId
		cc.Height = acc.FirstSeen
		cc.Script, _ = v.Script.MarshalBinary()
		contracts = append(contracts, cc)

		log.Debug(newLogClosure(func() string {
			var as, vs, ds, rs string
			if acc.IsActivated {
				as = " [activated]"
			}
			if acc.IsVesting {
				vs = " [vesting]"
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

	// init chain and supply counters from block and flows
	b.block.Chain.Update(b.block, b.dlgMap)
	b.block.Supply.Update(b.block, b.dlgMap)

	// adjust unclaimed supply because supply.Update() subtracts genesis activation flows
	// which are not supposed to be accounted for here
	b.block.Supply.Unclaimed += b.block.Supply.Activated

	// set initial unclaimed accounts to number of blinded accounts
	b.block.Chain.UnclaimedAccounts += int64(len(gen.Commitments))

	// adjust total supply on init and cross-check
	b.block.Supply.Total += b.block.Supply.Activated + b.block.Supply.Unvested + b.block.Supply.Unclaimed

	if genesisSupply := gen.Supply(); b.block.Supply.Total != genesisSupply {
		return nil, fmt.Errorf("Genesis supply mismatch exp=%d got=%d (active=%d unvested=%d unclaimed=%d)",
			genesisSupply, b.block.Supply.Total,
			b.block.Supply.Activated, b.block.Supply.Unvested, b.block.Supply.Unclaimed)
	}

	return b.block, nil
}

func (b *Builder) FixUpgradeBugs(ctx context.Context, prevparams, nextparams *chain.Params) error {
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
	if nextparams.Protocol.IsEqual(chain.ProtoV005_2) && nextparams.ChainId.IsEqual(chain.Mainnet) {
		if err := b.RunBabylonAirdrop(ctx, nextparams); err != nil {
			return err
		}
	}

	return nil
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
func (b *Builder) FixOriginationBug(ctx context.Context, params *chain.Params) error {
	drop := make([]*Account, 0)
	var count int
	for _, dlg := range b.dlgMap {
		if dlg.DelegateId > 0 {
			continue
		}
		if dlg.Balance() > 0 {
			dlg.DelegateId = dlg.RowId
			dlg.InitGracePeriod(b.block.Cycle, params)
			dlg.IsDirty = true
			dlg.IsActiveDelegate = true
			count++
		} else {
			drop = append(drop, dlg)
		}
	}
	log.Infof("Upgrade to v%03d: registered %d buggy delegates", params.Version, count)

	// unregister empty accounts
	for _, v := range drop {
		b.UnregisterDelegate(v)
	}
	log.Infof("Upgrade to v%03d: dropped %d empty delegates", params.Version, len(drop))
	return nil
}

// v005 airdrops 1 mutez to unfunded manager accounts to avoid origination burn
func (b *Builder) RunBabylonAirdrop(ctx context.Context, params *chain.Params) error {
	// collect all eligible addresses and inject airdrop flows
	table, err := b.idx.Table(index.AccountTableKey)
	if err != nil {
		return err
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

	// HACK due to improperly documented v002 upgrade (i.e. origination bug fix),
	// we do not precisely know which accounts are delegates and which are not;
	// a manual QA inspection revealed that on mainnet there are 78 accounts
	// that were not airdropped because they are delegates, so we exclude them
	// here too
	excludeList := make(map[string]struct{})
	for _, v := range []string{
		"tz1ajpiR5wkPXghYDdT4tizu3BG8iy4WJLz4",
		"tz1NC7TTSyNwB5N7bQWXmafvJbCVrPKGNPcS",
		"tz1hoFUMWpvRWy4fMUgLGZjwe3i5xxtN1Qci",
		"tz1ZSr8MfNZsFQJ2Gt67rJfNFeJks2P7cgwr",
		"tz1f4U4NUdnMgP8rkPHvUBVznZsgUG636nhz",
		"tz1fntgFVaRT3jxaMyHaxVua7w2TaNcPKeZP",
		"tz1RCpatyxtpTEzXYqQjsz6r2VrhMeF3pCY6",
		"tz1TwzoBefS8PEbe91h3eTkYsA4QAQEBMcVL",
		"tz1bVXGLBa8qhHaymZ3yEwgHjiAE7MDom13K",
		"tz1S8ocaHL58fSrneqJeF6Ure4LSjarPcDDx",
		"tz1XB7RRogXyqoDPVcRLd9LS2kJoQRGT4Eje",
		"tz1LUWkTyB62ZFpvn8ZrqbaVDPekXzcVMuFd",
		"tz1Ua95YukXAmcMbfUv67gEhxiJx1n9djMiU",
		"tz1L6a3SsVqzvcxESxzqvEJpAcU8Hs4SSHEF",
		"tz1QRKeabUMA4dExyk1y12v1MwqibWoczoZU",
		"tz1Nthwqk6zjHei1tEGdj228Awt7VsN86c6b",
		"tz1X4C6KvSAkavFAexxCJNpdyYtP8bftRcoe",
		"tz1UVB4Yt8raLZq8AH9k386aqr7CG7qSMMjU",
		"tz1Q3fqvAJmijgABnHbbNm1ou81rvFcmBipM",
		"tz1cs4Q98YbsUfNpch7ijQHtEgMqvdzTvnhW",
		"tz1LVHUSTmfNHn1NpDa8Mz8vq1Sh5CCMXX4V",
		"tz1PPVuUuJR258nGtdHEsUSmBHHsvFeLrRTW",
		"tz1PygG8dRGV5vev2DALRAqmdYAqReTD8987",
		"tz1RQMjZjF2hg4ySfMCuZH5hAzNLziqTkazH",
		"tz1UHQ7YYDaxSV4dY8boJRhUfmU7jKprEsZw",
		"tz1cP3XjgyQ4xY3kCJbxXLbq2QzkeMFUFBoh",
		"tz1NEV1TPAeF68AiyLBUG7CPBFNJ1txVYqu1",
		"tz1XkRTJT7gn41VczW8dx1KQjPFxWYVei8Cs",
		"tz1gJvShTiuxoaZtjcwMv3LHcGU2QFqx5dsE",
		"tz1LmJsZuRyxswNV4YghF3q5fmLLxrKST3gp",
		"tz1VayoLunKK13JkS6ZpLfHvB193VaZLnU3N",
		"tz1W7roMZucBCjh8QgwwgJsjEazW2YgA7sJ5",
		"tz1MRHkVE9zxbAgho7uNuqAcmct17d3Ej9VS",
		"tz1QJVCDbrGkfEjcdWD1eXy71fXYtbNg93Gp",
		"tz1Mz7ZZu5Rgg2LamJmu2dzozZ2KZ8Jb2rLP",
		"tz1e9jBy9dEGER2dKrtzcWtCpDfbbLNPTQab",
		"tz1UrBsKAUybPbqZHKaNp8ru4F8NcW2e1inG",
		"tz1VDRt5NL44SEECAW7Qft8nSCjhDWvhYPrb",
		"tz1VUunMWp6tfK7T7QQQTBcsrnp713CmCDYi",
		"tz1eNUaSdwY7RJfb3aVXFwPc3tiG6HeCADnq",
		"tz1duEr8qA9y2PUkRYnA7qE2nwmUpunANcQg",
		"tz1dFhaP5bWLgBswYtBxpTFEXec7mmzBskNw",
		"tz1NaujomKqcKKacopVcQtqh32DTNaLAdcNb",
		"tz1PCPMQ7WC62WqGxgHB1G48wVUCmvTbmoAE",
		"tz1foqx9ArpckkTvwbPiV4kjoYsxnbQdSE3o",
		"tz1S3ucpKQrtkp8Bz7mw4LJ1zPVqmWufC5aS",
		"tz1YVWh2g8Lne3RrJukx7bESXKWzryiXvyyV",
		"tz1g9e5poiqG2V2SC7aya93MTKJt6pbyWrEk",
		"tz1UcuaXouNppYnbJr3JWGV31Fa2fnzesmJ4",
		"tz1P4CZSLSmD6VVUm9dqNFpy9eV3ZU1LwwbQ",
		"tz1TJY3ouYwqdcyPQFWU9DEy5q4Y5qEusPqY",
		"tz1e5NtW8mi6F6U8DfKaMwSeRaiPjrxKxT3V",
		"tz1hE2bwMvNAJJuSnTLjxfLCdLbkuZwRumsW",
		"tz1SQ3fSVjscp2vjmVSiyWQL9Yapt3y6FZHJ",
		"tz1gkWnVtzqzavL8PJNsDTVYyP8mLhdwqF45",
		"tz1fc7jqJ4YuJx9Diyb8b4iiWAto34p7pqRT",
		"tz1Kvszu74tzrfjZRYW9d1r7ePK81rHxsZUB",
		"tz1azWX5Ux5Hizb3qj1vHF5LZwwCMFA8b4mZ",
		"tz1XymQfBfSJMDoeCAMmseR5SiHKMXCWMaNy",
		"tz1P6nfhyAx8uUapcZSuFmYtBzv4RmwF6qvg",
		"tz1Pk341z4zeN8rRTX1HwWXMfbzSsn6dwEYo",
		"tz1LrFegiq14oByxgcS7vGFnorj9uYBed6bD",
		"tz1bkhnnvrtmwcryKzHGbKp48yS2qNMRDehA",
		"tz1NuXPd1qePQeMzsMTZQAqy8a8DSkqYUVcb",
		"tz1Rctu7qNj3RyAyz7kdyJjYkbYxeTpNFQRF",
		"tz1LS6oGf95DV7c2mSZ17C6RsuoEiD9EwGWc",
		"tz1TWQmJTfosQPFGUXjbXUzV6Tj23s8zbXUs",
		"tz1ffqW9CQ6aCD8zwcq5CLs8Gth335LWAEDJ",
		"tz1Qsa82diwpvMbsyi3t57KVyV6dGZX5zkSg",
		"tz1VQuud7J1kmBCrhcKYsYHU1FX5nkFjtLpu",
		"tz1e1BgVt3DZgA1AuTMTRGS2cgS2vGP3hMRE",
		"tz1Szcfqv3iTVSsTb11X8YCCnxRsFP6uK3v5",
		"tz1bg9WkHYxigQ7J4n2sufKWcPn955UrF3Kb",
		"tz1cQM6iWcptjU68FGfy1b7TNLr6aKUTQbTT",
		"tz1Qk2Q8Ju3YCSqPv9QxCEafSYZM1ZwTTcCn",
		"tz1WeuWTkfMaViHypSX7joYjWX8NApHHC2sq",
		"tz1djECaHtJXhYP1kbK4KgJ2EHpgCVjvANnQ",
		"tz1PAcQy7L3EqKLaYZjpJ7sUNRXWe4NNnmEc",
	} {
		excludeList[v] = struct{}{}
	}

	// find eligible KT1 contracts where we need to check the manager
	q := pack.Query{
		Name: "etl.addr.babylon_airdrop_eligible",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.AddressTypeContract),
			},
		},
	}
	managers := make([]uint64, 0)
	contract := &Account{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(contract); err != nil {
			return err
		}
		// skip all excluded contracts that do not match the rules above
		if contract.IsContract {
			if !contract.IsSpendable && !contract.IsDelegatable {
				return nil
			}
		}
		managers = append(managers, contract.ManagerId.Value())
		return nil
	})
	if err != nil {
		return err
	}

	// find unfunded managers
	q = pack.Query{
		Name: "etl.addr.babylon_airdrop",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("f"), // is_funded
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
			pack.Condition{
				Field: table.Fields().Find("I"), // pk
				Mode:  pack.FilterModeIn,
				Value: vec.UniqueUint64Slice(managers), // make list unique
			},
		},
	}
	var count int
	err = table.Stream(ctx, q, func(r pack.Row) error {
		acc := AllocAccount()
		if err := r.Decode(acc); err != nil {
			acc.Free()
			return err
		}

		// HACK: skip registered delegates unless they are v002 origination bug delegates
		if acc.IsDelegate && !(acc.DelegateSince < 28083 && acc.NDelegation == 0) {
			log.Debugf("airdrop: skipping delegate %s", acc)
			return nil
		}

		// HACK; skip by address
		if _, ok := excludeList[acc.String()]; ok {
			log.Debugf("airdrop: skipping v002 delegate %s", acc)
			return nil
		}

		flow := NewFlow(b.block, acc, nil)
		flow.Category = FlowCategoryBalance
		flow.Operation = FlowTypeAirdrop
		flow.AmountIn = 1
		b.block.Flows = append(b.block.Flows, flow)
		count++
		log.Debugf("airdrop: %s %f", acc, params.ConvertValue(flow.AmountIn))
		// add account to builder map if not exist
		if _, ok := b.accMap[acc.RowId]; !ok {
			b.accMap[acc.RowId] = acc
		} else {
			acc.Free()
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Infof("Upgrade to v%03d: executed %d airdrops", params.Version, count)
	return nil
}

// big_map_diffs in proto < v005 lack id and action. Also allocs are not explicit.
// In order to satisfy further processing logic we patch in an alloc when we see a
// new contract using a bigmap.
// Contracts before v005 can only own a single bigmap which makes life a bit easier.
// Note: on zeronet big_map is a regular map due to protocol bug
func (b *Builder) PatchBigMapDiff(ctx context.Context, diff micheline.BigMapDiff, accId AccountID, script *micheline.Script) (micheline.BigMapDiff, error) {
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
		if _, ok := script.Code.Storage.FindType(micheline.T_BIG_MAP); !ok {
			return nil, nil
		}
	}

	// log.Infof("Patching bigmap for account %d at height %d", accId, b.block.Height)

	// load contract
	contract, err := b.idx.LookupContractId(ctx, accId)
	if err != nil {
		return nil, err
	}

	// either script is set (origination) or we lookup the contract (transaction)
	if script == nil {
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
	id, ok := staticAthensBigmapIds[contract.String()]
	if !ok {
		return nil, fmt.Errorf("bigmap patch unknown contract %s", contract.String())
	}

	// check if bigmap is allocated
	var needAlloc bool
	if _, _, err := b.idx.LookupBigmap(ctx, id, false); err != nil {
		if err != index.ErrNoBigMapEntry {
			return nil, err
		}
		needAlloc = true
	}

	// inject a synthetic alloc to satisfy processing logic
	if needAlloc {
		// find bigmap type definition
		typ, ok := script.Code.Storage.FindType(micheline.T_BIG_MAP)
		if ok {
			// create alloc for new bigmaps
			alloc := micheline.BigMapDiffElem{
				Action:    micheline.BigMapDiffActionAlloc,
				Id:        id,                 // alloc new id
				KeyType:   typ.Args[0].OpCode, // (Left) == key_type
				ValueType: typ.Args[1],        // (Right) == value_type
			}
			// prepend
			diff = append([]micheline.BigMapDiffElem{alloc}, diff...)
			// log.Infof("Alloc bigmap %d for account %d at height %d", id, accId, b.block.Height)

			// set id on all items
			for i := range diff {
				diff[i].Id = id
			}
		} else {
			return nil, fmt.Errorf("missing bigmap type def for contract/account %d", accId)
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
