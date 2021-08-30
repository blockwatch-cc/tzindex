// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/index"
	. "blockwatch.cc/tzindex/etl/model"
)

type Builder struct {
	idx        *Indexer                // storage reference
	accHashMap map[uint64]*Account     // hash(acc_hash) -> *Account (both known and new accounts)
	accMap     map[AccountID]*Account  // id -> *Account (both known and new accounts)
	accCache   *cache.AccountCache     // cache for non-delegate accounts
	dlgHashMap map[uint64]*Account     // bakers by hash
	dlgMap     map[AccountID]*Account  // bakers by id
	conMap     map[AccountID]*Contract // smart contracts by account id

	// build state
	validate  bool        // enables validation
	rpc       *rpc.Client // used for validation
	block     *Block
	parent    *Block
	baking    []Right
	endorsing []Right
	branches  map[string]*Block
}

func NewBuilder(idx *Indexer, cachesz int, c *rpc.Client, validate bool) *Builder {
	return &Builder{
		idx:        idx,
		accHashMap: make(map[uint64]*Account),
		accMap:     make(map[AccountID]*Account),
		accCache:   cache.NewAccountCache(cachesz),
		dlgMap:     make(map[AccountID]*Account),
		dlgHashMap: make(map[uint64]*Account),
		conMap:     make(map[AccountID]*Contract),
		baking:     make([]Right, 0, 64),
		endorsing:  make([]Right, 0, 256),
		branches:   make(map[string]*Block, 128),
		validate:   validate,
		rpc:        c,
	}
}

func (b *Builder) Params(height int64) *tezos.Params {
	return b.idx.ParamsByHeight(height)
}

func (b *Builder) IsLightMode() bool {
	return b.idx.lightMode
}

func (b *Builder) ClearCache() {
	b.accCache.Purge()
}

func (b *Builder) CacheStats() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["accounts"] = b.accCache.Stats()
	return stats
}

func (b *Builder) RegisterDelegate(acc *Account, activate bool) {
	// remove from cache and regular account maps
	hashkey := b.accCache.AccountHashKey(acc)
	delete(b.accMap, acc.RowId)
	delete(b.accHashMap, hashkey)
	b.accCache.Drop(acc)

	// update state
	acc.IsDelegate = true
	acc.LastSeen = b.block.Height
	acc.DelegateSince = b.block.Height
	acc.InitGracePeriod(b.block.Cycle, b.block.Params)
	acc.IsDirty = true

	isMagic := v001MagicDelegateFilter.Contains(acc.Address)

	// only activate when explicitly requested
	if activate || isMagic {
		acc.DelegateId = acc.RowId
		b.ActivateDelegate(acc)
		if isMagic {
			// inject an implicit baker registration
			acc.DelegateId = acc.RowId
			b.AppendMagicBakerRegistrationOp(context.Background(), acc, 0)
		}
	}

	// add to delegate map
	b.dlgMap[acc.RowId] = acc
	b.dlgHashMap[hashkey] = acc
}

// only called ofrom rollback and bug fix code
func (b *Builder) UnregisterDelegate(acc *Account) {
	// reset delegate state
	acc.DelegateId = 0
	acc.IsDelegate = false
	acc.IsActiveDelegate = false
	acc.DelegateSince = 0
	acc.DelegateUntil = 0
	acc.TotalDelegations = 0
	acc.ActiveDelegations = 0
	acc.GracePeriod = 0
	acc.IsDirty = true
	// move from delegate map to accounts
	hashkey := b.accCache.AccountHashKey(acc)
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
	acc.DelegateUntil = b.block.Height
	acc.IsDirty = true
}

func (b *Builder) AccountByAddress(addr tezos.Address) (*Account, bool) {
	key := b.accCache.AddressHashKey(addr)
	// lookup delegate accounts first
	acc, ok := b.dlgHashMap[key]
	if ok && acc.Type == addr.Type && acc.Address.Equal(addr) {
		return acc, true
	}
	// lookup regular accounts second
	acc, ok = b.accHashMap[key]
	if ok && acc.Type == addr.Type && acc.Address.Equal(addr) {
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

func (b *Builder) ContractById(id AccountID) (*Contract, bool) {
	con, ok := b.conMap[id]
	return con, ok
}

func (b *Builder) LoadContractByAccountId(ctx context.Context, id AccountID) (*Contract, error) {
	if con, ok := b.conMap[id]; ok {
		return con, nil
	}
	con, err := b.idx.LookupContractId(ctx, id)
	if err != nil {
		return nil, err
	}
	b.conMap[id] = con
	return con, nil
}

func (b *Builder) BranchByHash(h tezos.BlockHash) (*Block, bool) {
	branch, ok := b.branches[h.String()]
	return branch, ok
}

func (b *Builder) Accounts() map[AccountID]*Account {
	return b.accMap
}

func (b *Builder) Delegates() map[AccountID]*Account {
	return b.dlgMap
}

func (b *Builder) Contracts() map[AccountID]*Contract {
	return b.conMap
}

func (b *Builder) Rights(typ tezos.RightType) []Right {
	switch typ {
	case tezos.RightTypeBaking:
		return b.baking
	case tezos.RightTypeEndorsing:
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

	// edge-case: when the crawler stops at the last block of a protocol
	// deployment the protocol version in the header has already switched
	// to the next protocol
	version := b.parent.Version
	p := b.idx.ParamsByHeight(-1)
	if p.IsCycleEnd(tip.BestHeight) && version > 0 {
		version--
	}
	b.parent.Params, err = b.idx.ParamsByDeployment(version)
	if err != nil {
		return err
	}

	// to make our crawler happy, we also expose the last block
	// on load, so any reports can be republished if necessary
	b.block = b.parent
	b.block.Parent, err = b.idx.BlockByID(ctx, b.block.ParentId)
	if err != nil {
		return err
	}

	// load all registered bakers; Note: if we ever want to change this,
	// the cache strategy needs to be reworked (because bakers are not kept
	// in regular cache)
	if dlgs, err := b.idx.ListAllDelegates(ctx); err != nil {
		return err
	} else {
		// log.Debugf("Loaded %d total bakers", len(dlgs))
		for _, acc := range dlgs {
			b.dlgMap[acc.RowId] = acc
			b.dlgHashMap[b.accCache.AccountHashKey(acc)] = acc
		}
	}

	// add inital block baker
	if acc, ok := b.dlgMap[b.block.BakerId]; ok {
		b.block.Baker = acc
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

	// 5  sanity checks
	if b.validate {
		if err := b.CheckState(ctx); err != nil {
			b.DumpState()
			return nil, fmt.Errorf("build stage 5: %v", err)
		}
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

		// keep bakers out of cache
		if acc.IsDelegate && !acc.MustDelete {
			continue
		}

		// cache funded accounts only
		if acc.IsFunded && !acc.MustDelete {
			b.accCache.Add(acc)
		} else {
			b.accCache.Drop(acc)
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
	b.conMap = make(map[AccountID]*Contract)
	b.baking = b.baking[:0]
	b.endorsing = b.endorsing[:0]

	// free previous parent block
	if b.parent != nil {
		b.parent.Free()
		b.parent = nil
	}

	// clear branches (keep most recent 64 blocks only)
	for n, v := range b.branches {
		if v.Height < b.block.Height-int64(b.block.TZ.Block.Metadata.MaxOperationsTTL)-1 {
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
	// flush cash
	b.accCache.Purge()

	// clear build state
	b.accHashMap = make(map[uint64]*Account)
	b.accMap = make(map[AccountID]*Account)
	b.conMap = make(map[AccountID]*Contract)

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

	// 5  validity checks (expensive)
	if b.validate {
		if err := b.CheckState(ctx); err != nil {
			return nil, fmt.Errorf("build stage 5: %v", err)
		}
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

		// keep bakers out of cache
		if acc.IsDelegate && !acc.MustDelete {
			continue
		}

		// add to cache, will be free'd on eviction
		b.accCache.Add(acc)
	}

	// clear build state
	b.accHashMap = make(map[uint64]*Account)
	b.accMap = make(map[AccountID]*Account)
	b.conMap = make(map[AccountID]*Contract)
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
	addresses := tezos.NewAddressSet()

	// guard against unknown deactivated bakers
	for _, v := range b.block.TZ.Block.Metadata.BalanceUpdates {
		addr := v.Address()
		if _, ok := b.AccountByAddress(addr); !ok && addr.IsValid() {
			addresses.AddUnique(addr)
		}
	}

	// add unknown deactivated bakers (from parent block: we're deactivating
	// AFTER a block has been fully indexed at the start of the next block)
	if b.parent != nil {
		for _, v := range b.parent.TZ.Block.Metadata.Deactivated {
			if _, ok := b.AccountByAddress(v); !ok {
				addresses.AddUnique(v)
			}
		}
	}

	// collect from ops
	var op_n int
	for op_l, oll := range b.block.TZ.Block.Operations {
		for op_p, oh := range oll {
			// init branches
			br := oh.Branch.String()
			if _, ok := b.branches[br]; !ok {
				branch, err := b.idx.BlockByHash(ctx, oh.Branch, b.block.Height-int64(b.block.TZ.Block.Metadata.MaxOperationsTTL)-1, b.block.Height)
				if err != nil {
					return fmt.Errorf("op %s [%d:%d]: invalid branch %s: %v", oh.Hash, op_l, op_p, oh.Branch, err)
				}
				b.branches[br] = branch
			}
			// parse operations
			for op_c, op := range oh.Contents {
				switch kind := op.OpKind(); kind {
				case tezos.OpTypeActivateAccount:
					// need to search for blinded key
					// account may already be activated and this is a second claim
					// don't look for and allocate new account, this happens in
					// op processing
					aop := op.(*rpc.AccountActivationOp)
					bkey, err := tezos.BlindAddress(aop.Pkh, aop.Secret)
					if err != nil {
						return fmt.Errorf("activation op [%d:%d]: blinded address creation failed: %v",
							op_n, op_c, err)
					}
					addresses.AddUnique(bkey)

				case tezos.OpTypeBallot:
					// deactivated bakers can still cast votes
					addr := op.(*rpc.BallotOp).Source
					if _, ok := b.AccountByAddress(addr); !ok {
						addresses.AddUnique(addr)
					}

				case tezos.OpTypeDelegation:
					del := op.(*rpc.DelegationOp)
					addresses.AddUnique(del.Source)

					// deactive bakers may not be in map
					if del.Delegate.IsValid() {
						if _, ok := b.AccountByAddress(del.Delegate); !ok {
							addresses.AddUnique(del.Delegate)
						}
					}

				case tezos.OpTypeDoubleBakingEvidence:
					// empty

				case tezos.OpTypeDoubleEndorsementEvidence:
					// empty

				case tezos.OpTypeEndorsement:
					// deactive bakers may not be in map
					end := op.(*rpc.EndorsementOp)
					if _, ok := b.AccountByAddress(end.Metadata.Address()); !ok {
						addresses.AddUnique(end.Metadata.Address())
					}

				case tezos.OpTypeOrigination:
					orig := op.(*rpc.OriginationOp)
					addresses.AddUnique(orig.Source)
					if m := orig.Manager(); m.IsValid() {
						addresses.AddUnique(m)
					}
					for _, v := range orig.Metadata.Result.OriginatedContracts {
						addresses.AddUnique(v)
					}
					if orig.Delegate != nil {
						if _, ok := b.AccountByAddress(*orig.Delegate); !ok {
							addresses.AddUnique(*orig.Delegate)
						}
					}

				case tezos.OpTypeProposals:
					// deactivated bakers can still send proposals
					addr := op.(*rpc.ProposalsOp).Source
					if _, ok := b.AccountByAddress(addr); !ok {
						addresses.AddUnique(addr)
					}

				case tezos.OpTypeReveal:
					addresses.AddUnique(op.(*rpc.RevelationOp).Source)

				case tezos.OpTypeSeedNonceRevelation:
					// not necessary because this is done by the baker

				case tezos.OpTypeTransaction:
					tx := op.(*rpc.TransactionOp)
					addresses.AddUnique(tx.Source)
					addresses.AddUnique(tx.Destination)
					for _, res := range tx.Metadata.InternalResults {
						if res.Destination != nil {
							addresses.AddUnique(*res.Destination)
						}
						if res.Delegate != nil {
							addresses.AddUnique(*res.Delegate)
						}
						for _, v := range res.Result.OriginatedContracts {
							addresses.AddUnique(v)
						}
					}
				}
			}
			op_n++
		}
	}

	// collect from implicit block ops
	for i, op := range b.block.TZ.Block.Metadata.ImplicitOperationsResults {
		switch op.Kind {
		case tezos.OpTypeOrigination:
			for _, v := range op.OriginatedContracts {
				addresses.AddUnique(v)
			}
		case tezos.OpTypeTransaction:
			for _, v := range op.BalanceUpdates {
				addresses.AddUnique(v.Address())
			}
		default:
			return fmt.Errorf("implicit block op [%d]: unsupported op branch %s", i, op.Kind)
		}
	}

	// collect unknown/unloaded bakers for lookup or creation
	unknownDelegateIds := make([]uint64, 0)

	// fetch baking and endorsing rights for this block
	if !b.idx.lightMode {
		table, err := b.idx.Table(index.RightsTableKey)
		if err != nil {
			return err
		}
		err = pack.NewQuery("etl.rights.search", table).
			AndEqual("height", b.block.Height).
			AndEqual("type", tezos.RightTypeBaking).
			Execute(ctx, &b.baking)
		if err != nil {
			return err
		}
		if b.block.Height > 2 && len(b.baking) == 0 {
			return fmt.Errorf("empty baking rights")
		}

		// endorsements are for block-1
		err = pack.NewQuery("etl.rights.search", table).
			AndEqual("height", b.block.Height-1).
			AndEqual("type", tezos.RightTypeEndorsing).
			Execute(ctx, &b.endorsing)
		if err != nil {
			return err
		}
		if b.block.Height > 2 && len(b.endorsing) == 0 {
			return fmt.Errorf("empty endorsing rights")
		}

		// collect from rights: on genesis and when deactivated, bakers are not in map
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
	}

	// collect from future cycle rights when available
	for _, r := range b.block.TZ.Baking {
		a := r.Address()
		if _, ok := b.AccountByAddress(a); !ok {
			addresses.AddUnique(a)
		}
	}
	for _, r := range b.block.TZ.Endorsing {
		a := r.Address()
		if _, ok := b.AccountByAddress(a); !ok {
			addresses.AddUnique(a)
		}
	}

	// collect from invoices
	if b.parent != nil {
		parentProtocol := b.parent.TZ.Block.Metadata.Protocol
		blockProtocol := b.block.TZ.Block.Metadata.Protocol
		if !parentProtocol.Equal(blockProtocol) {
			for n, _ := range b.block.Params.Invoices {
				if a, err := tezos.ParseAddress(n); err == nil {
					addresses.AddUnique(a)
				}
			}
		}
	}

	// delegation to inactive bakers is not explicitly forbidden, so
	// we have to check if any inactive (or deactivated) delegate is still
	// referenced

	// search cached accounts and build map
	lookup := make([][]byte, 0)
	for _, v := range addresses.Map() {
		if !v.IsValid() {
			continue
		}

		// lookup in cache first
		hashKey, acc, ok := b.accCache.GetAddress(v)
		if ok {
			b.accHashMap[hashKey] = acc
			b.accMap[acc.RowId] = acc

			// collect unknown bakers when referenced
			if acc.DelegateId > 0 {
				if _, ok := b.AccountById(acc.DelegateId); !ok {
					unknownDelegateIds = append(unknownDelegateIds, acc.DelegateId.Value())
				}
			}

		} else {
			// skip bakers
			if _, ok := b.dlgHashMap[hashKey]; ok {
				continue
			}
			// skip duplicate addresses
			if _, ok := b.accHashMap[hashKey]; ok {
				continue
			}

			// when not found, create a new account and schedule for lookup
			acc = NewAccount(v)
			acc.FirstSeen = b.block.Height
			acc.LastSeen = b.block.Height
			lookup = append(lookup, v.Bytes22())
			// store in map, will be overwritten when resolved from db
			// or kept as new address when this is the first time we see it
			b.accHashMap[hashKey] = acc
		}
	}

	// lookup addr by hashes (non-existent addrs are expected to not resolve)
	table, err := b.idx.Table(index.AccountTableKey)
	if err != nil {
		return err
	}
	if len(lookup) > 0 {
		err = pack.NewQuery("etl.addr_hash.search", table).
			AndIn("address", lookup).
			Stream(ctx, func(r pack.Row) error {
				acc := AllocAccount()
				if err := r.Decode(acc); err != nil {
					return err
				}

				// skip bakers (should have not been looked up in the first place)
				if acc.IsDelegate {
					acc.Free()
					return nil
				}

				// collect unknown bakers when referenced
				if acc.DelegateId > 0 {
					if _, ok := b.AccountById(acc.DelegateId); !ok {
						unknownDelegateIds = append(unknownDelegateIds, acc.DelegateId.Value())
					}
				}

				hashKey := b.accCache.AccountHashKey(acc)

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

	// lookup inactive bakers
	if len(unknownDelegateIds) > 0 {
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
			// log.Warnf("Found unregistered baker %d %s", acc.RowId, acc)
			// handle like a regular account, overwrite account map
			hashKey := b.accCache.AccountHashKey(acc)
			b.accHashMap[hashKey] = acc
			b.accMap[acc.RowId] = acc
			return nil
		})
		if err != nil {
			return err
		}
	}

	// collect new addrs and bulk insert to generate ids
	// Note: due to random map walk in Go, address id allocation is non-deterministic,
	//       since address deletion on reorgs is also non-deterministic, we don't give
	//       any guarantee about row_ids. In fact, they may even differ between
	//       multiple instances of the indexer who saw different reorgs.
	//       Keep this in mind for downstream use of ids and databases!
	newacc := make([]pack.Item, 0)
	for _, v := range b.accHashMap {
		if v.IsNew {
			if v.RowId > 0 {
				log.Warnf("Existing account %d %s with NEW flag", v.RowId, v)
				continue
			}
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
		// first step: handle deactivated bakers from parent block
		// this is idempotent
		if b.block.Params.IsCycleStart(b.block.Height) && b.block.Height > 0 {
			// deactivate based on grace period
			for _, dlg := range b.dlgMap {
				if dlg.IsActiveDelegate && dlg.GracePeriod < b.block.Cycle {
					if rollback {
						b.ActivateDelegate(dlg)
					} else {
						b.DeactivateDelegate(dlg)
					}
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
						log.Warnf("Delegate %s forcefully deactivated with grace period %d at cycle %d",
							acc, acc.GracePeriod, b.block.Cycle-1)
						b.DeactivateDelegate(acc)
						// return fmt.Errorf("deactivate: found non-deactivated delegate %s", v)
					}
				}
			}
		}

		// check for new protocol
		parentProtocol := b.parent.TZ.Block.Metadata.Protocol
		blockProtocol := b.block.TZ.Block.Metadata.Protocol
		if !parentProtocol.Equal(blockProtocol) {
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
				err := b.MigrateProtocol(ctx, prevparams, nextparams)
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

		// update baker software version from block nonce
		baker.SetVersion(b.block.Nonce)

		// handle grace period
		if baker.IsActiveDelegate {
			baker.UpdateGracePeriod(b.block.Cycle, b.block.Params)
		} else {
			// reset after inactivity
			baker.IsActiveDelegate = true
			baker.DelegateUntil = 0
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
				if !b.idx.lightMode {
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
				if !b.idx.lightMode {
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
		}
	}

	// collect data from header balance updates, note these are no explicit 'operations'
	// in Tezos (i.e. no op hash exists); handles baker rewards, deposits, unfreeze
	// and seed nonce slashing
	if err := b.AppendImplicitOps(ctx); err != nil {
		return err
	}

	// collect implicit ops from protocol migrations and things like
	// Granada+ liquidity baking
	if err := b.AppendImplicitBlockOps(ctx); err != nil {
		return err
	}

	// process operations
	// - create new op and flow objects
	// - init/update accounts
	// - sum op volume, fees, rewards, deposits
	// - attach extra data if defined
	for op_l, ol := range b.block.TZ.Block.Operations {
		for op_p, oh := range ol {
			for op_c, o := range oh.Contents {
				var err error
				switch kind := o.OpKind(); kind {
				case tezos.OpTypeActivateAccount:
					err = b.AppendActivationOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeBallot:
					err = b.AppendBallotOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeDelegation:
					err = b.AppendDelegationOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeDoubleBakingEvidence:
					err = b.AppendDoubleBakingOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeDoubleEndorsementEvidence:
					err = b.AppendDoubleEndorsingOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeEndorsement:
					err = b.AppendEndorsementOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeOrigination:
					err = b.AppendOriginationOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeProposals:
					err = b.AppendProposalsOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeReveal:
					err = b.AppendRevealOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeSeedNonceRevelation:
					err = b.AppendSeedNonceOp(ctx, oh, op_l, op_p, op_c, rollback)
				case tezos.OpTypeTransaction:
					err = b.AppendTransactionOp(ctx, oh, op_l, op_p, op_c, rollback)
				}
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *Builder) ApplyInvoices(ctx context.Context) error {
	var count int
	for n, v := range b.block.Params.Invoices {
		addr, err := tezos.ParseAddress(n)
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
		if err := b.AppendInvoiceOp(ctx, acc, v, count); err != nil {
			return err
		}
		// log.Debugf("invoice: %s %f", acc, b.block.Params.ConvertValue(v))
		count++
	}
	return nil
}

func (b *Builder) RollbackInvoices(ctx context.Context) error {
	var count int
	for n, v := range b.block.Params.Invoices {
		addr, err := tezos.ParseAddress(n)
		if err != nil {
			return fmt.Errorf("decoding invoice address %s: %v", n, err)
		}
		acc, ok := b.AccountByAddress(addr)
		if !ok {
			return fmt.Errorf("rollback invoice: unknown invoice account %s", n, err)
		}
		if err := b.AppendInvoiceOp(ctx, acc, v, count); err != nil {
			return err
		}
		if acc.FirstSeen == b.block.Height {
			acc.MustDelete = true
		}
		count++
	}
	return nil
}

// update counters, totals and sums from flows and ops
func (b *Builder) UpdateStats(ctx context.Context) error {
	// init pre-funded state
	for _, acc := range b.accMap {
		acc.WasFunded = acc.Balance() > 0
	}
	for _, acc := range b.dlgMap {
		acc.WasFunded = acc.Balance() > 0
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
		// skip undelegated accounts and self-bakers
		if !acc.IsDelegated || acc.DelegateId == acc.RowId {
			continue
		}
		dlg, ok := b.AccountById(acc.DelegateId)
		if !ok {
			// in rare circumstances an account has an invalid delegate set
			log.Errorf("Missing delegate %d for account %s", acc.DelegateId, acc)
			continue
		}
		if !acc.IsFunded && acc.WasFunded {
			// remove active delegation
			dlg.ActiveDelegations--
			dlg.IsDirty = true
		} else if acc.IsFunded && !acc.WasFunded {
			// re-add active delegation
			dlg.ActiveDelegations++
			dlg.IsDirty = true
		}
	}

	// count endorsement slots missed
	if !b.idx.lightMode {
		erights := make(map[AccountID]int)
		var count int
		for _, r := range b.endorsing {
			acc, ok := b.AccountById(r.AccountId)
			if !ok {
				return fmt.Errorf("missing endorsement delegate %d: %#v", r.AccountId, r)
			}
			num, _ := erights[acc.RowId]
			c := vec.NewBitSetFromBytes(r.Slots, b.block.Params.EndorsersPerBlock).Count()
			erights[acc.RowId] = num + int(c)
			count += int(c)
		}

		for _, op := range b.block.Ops {
			if op.Type != tezos.OpTypeEndorsement {
				continue
			}
			o, _ := b.block.GetRpcOp(op.OpL, op.OpP, op.OpC)
			eop := o.(*rpc.EndorsementOp)
			acc, _ := b.AccountByAddress(eop.Metadata.Address())
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
		acc.WasFunded = acc.Balance() > 0
	}
	for _, acc := range b.dlgMap {
		acc.WasFunded = acc.Balance() > 0
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
			if !acc.IsFunded && acc.WasFunded {
				// remove active delegation
				dlg.ActiveDelegations--
				dlg.IsDirty = true
			}
			if acc.IsFunded && !acc.WasFunded {
				// re-add active delegation
				dlg.ActiveDelegations++
				dlg.IsDirty = true
			}
		}
	}

	// update supplies and totals
	b.block.Rollback(b.accMap, b.dlgMap)
	b.block.Chain.Rollback(b.block)
	b.block.Supply.Rollback(b.block)
	return nil
}
