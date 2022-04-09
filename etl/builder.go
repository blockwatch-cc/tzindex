// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

type Builder struct {
	idx          *Indexer                            // storage reference
	accHashMap   map[uint64]*model.Account           // hash(acc_hash) -> *Account (both known and new accounts)
	accMap       map[model.AccountID]*model.Account  // id -> *Account (both known and new accounts)
	accCache     *cache.AccountCache                 // cache for binary encodings of regular accounts
	bakerHashMap map[uint64]*model.Baker             // bakers by hash
	bakerMap     map[model.AccountID]*model.Baker    // bakers by id
	conMap       map[model.AccountID]*model.Contract // smart contracts by account id
	constDict    micheline.ConstantDict              // global constants used in smart contracts this block

	// build state
	validate bool
	rpc      *rpc.Client
	block    *model.Block
	parent   *model.Block
}

const buildMapSizeHint = 1024

func NewBuilder(idx *Indexer, cachesz int, c *rpc.Client, validate bool) *Builder {
	return &Builder{
		idx:          idx,
		accHashMap:   make(map[uint64]*model.Account, buildMapSizeHint),
		accMap:       make(map[model.AccountID]*model.Account, buildMapSizeHint),
		accCache:     cache.NewAccountCache(cachesz),
		bakerMap:     make(map[model.AccountID]*model.Baker, buildMapSizeHint),
		bakerHashMap: make(map[uint64]*model.Baker, buildMapSizeHint),
		conMap:       make(map[model.AccountID]*model.Contract, buildMapSizeHint),
		validate:     validate,
		rpc:          c,
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

func (b *Builder) RegisterBaker(acc *model.Account, activate bool) *model.Baker {
	// update state for already registered bakers
	baker, ok := b.bakerMap[acc.RowId]
	if ok {
		baker.IsActive = true
		baker.IsDirty = true
		baker.InitGracePeriod(b.block.Cycle, b.block.Params)
		baker.Account.BakerId = baker.Account.RowId
		baker.Account.LastSeen = b.block.Height
		baker.Account.IsDirty = true
		return baker
	}

	// remove from cache and regular account maps
	hashkey := b.accCache.AccountHashKey(acc)
	delete(b.accMap, acc.RowId)
	delete(b.accHashMap, hashkey)
	b.accCache.Drop(acc)

	// update state
	acc.IsBaker = true
	acc.LastSeen = b.block.Height
	acc.IsDirty = true
	baker = model.NewBaker(acc)
	baker.BakerSince = b.block.Height
	baker.InitGracePeriod(b.block.Cycle, b.block.Params)
	baker.IsDirty = true

	isMagic := v001MagicBakerFilter.Contains(acc.Address)

	// only activate when explicitly requested
	if activate || isMagic {
		b.ActivateBaker(baker)
		acc.BakerId = acc.RowId
		if isMagic {
			// inject an implicit baker registration
			b.AppendMagicBakerRegistrationOp(context.Background(), baker, 0)
		}
	}

	// add to delegate map
	b.bakerMap[acc.RowId] = baker
	b.bakerHashMap[hashkey] = baker
	return baker
}

// only called from rollback and bug fix code
func (b *Builder) UnregisterBaker(baker *model.Baker) {
	// reset state
	acc := baker.Account
	acc.BakerId = 0
	acc.IsBaker = false
	acc.IsDirty = true

	// move from baker map to accounts
	hashkey := b.accCache.AccountHashKey(acc)
	b.accMap[acc.RowId] = acc
	b.accHashMap[hashkey] = acc
	delete(b.bakerMap, acc.RowId)
	delete(b.bakerHashMap, hashkey)
}

func (b *Builder) ActivateBaker(bkr *model.Baker) {
	bkr.IsActive = true
	bkr.IsDirty = true
}

func (b *Builder) DeactivateBaker(bkr *model.Baker) {
	bkr.IsActive = false
	bkr.BakerUntil = b.block.Height
	bkr.IsDirty = true
}

func (b *Builder) AccountByAddress(addr tezos.Address) (*model.Account, bool) {
	key := b.accCache.AddressHashKey(addr)
	bkr, ok := b.bakerHashMap[key]
	if ok && bkr.Address.Equal(addr) {
		return bkr.Account, true
	}
	acc, ok := b.accHashMap[key]
	if ok && acc.Address.Equal(addr) {
		return acc, true
	}
	return nil, false
}

func (b *Builder) AccountById(id model.AccountID) (*model.Account, bool) {
	bkr, ok := b.bakerMap[id]
	if ok {
		return bkr.Account, true
	}
	acc, ok := b.accMap[id]
	return acc, ok
}

func (b *Builder) BakerByAddress(addr tezos.Address) (*model.Baker, bool) {
	key := b.accCache.AddressHashKey(addr)
	bkr, ok := b.bakerHashMap[key]
	if ok && bkr.Address.Equal(addr) {
		return bkr, true
	}
	return nil, false
}

func (b *Builder) BakerById(id model.AccountID) (*model.Baker, bool) {
	bkr, ok := b.bakerMap[id]
	return bkr, ok
}

func (b *Builder) ContractById(id model.AccountID) (*model.Contract, bool) {
	con, ok := b.conMap[id]
	return con, ok
}

func (b *Builder) LoadContractByAccountId(ctx context.Context, id model.AccountID) (*model.Contract, error) {
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

func (b *Builder) Accounts() map[model.AccountID]*model.Account {
	return b.accMap
}

func (b *Builder) Bakers() map[model.AccountID]*model.Baker {
	return b.bakerMap
}

func (b *Builder) Contracts() map[model.AccountID]*model.Contract {
	return b.conMap
}

func (b *Builder) Constants() micheline.ConstantDict {
	return b.constDict
}

func (b *Builder) Table(key string) (*pack.Table, error) {
	return b.idx.Table(key)
}

func (b *Builder) Init(ctx context.Context, tip *model.ChainTip, c *rpc.Client) error {
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
	p := b.idx.ParamsByHeight(tip.BestHeight)
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

	// load all registered bakers, bakers are always kept in builder
	// but not in account cache
	if bkrs, err := b.idx.ListBakers(ctx, false); err != nil {
		return err
	} else {
		// log.Debugf("Loaded %d total bakers", len(bkrs))
		for _, bkr := range bkrs {
			b.bakerMap[bkr.AccountId] = bkr
			b.bakerHashMap[b.accCache.AccountHashKey(bkr.Account)] = bkr
		}
	}

	// add inital block baker
	if bkr, ok := b.bakerMap[b.block.BakerId]; ok {
		b.block.Baker = bkr
	}
	if bkr, ok := b.bakerMap[b.block.ProposerId]; ok {
		b.block.Proposer = bkr
	}

	return nil
}

func (b *Builder) Build(ctx context.Context, tz *rpc.Bundle) (*model.Block, error) {
	// 1  create a new block structure to house extracted data
	var err error
	if b.block, err = model.NewBlock(tz, b.parent); err != nil {
		return nil, fmt.Errorf("build stage 1: %w", err)
	}

	// build genesis accounts at height 1 and return
	if b.block.TZ.Block.Header.Content != nil {
		return b.BuildGenesisBlock(ctx)
	}

	// 2  lookup accounts or create new accounts
	if err := b.InitAccounts(ctx); err != nil {
		return nil, fmt.Errorf("build stage 2: %w", err)
	}

	// 3  unpack operations and update accounts
	if err := b.Decorate(ctx, false); err != nil {
		return nil, fmt.Errorf("build stage 3: %w", err)
	}

	// 4  update block stats when all data is resolved
	if err := b.UpdateStats(ctx); err != nil {
		return nil, fmt.Errorf("build stage 4: %w", err)
	}

	// 5  sanity checks
	if b.validate {
		if err := b.AuditState(ctx, 0); err != nil {
			b.DumpState()
			return nil, fmt.Errorf("build stage 5: %w", err)
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
		acc.WasDust = false

		// keep bakers out of cache
		if acc.IsBaker && !acc.MustDelete {
			continue
		}

		// cache funded accounts only
		if acc.IsFunded && !acc.MustDelete {
			b.accCache.Add(acc)
		} else {
			b.accCache.Drop(acc)
		}
	}

	for _, bkr := range b.bakerMap {
		// reset flags
		bkr.IsNew = false
		bkr.IsDirty = false
		acc := bkr.Account
		acc.IsDirty = false
		acc.IsNew = false
		acc.WasFunded = false
		acc.WasDust = false
	}

	// clear build state, keep allocated maps around
	for n, _ := range b.accHashMap {
		delete(b.accHashMap, n)
	}
	for n, _ := range b.accMap {
		delete(b.accMap, n)
	}
	for n, _ := range b.conMap {
		delete(b.conMap, n)
	}
	b.constDict = nil

	// drop block contents
	b.block.Clean()

	// keep current block as parent
	if b.parent != nil {
		b.parent.Free()
		b.parent = nil
	}
	b.parent = b.block
	b.block = nil
}

// remove state on error
func (b *Builder) Purge() {
	// flush cash
	b.accCache.Purge()

	// clear build state
	b.accHashMap = make(map[uint64]*model.Account, buildMapSizeHint)
	b.accMap = make(map[model.AccountID]*model.Account, buildMapSizeHint)
	b.conMap = make(map[model.AccountID]*model.Contract, buildMapSizeHint)
	b.constDict = nil

	// clear delegate state
	b.bakerHashMap = make(map[uint64]*model.Baker, buildMapSizeHint)
	b.bakerMap = make(map[model.AccountID]*model.Baker, buildMapSizeHint)

	// free previous parent block
	if b.parent != nil {
		b.parent.Free()
		b.parent = nil
	}

	if b.block != nil {
		b.block.Free()
		b.block = nil
	}
}

// during reorg rpc and model blocks are already loaded, parent data is ignored
func (b *Builder) BuildReorg(ctx context.Context, tz *rpc.Bundle, parent *model.Block) (*model.Block, error) {
	// use parent as builder parent
	b.parent = parent

	// build reorg block
	var err error
	if b.block, err = model.NewBlock(tz, parent); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 1: %w", tz.Block.Header.Level, err)
	}

	// 2  lookup accounts
	if err := b.InitAccounts(ctx); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 2: %w", b.block.Height, err)
	}

	// 3  unpack operations and update accounts
	if err := b.Decorate(ctx, true); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 3: %w", b.block.Height, err)
	}

	// 4  stats
	if err := b.RollbackStats(ctx); err != nil {
		return nil, fmt.Errorf("block %d reorg-build stage 4: %w", b.block.Height, err)
	}

	// 5  validity checks (expensive)
	if b.validate {
		if err := b.AuditState(ctx, -1); err != nil {
			return nil, fmt.Errorf("build stage 5: %w", err)
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
		acc.WasDust = false

		// keep bakers out of cache
		if acc.IsBaker && !acc.MustDelete {
			continue
		}

		// add to cache, will be free'd on eviction
		b.accCache.Add(acc)
	}

	// clear build state
	b.accHashMap = make(map[uint64]*model.Account, buildMapSizeHint)
	b.accMap = make(map[model.AccountID]*model.Account, buildMapSizeHint)
	b.conMap = make(map[model.AccountID]*model.Contract, buildMapSizeHint)
	b.constDict = nil

	for _, bkr := range b.bakerMap {
		// reset flags
		bkr.IsNew = false
		bkr.IsDirty = false
		acc := bkr.Account
		acc.IsDirty = false
		acc.IsNew = false
		acc.WasFunded = false
		acc.WasDust = false
	}

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
	addUnique := func(a tezos.Address) {
		if !a.IsValid() {
			return
		}
		if _, ok := b.AccountByAddress(a); !ok {
			addresses.AddUnique(a)
		}
	}

	// collect from ops
	for _, oll := range b.block.TZ.Block.Operations {
		for _, oh := range oll {
			// check for pruned metadata
			if len(oh.Metadata) > 0 {
				return fmt.Errorf("metadata %s! Your archive node has pruned metadata and must be rebuilt. Always run Octez v12.x with --metadata-size-limit unlimited. Sorry for your trouble.", oh.Metadata)
			}
			// parse operations
			for _, op := range oh.Contents {
				switch kind := op.Kind(); kind {
				case tezos.OpTypeActivateAccount:
					// need to search for blinded key
					// account may already be activated and this is a second claim
					// don't look for and allocate new account, this happens in
					// op processing
					aop := op.(*rpc.Activation)
					bkey, err := tezos.BlindAddress(aop.Pkh, aop.Secret)
					if err != nil {
						return fmt.Errorf("activation op %s: blinded address creation failed: %w", oh.Hash, err)
					}
					addUnique(bkey)

				case tezos.OpTypeDelegation:
					del := op.(*rpc.Delegation)
					addUnique(del.Source)
					// not required, not even for baker registration
					// addUnique(del.Delegate)

				case tezos.OpTypeOrigination:
					orig := op.(*rpc.Origination)
					addUnique(orig.Source)
					addUnique(orig.ManagerAddress())
					for _, v := range orig.Metadata.Result.OriginatedContracts {
						addresses.AddUnique(v)
					}

				case tezos.OpTypeReveal:
					addUnique(op.(*rpc.Reveal).Source)

				case tezos.OpTypeTransaction:
					tx := op.(*rpc.Transaction)
					addUnique(tx.Source)
					addUnique(tx.Destination)
					for _, res := range tx.Metadata.InternalResults {
						addUnique(res.Destination)
						addUnique(res.Delegate)
						for _, v := range res.Result.OriginatedContracts {
							addresses.AddUnique(v)
						}
					}

				case tezos.OpTypeRegisterConstant:
					addUnique(op.(*rpc.ConstantRegistration).Source)

				// all bakers are already known
				// case tezos.OpTypeBallot:
				// case tezos.OpTypeProposals:
				// case tezos.OpTypeSeedNonceRevelation:
				// case tezos.OpTypeDoubleBakingEvidence:
				// case tezos.OpTypeDoubleEndorsementEvidence:
				// case tezos.OpTypeDoublePreendorsementEvidence:
				// case tezos.OpTypeEndorsement, tezos.OpTypeEndorsementWithSlot:
				case tezos.OpTypeSetDepositsLimit:
					// successful ops have a baker, but failed ops may use
					// a non-baker account
					dop := op.(*rpc.SetDepositsLimit)
					if !dop.Result().Status.IsSuccess() {
						addUnique(dop.Source)
					}
				}
			}
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
				addUnique(v.Address())
			}
		default:
			return fmt.Errorf("implicit block op [%d]: unsupported op %s", i, op.Kind)
		}
	}

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
		} else {
			// skip bakers
			if _, ok := b.bakerHashMap[hashKey]; ok {
				continue
			}
			// skip duplicate addresses
			if _, ok := b.accHashMap[hashKey]; ok {
				continue
			}

			// when not found, create a new account and schedule for lookup
			acc = model.NewAccount(v)
			acc.FirstSeen = b.block.Height
			acc.LastSeen = b.block.Height
			lookup = append(lookup, v.Bytes22())
			// log.Infof("Lookup addr %s %x", v, v.Bytes22())
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
				acc := model.AllocAccount()
				if err := r.Decode(acc); err != nil {
					return err
				}

				// skip bakers (should have not been looked up in the first place)
				if acc.IsBaker {
					acc.Free()
					return nil
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
			acc := v.(*model.Account)
			b.accMap[acc.RowId] = acc
		}
	}

	// identify the baker account id (should be in delegate map)
	// Note: blocks 0 and 1 don't have a baker, blocks 0-2 have no endorsements
	// Note: Ithaca introduces a block proposer (build payload) which may be
	// different from the baker, both get rewards
	if addr := b.block.TZ.Block.Metadata.Baker; addr.IsValid() {
		baker, ok := b.BakerByAddress(addr)
		if !ok {
			return fmt.Errorf("missing baker account %s", addr)
		}
		b.block.Baker = baker
		b.block.BakerId = baker.AccountId
	}

	if addr := b.block.TZ.Block.Metadata.Proposer; addr.IsValid() {
		proposer, ok := b.BakerByAddress(addr)
		if !ok {
			return fmt.Errorf("missing proposer account %s", addr)
		}
		b.block.Proposer = proposer
		b.block.ProposerId = proposer.AccountId
	} else {
		b.block.Proposer = b.block.Baker
		b.block.ProposerId = b.block.BakerId
	}

	return nil
}

func (b *Builder) Decorate(ctx context.Context, rollback bool) error {
	// handle upgrades and end of cycle events right before processing the next block
	if b.parent != nil {
		// first step: handle deactivated bakers from parent block
		if err := b.OnDeactivate(ctx, rollback); err != nil {
			return err
		}

		// check for new protocol
		if err := b.OnUpgrade(ctx, rollback); err != nil {
			return err
		}
	}

	// collect data from header balance updates, note these are no explicit 'operations'
	// in Tezos (i.e. no op hash exists); handles baker rewards, deposits, unfreeze
	// and seed nonce slashing
	if err := b.AppendImplicitEvents(ctx); err != nil {
		return err
	}

	// collect implicit ops from protocol migrations and things like
	// Granada+ liquidity baking
	if err := b.AppendImplicitBlockOps(ctx); err != nil {
		return err
	}

	// process regular operations
	// - create new op and flow objects
	// - init/update accounts
	// - sum op volume, fees, rewards, deposits
	// - attach extra data if defined
	if err := b.AppendRegularBlockOps(ctx, rollback); err != nil {
		return err
	}

	// load global constants that are referenced by newly originated contracts
	if err := b.LoadConstants(ctx); err != nil {
		return err
	}

	return nil
}

func (b *Builder) OnDeactivate(ctx context.Context, rollback bool) error {
	if b.block.Params.IsCycleStart(b.block.Height) && b.block.Height > 0 {
		// deactivate based on grace period
		for _, bkr := range b.bakerMap {
			if bkr.IsActive && bkr.GracePeriod < b.block.Cycle {
				if rollback {
					b.ActivateBaker(bkr)
				} else {
					b.DeactivateBaker(bkr)
					// log.Infof("deactivate: baker %s %d beyond grace period at block %d",
					// 	bkr, bkr.RowId, b.block.Height)
				}
			}
		}

		// cross check if we have missed a deactivation from cycle before
		// we still keep the deactivated list around in parent block
		for _, v := range b.parent.TZ.Block.Metadata.Deactivated {
			bkr, ok := b.BakerByAddress(v)
			if !ok {
				return fmt.Errorf("deactivate: missing baker account %s", v)
			}
			if rollback {
				if !bkr.IsActive {
					return fmt.Errorf("deactivate: found non-reactivated baker %s", v)
				}
			} else {
				if bkr.IsActive {
					log.Debugf("Baker %s deactivated with grace period %d at cycle %d",
						bkr, bkr.GracePeriod, b.block.Cycle-1)
					b.DeactivateBaker(bkr)
				}
			}
		}
	}
	return nil
}

func (b *Builder) OnUpgrade(ctx context.Context, rollback bool) error {
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
		if !rollback {
			err := b.MigrateProtocol(ctx, prevparams, nextparams)
			if err != nil {
				return err
			}
		}

	} else if b.block.Params != nil && b.block.Params.IsCycleStart(b.block.Height) {
		// update params at start of cycle (to capture early ramp up data)
		b.idx.reg.Register(b.block.Params)
	}
	return nil
}

func (b *Builder) LoadConstants(ctx context.Context) error {
	// analyze contract originations and load referenced global constants
	requiredConstants := make([]tezos.ExprHash, 0)
	for _, ol := range b.block.TZ.Block.Operations {
		for _, oh := range ol {
			for _, o := range oh.Contents {
				switch kind := o.Kind(); kind {
				case tezos.OpTypeOrigination:
					// direct
					oop := o.(*rpc.Origination)
					if oop.Script != nil && oop.Script.Features().Contains(micheline.FeatureGlobalConstant) {
						requiredConstants = append(requiredConstants, oop.Script.Constants()...)
					}
				case tezos.OpTypeTransaction:
					// may contain internal originations
					top := o.(*rpc.Transaction)
					for _, iop := range top.Metadata.InternalResults {
						if iop.Kind == tezos.OpTypeOrigination {
							if iop.Script != nil && iop.Script.Features().Contains(micheline.FeatureGlobalConstant) {
								requiredConstants = append(requiredConstants, iop.Script.Constants()...)
							}
						}
					}

				case tezos.OpTypeRegisterConstant:
					// register new constants to in case they are used right away
					cop := o.(*rpc.ConstantRegistration)
					b.constDict.Add(cop.Metadata.Result.GlobalAddress, cop.Value)
				}
			}
		}
	}
	// load all required constants
	for {
		// constants can recursively include other constants, so we must keep going
		// until everything is resolved
		more := make([]tezos.ExprHash, 0)
		for _, v := range requiredConstants {
			if b.constDict.Has(v) {
				continue
			}
			if c, err := b.idx.LookupConstant(ctx, v); err != nil {
				return fmt.Errorf("missing constant %s", v)
			} else {
				var p micheline.Prim
				_ = p.UnmarshalBinary(c.Value)
				b.constDict.Add(c.Address, p)
				more = append(more, p.Constants()...)
			}
		}
		if len(more) == 0 {
			break
		}
		requiredConstants = more
	}
	return nil
}

// update counters, totals and sums from flows and ops
func (b *Builder) UpdateStats(ctx context.Context) error {
	// update baker stats
	if b.block.Baker != nil {
		// update baker software version from block nonce
		b.block.Baker.SetVersion(b.block.Nonce)

		// handle grace period
		b.block.Baker.UpdateGracePeriod(b.block.Cycle, b.block.Params)
		b.block.Proposer.UpdateGracePeriod(b.block.Cycle, b.block.Params)

		// stats
		b.block.Proposer.BlocksProposed++
		b.block.Baker.BlocksBaked++

		// last seen
		b.block.Proposer.Account.LastSeen = b.block.Height
		b.block.Baker.Account.LastSeen = b.block.Height
		b.block.Proposer.Account.IsDirty = true
		b.block.Baker.Account.IsDirty = true
	}

	// init pre-funded state
	for _, acc := range b.accMap {
		acc.WasFunded = acc.Balance() > 0
		acc.WasDust = acc.IsDust()
		acc.PrevBalance = acc.SpendableBalance
		acc.PrevSeen = util.Max64(acc.LastIn, acc.LastOut)
	}
	for _, bkr := range b.bakerMap {
		bkr.Account.WasFunded = bkr.Balance() > 0 // !sic
		bkr.Account.WasDust = bkr.Account.IsDust()
		bkr.Account.PrevBalance = bkr.Account.SpendableBalance
		bkr.Account.PrevSeen = util.Max64(bkr.Account.LastIn, bkr.Account.LastOut)
	}

	// apply pure in-flows
	for _, f := range b.block.Flows {
		// skip any out flow
		if f.AmountOut > 0 {
			continue
		}
		// try baker update first (will recursively update related account balance)
		bkr, ok := b.BakerById(f.AccountId)
		if ok {
			if err := bkr.UpdateBalance(f); err != nil {
				return err
			}
			continue
		}
		// otherwise try simple account update
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
		// try baker update first (will recursively update related account balance)
		bkr, ok := b.BakerById(f.AccountId)
		if ok {
			if err := bkr.UpdateBalance(f); err != nil {
				return err
			}
			continue
		}
		// otherwise try simple account update
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
		// skip undelegated accounts and bakers
		if !acc.IsDelegated || acc.IsBaker {
			continue
		}
		bkr, ok := b.BakerById(acc.BakerId)
		if !ok {
			// in rare circumstances an account has an invalid baker
			log.Errorf("Missing baker %d for account %s", acc.BakerId, acc)
			continue
		}
		if !acc.IsFunded && acc.WasFunded {
			// remove active delegation
			bkr.ActiveDelegations--
			bkr.IsDirty = true
		} else if acc.IsFunded && !acc.WasFunded {
			// re-add active delegation
			bkr.ActiveDelegations++
			bkr.IsDirty = true
		}
	}

	// update supplies and totals
	b.block.Update(b.accMap, b.bakerMap)
	b.block.Chain.Update(b.block, b.accMap, b.bakerMap)
	b.block.Supply.Update(b.block, b.bakerMap)
	return nil
}

func (b *Builder) RollbackStats(ctx context.Context) error {
	// update baker stats
	if b.block.Baker != nil {
		b.block.Proposer.BlocksProposed--
		b.block.Baker.BlocksBaked--
	}

	// init pre-funded state (at the end of block processing using current state)
	for _, acc := range b.accMap {
		acc.WasFunded = acc.Balance() > 0
		acc.WasDust = acc.IsDust()
		acc.MustDelete = acc.FirstSeen == b.block.Height
	}
	for _, bkr := range b.bakerMap {
		bkr.Account.WasFunded = bkr.Balance() > 0
		bkr.Account.WasDust = bkr.Account.IsDust()
		bkr.Account.MustDelete = bkr.Account.FirstSeen == b.block.Height
	}

	// reverse apply out-flows and in/out-flows
	for _, f := range b.block.Flows {
		// skip any non-out flow
		if f.AmountOut == 0 {
			continue
		}
		// try baker update first (will recursively update related account balance)
		bkr, ok := b.BakerById(f.AccountId)
		if ok {
			if err := bkr.RollbackBalance(f); err != nil {
				return err
			}
			continue
		}
		// otherwise try simple account update
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
		// try baker update first (will recursively update related account balance)
		bkr, ok := b.BakerById(f.AccountId)
		if ok {
			if err := bkr.RollbackBalance(f); err != nil {
				return err
			}
			continue
		}
		// otherwise try simple account update
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
		if acc.IsDelegated && !acc.IsBaker {
			bkr, _ := b.BakerById(acc.BakerId)
			if !acc.IsFunded && acc.WasFunded {
				// remove active delegation
				bkr.ActiveDelegations--
				bkr.IsDirty = true
			}
			if acc.IsFunded && !acc.WasFunded {
				// re-add active delegation
				bkr.ActiveDelegations++
				bkr.IsDirty = true
			}
		}
	}

	// update supplies and totals
	b.block.Rollback(b.accMap, b.bakerMap)
	b.block.Chain.Rollback(b.block)
	b.block.Supply.Rollback(b.block)
	return nil
}
