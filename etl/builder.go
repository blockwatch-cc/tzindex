// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/cache"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

type Builder struct {
	idx             *Indexer                            // storage reference
	accHashMap      map[uint64]*model.Account           // hash(acc_hash) -> *Account (both known and new accounts)
	accMap          map[model.AccountID]*model.Account  // id -> *Account (both known and new accounts)
	conMap          map[model.AccountID]*model.Contract // id -> *Contract contracts seen this block
	accCache        *cache.AccountCache                 // cache for binary encodings of regular accounts
	conCache        *cache.ContractCache                // cache for smart contracts
	bakerHashMap    map[uint64]*model.Baker             // bakers by hash
	bakerMap        map[model.AccountID]*model.Baker    // bakers by id
	constDict       micheline.ConstantDict              // global constants used in smart contracts this block
	bakeRights      map[model.AccountID]*vec.BitSet
	endorseRights   map[model.AccountID]*vec.BitSet
	absentEndorsers map[model.AccountID]struct{}

	// build state
	validate bool
	rpc      *rpc.Client
	block    *model.Block
	parent   *model.Block
}

const buildMapSizeHint = 1024

func NewBuilder(idx *Indexer, c *rpc.Client, validate bool) *Builder {
	return &Builder{
		idx:             idx,
		accHashMap:      make(map[uint64]*model.Account, buildMapSizeHint),
		accMap:          make(map[model.AccountID]*model.Account, buildMapSizeHint),
		conMap:          make(map[model.AccountID]*model.Contract, buildMapSizeHint),
		accCache:        cache.NewAccountCache(0),
		conCache:        cache.NewContractCache(0),
		bakerMap:        make(map[model.AccountID]*model.Baker, buildMapSizeHint),
		bakerHashMap:    make(map[uint64]*model.Baker, buildMapSizeHint),
		bakeRights:      make(map[model.AccountID]*vec.BitSet),
		endorseRights:   make(map[model.AccountID]*vec.BitSet),
		absentEndorsers: make(map[model.AccountID]struct{}),
		validate:        validate,
		rpc:             c,
	}
}

func (b *Builder) Params(height int64) *rpc.Params {
	return b.idx.ParamsByHeight(height)
}

func (b *Builder) IsLightMode() bool {
	return b.idx.lightMode
}

func (b *Builder) ClearCache() {
	b.accCache.Purge()
	b.conCache.Purge()
}

func (b *Builder) CacheStats() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["accounts"] = b.accCache.Stats()
	stats["contracts"] = b.conCache.Stats()
	return stats
}

func (b *Builder) RegisterBaker(acc *model.Account, enable bool) *model.Baker {
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

	// track active but non-enabled bakers when explicitly requested,
	// this ensures we know origination bug bakers from protocol v001
	// and can handle them during v002 migration
	b.ActivateBaker(baker)
	if enable {
		acc.BakerId = acc.RowId
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
	if ok && bkr.Address == addr {
		return bkr.Account, true
	}
	acc, ok := b.accHashMap[key]
	if ok && acc.Address == addr {
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
	if ok && bkr.Address == addr {
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
	if con, ok := b.conCache.Get(id); ok {
		return con, nil
	}
	con, err := b.idx.LookupContractId(ctx, id)
	if err != nil {
		return nil, err
	}
	b.conMap[id] = con
	b.conCache.Add(con)
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
	if tip.BestHeight == p.EndHeight && version > 0 {
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
	for n := range b.accHashMap {
		delete(b.accHashMap, n)
	}
	for n := range b.accMap {
		delete(b.accMap, n)
	}
	for n := range b.conMap {
		delete(b.conMap, n)
	}
	for n := range b.absentEndorsers {
		delete(b.absentEndorsers, n)
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
	b.conCache.Purge()

	// clear build state
	b.accHashMap = make(map[uint64]*model.Account, buildMapSizeHint)
	b.accMap = make(map[model.AccountID]*model.Account, buildMapSizeHint)
	b.conMap = make(map[model.AccountID]*model.Contract, buildMapSizeHint)
	b.constDict = nil

	// clear delegate state
	b.bakerHashMap = make(map[uint64]*model.Baker, buildMapSizeHint)
	b.bakerMap = make(map[model.AccountID]*model.Baker, buildMapSizeHint)

	// clear rights
	b.bakeRights = make(map[model.AccountID]*vec.BitSet)
	b.endorseRights = make(map[model.AccountID]*vec.BitSet)
	b.absentEndorsers = make(map[model.AccountID]struct{})

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
	for n := range b.accHashMap {
		delete(b.accHashMap, n)
	}
	for n := range b.accMap {
		delete(b.accMap, n)
	}
	for n := range b.conMap {
		delete(b.conMap, n)
	}
	b.constDict = nil

	// clear rights
	for n := range b.bakeRights {
		delete(b.bakeRights, n)
	}
	for n := range b.endorseRights {
		delete(b.endorseRights, n)
	}
	for n := range b.absentEndorsers {
		delete(b.absentEndorsers, n)
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

	// release, but do not free parent block (it may be fork block which we'll need
	// for forward reorg and later)
	b.parent = nil

	// free current block (no longer needed during rollback)
	if b.block != nil {
		b.block.Free()
	}
	b.block = nil
}

func (b *Builder) InitAccounts(ctx context.Context) error {
	// collect unique accounts/addresses
	set := tezos.NewAddressSet()
	addUnique := func(a tezos.Address) {
		if !a.IsValid() {
			return
		}
		if _, ok := b.AccountByAddress(a); !ok {
			set.AddUnique(a)
		}
	}

	// collect all addresses referenced in this block's header and operations
	// including addresses from contract parameters, storage, bigmap updates
	if err := b.block.TZ.Block.CollectAddresses(addUnique); err != nil {
		return err
	}

	// collect from future cycle rights when available
	for _, r := range b.block.TZ.Baking {
		for _, rr := range r {
			addUnique(rr.Address())
		}
	}
	for _, r := range b.block.TZ.Endorsing {
		for _, rr := range r {
			addUnique(rr.Address())
		}
	}

	// search cached accounts and build map
	lookup := make([][]byte, 0)
	for _, v := range set.Map() {
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
			lookup = append(lookup, acc.Address[:])
			// log.Infof("Lookup addr %s %x hashkey=%016x", v, v[:], hashKey)
			// store in map, will be overwritten when resolved from db
			// or kept as new address when this is the first time we see it
			b.accHashMap[hashKey] = acc
		}
	}

	// lookup addr by hashes (non-existent addrs are expected to not resolve)
	table, err := b.idx.Table(model.AccountTableKey)
	if err != nil {
		return err
	}
	if len(lookup) > 0 {
		err = pack.NewQuery("etl.addr_hash.search").
			WithTable(table).
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
				// log.Infof("Found addr %s hashkey=%016x", acc, hashKey)

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
			// log.Infof("Addr %s looks like a new account", v)
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
			// log.Infof("Create A_%d for addr %s", acc.RowId, acc)
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
		b.block.BakerConsensusKeyId = baker.AccountId
	}

	if addr := b.block.TZ.Block.Metadata.Proposer; addr.IsValid() {
		proposer, ok := b.BakerByAddress(addr)
		if !ok {
			return fmt.Errorf("missing proposer account %s", addr)
		}
		b.block.Proposer = proposer
		b.block.ProposerId = proposer.AccountId
		b.block.ProposerConsensusKeyId = proposer.AccountId
	} else {
		b.block.Proposer = b.block.Baker
		b.block.ProposerId = b.block.BakerId
		b.block.ProposerConsensusKeyId = b.block.BakerId
	}

	if b.block.ProposerId != b.block.ProposerConsensusKeyId {
		acc, ok := b.AccountById(b.block.ProposerConsensusKeyId)
		if !ok {
			return fmt.Errorf("missing proposer consensus key account %d", b.block.ProposerConsensusKeyId)
		}
		b.block.ProposerConsensusKeyId = acc.RowId
	}
	if b.block.BakerId != b.block.BakerConsensusKeyId {
		acc, ok := b.AccountById(b.block.BakerConsensusKeyId)
		if !ok {
			return fmt.Errorf("missing baker consensus key account %d", b.block.BakerConsensusKeyId)
		}
		b.block.BakerConsensusKeyId = acc.RowId
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

	// load global constants that are referenced by newly originated contracts
	if err := b.LoadConstants(ctx); err != nil {
		return err
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

	// identify absent rights holders
	if err := b.LoadAbsentRightsHolders(ctx); err != nil {
		return err
	}

	return nil
}

func (b *Builder) LoadAbsentRightsHolders(ctx context.Context) error {
	if b.IsLightMode() || b.block.Height == 0 {
		return nil
	}

	if b.block.TZ.IsCycleStart() {
		// 1 use endorse rights from last block of previous cycle
		adj := b.parent.Params.BlocksPerCycle - 1
		for n, bits := range b.endorseRights {
			if bits.IsSet(int(adj)) {
				b.absentEndorsers[n] = struct{}{}
			}
		}

		// 2 drop past cycle rights
		for n := range b.bakeRights {
			delete(b.bakeRights, n)
		}
		for n := range b.endorseRights {
			delete(b.endorseRights, n)
		}

		// 3 load new rights for current cycle
		rights, err := b.idx.Table(model.RightsTableKey)
		if err != nil {
			return err
		}
		err = pack.NewQuery("load_cycle_rights").
			WithTable(rights).
			WithFields("account_id", "baking_rights", "endorsing_rights").
			AndEqual("cycle", b.block.Cycle).
			Stream(ctx, func(r pack.Row) error {
				right := &model.Right{}
				if err := r.Decode(right); err != nil {
					return err
				}
				b.bakeRights[right.AccountId] = &right.Bake
				b.endorseRights[right.AccountId] = &right.Endorse
				return nil
			})
		if err != nil {
			return err
		}
	} else {
		ofs := b.block.TZ.GetCyclePosition()
		// use endorse rights from previous block
		for n, bits := range b.endorseRights {
			if bits.IsSet(int(ofs - 1)) {
				b.absentEndorsers[n] = struct{}{}
			}
		}
	}

	// remove seen endorsers from absent map
	for _, v := range b.block.Ops {
		if v.Type != model.OpTypeEndorsement {
			continue
		}
		delete(b.absentEndorsers, v.SenderId)
	}
	// produce absentee list in block
	if l := len(b.absentEndorsers); l > 0 {
		b.block.AbsentEndorsers = make([]model.AccountID, 0, l)
		for n := range b.absentEndorsers {
			b.block.AbsentEndorsers = append(b.block.AbsentEndorsers, n)
		}
	}

	// if prio/round is > 0, identify absent first baker
	if b.block.Round > 0 {
		ofs := b.block.TZ.GetCyclePosition()
		for n, bits := range b.bakeRights {
			if bits.IsSet(int(ofs)) {
				b.block.AbsentBaker = n
				break
			}
		}
	}
	return nil
}

func (b *Builder) OnDeactivate(ctx context.Context, rollback bool) error {
	// if b.block.Params.IsCycleStart(b.block.Height) && b.block.Height > 0 {
	if b.block.TZ.IsCycleStart() && b.block.Height > 0 {
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
	from, to := b.parent.TZ.Protocol(), b.block.TZ.Protocol()
	if from != to {
		next := b.block.Params
		prev, err := b.idx.ParamsByProtocol(from)
		if err != nil {
			return err
		}

		// special actions on protocol upgrades
		if !rollback {
			// register new protocol (will save as new deployment)
			log.Infof("New protocol %s detected at %d", to, b.block.Height)
			log.Infof("Upgrading from %s => %s", from, to)
			if err := b.idx.ConnectProtocol(ctx, next, prev); err != nil {
				return err
			}

			if err := b.MigrateProtocol(ctx, prev, next); err != nil {
				return err
			}
		}
	} else if b.block.TZ.IsCycleStart() {
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
					// register new constants in case they are used right away
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

	// absentees
	if b.block.AbsentBaker > 0 {
		bkr, ok := b.BakerById(b.block.AbsentBaker)
		if ok {
			bkr.BlocksNotBaked++
			bkr.IsDirty = true
		}
	}
	for _, v := range b.block.AbsentEndorsers {
		bkr, ok := b.BakerById(v)
		if ok {
			bkr.BlocksNotEndorsed++
			bkr.IsDirty = true
		}
	}

	// init pre-funded state
	for _, acc := range b.accMap {
		acc.WasFunded = acc.Balance() > 0
		acc.WasDust = acc.IsDust()
		acc.PrevBalance = acc.Balance()
	}
	for _, bkr := range b.bakerMap {
		bkr.Account.WasFunded = bkr.Balance() > 0 // !sic
		bkr.Account.WasDust = bkr.Account.IsDust()
		bkr.Account.PrevBalance = bkr.Account.Balance()
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

	// update supplies and totals (order matters: block >> chain >> stake)
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

	// absentees
	if b.block.AbsentBaker > 0 {
		bkr, ok := b.BakerById(b.block.AbsentBaker)
		if ok {
			bkr.BlocksNotBaked--
			bkr.IsDirty = true
		}
	}
	for _, v := range b.block.AbsentEndorsers {
		bkr, ok := b.BakerById(v)
		if ok {
			bkr.BlocksNotEndorsed--
			bkr.IsDirty = true
		}
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

	// update supplies and totals
	b.block.Rollback(b.accMap, b.bakerMap)
	b.block.Chain.Rollback(b.block)
	b.block.Supply.Rollback(b.block)
	return nil
}
