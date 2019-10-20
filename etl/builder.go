// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

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
	b.parent.Params, err = b.idx.ParamsByVersion(b.parent.Version)
	if err != nil {
		return err
	}

	// to make our crawler happy, we also expose the last block
	// on load, so any reports can be republished if necessary
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
		for _, ol := range oll {
			for op_c, op := range ol.Contents {
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
	// handle upgrades and end of cycle events
	if b.parent != nil {
		// first step: handle deactivated delegates from parent block
		// this is idempotent
		if b.block.Params.IsCycleStart(b.block.Height) {
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
					if err := b.NewActivationOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeBallot:
					if err := b.NewBallotOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeDelegation:
					if err := b.NewDelegationOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeDoubleBakingEvidence:
					if err := b.NewDoubleBakingOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeDoubleEndorsementEvidence:
					if err := b.NewDoubleEndorsingOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeEndorsement:
					if err := b.NewEndorsementOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeOrigination:
					if err := b.NewOriginationOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeProposals:
					if err := b.NewProposalsOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeReveal:
					if err := b.NewRevealOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeSeedNonceRevelation:
					if err := b.NewSeedNonceOp(oh, op_n, op_c, rollback); err != nil {
						return err
					}
				case chain.OpTypeTransaction:
					if err := b.NewTransactionOp(oh, op_n, op_c, rollback); err != nil {
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
	table, err := b.idx.Table(index.AccountTableKey)
	if err != nil {
		return err
	}
	for n, v := range b.block.Params.Invoices {
		addr, err := chain.ParseAddress(n)
		if err != nil {
			return fmt.Errorf("decoding invoice address %s: %v", n, err)
		}
		acc, ok := b.AccountByAddress(addr)
		if !ok {
			acc = NewAccount(addr)
			acc.FirstSeen = b.block.Height
			acc.LastSeen = b.block.Height
			if err := table.Insert(ctx, acc); err != nil {
				return err
			}
			b.accMap[acc.RowId] = acc
			b.accHashMap[accountHashKey(acc)] = acc
		}
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
		// skip any non-out flow
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
// We usually register all delegates as soon as they send an op to include them into
// snapshots. In protocols that have params.HasOriginationBug set we do this as soon
// as the origination is sent to a non registered delegate. That's why here we
// re-register such delegates to update their grade period and set a proper delegate id
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

	q := pack.Query{
		Name: "etl.addr.babylon_airdrop",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("f"), // is_funded
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
			pack.Condition{
				Field: table.Fields().Find("5"), // n_origination
				Mode:  pack.FilterModeGt,
				Value: int64(0),
			},
			pack.Condition{
				Field: table.Fields().Find("t"), // address_type
				Mode:  pack.FilterModeNotEqual,
				Value: int64(chain.AddressTypeContract),
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
