// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	. "blockwatch.cc/tzindex/etl/model"
)

var (
	RightsPackSizeLog2    = 15 // 32k packs ~3.6M
	RightsJournalSizeLog2 = 16 // 65k - can be big, no search required
	RightsCacheSize       = 8  // ~30M
	RightsFillLevel       = 100
	RightsIndexKey        = "rights"
	RightsTableKey        = "rights"
)

var (
	// ErrNoRightsEntry is an error that indicates a requested entry does
	// not exist in the chain table.
	ErrNoRightsEntry = errors.New("right not found")
)

type RightsIndex struct {
	db    *pack.DB
	opts  pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*RightsIndex)(nil)

func NewRightsIndex(opts pack.Options) *RightsIndex {
	return &RightsIndex{opts: opts}
}

func (idx *RightsIndex) DB() *pack.DB {
	return idx.db
}

func (idx *RightsIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *RightsIndex) Key() string {
	return RightsIndexKey
}

func (idx *RightsIndex) Name() string {
	return RightsIndexKey + " index"
}

func (idx *RightsIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(Right{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		RightsTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, RightsPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, RightsJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, RightsCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, RightsFillLevel),
		})
	return err
}

func (idx *RightsIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(RightsTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, RightsJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, RightsCacheSize),
	})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *RightsIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s table: %v", idx.Name(), err)
		}
		idx.table = nil
	}
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *RightsIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	upd := make([]pack.Item, 0, 32+block.Priority+block.NSeedNonce)
	// load and update rights when seed nonces are published
	if block.NSeedNonce > 0 {
		for _, v := range block.Ops {
			if v.Type != tezos.OpTypeSeedNonceRevelation {
				continue
			}
			// find and type-cast the seed nonce op
			op, ok := block.GetRpcOp(v.OpL, v.OpP, v.OpC)
			if !ok {
				return fmt.Errorf("rights: missing seed nonce op [%d:%d]", v.OpL, v.OpP)
			}
			sop, ok := op.(*rpc.SeedNonceOp)
			if !ok {
				return fmt.Errorf("rights: seed nonce op [%d:%d]: unexpected type %T ",
					v.OpL, v.OpP, op)
			}
			// seed nonces are injected by the current block's baker, but may originate
			// from another baker who was required to publish them as message into the
			// network
			err := pack.NewQuery("rights.search_seed", idx.table).
				AndEqual("height", sop.Level).
				AndEqual("type", tezos.RightTypeBaking).
				AndEqual("is_seed_required", true).
				Stream(ctx, func(r pack.Row) error {
					right := &Right{}
					if err := r.Decode(right); err != nil {
						return err
					}
					right.IsSeedRevealed = true
					upd = append(upd, right)
					return nil
				})
			if err != nil {
				return fmt.Errorf("rights: seed nonce right %s %d: %v", block.Baker, sop.Level, err)
			}
		}
	}

	// update baking and endorsing rights
	// careful: rights is slice of structs, not pointers
	rights := builder.Rights(tezos.RightTypeBaking)
	for i := range rights {
		pd := rights[i].Priority - block.Priority
		if pd > 0 {
			continue
		}
		if pd < 0 {
			rights[i].IsLost = true
			// find out if baker was underfunded
			if dlg, ok := builder.AccountById(rights[i].AccountId); ok {
				rights[i].IsBondMiss = dlg.SpendableBalance < block.Params.BlockSecurityDeposit
			}
		} else if pd == 0 {
			rights[i].IsStolen = block.Priority > 0
			rights[i].IsUsed = true
			rights[i].IsSeedRequired = block.Height%block.Params.BlocksPerCommitment == 0
		}
		upd = append(upd, &(rights[i]))
	}

	// endorsing rights are for parent block
	if block.Parent != nil {
		missed := ^block.Parent.SlotsEndorsed
		// careful: rights is slice of structs, not pointers
		rights := builder.Rights(tezos.RightTypeEndorsing)
		for i := range rights {
			if missed&(0x1<<uint(rights[i].Priority)) == 0 {
				rights[i].IsUsed = true
			} else {
				rights[i].IsMissed = true
				// find out if baker was underfunded
				if dlg, ok := builder.AccountById(rights[i].AccountId); ok {
					rights[i].IsBondMiss = dlg.SpendableBalance < block.Params.EndorsementSecurityDeposit
				}
			}
			upd = append(upd, &(rights[i]))
		}
	}
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}

	// at the first reorg-safe block of a new cycle, remove all unused rights from previous cycles
	isReorgSafe := block.Height == block.Params.CycleStartHeight(block.Cycle)+tezos.MaxBranchDepth
	if block.Cycle > 0 && isReorgSafe {
		n, err := pack.NewQuery("rights.clear", idx.table).
			AndEqual("cycle", block.Cycle-1).
			AndEqual("is_used", false).
			AndEqual("is_lost", false).
			AndEqual("is_stolen", false).
			AndEqual("is_missed", false).
			Delete(ctx)
		if err != nil {
			return err
		}
		log.Debugf("Rights: deleted %d unused rights from cycle %d at block %d [%d]",
			n, block.Cycle-1, block.Height, block.Cycle)
		// compact
		if err := idx.table.Compact(ctx); err != nil {
			return err
		}
	}

	// nothing more to do when no new rights are available
	if len(block.TZ.Baking) == 0 && len(block.TZ.Endorsing) == 0 {
		return nil
	}

	// insert all baking rights for a cycle, then all endorsing rights
	ins := make([]pack.Item, 0, (64+32)*block.Params.BlocksPerCycle)
	for _, v := range block.TZ.Baking {
		acc, ok := builder.AccountByAddress(v.Address())
		if !ok {
			return fmt.Errorf("rights: missing baker account %s", v.Address())
		}
		ins = append(ins, &Right{
			Type:      tezos.RightTypeBaking,
			Height:    v.Level,
			Cycle:     block.Params.CycleFromHeight(v.Level),
			Priority:  v.Priority,
			AccountId: acc.RowId,
		})
	}
	// sort endorsing rights by slot, they are only sorted by height here
	height := block.TZ.Endorsing[0].Level
	erights := make([]*Right, 0, block.Params.EndorsersPerBlock)
	for _, v := range block.TZ.Endorsing {
		// sort and flush into insert
		if v.Level > height {
			sort.Slice(erights, func(i, j int) bool { return erights[i].Priority < erights[j].Priority })
			for _, r := range erights {
				ins = append(ins, r)
			}
			erights = erights[:0]
			height = v.Level
		}
		acc, ok := builder.AccountByAddress(v.Address())
		if !ok {
			return fmt.Errorf("rights: missing endorser account %s", v.Address())
		}
		for _, slot := range sort.IntSlice(v.Slots) {
			erights = append(erights, &Right{
				Type:      tezos.RightTypeEndorsing,
				Height:    v.Level,
				Cycle:     block.Params.CycleFromHeight(v.Level),
				Priority:  slot,
				AccountId: acc.RowId,
			})
		}
	}
	// sort and flush the last bulk
	sort.Slice(erights, func(i, j int) bool { return erights[i].Priority < erights[j].Priority })
	for _, r := range erights {
		ins = append(ins, r)
	}
	return idx.table.Insert(ctx, ins)
}

// Does not work on rollback of the first cycle block!
func (idx *RightsIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// reverse right updates
	upd := make([]pack.Item, 0, 32+block.Priority+block.NSeedNonce)
	// load and update rights when seed nonces are published
	if block.NSeedNonce > 0 {
		for _, v := range block.Ops {
			if v.Type != tezos.OpTypeSeedNonceRevelation {
				continue
			}
			// find and type-cast the seed nonce op
			op, ok := block.GetRpcOp(v.OpL, v.OpP, v.OpC)
			if !ok {
				return fmt.Errorf("rights: missing seed nonce op [%d:%d]", v.OpL, v.OpP)
			}
			sop, ok := op.(*rpc.SeedNonceOp)
			if !ok {
				return fmt.Errorf("rights: seed nonce op [%d:%d]: unexpected type %T ",
					v.OpL, v.OpP, op)
			}
			// seed nonces are injected by the current block's baker!
			// we assume each baker has only one priority level per block
			err := pack.NewQuery("rights.search_seed", idx.table).
				AndEqual("height", sop.Level).
				AndEqual("type", tezos.RightTypeBaking).
				AndEqual("account_id", block.Baker.RowId).
				Stream(ctx, func(r pack.Row) error {
					right := &Right{}
					if err := r.Decode(right); err != nil {
						return err
					}
					right.IsSeedRevealed = false
					upd = append(upd, right)
					return nil
				})
			if err != nil {
				return fmt.Errorf("rights: seed nonce right %s %d: %v", block.Baker, sop.Level, err)
			}
		}
	}

	// update baking and endorsing rights
	if block.Priority > 0 {
		// careful: rights is slice of structs, not pointers
		rights := builder.Rights(tezos.RightTypeBaking)
		for i := range rights {
			rights[i].IsUsed = false
			rights[i].IsLost = false
			rights[i].IsStolen = false
			upd = append(upd, &(rights[i]))
		}
	}
	// endorsing rights are for parent block
	// careful: rights is slice of structs, not pointers
	rights := builder.Rights(tezos.RightTypeEndorsing)
	for i := range rights {
		rights[i].IsUsed = false
		rights[i].IsMissed = false
		upd = append(upd, &(rights[i]))
	}
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}

	// new rights are fetched in cycles
	if block.Params.IsCycleStart(block.Height) {
		return idx.DeleteCycle(ctx, block.Cycle+block.Params.PreservedCycles)
	}
	return nil
}

func (idx *RightsIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *RightsIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting rights for cycle %d", cycle)
	_, err := pack.NewQuery("etl.rights.delete", idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}
