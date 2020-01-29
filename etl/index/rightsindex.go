// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

var (
	RightsPackSizeLog2    = 15 // 32k packs
	RightsJournalSizeLog2 = 16 // 65k - can be big, no search required
	RightsCacheSize       = 2
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
			if v.Type != chain.OpTypeSeedNonceRevelation {
				continue
			}
			// find and type-cast the seed nonce op
			op, ok := block.GetRPCOp(v.OpN, v.OpC)
			if !ok {
				return fmt.Errorf("rights: missing seed nonce op [%d:%d]", v.OpN, v.OpC)
			}
			sop, ok := op.(*rpc.SeedNonceOp)
			if !ok {
				return fmt.Errorf("rights: seed nonce op [%d:%d]: unexpected type %T ", v.OpN, v.OpC, op)
			}
			// seed nonces are injected by the current block's baker, but may originate
			// from another baker who was required to publish them as message into the
			// network
			err := idx.table.Stream(ctx,
				pack.Query{
					Name:   "rights.search_seed",
					Fields: idx.table.Fields(),
					Conditions: pack.ConditionList{
						pack.Condition{
							Field: idx.table.Fields().Find("h"), // from block height
							Mode:  pack.FilterModeEqual,
							Value: sop.Level,
						}, pack.Condition{
							Field: idx.table.Fields().Find("t"), // type == baking
							Mode:  pack.FilterModeEqual,
							Value: int64(chain.RightTypeBaking),
						},
						pack.Condition{
							Field: idx.table.Fields().Find("R"), // seed required
							Mode:  pack.FilterModeEqual,
							Value: true,
						},
					}},
				func(r pack.Row) error {
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
	rights := builder.Rights(chain.RightTypeBaking)
	for i := range rights {
		pd := rights[i].Priority - block.Priority
		if pd > 0 {
			continue
		}
		rights[i].IsLost = pd < 0
		if pd == 0 {
			rights[i].IsStolen = block.Priority > 0
			rights[i].IsSeedRequired = block.Height%block.Params.BlocksPerCommitment == 0
		}
		upd = append(upd, &(rights[i]))
	}

	// endorsing rights are for parent block
	if block.Parent != nil {
		if missed := ^block.Parent.SlotsEndorsed; missed > 0 {
			// careful: rights is slice of structs, not pointers
			rights := builder.Rights(chain.RightTypeEndorsing)
			for i := range rights {
				if missed&(0x1<<uint(rights[i].Priority)) == 0 {
					continue
				}
				rights[i].IsMissed = true
				upd = append(upd, &(rights[i]))
			}
		}
	}
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}

	// nothing more to do when no new rights are available
	if len(block.TZ.Baking) == 0 && len(block.TZ.Endorsing) == 0 {
		return nil
	}

	// insert all baking rights for a cycle, then all endorsing rights
	ins := make([]pack.Item, 0, (64+32)*block.Params.BlocksPerCycle)
	for _, v := range block.TZ.Baking {
		acc, ok := builder.AccountByAddress(v.Delegate)
		if !ok {
			return fmt.Errorf("rights: missing baker account %s", v.Delegate)
		}
		ins = append(ins, &Right{
			Type:      chain.RightTypeBaking,
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
		acc, ok := builder.AccountByAddress(v.Delegate)
		if !ok {
			return fmt.Errorf("rights: missing endorser account %s", v.Delegate)
		}
		for _, slot := range sort.IntSlice(v.Slots) {
			erights = append(erights, &Right{
				Type:      chain.RightTypeEndorsing,
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

func (idx *RightsIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// reverse right updates
	upd := make([]pack.Item, 0, 32+block.Priority+block.NSeedNonce)
	// load and update rights when seed nonces are published
	if block.NSeedNonce > 0 {
		for _, v := range block.Ops {
			if v.Type != chain.OpTypeSeedNonceRevelation {
				continue
			}
			// find and type-cast the seed nonce op
			op, ok := block.GetRPCOp(v.OpN, v.OpC)
			if !ok {
				return fmt.Errorf("rights: missing seed nonce op [%d:%d]", v.OpN, v.OpC)
			}
			sop, ok := op.(*rpc.SeedNonceOp)
			if !ok {
				return fmt.Errorf("rights: seed nonce op [%d:%d]: unexpected type %T ", v.OpN, v.OpC, op)
			}
			// seed nonces are injected by the current block's baker!
			// we assume each baker has only one priority level per block
			err := idx.table.Stream(ctx,
				pack.Query{
					Name:   "rights.search_seed",
					Fields: idx.table.Fields(),
					Conditions: pack.ConditionList{
						pack.Condition{
							Field: idx.table.Fields().Find("h"), // from block height
							Mode:  pack.FilterModeEqual,
							Value: sop.Level,
						}, pack.Condition{
							Field: idx.table.Fields().Find("t"), // type == baking
							Mode:  pack.FilterModeEqual,
							Value: int64(chain.RightTypeBaking),
						},
						pack.Condition{
							Field: idx.table.Fields().Find("A"), // delegate account
							Mode:  pack.FilterModeEqual,
							Value: block.Baker.RowId.Value(),
						},
					}},
				func(r pack.Row) error {
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
		rights := builder.Rights(chain.RightTypeBaking)
		for i := range rights {
			rights[i].IsLost = false
			rights[i].IsStolen = false
			upd = append(upd, &(rights[i]))
		}
	}
	// endorsing rights are for parent block
	// careful: rights is slice of structs, not pointers
	rights := builder.Rights(chain.RightTypeEndorsing)
	for i := range rights {
		rights[i].IsMissed = false
		upd = append(upd, &(rights[i]))
	}
	if err := idx.table.Update(ctx, upd); err != nil {
		return err
	}

	// new rights are fetched in cycles
	if block.Params.IsCycleStart(block.Height) {
		return idx.DeleteCycle(ctx, block.Height)
	}
	return nil
}

func (idx *RightsIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *RightsIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	log.Debugf("Rollback deleting rights for cycle %d", cycle)
	q := pack.Query{
		Name: "etl.rights.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.table.Fields().Find("c"), // cycle (!)
			Mode:  pack.FilterModeEqual,
			Value: cycle,
		}},
	}
	_, err := idx.table.Delete(ctx, q)
	return err
}
