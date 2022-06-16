// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

var (
	RightsPackSizeLog2    = 13 // 8k
	RightsJournalSizeLog2 = 14 // 16k
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
	cache map[model.AccountID]*model.Right
	cycle int64
}

var _ model.BlockIndexer = (*RightsIndex)(nil)

func NewRightsIndex(opts pack.Options) *RightsIndex {
	return &RightsIndex{
		opts:  opts,
		cache: make(map[model.AccountID]*model.Right),
	}
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
	fields, err := pack.Fields(model.Right{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
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

func (idx *RightsIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *RightsIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s table: %s", idx.Name(), err)
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

func (idx *RightsIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	p := block.Params

	// reset cache at start of cycle
	if block.Params.IsCycleStart(block.Height) {
		idx.cache = make(map[model.AccountID]*model.Right)
		idx.cycle = block.Cycle
	}

	// load and update rights when seed nonces are published
	if block.HasSeeds {
		rightsByBaker := make(map[model.AccountID]*model.Right)
		upd := make([]pack.Item, 0)
		for _, op := range block.Ops {
			if op.Type != model.OpTypeNonceRevelation {
				continue
			}
			// find and type-cast the seed nonce op
			sop, ok := op.Raw.(*rpc.SeedNonce)
			if !ok {
				return fmt.Errorf("rights: seed nonce op [%d:%d]: unexpected type %T ", 2, op.OpP, op.Raw)
			}
			right, ok := rightsByBaker[op.CreatorId]
			if !ok {
				// seed nonces are injected by the current block's baker, but may be sent
				// by another baker, rights are from cycle - 1 !!
				right = &model.Right{}
				err := pack.NewQuery("rights.search_seed", idx.table).
					AndEqual("cycle", block.Cycle-1).
					AndEqual("account_id", op.CreatorId).
					Execute(ctx, right)
				if err != nil || right.RowId == 0 {
					return fmt.Errorf("rights: missing seed nonce right for block %d creator %d: %v", sop.Level, op.CreatorId, err)
				}
				rightsByBaker[right.AccountId] = right
				upd = append(upd, right)
			}
			// use params that were active at the seed block
			params := p.ForHeight(sop.Level)
			start := params.CycleStartHeight(params.CycleFromHeight(sop.Level))
			pos := int((sop.Level - start) / params.BlocksPerCommitment)
			right.Seeded.Set(pos)
		}

		// write back to table
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
	}

	// update baking right
	if block.ProposerId > 0 {
		right, err := idx.loadRight(ctx, block.Cycle, block.ProposerId)
		if err != nil {
			log.Warnf("rights: missing baking right for block %d proposer %d: %v", block.Height, block.ProposerId, err)
		} else {
			start := p.CycleStartHeight(block.Cycle)
			right.Baked.Set(int(block.Height - start))
			if p.IsSeedRequired(block.Height) {
				right.Seed.Set(int((block.Height - start) / p.BlocksPerCommitment))
			}
			if err := idx.table.Update(ctx, right); err != nil {
				return err
			}
			// update reliability
			bkr, ok := builder.BakerById(right.AccountId)
			if ok {
				bkr.Reliability = right.Reliability(int(p.CyclePosition(block.Height)))
			}
		}
	}

	// update endorsing rights
	if block.Parent != nil {
		endorsers := make(map[model.AccountID]struct{})
		for _, op := range block.Ops {
			if op.Type != model.OpTypeEndorsement {
				continue
			}
			endorsers[op.SenderId] = struct{}{}
		}
		// endorsing rights are for parent block
		cycle := block.Parent.Cycle

		// all endorsements are for block - 1, make sure we use the correct settings
		// for the cycle (in case of first block)
		param := p.ForCycle(cycle)
		start := param.CycleStartHeight(cycle)
		pos := int(block.Height-start) - 1
		upd := make([]pack.Item, 0)
		for id, _ := range endorsers {
			right, err := idx.loadRight(ctx, cycle, id)
			if err != nil {
				// on protocol upgrades we may see extra endorsers, create rights here
				right = model.NewRight(id, cycle, int(param.BlocksPerCycle), int(param.BlocksPerCycle/param.BlocksPerCommitment))
				log.Debugf("rights: add extra endorsing right for account %d on block %d (-1)", id, block.Height)
				if err := idx.table.Insert(ctx, right); err != nil {
					return fmt.Errorf("rights: adding extra exdorsing right: %v", err)
				}
				// don't add to cache
			}
			right.Endorsed.Set(pos)
			upd = append(upd, right)
			bkr, ok := builder.BakerById(right.AccountId)
			if ok {
				bkr.Reliability = right.Reliability(int(p.CyclePosition(block.Height)))
			}
		}

		// write back to table
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
	}

	// nothing more to do when no new rights are available
	if len(block.TZ.Baking) == 0 && len(block.TZ.Endorsing) == 0 {
		return nil
	}

	// get params at last baking block height (this avoids re-adjusting params
	// for every call to CycleFromHeight() during bootstrap on block 1)
	pp := p.ForHeight(block.TZ.Baking[0][len(block.TZ.Baking)-1].Level)

	// create new rights data for each baker, add baking and endorsing rights
	for c := range block.TZ.Baking {
		// process cycle by cycle (only during bootstrap/genesis we handle multiple cycles)
		cycle := pp.CycleFromHeight(block.TZ.Baking[c][0].Level)
		start := pp.CycleStartHeight(cycle)

		// sort rights by baker to speed up processing, we don't really care when
		// levels end up unsorted, so we use the faster quicksort
		sort.Slice(block.TZ.Baking[c], func(i, j int) bool {
			return block.TZ.Baking[c][i].Delegate < block.TZ.Baking[c][j].Delegate
		})
		sort.Slice(block.TZ.Endorsing[c], func(i, j int) bool {
			return block.TZ.Endorsing[c][i].Delegate < block.TZ.Endorsing[c][j].Delegate
		})

		log.Debugf("rights: adding %d baking and %d endorsing rights for cycle %d starting at %d",
			len(block.TZ.Baking[c]), len(block.TZ.Endorsing[c]), cycle, start)

		// build rights bitmaps for each baker
		rightsByBaker := make(map[model.AccountID]*model.Right)
		ins := make([]pack.Item, 0)
		var (
			baker      string
			right      *model.Right
			ecnt, bcnt int
		)
		for _, v := range block.TZ.Baking[c] {
			if v.Round > 0 {
				continue
			}
			if baker != v.Delegate {
				baker = v.Delegate
				acc, ok := builder.AccountByAddress(v.Address())
				if !ok {
					log.Errorf("rights: missing baker account %s", v.Delegate)
					continue
				}
				// create a new right and insert its pointer for lookup
				right = model.NewRight(acc.RowId, cycle, int(pp.BlocksPerCycle), int(pp.BlocksPerCycle/pp.BlocksPerCommitment))
				rightsByBaker[acc.RowId] = right
				ins = append(ins, right)
			}
			right.Bake.Set(int(v.Level - start))
			bcnt++
		}

		// same for endorsing rights
		baker = ""
		for _, v := range block.TZ.Endorsing[c] {
			if baker != v.Delegate {
				baker = v.Delegate
				acc, ok := builder.AccountByAddress(v.Address())
				if !ok {
					log.Errorf("rights: missing baker account %s", v.Delegate)
					continue
				}
				// reuse existing rights
				right, ok = rightsByBaker[acc.RowId]
				if !ok {
					// create a new right for endorsers who are not yet known from above
					right = model.NewRight(acc.RowId, cycle, int(pp.BlocksPerCycle), int(pp.BlocksPerCycle/pp.BlocksPerCommitment))
					rightsByBaker[acc.RowId] = right
					ins = append(ins, right)
				}
			}
			right.Endorse.Set(int(v.Level - start))
			ecnt++
		}

		// add empty rights for all known bakers with rolls in snapshot
		if sn := block.TZ.Snapshot; sn != nil {
			snap, err := builder.Table(SnapshotIndexKey)
			if err != nil {
				return err
			}
			s := &model.Snapshot{}
			err = pack.NewQuery("snapshot.create_rights", snap).
				WithoutCache().
				AndEqual("cycle", sn.Base).  // source snapshot cycle
				AndEqual("index", sn.Index). // selected index
				AndEqual("is_baker", true).  // bakers only (active+inactive)
				Stream(ctx, func(r pack.Row) error {
					if err := r.Decode(s); err != nil {
						return err
					}
					if s.Rolls == 0 {
						return nil
					}
					if _, ok := rightsByBaker[s.AccountId]; ok {
						return nil
					}
					right = model.NewRight(s.AccountId, cycle, int(pp.BlocksPerCycle), int(pp.BlocksPerCycle/pp.BlocksPerCommitment))
					rightsByBaker[s.AccountId] = right
					ins = append(ins, right)
					return nil
				})
			if err != nil {
				return err
			}
		}

		// sort by account id for improved table compression
		sort.Slice(ins, func(i, j int) bool { return ins[i].(*model.Right).AccountId < ins[j].(*model.Right).AccountId })

		// insert full cycle worth of rights
		log.Debugf("Inserting %d records with %d baking and %d endorsing rights", len(ins), bcnt, ecnt)
		if err := idx.table.Insert(ctx, ins); err != nil {
			return err
		}
		ins = ins[:0]
	}
	return nil
}

// Does not work on rollback of the first cycle block!
func (idx *RightsIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// reverse right updates by clearing all flags across all bakers for this block
	cycle := block.Cycle
	upd := make([]pack.Item, 0)

	// on first block of cycle, also clear endorsing rights of previous cycle
	// and entire last future cycle
	if block.Params.IsCycleStart(block.Height) && block.Parent != nil {
		prevBlocksPerCycle := block.Parent.Params.BlocksPerCycle
		err := pack.NewQuery("rights.disconnect_start_of_cycle", idx.table).
			AndRange("cycle", cycle-1, cycle).
			Stream(ctx, func(row pack.Row) error {
				right := &model.Right{}
				if err := row.Decode(right); err != nil {
					return err
				}
				if right.Cycle == cycle {
					right.Baked.Clear(0)
					right.Seed.Clear(0)
				} else {
					right.Endorsed.Clear(int(prevBlocksPerCycle))
				}
				upd = append(upd, right)
				return nil
			})
		if err != nil {
			return fmt.Errorf("rights: SOC disconnect for block %d: %w", block.Height, err)
		}
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
		return idx.DeleteCycle(ctx, block.Cycle+block.Params.PreservedCycles)
	} else {
		// on a regular mid-cycle block, clear same-cycle bits only
		p := block.Params
		start := p.CycleStartHeight(cycle)
		bakePos := int(block.Height - start)
		seedPos := -1
		if p.IsSeedRequired(block.Height) {
			seedPos = int((block.Height - start) / p.BlocksPerCommitment)
		}
		err := pack.NewQuery("rights.disconnect_mid_cycle", idx.table).
			AndEqual("cycle", cycle).
			Stream(ctx, func(row pack.Row) error {
				right := &model.Right{}
				if err := row.Decode(right); err != nil {
					return err
				}
				right.Baked.Clear(bakePos)
				right.Endorsed.Clear(bakePos - 1)
				if seedPos >= 0 {
					right.Seed.Clear(seedPos)
				}
				upd = append(upd, right)
				return nil
			})
		if err != nil {
			return fmt.Errorf("rights: MOC disconnect for block %d: %w", block.Height, err)
		}
		return idx.table.Update(ctx, upd)
	}
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

func (idx *RightsIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (idx *RightsIndex) loadRight(ctx context.Context, cycle int64, id model.AccountID) (*model.Right, error) {
	right, ok := idx.cache[id]
	if ok && right.Cycle == cycle {
		return right, nil
	}
	right = &model.Right{}
	err := pack.NewQuery("rights.search_bake", idx.table).
		AndEqual("cycle", cycle).
		AndEqual("account_id", id).
		Execute(ctx, right)
	if err != nil {
		return nil, err
	}
	if right.RowId == 0 {
		return nil, ErrNoRightsEntry
	}
	if right.Cycle == idx.cycle {
		idx.cache[right.AccountId] = right
	}
	return right, nil
}
