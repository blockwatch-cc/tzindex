// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

var RightsIndexKey = "rights"

type RightsIndex struct {
	db    *pack.DB
	table *pack.Table
	cache map[model.AccountID]*model.Right
	cycle int64
}

var _ model.BlockIndexer = (*RightsIndex)(nil)

func NewRightsIndex() *RightsIndex {
	return &RightsIndex{
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
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	m := model.Right{}
	key := m.TableKey()
	fields, err := pack.Fields(m)
	if err != nil {
		return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
	}

	_, err = db.CreateTableIfNotExists(key, fields, m.TableOpts().Merge(readConfigOpts(key)))
	return err
}

func (idx *RightsIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	m := model.Right{}
	key := m.TableKey()

	idx.table, err = idx.db.Table(key, m.TableOpts().Merge(readConfigOpts(key)))
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
	// reset cache at start of cycle
	if block.TZ.IsCycleStart() {
		for n, v := range idx.cache {
			v.Free()
			delete(idx.cache, n)
		}
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
				err := pack.NewQuery("etl.search_seed").
					WithTable(idx.table).
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
			p := builder.Params(sop.Level)
			start := block.TZ.GetCycleStart() - p.BlocksPerCycle // -1 cycle!
			pos := int((sop.Level - start) / p.BlocksPerCommitment)
			right.Seeded.Set(pos)
		}

		// write back to table
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
	}

	// use current block params
	p := block.Params

	// update baking right
	if block.ProposerId > 0 {
		right, err := idx.loadRight(ctx, block.Cycle, block.ProposerId)
		if err != nil {
			log.Warnf("rights: missing baking right for block %d proposer %d: %v", block.Height, block.ProposerId, err)
		} else {
			pos := int(block.TZ.GetCyclePosition())
			right.Baked.Set(pos)
			if block.TZ.IsSeedRequired() {
				right.Seed.Set(pos / int(p.BlocksPerCommitment))
			}
			if err := idx.table.Update(ctx, right); err != nil {
				return err
			}
			// update reliability
			bkr, ok := builder.BakerById(right.AccountId)
			if ok {
				bkr.Reliability = right.Reliability(int(block.TZ.GetCyclePosition()))
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
		params := block.Parent.Params

		// all endorsements are for block - 1, make sure we use the correct settings
		// for the cycle (in case of first block)
		start := block.Parent.TZ.GetCycleStart()
		pos := int(block.Height - 1 - start)
		upd := make([]pack.Item, 0)
		for id := range endorsers {
			right, err := idx.loadRight(ctx, cycle, id)
			if err != nil {
				// on protocol upgrades we may see extra endorsers, create rights here
				right = model.NewRight(id, start, cycle, int(params.BlocksPerCycle), int(params.BlocksPerCycle/params.BlocksPerCommitment))
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
				bkr.Reliability = right.Reliability(int(block.TZ.GetCyclePosition()))
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

	// we use current block's params as they are the most recent ones,
	// - during regular block processing this
	// - during protocol migration this is the set of new params
	start := block.TZ.Baking[0][0].Level
	cycle := p.HeightToCycle(start)

	// create new rights data for each baker, add baking and endorsing rights
	// process cycle by cycle (only during bootstrap/genesis we handle multiple cycles)
	for c := range block.TZ.Baking {
		// advance to next cycle (only on genesis)
		if c > 0 {
			start += p.BlocksPerCycle
			cycle++
		}
		bake := block.TZ.Baking[c]
		endorse := block.TZ.Endorsing[c]

		// sort rights by baker to speed up processing, we don't care when
		// levels end up unsorted, so we use the faster quicksort
		sort.Slice(bake, func(i, j int) bool {
			return bake[i].Delegate < bake[j].Delegate
		})
		sort.Slice(endorse, func(i, j int) bool {
			return endorse[i].Delegate < endorse[j].Delegate
		})

		log.Debugf("rights: adding %d baking and %d endorsing rights for cycle %d starting at %d",
			len(bake), len(endorse), cycle, start)

		// build rights bitmaps for each baker
		rightsByBaker := make(map[model.AccountID]*model.Right)
		ins := make([]pack.Item, 0)
		var (
			baker      string
			right      *model.Right
			ecnt, bcnt int
		)
		for _, v := range bake {
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
				right = model.NewRight(acc.RowId, start, cycle, int(p.BlocksPerCycle), int(p.BlocksPerCycle/p.BlocksPerCommitment))
				rightsByBaker[acc.RowId] = right
				ins = append(ins, right)
			}
			right.Bake.Set(int(v.Level - start))
			bcnt++
		}

		// same for endorsing rights
		baker = ""
		for _, v := range endorse {
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
					right = model.NewRight(acc.RowId, start, cycle, int(p.BlocksPerCycle), int(p.BlocksPerCycle/p.BlocksPerCommitment))
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
			err = pack.NewQuery("etl.create_rights").
				WithTable(snap).
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
					right = model.NewRight(s.AccountId, start, cycle, int(p.BlocksPerCycle), int(p.BlocksPerCycle/p.BlocksPerCommitment))
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
		log.Debugf("rights: inserting %d records with %d baking and %d endorsing rights", len(ins), bcnt, ecnt)
		if err := idx.table.Insert(ctx, ins); err != nil {
			return err
		}
		for _, v := range ins {
			v.(*model.Right).Free()
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
	if block.TZ.IsCycleStart() && block.Parent != nil {
		prevBlocksPerCycle := block.Parent.Params.BlocksPerCycle
		err := pack.NewQuery("etl.rollback_start").
			WithTable(idx.table).
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
			return fmt.Errorf("rights: rollback start block %d: %w", block.Height, err)
		}
		if err := idx.table.Update(ctx, upd); err != nil {
			return err
		}
		return idx.DeleteCycle(ctx, block.Cycle+block.Params.PreservedCycles)
	} else {
		// on a regular mid-cycle block, clear same-cycle bits only
		p := block.Params
		bakePos := int(block.TZ.GetCyclePosition())
		seedPos := -1
		if block.TZ.IsSeedRequired() {
			seedPos = bakePos / int(p.BlocksPerCommitment)
		}
		err := pack.NewQuery("etl.rollback_middle").
			WithTable(idx.table).
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
			return fmt.Errorf("rights: rollback block %d: %w", block.Height, err)
		}
		return idx.table.Update(ctx, upd)
	}
}

func (idx *RightsIndex) DeleteBlock(ctx context.Context, height int64) error {
	return nil
}

func (idx *RightsIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// log.Debugf("Rollback deleting rights for cycle %d", cycle)
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		Delete(ctx)
	return err
}

func (idx *RightsIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *RightsIndex) loadRight(ctx context.Context, cycle int64, id model.AccountID) (*model.Right, error) {
	right, ok := idx.cache[id]
	if ok && right.Cycle == cycle {
		return right, nil
	}
	right = model.AllocRight()
	err := pack.NewQuery("etl.search_bake").
		WithTable(idx.table).
		AndEqual("cycle", cycle).
		AndEqual("account_id", id).
		Execute(ctx, right)
	if err != nil {
		right.Free()
		return nil, err
	}
	if right.RowId == 0 {
		right.Free()
		return nil, model.ErrNoRights
	}
	if right.Cycle == idx.cycle {
		idx.cache[right.AccountId] = right
	}
	return right, nil
}
