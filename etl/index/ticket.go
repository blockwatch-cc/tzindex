// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
	"blockwatch.cc/tzindex/rpc"

	lru "github.com/hashicorp/golang-lru/v2"
)

const TicketIndexKey = "ticket"

type TicketIndex struct {
	db          *pack.DB
	tables      map[string]*pack.Table
	ticketCache *lru.Cache[uint64, *model.Ticket]
	ownerCache  *lru.Cache[uint64, *model.TicketOwner]
}

var _ model.BlockIndexer = (*TicketIndex)(nil)

func NewTicketIndex() *TicketIndex {
	c, _ := lru.New[uint64, *model.Ticket](1 << 15)       // 32k
	oc, _ := lru.New[uint64, *model.TicketOwner](1 << 15) // 32k
	return &TicketIndex{
		tables:      make(map[string]*pack.Table),
		ticketCache: c,
		ownerCache:  oc,
	}
}

func (idx *TicketIndex) DB() *pack.DB {
	return idx.db
}

func (idx *TicketIndex) Tables() []*pack.Table {
	t := []*pack.Table{}
	for _, v := range idx.tables {
		t = append(t, v)
	}
	return t
}

func (idx *TicketIndex) Key() string {
	return TicketIndexKey
}

func (idx *TicketIndex) Name() string {
	return TicketIndexKey + " index"
}

func (idx *TicketIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	for _, m := range []model.Model{
		model.Ticket{},
		model.TicketUpdate{},
		model.TicketEvent{},
		model.TicketOwner{},
	} {
		key := m.TableKey()
		fields, err := pack.Fields(m)
		if err != nil {
			return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
		}
		opts := m.TableOpts().Merge(model.ReadConfigOpts(key))
		_, err = db.CreateTableIfNotExists(key, fields, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *TicketIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	for _, m := range []model.Model{
		model.Ticket{},
		model.TicketUpdate{},
		model.TicketEvent{},
		model.TicketOwner{},
	} {
		key := m.TableKey()
		t, err := idx.db.Table(key, m.TableOpts().Merge(model.ReadConfigOpts(key)))
		if err != nil {
			idx.Close()
			return err
		}
		idx.tables[key] = t
	}
	return nil
}

func (idx *TicketIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *TicketIndex) Close() error {
	for n, v := range idx.tables {
		if err := v.Close(); err != nil {
			log.Errorf("Closing %s table: %s", n, err)
		}
		delete(idx.tables, n)
	}
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *TicketIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
	ins := make([]pack.Item, 0)

	for _, op := range block.Ops {
		for _, up := range op.RawTicketUpdates {
			// load or create type
			tick, err := idx.getOrCreateTicket(ctx, up.Ticket, op, b)
			if err != nil {
				return fmt.Errorf("ticket: load/create type for %s: %v", op.Hash, err)
			}

			// process balance changes
			for _, bal := range up.Updates {
				acc, ok := b.AccountByAddress(bal.Account)
				if !ok {
					log.Errorf("ticket: missing owner account %s in %s", bal.Account, op.Hash)
					continue
				}

				tu := model.NewTicketUpdate()
				tu.TicketId = tick.Id
				tu.AccountId = acc.RowId
				tu.Amount = bal.Amount
				tu.Height = op.Height
				tu.Time = op.Timestamp
				tu.OpId = op.Id()
				ins = append(ins, tu)
			}

			// reconcile events
			events, err := idx.reconcileEvents(tick, up.Updates, b)
			if err != nil {
				log.Errorf("ticket: %d %s reconcile: %v", op.Height, op.Hash, err)
				continue
			}

			// stop here if we haven't seen any events
			if len(events) == 0 {
				continue
			}

			// complete events
			for _, ev := range events {
				ev.Height = op.Height
				ev.Time = op.Timestamp
				ev.OpId = op.Id()
			}

			// store events
			if err := model.StoreTicketEvents(ctx, idx.tables[model.TicketEventTableKey], events); err != nil {
				log.Errorf("ticket: %d %s store events: %v", op.Height, op.Hash, err)
				continue
			}

			// update owners, balances, token supply from event
			if err := idx.processEvents(ctx, tick, events); err != nil {
				log.Errorf("ticket: %d %s process events: %v", op.Height, op.Hash, err)
				continue
			}
		}
	}

	// batch insert all updates
	if len(ins) > 0 {
		if err := idx.tables[model.TicketUpdateTableKey].Insert(ctx, ins); err != nil {
			return fmt.Errorf("ticket: insert: %w", err)
		}
	}

	return nil
}

func (idx *TicketIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *TicketIndex) DeleteBlock(ctx context.Context, height int64) error {
	updates := idx.tables[model.TicketUpdateTableKey]
	events := idx.tables[model.TicketEventTableKey]
	owners := idx.tables[model.TicketOwnerTableKey]
	tickets := idx.tables[model.TicketTableKey]
	_, err := pack.NewQuery("etl.delete").
		WithTable(updates).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}

	// rollback owner balances, stats and ticket stats from events
	list, err := model.ListTicketEvents(ctx, events, pack.NewQuery("etl.rollback.list_ticket_events").
		AndEqual("height", height).
		WithDesc(),
	)
	if err != nil {
		return fmt.Errorf("list token events: %v", err)
	}
	for _, ev := range list {
		tick, err := model.GetTicketId(ctx, tickets, ev.Ticket)
		if err != nil {
			return fmt.Errorf("load ticket %d: %v", ev.Ticket, err)
		}

		switch ev.Type {
		case model.TicketEventTypeMint:
			recv, err := model.GetTicketOwner(ctx, owners, ev.Receiver, ev.Ticket)
			if err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}
			recv.NumMints--
			recv.VolMint = recv.VolMint.Sub(ev.Amount)
			recv.Balance = recv.Balance.Sub(ev.Amount)

			if err := recv.Store(ctx, owners); err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}

			if !recv.WasZero && recv.Balance.IsZero() {
				tick.NumHolders--
			}
			tick.Supply = tick.Supply.Sub(ev.Amount)
			tick.TotalMint = tick.TotalMint.Sub(ev.Amount)

		case model.TicketEventTypeBurn:
			sndr, err := model.GetTicketOwner(ctx, owners, ev.Sender, ev.Ticket)
			if err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}
			sndr.NumBurns--
			sndr.VolBurn = sndr.VolBurn.Sub(ev.Amount)
			sndr.Balance = sndr.Balance.Add(ev.Amount)

			if err := sndr.Store(ctx, owners); err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}

			if sndr.WasZero && !sndr.Balance.IsZero() {
				tick.NumHolders++
			}
			tick.Supply = tick.Supply.Add(ev.Amount)
			tick.TotalBurn = tick.TotalBurn.Sub(ev.Amount)

		case model.TicketEventTypeTransfer:
			sndr, err := model.GetTicketOwner(ctx, owners, ev.Sender, ev.Ticket)
			if err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}
			recv, err := model.GetTicketOwner(ctx, owners, ev.Receiver, ev.Ticket)
			if err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}
			sndr.NumTransfers--
			sndr.VolSent = sndr.VolSent.Sub(ev.Amount)
			sndr.Balance = sndr.Balance.Add(ev.Amount)

			recv.NumTransfers--
			recv.VolRecv = recv.VolRecv.Sub(ev.Amount)
			recv.Balance = recv.Balance.Sub(ev.Amount)

			if err := sndr.Store(ctx, owners); err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}

			if err := recv.Store(ctx, owners); err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}

			if sndr.WasZero && !sndr.Balance.IsZero() {
				tick.NumHolders++
			}
			if !recv.WasZero && recv.Balance.IsZero() {
				tick.NumHolders--
			}
			tick.NumTransfers--
		}

		if err := tick.Store(ctx, tickets); err != nil {
			return fmt.Errorf("T_%d update: %w", tick.Id, err)
		}
	}

	// remove events
	_, err = pack.NewQuery("etl.rollback.remove_ticket_events").
		WithTable(events).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return fmt.Errorf("delete ticket events: %v", err)
	}

	return nil
}

func (idx *TicketIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// _, err := pack.NewQuery("etl.delete").
	//     WithTable(idx.table).
	//     AndRange("height", params.CycleStartHeight(cycle), params.CycleEndHeight(cycle),).
	//     Delete(ctx)
	return nil
}

func (idx *TicketIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (idx *TicketIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}

func (idx *TicketIndex) reconcileEvents(
	tick *model.Ticket,
	updates []rpc.TicketBalanceUpdate,
	builder model.BlockBuilder,
) ([]*model.TicketEvent, error) {

	// track ticket balances assuming RPC response contains FIFO order
	var sumIn, sumOut, sumMinted, sumBurned, sumTransfer tezos.Z

	// track running balances
	type flow struct {
		account model.AccountID
		amount  tezos.Z
		isOut   bool
	}

	sources := make([]flow, 0)
	sinks := make([]flow, 0)

	// load owners and analyze flows
	for _, v := range updates {
		acc, ok := builder.AccountByAddress(v.Account)
		if !ok {
			return nil, fmt.Errorf("lookup missing balance owner %s T_%d", v.Account, tick.Id)
		}
		if v.Amount.IsNeg() {
			sumOut = sumOut.Sub(v.Amount)
			sources = append(sources, flow{account: acc.RowId, amount: v.Amount.Neg(), isOut: true})
		} else {
			sumIn = sumIn.Add(v.Amount)
			sinks = append(sinks, flow{account: acc.RowId, amount: v.Amount, isOut: false})
		}
	}

	// cases
	// - mint only
	// - burn only
	// - transfer only
	// - mixed

	events := make([]*model.TicketEvent, 0)
	for _, src := range sources {
		// distribute amount over sinks
		for {
			if len(sinks) == 0 {
				break
			}
			amount := tezos.MinZ(src.amount, sinks[0].amount)

			// transfer event
			events = append(events, &model.TicketEvent{
				Type:     model.TicketEventTypeTransfer,
				Ticket:   tick.Id,
				Ticketer: tick.Ticketer,
				Sender:   src.account,
				Receiver: sinks[0].account,
				Amount:   amount,
			})
			sumTransfer = sumTransfer.Add(amount)

			// update tracking amounts
			src.amount = src.amount.Sub(amount)
			sinks[0].amount = sinks[0].amount.Sub(amount)

			// drop sink when fully reconciled
			if sinks[0].amount.IsZero() {
				sinks = sinks[1:]
			}

			// next source
			if src.amount.IsZero() {
				break
			}
		}

		// remainder is burned
		if src.amount.IsZero() {
			continue
		}

		// burn event
		events = append(events, &model.TicketEvent{
			Type:     model.TicketEventTypeBurn,
			Ticket:   tick.Id,
			Ticketer: tick.Ticketer,
			Sender:   src.account,
			Receiver: 0,
			Amount:   src.amount,
		})
		sumBurned = sumBurned.Add(src.amount)
		src.amount = tezos.Zero
	}

	// any amount still in sinks is minted
	for _, sink := range sinks {
		// mint event
		events = append(events, &model.TicketEvent{
			Type:     model.TicketEventTypeMint,
			Ticket:   tick.Id,
			Ticketer: tick.Ticketer,
			Sender:   0,
			Receiver: sink.account,
			Amount:   sink.amount,
		})
		sumMinted = sumMinted.Add(sink.amount)
	}

	// cross-check
	if !sumIn.Equal(sumMinted.Add(sumTransfer)) {
		return nil, fmt.Errorf("flow mismatch: in=%s != minted=%s + transfer=%s", sumIn, sumMinted, sumTransfer)
	}
	if !sumOut.Equal(sumBurned.Add(sumTransfer)) {
		return nil, fmt.Errorf("flow mismatch: out=%s != burned=%s + transfer=%s", sumOut, sumBurned, sumTransfer)
	}

	return events, nil
}

func (idx *TicketIndex) processEvents(ctx context.Context, tick *model.Ticket, events []*model.TicketEvent) error {
	owners := idx.tables[model.TicketOwnerTableKey]
	for _, ev := range events {
		switch ev.Type {
		case model.TicketEventTypeMint:
			recv, err := idx.getOrCreateOwner(ctx, ev.Receiver, tick.Ticketer, tick.Id, ev.Height, ev.Time)
			if err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}
			recv.LastBlock = ev.Height
			recv.LastTime = ev.Time
			recv.NumMints++
			recv.VolMint = recv.VolMint.Add(ev.Amount)
			recv.Balance = recv.Balance.Add(ev.Amount)

			if err := recv.Store(ctx, owners); err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}

			if recv.WasZero && !recv.Balance.IsZero() {
				tick.NumHolders++
			}
			tick.LastBlock = ev.Height
			tick.LastTime = ev.Time
			tick.Supply = tick.Supply.Add(ev.Amount)
			tick.TotalMint = tick.TotalMint.Add(ev.Amount)

		case model.TicketEventTypeBurn:
			sndr, err := idx.getOrCreateOwner(ctx, ev.Sender, tick.Ticketer, tick.Id, ev.Height, ev.Time)
			if err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}
			sndr.LastBlock = ev.Height
			sndr.LastTime = ev.Time
			sndr.NumBurns++
			sndr.VolBurn = sndr.VolBurn.Add(ev.Amount)
			sndr.Balance = sndr.Balance.Sub(ev.Amount)

			if err := sndr.Store(ctx, owners); err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}

			if sndr.Balance.IsZero() {
				tick.NumHolders--
			}
			tick.LastBlock = ev.Height
			tick.LastTime = ev.Time
			tick.Supply = tick.Supply.Sub(ev.Amount)
			tick.TotalBurn = tick.TotalBurn.Add(ev.Amount)

		case model.TicketEventTypeTransfer:
			sndr, err := idx.getOrCreateOwner(ctx, ev.Sender, tick.Ticketer, tick.Id, ev.Height, ev.Time)
			if err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}
			recv, err := idx.getOrCreateOwner(ctx, ev.Receiver, tick.Ticketer, tick.Id, ev.Height, ev.Time)
			if err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}
			sndr.LastBlock = ev.Height
			sndr.LastTime = ev.Time
			sndr.NumTransfers++
			sndr.VolSent = sndr.VolSent.Add(ev.Amount)
			sndr.Balance = sndr.Balance.Sub(ev.Amount)

			recv.LastBlock = ev.Height
			recv.LastTime = ev.Time
			recv.NumTransfers++
			recv.VolRecv = recv.VolRecv.Add(ev.Amount)
			recv.Balance = recv.Balance.Add(ev.Amount)

			if err := sndr.Store(ctx, owners); err != nil {
				return fmt.Errorf("sender %d: %w", ev.Sender, err)
			}

			if err := recv.Store(ctx, owners); err != nil {
				return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
			}

			if sndr.Balance.IsZero() {
				tick.NumHolders--
			}
			if recv.WasZero && !recv.Balance.IsZero() {
				tick.NumHolders++
			}
			tick.LastBlock = ev.Height
			tick.LastTime = ev.Time
			tick.NumTransfers++
		}
	}

	if err := tick.Store(ctx, idx.tables[model.TicketTableKey]); err != nil {
		return fmt.Errorf("T_%d update: %w", tick.Id, err)
	}
	return nil
}

func (idx *TicketIndex) getOrCreateTicket(ctx context.Context, t rpc.Ticket, op *model.Op, builder model.BlockBuilder) (*model.Ticket, error) {
	key := t.Hash64()
	typ, ok := idx.ticketCache.Get(key)
	if ok {
		return typ, nil
	}
	typ = model.NewTicket()
	err := pack.NewQuery("etl.find_ticket_type").
		WithTable(idx.tables[model.TicketTableKey]).
		AndEqual("address", t.Ticketer).
		AndEqual("hash", key).
		Execute(ctx, typ)
	if err != nil {
		return nil, err
	}
	if typ.Id == 0 {
		acc, err := builder.LoadAccountByAddress(ctx, t.Ticketer)
		if err != nil {
			return nil, fmt.Errorf("lookup ticket issuer %s: %v", t.Ticketer, err)
		}
		typ.Address = t.Ticketer
		typ.Ticketer = acc.RowId
		typ.Type = t.Type
		typ.Content = t.Content
		typ.Hash = key
		typ.Creator = op.SenderId
		typ.FirstBlock = op.Height
		typ.FirstTime = op.Timestamp
		typ.LastBlock = op.Height
		typ.LastTime = op.Timestamp
		if err := typ.Store(ctx, idx.tables[model.TicketTableKey]); err != nil {
			return nil, err
		}
	}
	idx.ticketCache.Add(key, typ)
	return typ, nil
}

func ticketOwnerCacheKey(owner model.AccountID, id model.TicketID) uint64 {
	return uint64(owner)<<32 | uint64(id)
}

func (idx *TicketIndex) getOrCreateOwner(ctx context.Context, ownerId, issuerId model.AccountID, id model.TicketID, height int64, tm time.Time) (*model.TicketOwner, error) {
	owners := idx.tables[model.TicketOwnerTableKey]
	key := ticketOwnerCacheKey(ownerId, id)
	owner, ok := idx.ownerCache.Get(key)
	if ok {
		owner.WasZero = owner.Balance.IsZero()
		return owner, nil
	}
	ownr, err := model.GetOrCreateTicketOwner(ctx, owners, ownerId, issuerId, id, height, tm)
	if err != nil {
		return nil, err
	}
	if ownr.Id > 0 {
		idx.ownerCache.Add(key, ownr)
	}
	return ownr, nil
}
