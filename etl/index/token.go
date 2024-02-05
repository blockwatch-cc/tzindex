// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/big"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"github.com/echa/config"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/tidwall/gjson"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

// we use 6 distinct data sets for data locality
// - `contract` for storing ledger identity
// - `metadata` for storing ledger and token metadata
// - `token` for storing token identity and header metadata
// - `token_event` for storing updates transfer/mint/burn
// - `token_owners` for live token balances per owner and running stats

const TokenIndexKey = "token"

var bigzero = big.NewInt(0)

type TokenIndex struct {
	db          *pack.DB
	tables      map[string]*pack.Table
	tokenCache  *lru.Cache[uint64, *model.Token]
	ownerCache  *lru.Cache[uint64, *model.TokenOwner]
	metaBaseUrl string
}

var _ model.BlockIndexer = (*TokenIndex)(nil)

func NewTokenIndex() *TokenIndex {
	tc, _ := lru.New[uint64, *model.Token](1 << 15)      // 32k
	oc, _ := lru.New[uint64, *model.TokenOwner](1 << 15) // 32k
	return &TokenIndex{
		tables:      make(map[string]*pack.Table),
		tokenCache:  tc,
		ownerCache:  oc,
		metaBaseUrl: config.GetString("meta.token.url"),
	}
}

func (idx *TokenIndex) DB() *pack.DB {
	return idx.db
}

func (idx *TokenIndex) Tables() []*pack.Table {
	t := []*pack.Table{}
	for _, v := range idx.tables {
		t = append(t, v)
	}
	return t
}

func (idx *TokenIndex) Key() string {
	return TokenIndexKey
}

func (idx *TokenIndex) Name() string {
	return TokenIndexKey + " index"
}

func (idx *TokenIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	for _, m := range []model.Model{
		model.Token{},
		model.TokenMeta{},
		model.TokenEvent{},
		model.TokenOwner{},
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

func (idx *TokenIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	for _, m := range []model.Model{
		model.Token{},
		model.TokenMeta{},
		model.TokenEvent{},
		model.TokenOwner{},
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

func (idx *TokenIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *TokenIndex) Close() error {
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

func (idx *TokenIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
	for _, op := range block.Ops {
		// skip non-contract calls
		if !op.IsContract || op.Contract == nil {
			continue
		}
		ldgr := op.Contract

		// skip non-ledger calls
		if !ldgr.LedgerType.IsValid() || !ldgr.LedgerSchema.IsValid() {
			continue
		}

		// skip when ledger has not yet been created
		if ldgr.LedgerBigmap == 0 {
			continue
		}

		// identify token metadata updates (any write to a bigmap called token_metadata)
		// Note: ledger metadata is resolved and updated in metadata index
		if upd := op.BigmapEvents.Filter(ldgr.MetadataBigmap); len(upd) > 0 {
			for _, v := range upd {
				if v.Action != micheline.DiffActionUpdate {
					continue
				}
				if v.Key.Int == nil {
					continue
				}
				// find token
				tokn, err := model.GetToken(ctx, idx.tables[model.TokenTableKey], op.Contract, tezos.NewBigZ(v.Key.Int))
				if err != nil {
					continue
				}

				// schedule task
				req := task.TaskRequest{
					Index:   idx.Key(),
					Decoder: 0,
					Owner:   op.Contract.Address,
					Flags:   uint64(tokn.Id),
					Url: strings.Replace(
						idx.metaBaseUrl,
						"{addr}",
						tezos.NewToken(op.Contract.Address, tokn.TokenId).String(),
						1,
					),
				}
				_ = b.Sched().Run(req)
				tokn.Free()
			}
		}

		// decode balance updates
		upd, err := ldgr.LedgerSchema.DecodeBalanceUpdates(op.BigmapEvents, ldgr.LedgerBigmap)
		if err != nil {
			log.Errorf("token: %d %s decoding %s balance updates: %v", op.Height, op.Hash, ldgr, err)
			continue
		}
		if len(upd) == 0 {
			continue
		}

		// on transactions, also decode transfer events
		var xfers []model.TokenTransfer
		if op.Type == model.OpTypeTransaction {
			var p micheline.Parameters
			_ = p.UnmarshalBinary(op.Parameters)

			// convert call parameters
			params := ldgr.ConvertParams(p)

			// try decode transfers (ignore entrypoint name, ignore errors)
			// e.g. mint/burn and other calls may not have the expected structure
			// if strings.HasPrefix(params.Entrypoint, "transfer") && ptyp.Matches(params.Value) {
			if ldgr.LedgerType.Matches(params.Value) {
				xfers, err = ldgr.LedgerType.DecodeTransfers(params.Value)
				if err != nil {
					log.Debugf("token: %d %s decode %s transfers: %v", op.Height, op.Hash, ldgr.LedgerType, err)
					continue
				}
			}
		}

		// Note: external and internal call set sender = tx signer
		// should this ever change, this code must be updated
		sgnr, ok := b.AccountById(op.SenderId)
		if !ok {
			continue
		}

		// reconcile transfers with balance updates to produce events
		events, err := idx.reconcileEvents(ctx, ldgr, sgnr, upd, xfers, op.Height, op.Timestamp, b)
		if err != nil {
			log.Errorf("token: %d %s reconcile: %v", op.Height, op.Hash, err)
			continue
		}
		// log.Infof("%d %s %d updates, %d xfers %d events", op.Height, op.Hash, len(upd), len(xfers), len(events))

		// stop here if we haven't seen any events
		if len(events) == 0 {
			continue
		}

		// complete events
		for _, ev := range events {
			ev.OpId = op.RowId
			// log.Infof("> %s %s", ev.Type, ev.Amount)
		}

		// store events
		if err := model.StoreTokenEvents(ctx, idx.tables[model.TokenEventTableKey], events); err != nil {
			log.Errorf("token: %d %sstore events: %v", op.Height, op.Hash, err)
			continue
		}

		// update owners, balances, token supply from event
		if err := idx.processEvents(ctx, events); err != nil {
			log.Errorf("token: %d %s process events: %v", op.Height, op.Hash, err)
		}

		// resolve metadata for new tokens (call after reconcile which adds TokenRef)
		for _, bal := range upd {
			if bal.TokenRef.FirstBlock < op.Height {
				continue
			}
			// schedule task
			req := task.TaskRequest{
				Index:   idx.Key(),
				Decoder: 0,
				Owner:   op.Contract.Address,
				Flags:   uint64(bal.TokenRef.Id),
				Url: strings.Replace(
					idx.metaBaseUrl,
					"{addr}",
					tezos.NewToken(op.Contract.Address, bal.TokenRef.TokenId).String(),
					1,
				),
			}
			_ = b.Sched().Run(req)
		}

		// free resources
		for _, v := range events {
			v.Free()
		}
	}
	return nil
}

func (idx *TokenIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *TokenIndex) DeleteBlock(ctx context.Context, height int64) error {
	// - rollback owner balances, stats and token stats from events
	events := idx.tables[model.TokenEventTableKey]
	owners := idx.tables[model.TokenOwnerTableKey]
	tokens := idx.tables[model.TokenTableKey]
	list := make([]*model.TokenEvent, 0)
	err := pack.NewQuery("etl.rollback.list_token_events").
		WithTable(events).
		AndEqual("height", height).
		WithDesc().
		Execute(ctx, &list)
	if err != nil {
		return fmt.Errorf("list token events: %v", err)
	}
	for _, ev := range list {
		tokn, err := idx.findTokenId(ctx, ev.Token)
		if err != nil {
			return fmt.Errorf("load token %d: %v", ev.Token, err)
		}

		switch ev.Type {
		case model.TokenEventTypeMint:
			recv, err := model.GetTokenOwner(ctx, owners, ev.Receiver, ev.Token)
			if err != nil {
				return fmt.Errorf("load receiver %d: %v", ev.Receiver, err)
			}
			recv.NumMints--
			recv.VolMint = recv.VolMint.Sub(ev.Amount)
			recv.Balance = recv.Balance.Sub(ev.Amount)
			tokn.TotalMint = tokn.TotalMint.Sub(ev.Amount)
			tokn.Supply = tokn.Supply.Sub(ev.Amount)
			if !recv.WasZero && recv.Balance.IsZero() {
				tokn.NumHolders--
			}
			if err := owners.Update(ctx, recv); err != nil {
				return fmt.Errorf("save receiver %d: %v", ev.Receiver, err)
			}
			if err := tokens.Update(ctx, tokn); err != nil {
				return fmt.Errorf("save token %d: %v", ev.Token, err)
			}
		case model.TokenEventTypeBurn:
			sndr, err := model.GetTokenOwner(ctx, owners, ev.Sender, ev.Token)
			if err != nil {
				return fmt.Errorf("load sender %d: %v", ev.Sender, err)
			}
			sndr.NumBurns--
			sndr.VolBurn = sndr.VolBurn.Sub(ev.Amount)
			sndr.Balance = sndr.Balance.Add(ev.Amount)
			tokn.TotalBurn = tokn.TotalBurn.Sub(ev.Amount)
			tokn.Supply = tokn.Supply.Add(ev.Amount)
			if sndr.WasZero && !sndr.Balance.IsZero() {
				tokn.NumHolders++
			}
			if err := owners.Update(ctx, sndr); err != nil {
				return fmt.Errorf("save sender %d: %v", ev.Sender, err)
			}
			if err := tokens.Update(ctx, tokn); err != nil {
				return fmt.Errorf("save token %d: %v", ev.Token, err)
			}
		case model.TokenEventTypeTransfer:
			sndr, err := model.GetTokenOwner(ctx, owners, ev.Sender, ev.Token)
			if err != nil {
				return fmt.Errorf("load sender %d: %v", ev.Sender, err)
			}
			recv, err := model.GetTokenOwner(ctx, owners, ev.Receiver, ev.Token)
			if err != nil {
				return fmt.Errorf("load receiver %d: %v", ev.Sender, err)
			}
			sndr.NumTransfers--
			recv.NumTransfers--
			tokn.NumTransfers--
			sndr.VolSent = sndr.VolSent.Sub(ev.Amount)
			sndr.Balance = sndr.Balance.Add(ev.Amount)
			recv.VolRecv = recv.VolRecv.Sub(ev.Amount)
			recv.Balance = recv.Balance.Sub(ev.Amount)
			if sndr.WasZero && !sndr.Balance.IsZero() {
				tokn.NumHolders++
			}
			if recv.Balance.IsZero() && !recv.WasZero {
				tokn.NumHolders--
			}
			if err := owners.Update(ctx, sndr); err != nil {
				return fmt.Errorf("save sender %d: %v", ev.Sender, err)
			}
			if err := owners.Update(ctx, recv); err != nil {
				return fmt.Errorf("save receiver %d: %v", ev.Receiver, err)
			}
			if err := tokens.Update(ctx, tokn); err != nil {
				return fmt.Errorf("save token %d: %v", ev.Token, err)
			}
		}
	}

	// - remove events
	_, err = pack.NewQuery("etl.rollback.remove_token_events").
		WithTable(events).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return fmt.Errorf("delete token events: %v", err)
	}

	return nil
}

func (idx *TokenIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	// unused
	return nil
}

func (idx *TokenIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (idx *TokenIndex) OnTaskComplete(ctx context.Context, res *task.TaskResult) error {
	// check success
	if res.Status != task.TaskStatusSuccess {
		return nil
	}

	// check response is proper JSON
	if !gjson.ValidBytes(res.Data) {
		return nil
	}

	// store metadata update
	meta := &model.TokenMeta{
		Token: model.TokenID(res.Flags),
		Data:  res.Data,
	}

	if err := idx.tables[model.TokenMetaTableKey].Insert(ctx, meta); err != nil {
		return fmt.Errorf("token: store token T_%d metadata: %v", res.Flags, err)
	}

	return nil
}

func (idx *TokenIndex) reconcileEvents(
	ctx context.Context,
	ldgr *model.Contract,
	sgnr *model.Account,
	updates []*model.LedgerBalance,
	xfers []model.TokenTransfer,
	height int64,
	tm time.Time,
	builder model.BlockBuilder,
) ([]*model.TokenEvent, error) {
	// track token balances
	running := make(map[model.TokenOwnerID]*big.Int)
	events := make([]*model.TokenEvent, 0)

	for _, xfer := range xfers {
		// load related records
		tokn, err := idx.getOrCreateToken(ctx, ldgr, sgnr, xfer.TokenId, height, tm)
		if err != nil {
			return nil, fmt.Errorf("lookup tokenid %s in ledger %s:%d: %w", xfer.TokenId, ldgr.Address, ldgr.LedgerBigmap, err)
		}
		from, ok := builder.AccountByAddress(xfer.From)
		if !ok {
			return nil, fmt.Errorf("lookup missing from account %s", xfer.From)
		}
		to, ok := builder.AccountByAddress(xfer.To)
		if !ok {
			return nil, fmt.Errorf("lookup missing to account %s", xfer.From)
		}
		sndr, err := idx.getOrCreateOwner(ctx, from.RowId, tokn.Id, tokn.Ledger)
		if err != nil {
			return nil, fmt.Errorf("lookup ownership for acc=%s tokid=%d: %w", xfer.From, tokn.Id, err)
		}
		recv, err := idx.getOrCreateOwner(ctx, to.RowId, tokn.Id, tokn.Ledger)
		if err != nil {
			return nil, fmt.Errorf("lookup ownership for acc=%s tokid=%d: %w", xfer.To, tokn.Id, err)
		}

		// sanity checks
		// if sndr.Balance.Big().Cmp(xfer.Amount.Big()) < 0 {
		//  ctx.Log.Errorf("%d T_%d %s-%s: sender balance %s < outgoing amount %s",
		//      height,
		//      tokn.Id, ldgr.Address, tokn.TokenId,
		//      sndr.Balance, xfer.Amount)
		// }

		// calculate running balances by applying transfers
		bal, ok := running[sndr.Id]
		if !ok {
			// ctx.Log.Debugf("> before %s (A_%d) = %s", xfer.From, from.Id, sndr.Balance)
			bal = new(big.Int).Set(sndr.Balance.Big())
			running[sndr.Id] = bal
		}
		running[sndr.Id] = bal.Sub(bal, xfer.Amount.Big())

		bal, ok = running[recv.Id]
		if !ok {
			// ctx.Log.Debugf("> before %s (A_%d) = %s", xfer.To, to.Id, recv.Balance)
			bal = new(big.Int).Set(recv.Balance.Big())
			running[recv.Id] = bal
		}
		running[recv.Id] = bal.Add(bal, xfer.Amount.Big())

		// produce a transfer event
		events = append(events, &model.TokenEvent{
			Ledger:   tokn.Ledger,
			Token:    tokn.Id,
			Type:     model.TokenEventTypeTransfer,
			Signer:   sgnr.RowId,
			Sender:   from.RowId,
			Receiver: to.RowId,
			Amount:   xfer.Amount,
			Height:   height,
			Time:     tm,
			TokenRef: tokn,
		})
	}

	// subtract end balances read from ledger, ideally the result should be zero
	// unless there has been a mint or burn event
	for _, bal := range updates {
		// load related records
		tokn, err := idx.getOrCreateToken(ctx, ldgr, sgnr, bal.TokenId, height, tm)
		if err != nil {
			return nil, fmt.Errorf("lookup tokenid %s in ledger %s:%d: %w", bal.TokenId, ldgr.Address, ldgr.LedgerBigmap, err)
		}

		// SPECIAL CASE
		// some NFT ledgers use token_id -> address where owner is missing on burn (remove)
		// the address is missing
		if ldgr.LedgerSchema == model.LedgerSchemaNFT2 && !bal.Owner.IsValid() {
			// find the current token owner in DB
			ownr, err := model.GetCurrentTokenOwner(ctx, idx.tables[model.TokenOwnerTableKey], tokn.Id)
			if err != nil {
				return nil, fmt.Errorf("lookup current token owner for %s-%s: %w", ldgr.Address, bal.TokenId, err)
			}
			curr, ok := running[ownr.Id]
			if !ok {
				curr = new(big.Int).Set(ownr.Balance.Big())
				running[ownr.Id] = curr
			}
			running[ownr.Id] = curr.Sub(curr, bal.Balance.Big())

			// set zero flag and balance here because we will use this Owner record
			// during updateHolders(), but do not access the same record instance
			// in updateOwnership() since this func will load from cache and hence
			// see a different copy
			ownr.WasZero = false
			ownr.Balance.SetInt64(0)

			// reference owner/token/ledger for further processing
			bal.OwnerRef = ownr
			bal.TokenRef = tokn

		} else {
			acc, ok := builder.AccountByAddress(bal.Owner)
			if !ok {
				return nil, fmt.Errorf("lookup missing balance owner account %s", bal.Owner)
			}
			ownr, err := idx.getOrCreateOwner(ctx, acc.RowId, tokn.Id, tokn.Ledger)
			if err != nil {
				return nil, fmt.Errorf("lookup ownership for acc=%s tokid=%d: %w", bal.Owner, tokn.Id, err)
			}
			ownr.WasZero = ownr.Balance.IsZero()
			curr, ok := running[ownr.Id]
			if !ok {
				curr = new(big.Int).Set(ownr.Balance.Big())
				running[ownr.Id] = curr
			}
			running[ownr.Id] = curr.Sub(curr, bal.Balance.Big())

			// reference owner/token/ledger for further processing
			bal.OwnerRef = ownr
			bal.TokenRef = tokn
		}
	}

	// detect balance changes unexplained by transfers (implicit transfers)
	for x, xbal := range updates {
		xcurr := running[xbal.OwnerRef.Id]
		xdiff := xcurr.Cmp(bigzero)
		if xdiff == 0 {
			continue
		}
		// search for later balance changes
		for _, ybal := range updates[x+1:] {
			ycurr := running[ybal.OwnerRef.Id]
			ydiff := ycurr.Cmp(bigzero)
			// must be same token and opposite sign
			if ydiff == 0 || xdiff == ydiff || !xbal.TokenId.Equal(ybal.TokenId) {
				continue
			}
			// amount is the smaller Abs value of both
			amount := new(big.Int)
			if ycurr.CmpAbs(xcurr) <= 0 {
				amount.Abs(ycurr)
			} else {
				amount.Abs(xcurr)
			}
			// create implicit transfer
			if xdiff < ydiff {
				// transfer y -> x
				events = append(events, &model.TokenEvent{
					Type:     model.TokenEventTypeTransfer,
					Ledger:   xbal.OwnerRef.Ledger,
					Token:    xbal.OwnerRef.Token,
					Signer:   sgnr.RowId,
					Sender:   ybal.OwnerRef.Account,
					Receiver: xbal.OwnerRef.Account,
					Amount:   tezos.NewBigZ(amount),
					Height:   height,
					Time:     tm,
					TokenRef: xbal.TokenRef,
				})
				xcurr = xcurr.Add(xcurr, amount)
				_ = ycurr.Sub(ycurr, amount)
			} else {
				// transfer x -> y
				events = append(events, &model.TokenEvent{
					Type:     model.TokenEventTypeTransfer,
					Ledger:   xbal.OwnerRef.Ledger,
					Token:    xbal.OwnerRef.Token,
					Signer:   sgnr.RowId,
					Sender:   xbal.OwnerRef.Account,
					Receiver: ybal.OwnerRef.Account,
					Amount:   tezos.NewBigZ(amount),
					Height:   height,
					Time:     tm,
					TokenRef: xbal.TokenRef,
				})
				xcurr = xcurr.Sub(xcurr, amount)
				_ = ycurr.Add(ycurr, amount)
			}
		}
	}

	// detect balance changes unexplained by transfers (<0 == mint, >0 == burn)
	// remaining non-zero running balances are
	// - mints when <0 (after > before + transfer)
	// - burns when >0 (after < before + transfer)
	for _, bal := range updates {
		curr := running[bal.OwnerRef.Id]
		switch curr.Cmp(bigzero) {
		case 0:
			// all good
		case -1:
			// mint
			events = append(events, &model.TokenEvent{
				Type:     model.TokenEventTypeMint,
				Ledger:   bal.OwnerRef.Ledger,
				Token:    bal.OwnerRef.Token,
				Signer:   sgnr.RowId,
				Sender:   0,
				Receiver: bal.OwnerRef.Account,
				Amount:   tezos.NewBigZ(new(big.Int).Abs(curr)),
				Height:   height,
				Time:     tm,
				TokenRef: bal.TokenRef,
			})
			running[bal.OwnerRef.Id] = bigzero
		case 1:
			// burn
			events = append(events, &model.TokenEvent{
				Type:     model.TokenEventTypeBurn,
				Ledger:   bal.OwnerRef.Ledger,
				Token:    bal.OwnerRef.Token,
				Signer:   sgnr.RowId,
				Sender:   bal.OwnerRef.Account,
				Receiver: 0,
				Amount:   tezos.NewBigZ(curr),
				Height:   height,
				Time:     tm,
				TokenRef: bal.TokenRef,
			})
			running[bal.OwnerRef.Id] = bigzero
		}
	}

	// detect leftover running balances (secret mints or burns, e.g.
	// caused by transfers with empty accounts); this should not happen
	// at all, but may happen when contracts cheat
	for id, bal := range running {
		diff := bal.Cmp(bigzero)
		if diff == 0 {
			continue
		}
		// find the owner
		ownr, err := model.GetOwnerId(ctx, idx.tables[model.TokenOwnerTableKey], id)
		if err != nil {
			log.Errorf("loading owner %d: %v", id, err)
			continue
		}
		if ownr.Id == 0 {
			log.Errorf("owner %d for open balance %s does not exist", id, bal)
			continue
		}
		// find token
		tokn, err := idx.findTokenId(ctx, ownr.Token)
		if err != nil {
			log.Errorf("loading token %d: %v", ownr.Token, err)
			continue
		}
		if tokn.Id == 0 {
			log.Errorf("token %d for open balance %s does not exist", ownr.Token, bal)
			continue
		}
		if diff < 0 {
			// mint
			events = append(events, &model.TokenEvent{
				Type:     model.TokenEventTypeMint,
				Ledger:   ownr.Ledger,
				Token:    ownr.Token,
				Signer:   sgnr.RowId,
				Sender:   0,
				Receiver: ownr.Account,
				Amount:   tezos.NewBigZ(new(big.Int).Abs(bal)),
				Height:   height,
				Time:     tm,
				TokenRef: tokn,
			})
			running[id] = bigzero
		} else {
			// burn
			events = append(events, &model.TokenEvent{
				Type:     model.TokenEventTypeBurn,
				Ledger:   ownr.Ledger,
				Token:    ownr.Token,
				Signer:   sgnr.RowId,
				Sender:   ownr.Account,
				Receiver: 0,
				Amount:   tezos.NewBigZ(bal),
				Height:   height,
				Time:     tm,
				TokenRef: tokn,
			})
			running[id] = bigzero
		}
	}

	return events, nil
}

func (idx *TokenIndex) processEvents(ctx context.Context, events []*model.TokenEvent) error {
	for _, ev := range events {
		if err := idx.updateOwnership(ctx, ev); err != nil {
			return fmt.Errorf("%s: %w", ev.Type, err)
		}

		ev.TokenRef.LastBlock = ev.Height
		ev.TokenRef.LastTime = ev.Time

		// update token supply
		switch ev.Type {
		case model.TokenEventTypeMint:
			ev.TokenRef.Supply = ev.TokenRef.Supply.Add(ev.Amount)
			ev.TokenRef.TotalMint = ev.TokenRef.TotalMint.Add(ev.Amount)
		case model.TokenEventTypeBurn:
			ev.TokenRef.Supply = ev.TokenRef.Supply.Sub(ev.Amount)
			ev.TokenRef.TotalBurn = ev.TokenRef.TotalBurn.Add(ev.Amount)
		case model.TokenEventTypeTransfer:
			ev.TokenRef.NumTransfers++
		}

		if err := idx.tables[model.TokenTableKey].Update(ctx, ev.TokenRef); err != nil {
			return fmt.Errorf("T_%d supply update: %w", ev.TokenRef.Id, err)
		}
	}
	return nil
}

func (idx *TokenIndex) updateOwnership(ctx context.Context, ev *model.TokenEvent) error {
	sndr, err := idx.getOrCreateOwner(ctx, ev.Sender, ev.Token, ev.Ledger)
	if err != nil {
		return fmt.Errorf("sender %d: %w", ev.Sender, err)
	}
	sndr.FirstBlock = util.NonZero64(sndr.FirstBlock, ev.Height)
	sndr.LastBlock = ev.Height
	sndr.WasZero = sndr.Balance.IsZero() // used in holder update

	recv := sndr
	if ev.Receiver != 0 && recv.Account != ev.Receiver {
		recv, err = idx.getOrCreateOwner(ctx, ev.Receiver, ev.Token, ev.Ledger)
		if err != nil {
			return fmt.Errorf("receiver %d: %w", ev.Receiver, err)
		}
		recv.FirstBlock = util.NonZero64(recv.FirstBlock, ev.Height)
		recv.LastBlock = ev.Height
		recv.WasZero = recv.Balance.IsZero() // used in holder update
	}

	switch ev.Type {
	case model.TokenEventTypeMint:
		// owner table
		sndr.VolMint = sndr.VolMint.Add(ev.Amount)
		sndr.NumMints++
		recv.Balance = recv.Balance.Add(ev.Amount)

		if recv.WasZero && !recv.Balance.IsZero() {
			ev.TokenRef.NumHolders++
		}

	case model.TokenEventTypeBurn:
		// owner table
		sndr.VolBurn = sndr.VolBurn.Add(ev.Amount)
		sndr.NumBurns++
		sndr.Balance = sndr.Balance.Sub(ev.Amount)

		if sndr.Balance.IsZero() {
			ev.TokenRef.NumHolders--
		}

	case model.TokenEventTypeTransfer:
		// owner table
		sndr.VolSent = sndr.VolSent.Add(ev.Amount)
		sndr.NumTransfers++
		sndr.Balance = sndr.Balance.Sub(ev.Amount)
		recv.VolRecv = recv.VolRecv.Add(ev.Amount)
		recv.NumTransfers++
		recv.Balance = recv.Balance.Add(ev.Amount)

		if sndr.Balance.IsZero() {
			ev.TokenRef.NumHolders--
		}
		if recv.WasZero && !recv.Balance.IsZero() {
			ev.TokenRef.NumHolders++
		}
	}

	_ = idx.tables[model.TokenOwnerTableKey].Update(ctx, sndr)
	if sndr.Id != recv.Id {
		_ = idx.tables[model.TokenOwnerTableKey].Update(ctx, recv)
	}

	return nil
}

func tokenCacheKey(a tezos.Address, id tezos.Z) uint64 {
	h := fnv.New64a()
	h.Write(a[:])
	h.Write(id.Bytes())
	return h.Sum64()
}

func (idx *TokenIndex) getOrCreateToken(ctx context.Context, ledger *model.Contract, signer *model.Account, tokenId tezos.Z, height int64, tm time.Time) (*model.Token, error) {
	key := tokenCacheKey(ledger.Address, tokenId)
	itok, ok := idx.tokenCache.Get(key)
	if ok && itok.Ledger == ledger.AccountId {
		return itok, nil
	}
	tokn, err := model.GetOrCreateToken(ctx, idx.tables[model.TokenTableKey], ledger, signer, tokenId, height, tm)
	if err != nil {
		return nil, err
	}
	idx.tokenCache.Add(key, tokn)
	return tokn, nil
}

func (idx *TokenIndex) findTokenId(ctx context.Context, id model.TokenID) (*model.Token, error) {
	tokn, err := model.FindTokenId(ctx, idx.tables[model.TokenTableKey], id)
	if err != nil {
		return nil, err
	}

	// FIXME: cross-check cached version

	return tokn, nil
}

func ownerTokenCacheKey(owner model.AccountID, token model.TokenID) uint64 {
	return uint64(owner)<<32 | uint64(token)
}

func (idx *TokenIndex) getOrCreateOwner(ctx context.Context, ownerId model.AccountID, tokenId model.TokenID, ledgerId model.AccountID) (*model.TokenOwner, error) {
	key := ownerTokenCacheKey(ownerId, tokenId)
	iOwner, ok := idx.ownerCache.Get(key)
	if ok {
		return iOwner, nil
	}
	ownr, err := model.GetOrCreateOwner(ctx, idx.tables[model.TokenOwnerTableKey], ownerId, tokenId, ledgerId)
	if err != nil {
		return nil, err
	}
	if ownr.Id > 0 {
		idx.ownerCache.Add(key, ownr)
	}
	return ownr, nil
}
