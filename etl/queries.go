// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) ChainByHeight(ctx context.Context, height int64) (*model.Chain, error) {
	table, err := m.Table(index.ChainTableKey)
	if err != nil {
		return nil, err
	}
	c := &model.Chain{}
	err = pack.NewQuery("chain_by_height", table).
		AndEqual("height", height).
		WithLimit(1).
		Execute(ctx, c)
	if err != nil {
		return nil, err
	}
	if c.RowId == 0 {
		return nil, index.ErrNoChainEntry
	}
	return c, nil
}

func (m *Indexer) SupplyByHeight(ctx context.Context, height int64) (*model.Supply, error) {
	table, err := m.Table(index.SupplyTableKey)
	if err != nil {
		return nil, err
	}
	s := &model.Supply{}
	err = pack.NewQuery("supply_by_height", table).
		AndEqual("height", height).
		WithLimit(1).
		Execute(ctx, s)
	if err != nil {
		return nil, err
	}
	if s.RowId == 0 {
		return nil, index.ErrNoSupplyEntry
	}
	return s, nil
}

func (m *Indexer) SupplyByTime(ctx context.Context, t time.Time) (*model.Supply, error) {
	table, err := m.Table(index.SupplyTableKey)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	from, to := t, now
	if from.After(to) {
		from, to = to, from
	}
	s := &model.Supply{}
	err = pack.NewQuery("supply_by_time", table).
		AndRange("time", from, to). // search for timestamp
		AndGte("height", 1).        // height larger than supply init block 1
		WithLimit(1).
		Execute(ctx, s)
	if err != nil {
		return nil, err
	}
	if s.RowId == 0 {
		return nil, index.ErrNoSupplyEntry
	}
	return s, nil
}

type Growth struct {
	NewAccounts         int64
	NewImplicitAccounts int64
	NewManagedAccounts  int64
	NewContracts        int64
	ClearedAccounts     int64
	FundedAccounts      int64
}

func (m *Indexer) GrowthByDuration(ctx context.Context, to time.Time, d time.Duration) (*Growth, error) {
	table, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	type XBlock struct {
		NewAccounts         int64 `pack:"A"`
		NewImplicitAccounts int64 `pack:"i"`
		NewManagedAccounts  int64 `pack:"m"`
		NewContracts        int64 `pack:"C"`
		ClearedAccounts     int64 `pack:"E"`
		FundedAccounts      int64 `pack:"J"`
	}
	from := to.Add(-d)
	x := &XBlock{}
	g := &Growth{}
	err = pack.NewQuery("aggregate_growth", table).
		WithFields("A", "i", "m", "C", "E", "J").
		AndRange("time", from, to). // search for timestamp
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(x); err != nil {
				return err
			}
			g.NewAccounts += x.NewAccounts
			g.NewImplicitAccounts += x.NewImplicitAccounts
			g.NewManagedAccounts += x.NewManagedAccounts
			g.NewContracts += x.NewContracts
			g.ClearedAccounts += x.ClearedAccounts
			g.FundedAccounts += x.FundedAccounts
			return nil
		})
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (m *Indexer) BlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	if id == 0 {
		return nil, index.ErrNoBlockEntry
	}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := &model.Block{}
	err = pack.NewQuery("block_by_parent_id", blocks).
		AndEqual("I", id).
		Execute(ctx, b)
	if err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

// find a block's canonical successor (non-orphan)
func (m *Indexer) BlockByParentId(ctx context.Context, id uint64) (*model.Block, error) {
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := &model.Block{}
	err = pack.NewQuery("block_by_parent_id", blocks).
		AndEqual("parent_id", id).
		AndEqual("is_orphan", false).
		WithLimit(1).
		Execute(ctx, b)
	if err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

func (m *Indexer) BlockHashByHeight(ctx context.Context, height int64) (tezos.BlockHash, error) {
	type XBlock struct {
		Hash tezos.BlockHash `pack:"H"`
	}
	b := &XBlock{}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return b.Hash, err
	}
	err = pack.NewQuery("block_hash_by_height", blocks).
		AndEqual("height", height).
		AndEqual("is_orphan", false).
		WithLimit(1).
		Execute(ctx, b)
	if err != nil {
		return b.Hash, err
	}
	if !b.Hash.IsValid() {
		return b.Hash, index.ErrNoBlockEntry
	}
	return b.Hash, nil
}

func (m *Indexer) BlockHashById(ctx context.Context, id uint64) (tezos.BlockHash, error) {
	type XBlock struct {
		Hash tezos.BlockHash `pack:"H"`
	}
	b := &XBlock{}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return b.Hash, err
	}
	err = pack.NewQuery("block_hash_by_id", blocks).
		WithFields("H").
		AndEqual("I", id).
		Execute(ctx, b)
	if err != nil {
		return b.Hash, err
	}
	if !b.Hash.IsValid() {
		return b.Hash, index.ErrNoBlockEntry
	}
	return b.Hash, nil
}

func (m *Indexer) BlockByHeight(ctx context.Context, height int64) (*model.Block, error) {
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := &model.Block{}
	err = pack.NewQuery("block_hash_by_height", blocks).
		AndEqual("height", height).
		AndEqual("is_orphan", false).
		Execute(ctx, b)
	if err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

func (m *Indexer) BlockByHash(ctx context.Context, h tezos.BlockHash, from, to int64) (*model.Block, error) {
	if !h.IsValid() {
		return nil, fmt.Errorf("invalid block hash %s", h)
	}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("block_hash_by_hash", blocks).WithLimit(1).WithDesc()
	if from > 0 {
		q = q.AndGte("height", from)
	}
	if to > 0 {
		q = q.AndLte("height", to)
	}
	// most expensive condition last
	q = q.AndEqual("hash", h.Hash.Hash[:])
	b := &model.Block{}
	if err = q.Execute(ctx, b); err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

func (m *Indexer) LookupBlock(ctx context.Context, blockIdent string) (*model.Block, error) {
	var (
		b   *model.Block
		err error
	)
	switch true {
	case blockIdent == "head":
		b, err = m.BlockByHeight(ctx, m.tips[index.BlockTableKey].Height)
	case len(blockIdent) == tezos.HashTypeBlock.Base58Len() || tezos.HashTypeBlock.MatchPrefix(blockIdent):
		// assume it's a hash
		var blockHash tezos.BlockHash
		blockHash, err = tezos.ParseBlockHash(blockIdent)
		if err != nil {
			return nil, index.ErrInvalidBlockHash
		}
		b, err = m.BlockByHash(ctx, blockHash, 0, 0)
	default:
		// try parsing as height
		var blockHeight int64
		blockHeight, err = strconv.ParseInt(blockIdent, 10, 64)
		if err != nil {
			return nil, index.ErrInvalidBlockHeight
		}
		b, err = m.BlockByHeight(ctx, blockHeight)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (m *Indexer) LookupLastBakedBlock(ctx context.Context, a *model.Account) (*model.Block, error) {
	if a.BlocksBaked == 0 {
		return nil, index.ErrNoBlockEntry
	}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := &model.Block{}
	err = pack.NewQuery("last_baked_block", blocks).
		WithLimit(1).
		WithDesc().
		AndRange("height", a.FirstSeen, a.LastSeen).
		AndEqual("baker_id", a.RowId).
		Execute(ctx, b)
	if err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	return b, nil
}

func (m *Indexer) LookupLastEndorsedBlock(ctx context.Context, a *model.Account) (*model.Block, error) {
	if a.BlocksEndorsed == 0 {
		return nil, index.ErrNoBlockEntry
	}
	ops, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	var op model.Op
	err = pack.NewQuery("last_endorse_op", ops).
		WithFields("h").
		WithLimit(1).
		WithDesc().
		AndRange("height", a.FirstSeen, a.LastSeen).
		AndEqual("sender_id", a.RowId).
		AndEqual("type", tezos.OpTypeEndorsement).
		Execute(ctx, &op)
	if err != nil {
		return nil, err
	}
	if op.Height == 0 {
		return nil, index.ErrNoBlockEntry
	}
	return m.BlockByHeight(ctx, op.Height)
}

func (m *Indexer) LookupNextRight(ctx context.Context, a *model.Account, height int64, typ tezos.RightType, prio int64) (*model.Right, error) {
	rights, err := m.Table(index.RightsTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("next_right", rights).
		WithFields("h", "t", "A", "p").
		WithLimit(1).
		AndGt("height", height).        // from block height
		AndEqual("type", typ).          // right type
		AndEqual("account_id", a.RowId) // delegate id
	if prio >= 0 {
		q = q.AndEqual("priority", 0)
	}
	right := &model.Right{}
	err = q.Execute(ctx, right)
	if err != nil {
		return nil, err
	}
	if right.RowId == 0 {
		return nil, index.ErrNoRightsEntry
	}
	return right, nil
}

// assuming the lock is more expensive than streaming/decoding results
func (m *Indexer) LookupNextRights(ctx context.Context, a *model.Account, height int64) (bakeright, endorseright model.Right, rerr error) {
	rights, err := m.Table(index.RightsTableKey)
	if err != nil {
		rerr = err
		return
	}
	q := pack.NewQuery("next_rights", rights).
		WithFields("h", "t", "A", "p").
		AndGt("height", height).
		AndEqual("account_id", a.RowId).
		AndLte("priority", 31) // priority for bake & endorse
	right := &model.Right{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(right); err != nil {
			return err
		}
		switch right.Type {
		case tezos.RightTypeBaking:
			if right.Priority > 0 {
				return nil
			}
			if bakeright.RowId > 0 {
				if endorseright.RowId > 0 {
					return io.EOF
				}
				return nil
			}
			bakeright = *right
		case tezos.RightTypeEndorsing:
			if endorseright.RowId > 0 {
				if bakeright.RowId > 0 {
					return io.EOF
				}
				return nil
			}
			endorseright = *right
		}
		return nil
	})
	if err != nil && err != io.EOF {
		rerr = err
		return
	}
	return
}

func (m *Indexer) ListBlockRights(ctx context.Context, height int64, typ tezos.RightType) ([]model.Right, error) {
	rights, err := m.Table(index.RightsTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_rights", rights).
		AndEqual("height", height)
	if typ.IsValid() {
		q = q.AndEqual("type", typ)
	}
	resp := make([]model.Right, 0, 64+32)
	err = q.Stream(ctx, func(r pack.Row) error {
		right := model.Right{}
		if err := r.Decode(&right); err != nil {
			return err
		}
		resp = append(resp, right)
		return nil
	})
	return resp, nil
}

func (m *Indexer) LookupAccount(ctx context.Context, addr tezos.Address) (*model.Account, error) {
	if !addr.IsValid() {
		return nil, ErrInvalidHash
	}
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	acc := &model.Account{}
	err = pack.NewQuery("account_by_hash", table).
		AndEqual("address", addr.Bytes22()).
		Execute(ctx, acc)
	if acc.RowId == 0 {
		err = index.ErrNoAccountEntry
	}
	if err != nil {
		return nil, err
	}
	return acc, nil
}

func (m *Indexer) LookupContract(ctx context.Context, addr tezos.Address) (*model.Contract, error) {
	if !addr.IsValid() {
		return nil, ErrInvalidHash
	}
	table, err := m.Table(index.ContractTableKey)
	if err != nil {
		return nil, err
	}
	cc := &model.Contract{}
	err = pack.NewQuery("contract_by_hash", table).
		AndEqual("address", addr.Bytes22()).
		Execute(ctx, cc)
	if err != nil {
		return nil, err
	}
	if cc.RowId == 0 {
		return nil, index.ErrNoContractEntry
	}
	return cc, nil
}

func (m *Indexer) LookupContractId(ctx context.Context, id model.AccountID) (*model.Contract, error) {
	table, err := m.Table(index.ContractTableKey)
	if err != nil {
		return nil, err
	}
	cc := &model.Contract{}
	err = pack.NewQuery("contract_by_id", table).
		AndEqual("account_id", id).
		Execute(ctx, cc)
	if err != nil {
		return nil, err
	}
	if cc.RowId == 0 {
		return nil, index.ErrNoContractEntry
	}
	return cc, nil
}

func (m *Indexer) LookupAccountId(ctx context.Context, id model.AccountID) (*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	acc := &model.Account{}
	err = pack.NewQuery("account_by_id", table).
		AndEqual("I", id).
		Execute(ctx, acc)
	if err != nil {
		return nil, err
	}
	if acc.RowId == 0 {
		return nil, index.ErrNoAccountEntry
	}
	return acc, nil
}

func (m *Indexer) LookupAccountIds(ctx context.Context, ids []uint64) ([]*model.Account, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	accs := make([]*model.Account, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(accs) {
			return io.EOF
		}
		a := &model.Account{}
		if err := r.Decode(a); err != nil {
			return err
		}
		accs[count] = a
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoAccountEntry
	}
	accs = accs[:count]
	return accs, nil
}

func (m *Indexer) ListAllDelegates(ctx context.Context) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	accs := make([]*model.Account, 0)
	err = pack.NewQuery("list_bakers", table).
		AndEqual("is_delegate", true).
		Stream(ctx, func(r pack.Row) error {
			acc := &model.Account{}
			if err := r.Decode(acc); err != nil {
				return err
			}
			accs = append(accs, acc)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return accs, nil
}

func (m *Indexer) ListActiveDelegates(ctx context.Context) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	accs := make([]*model.Account, 0)
	err = pack.NewQuery("list_active_bakers", table).
		AndEqual("is_delegate", true).
		AndEqual("is_active_delegate", true).
		Stream(ctx, func(r pack.Row) error {
			acc := &model.Account{}
			if err := r.Decode(acc); err != nil {
				return err
			}
			accs = append(accs, acc)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return accs, nil
}

func (m *Indexer) ListManaged(ctx context.Context, id model.AccountID, offset, limit uint, cursor uint64, order pack.OrderType) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if cursor > 0 {
		offset = 0
	}
	q := pack.NewQuery("list_created_contracts", table).
		AndEqual("creator_id", id). // manager/creator id
		WithOrder(order)
	if cursor > 0 {
		if order == pack.OrderDesc {
			q = q.AndLt("I", cursor)
		} else {
			q = q.AndGt("I", cursor)
		}
	}
	accs := make([]*model.Account, 0)
	err = q.Stream(ctx, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		acc := &model.Account{}
		if err := r.Decode(acc); err != nil {
			return err
		}
		accs = append(accs, acc)
		if limit > 0 && len(accs) >= int(limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return accs, nil
}

func (m *Indexer) LookupOp(ctx context.Context, opIdent string) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("find_tx", table)
	switch true {
	case len(opIdent) == tezos.HashTypeOperation.Base58Len() || tezos.HashTypeOperation.MatchPrefix(opIdent):
		// assume it's a hash
		oh, err := tezos.ParseOpHash(opIdent)
		if err != nil {
			return nil, ErrInvalidHash
		}
		q = q.AndEqual("hash", oh.Hash.Hash[:])
	default:
		// try parsing as row_id
		rowId, err := strconv.ParseUint(opIdent, 10, 64)
		if err != nil {
			return nil, index.ErrInvalidOpID
		}
		q = q.AndEqual("I", rowId)
	}
	ops := make([]*model.Op, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		op := &model.Op{}
		if err := r.Decode(op); err != nil {
			return err
		}
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, index.ErrNoOpEntry
	}
	return ops, nil
}

func (m *Indexer) FindActivatedAccount(ctx context.Context, addr tezos.Address) (*model.Account, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	type Xop struct {
		SenderId  model.AccountID `pack:"S"`
		CreatorId model.AccountID `pack:"M"`
		Data      string          `pack:"a"`
	}
	var o Xop
	err = pack.NewQuery("find_activation", table).
		WithFields("sender_id", "creator_id", "data").
		WithoutCache().
		AndEqual("type", tezos.OpTypeActivateAccount).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(&o); err != nil {
				return err
			}
			// data contains hex(secret),blinded_address
			data := strings.Split(o.Data, ",")
			if len(data) != 2 {
				// skip broken records
				return nil
			}
			ba, err := tezos.DecodeBlindedAddress(data[1])
			if err != nil {
				// skip broken records
				return nil
			}
			if addr.Equal(ba) {
				return io.EOF // found
			}
			return nil
		})
	if err != io.EOF {
		if err == nil {
			err = index.ErrNoAccountEntry
		}
		return nil, err
	}
	// lookup account by id
	if o.CreatorId != 0 {
		return m.LookupAccountId(ctx, o.CreatorId)
	}
	return m.LookupAccountId(ctx, o.SenderId)
}

func (m *Indexer) FindLatestDelegation(ctx context.Context, id model.AccountID) (*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	o := &model.Op{}
	err = pack.NewQuery("find_last_delegation", table).
		WithoutCache().
		WithDesc().
		WithLimit(1).
		AndEqual("type", tezos.OpTypeDelegation). // type
		AndEqual("sender_id", id).                // search for sender account id
		AndNotEqual("delegate_id", 0).            // delegate id
		Execute(ctx, o)
	if err != nil {
		return nil, err
	}
	if o.RowId == 0 {
		return nil, index.ErrNoOpEntry
	}
	return o, nil
}

func (m *Indexer) FindOrigination(ctx context.Context, id model.AccountID, height int64) (*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	o := &model.Op{}
	err = pack.NewQuery("find_origination", table).
		WithoutCache().
		WithDesc().
		WithLimit(1).
		AndGte("height", height).                  // first seen height
		AndEqual("type", tezos.OpTypeOrigination). // type
		AndEqual("receiver_id", id).               // search for receiver account id
		Execute(ctx, o)
	if err != nil {
		return nil, err
	}
	if o.RowId == 0 {
		return nil, index.ErrNoOpEntry
	}
	return o, nil
}

func (m *Indexer) LookupOpIds(ctx context.Context, ids []uint64) ([]*model.Op, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	ops := make([]*model.Op, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(ops) {
			return io.EOF
		}
		op := &model.Op{}
		if err := r.Decode(op); err != nil {
			return err
		}
		ops[count] = op
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoOpEntry
	}
	ops = ops[:count]
	return ops, nil
}

type ListRequest struct {
	Account     *model.Account
	Mode        pack.FilterMode
	Typs        []tezos.OpType
	Since       int64
	Until       int64
	Offset      uint
	Limit       uint
	Cursor      uint64
	Order       pack.OrderType
	SenderId    model.AccountID
	ReceiverId  model.AccountID
	Entrypoints []int64
	Period      int64
}

// Note: offset and limit count in atomar operations
func (m *Indexer) ListBlockOps(ctx context.Context, r ListRequest) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}
	q := pack.NewQuery("list_block_ops", table).
		WithOrder(r.Order).
		AndEqual("height", r.Since).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset))

	if r.SenderId > 0 {
		q = q.AndEqual("sender_id", r.SenderId)
	}
	if r.ReceiverId > 0 {
		q = q.AndEqual("receiver_id", r.ReceiverId)
	}

	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if len(r.Typs) > 0 && r.Mode.IsValid() {
		if r.Mode.IsScalar() {
			q = q.AndCondition("type", r.Mode, r.Typs[0])
		} else {
			q = q.AndCondition("type", r.Mode, r.Typs)
		}
	}
	ops := make([]*model.Op, 0, r.Limit)
	if err = q.Execute(ctx, &ops); err != nil {
		return nil, err
	}
	return ops, nil
}

// Note: offset and limit count in full batches of transactions
func (m *Indexer) ListBlockOpsCollapsed(ctx context.Context, r ListRequest) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_block_ops", table).
		WithOrder(r.Order).
		AndEqual("height", r.Since)

	if r.SenderId > 0 {
		q = q.AndEqual("sender_id", r.SenderId)
	}
	if r.ReceiverId > 0 {
		q = q.AndEqual("receiver_id", r.ReceiverId)
	}

	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	} else {
		if r.Offset > 0 {
			q.AndGte("op_n", r.Offset)
		}
		if r.Limit > 0 {
			q.AndLte("op_n", r.Offset+r.Limit)
		}
	}
	if len(r.Typs) > 0 && r.Mode.IsValid() {
		if r.Mode.IsScalar() {
			q = q.AndCondition("type", r.Mode, r.Typs[0])
		} else {
			q = q.AndCondition("type", r.Mode, r.Typs)
		}
	}

	var (
		lastN int = -1
		count int
	)
	ops := make([]*model.Op, 0)
	err = q.Stream(ctx, func(rx pack.Row) error {
		op := &model.Op{}
		if err := rx.Decode(op); err != nil {
			return err
		}

		// detect next op group (works in both directions)
		isNext := op.OpN != lastN
		lastN = op.OpN

		// stop at first result after group end
		if isNext && r.Limit > 0 && count == int(r.Limit) {
			return io.EOF
		}

		ops = append(ops, op)

		// count op groups
		if isNext {
			count++
		}

		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return ops, nil
}

// Note:
// - order is defined by funding or spending operation
// - offset and limit counts in atomar ops
// - high traffic addresses may have many, so we use query limits
func (m *Indexer) ListAccountOps(ctx context.Context, r ListRequest) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}

	// clamp time range to account lifetime
	r.Since = util.Max64(r.Since, r.Account.FirstSeen-1)
	r.Until = util.NonZeroMin64(r.Until, r.Account.LastSeen)

	// list all ops where this address is any of
	// - sender
	// - receiver
	// - delegate
	q := pack.NewQuery("list_account_ops", table).
		WithOrder(r.Order).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset))

	switch {
	case r.SenderId > 0:
		// FIXME: packdb condition tree bug, having AND(sender) first produces
		// AND
		// + OR
		// | + AND
		// | | + S = 36740
		// | + R = 39598
		// | + D = 39598
		// + h > 25768
		// + h <= 25773
		q = q.Or(
			pack.Equal("receiver_id", r.Account.RowId),
			pack.Equal("delegate_id", r.Account.RowId),
		)
		q = q.AndEqual("sender_id", r.SenderId)
	case r.ReceiverId > 0:
		// FIXME: packdb condition bug
		q = q.Or(
			pack.Equal("receiver_id", r.ReceiverId),
			pack.Equal("delegate_id", r.ReceiverId),
		)
		q = q.AndEqual("sender_id", r.Account.RowId)
	default:
		q = q.Or(
			pack.Equal("sender_id", r.Account.RowId),
			pack.Equal("receiver_id", r.Account.RowId),
			pack.Equal("delegate_id", r.Account.RowId),
		)
	}

	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.Since > 0 || r.Account.FirstSeen > 0 {
		q = q.AndGt("height", util.Max64(r.Since, r.Account.FirstSeen-1))
	}
	if r.Until > 0 || r.Account.LastSeen > 0 {
		q = q.AndLte("height", util.NonZeroMin64(r.Until, r.Account.LastSeen))
	}
	if len(r.Typs) > 0 && r.Mode.IsValid() {
		if r.Mode.IsScalar() {
			q = q.AndCondition("type", r.Mode, r.Typs[0])
		} else {
			q = q.AndCondition("type", r.Mode, r.Typs)
		}
	}

	ops := make([]*model.Op, 0)
	if err := q.Execute(ctx, &ops); err != nil {
		return nil, err
	}
	return ops, nil
}

// Note:
// - order is defined by funding or spending operation
// - offset and limit counts in collapsed ops (all batch/internal contents)
// - high traffic addresses may have many, so we use query limits
func (m *Indexer) ListAccountOpsCollapsed(ctx context.Context, r ListRequest) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}

	// clamp time range to account lifetime
	r.Since = util.Max64(r.Since, r.Account.FirstSeen-1)
	r.Until = util.NonZeroMin64(r.Until, r.Account.LastSeen)

	// list all ops where this address is any of
	// - sender
	// - receiver
	// - delegate
	q := pack.NewQuery("list_account_ops", table).
		WithOrder(r.Order)

	switch {
	case r.SenderId > 0:
		// FIXME: packdb condition bug
		q = q.Or(
			pack.Equal("receiver_id", r.Account.RowId),
			pack.Equal("delegate_id", r.Account.RowId),
		)
		q = q.AndEqual("sender_id", r.SenderId)

	case r.ReceiverId > 0:
		// FIXME: packdb condition bug
		q = q.Or(
			pack.Equal("receiver_id", r.ReceiverId),
			pack.Equal("delegate_id", r.ReceiverId),
		)
		q = q.AndEqual("sender_id", r.Account.RowId)
	default:
		q = q.Or(
			pack.Equal("sender_id", r.Account.RowId),
			pack.Equal("receiver_id", r.Account.RowId),
			pack.Equal("delegate_id", r.Account.RowId),
		)
	}

	// FIXME:
	// - if S/R is only in one internal op, pull the entire op group
	ops := make([]*model.Op, 0)

	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.Since > 0 || r.Account.FirstSeen > 0 {
		q = q.AndGt("height", util.Max64(r.Since, r.Account.FirstSeen-1))
	}
	if r.Until > 0 || r.Account.LastSeen > 0 {
		q = q.AndLte("height", util.NonZeroMin64(r.Until, r.Account.LastSeen))
	}
	if len(r.Typs) > 0 && r.Mode.IsValid() {
		if r.Mode.IsScalar() {
			q = q.AndCondition("type", r.Mode, r.Typs[0])
		} else {
			q = q.AndCondition("type", r.Mode, r.Typs)
		}
	}
	var (
		lastN      int = -1
		lastHeight int64
		count      int
	)
	err = q.Stream(ctx, func(rx pack.Row) error {
		op := model.AllocOp()
		if err := rx.Decode(op); err != nil {
			return err
		}
		// detect next op group (works in both directions)
		isFirst := lastN < 0
		isNext := op.OpN != lastN || op.Height != lastHeight
		lastN, lastHeight = op.OpN, op.Height

		// skip offset groups
		if r.Offset > 0 {
			if isNext && !isFirst {
				r.Offset--
			} else {
				return nil
			}
			if r.Offset > 0 {
				return nil
			}
		}

		// stop at first result after group end
		if isNext && r.Limit > 0 && count == int(r.Limit) {
			return io.EOF
		}

		ops = append(ops, op)

		// count op groups
		if isNext {
			count++
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return ops, nil
}

func (m *Indexer) ListContractCalls(ctx context.Context, r ListRequest) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}

	// clamp time range to account lifetime
	r.Since = util.Max64(r.Since, r.Account.FirstSeen-1)
	r.Until = util.NonZeroMin64(r.Until, r.Account.LastSeen)

	// list all tx (calls) received by this contract
	q := pack.NewQuery("list_calls_recv", table).
		WithOrder(r.Order).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset)).
		AndEqual("receiver_id", r.Account.RowId)

	if r.SenderId > 0 {
		q = q.AndEqual("sender_id", r.SenderId)
	}

	// add entrypoint filter
	switch len(r.Entrypoints) {
	case 0:
		// none, search op type
		q = q.AndIn("type", []uint8{
			uint8(tezos.OpTypeTransaction),
			uint8(tezos.OpTypeOrigination),
		})
	case 1:
		// any single
		q = q.
			AndEqual("type", tezos.OpTypeTransaction).              // search op type
			AndEqual("is_success", true).                           // successful ops only
			AndCondition("entrypoint_id", r.Mode, r.Entrypoints[0]) // entrypoint_id
	default:
		// in/nin
		q = q.
			AndEqual("type", tezos.OpTypeTransaction).           // search op type
			AndEqual("is_success", true).                        // successful ops only
			AndCondition("entrypoint_id", r.Mode, r.Entrypoints) // entrypoint_ids
	}
	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.Since > 0 {
		q = q.AndGt("height", r.Since)
	}
	if r.Until > 0 {
		q = q.AndLte("height", r.Until)
	}
	ops := make([]*model.Op, 0, util.NonZero(int(r.Limit), 512))
	if err := q.Execute(ctx, &ops); err != nil {
		return nil, err
	}
	return ops, nil
}

func (m *Indexer) FindLastCall(ctx context.Context, acc model.AccountID, from, to int64) (*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("last_call", table).
		WithDesc().
		WithLimit(1)
	if from > 0 {
		q = q.AndGt("height", from)
	}
	if to > 0 {
		q = q.AndLte("height", to)
	}
	op := &model.Op{}
	err = q.AndEqual("receiver_id", acc).
		AndEqual("type", tezos.OpTypeTransaction).
		AndEqual("is_contract", true).
		AndEqual("is_success", true).
		AndEqual("has_data", true).
		Execute(ctx, op)
	if err != nil {
		return nil, err
	}
	if op.RowId == 0 {
		return nil, index.ErrNoOpEntry
	}
	return op, nil
}

func (m *Indexer) ListContractBigMapIds(ctx context.Context, acc model.AccountID) ([]int64, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_bigmaps", table).
		AndEqual("account_id", acc).
		AndEqual("action", micheline.DiffActionAlloc).
		AndEqual("is_deleted", false)
	ids := make([]int64, 0)
	bmi := &model.BigmapItem{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(bmi); err != nil {
			return err
		}
		ids = append(ids, bmi.BigmapId)
		return nil
	})
	if err != nil {
		return nil, err
	}
	vec.Int64Sorter(ids).Sort()
	return ids, nil
}

func (m *Indexer) ElectionByHeight(ctx context.Context, height int64) (*model.Election, error) {
	table, err := m.Table(index.ElectionTableKey)
	if err != nil {
		return nil, err
	}
	// we are looking for the last election with start_height <= height
	e := &model.Election{}
	err = pack.NewQuery("election_height", table).
		WithDesc().WithoutCache().WithLimit(1).
		AndLte("start_height", height).
		Execute(ctx, e)
	if err != nil {
		return nil, err
	}
	if e.RowId == 0 {
		return nil, index.ErrNoElectionEntry
	}
	return e, nil
}

func (m *Indexer) ElectionById(ctx context.Context, id model.ElectionID) (*model.Election, error) {
	table, err := m.Table(index.ElectionTableKey)
	if err != nil {
		return nil, err
	}
	e := &model.Election{}
	err = pack.NewQuery("election_id", table).
		WithoutCache().
		WithLimit(1).
		AndEqual("I", id).
		Execute(ctx, e)
	if err != nil {
		return nil, err
	}
	if e.RowId == 0 {
		return nil, index.ErrNoElectionEntry
	}
	return e, nil
}

func (m *Indexer) VotesByElection(ctx context.Context, id model.ElectionID) ([]*model.Vote, error) {
	table, err := m.Table(index.VoteTableKey)
	if err != nil {
		return nil, err
	}
	votes := make([]*model.Vote, 0)
	err = pack.NewQuery("list_votes", table).
		WithoutCache().
		WithLimit(1).
		AndEqual("election_id", id).
		Execute(ctx, &votes)
	if err != nil {
		return nil, err
	}
	if len(votes) == 0 {
		return nil, index.ErrNoVoteEntry
	}
	return votes, nil
}

func (m *Indexer) ListVoters(ctx context.Context, r ListRequest) ([]*model.Voter, error) {
	// use params from one cycle before the vote started (necessary during protocol upgrades)
	params := m.ParamsByHeight(r.Since - 1)

	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}

	// Step 1
	// collect eligible voters from previous roll snapshot
	//
	// Note this is not entirely correct because we don't collect a gov
	// snapshot but instead create a virtual gov snapshot from an existing roll
	// snapshot at the end of the previous cycle. The differences are
	//
	// - some bakers may have been deactivated right after the roll snapshot
	//   and before the gov snapshot (we express this in the snapshot by setting
	//   is_active = false)
	// - the roll snapshot does not contain unfrozen rewards, but the
	//   governance snapshot should include this
	//
	snapshotTable, err := m.Table(index.SnapshotTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_voters", snapshotTable).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset)).
		AndEqual("height", r.Since-1). // end of previous cycle snapshot
		AndEqual("is_active", true)

	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	voters := make(map[model.AccountID]*model.Voter)
	snap := &model.Snapshot{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(snap); err != nil {
			return err
		}
		voters[snap.AccountId] = &model.Voter{
			RowId: snap.AccountId,
			Rolls: snap.Rolls,
			Stake: snap.Balance + snap.Delegated,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Step 2
	// adjust rolls up by unfrozen rewards which are applied after the roll snapshot
	// roll snapshot after last block of previous cycle, unfreeze at first block
	flowTable, err := m.Table(index.FlowTableKey)
	if err != nil {
		return nil, err
	}
	type XFlow struct {
		AccountId model.AccountID `pack:"A"`
		AmountOut int64           `pack:"o"`
	}
	xf := &XFlow{}
	err = pack.NewQuery("list_voters", flowTable).
		AndEqual("height", r.Since).
		AndEqual("category", model.FlowCategoryRewards).
		AndEqual("operation", model.FlowTypeInternal).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(xf); err != nil {
				return err
			}
			if voter, ok := voters[xf.AccountId]; ok {
				voter.Stake += xf.AmountOut
				voter.Rolls = voter.Stake / params.TokensPerRoll
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	// Step 3: list ballots
	ballotTable, err := m.Table(index.BallotTableKey)
	if err != nil {
		return nil, err
	}
	ballot := &model.Ballot{}
	err = pack.NewQuery("list_voters", ballotTable).
		AndEqual("voting_period", r.Period).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(ballot); err != nil {
				return err
			}
			if voter, ok := voters[ballot.SourceId]; ok {
				voter.Ballot = ballot.Ballot
				voter.Time = ballot.Time
				voter.HasVoted = true
				found := false
				for _, v := range voter.Proposals {
					if v != ballot.ProposalId {
						continue
					}
					found = true
					break
				}
				if !found {
					voter.Proposals = append(voter.Proposals, ballot.ProposalId)
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	out := make([]*model.Voter, 0, len(voters))
	for _, v := range voters {
		out = append(out, v)
	}
	if r.Order == pack.OrderAsc {
		sort.Slice(out, func(i, j int) bool { return out[i].RowId < out[j].RowId })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].RowId > out[j].RowId })
	}
	return out, nil
}

func (m *Indexer) ProposalsByElection(ctx context.Context, id model.ElectionID) ([]*model.Proposal, error) {
	table, err := m.Table(index.ProposalTableKey)
	if err != nil {
		return nil, err
	}
	proposals := make([]*model.Proposal, 0)
	err = pack.NewQuery("list_proposals", table).
		WithoutCache().
		AndEqual("election_id", id).
		Execute(ctx, &proposals)
	if err != nil {
		return nil, err
	}
	return proposals, nil
}

func (m *Indexer) LookupProposal(ctx context.Context, proto tezos.ProtocolHash) (*model.Proposal, error) {
	if !proto.IsValid() {
		return nil, ErrInvalidHash
	}

	table, err := m.Table(index.ProposalTableKey)
	if err != nil {
		return nil, err
	}

	// use hash and type to protect against duplicates
	prop := &model.Proposal{}
	err = pack.NewQuery("proposal_by_hash", table).
		AndEqual("hash", proto.Hash.Hash).
		Execute(ctx, prop)
	if err != nil {
		return nil, err
	}
	if prop.RowId == 0 {
		return nil, index.ErrNoProposalEntry
	}
	return prop, nil
}

func (m *Indexer) LookupProposalIds(ctx context.Context, ids []uint64) ([]*model.Proposal, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	table, err := m.Table(index.ProposalTableKey)
	if err != nil {
		return nil, err
	}
	props := make([]*model.Proposal, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(props) {
			return io.EOF
		}
		p := &model.Proposal{}
		if err := r.Decode(p); err != nil {
			return err
		}
		props[count] = p
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoProposalEntry
	}
	props = props[:count]
	return props, nil
}

func (m *Indexer) ListAccountBallots(ctx context.Context, r ListRequest) ([]*model.Ballot, error) {
	table, err := m.Table(index.BallotTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}
	// clamp time range to account lifetime
	r.Since = util.Max64(r.Since, r.Account.FirstSeen-1)
	r.Until = util.NonZeroMin64(r.Until, r.Account.LastSeen)

	q := pack.NewQuery("list_account_ballots", table).
		WithOrder(r.Order).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset)).
		AndEqual("source_id", r.Account.RowId)
	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.Since > 0 {
		q = q.AndGt("height", r.Since)
	}
	if r.Until > 0 {
		q = q.AndLte("height", r.Until)
	}
	ballots := make([]*model.Ballot, 0)
	if err := q.Execute(ctx, &ballots); err != nil {
		return nil, err
	}
	return ballots, nil
}

func (m *Indexer) ListBallots(ctx context.Context, r ListRequest) ([]*model.Ballot, error) {
	table, err := m.Table(index.BallotTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}
	q := pack.NewQuery("list_ballots", table).
		WithoutCache().
		WithOrder(r.Order).
		WithOffset(int(r.Offset)).
		WithLimit(int(r.Limit)).
		AndEqual("voting_period", r.Period)
	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	ballots := make([]*model.Ballot, 0)
	if err := q.Execute(ctx, &ballots); err != nil {
		return nil, err
	}
	return ballots, nil
}

func (m *Indexer) LookupSnapshot(ctx context.Context, accId model.AccountID, cycle, idx int64) (*model.Snapshot, error) {
	table, err := m.Table(index.SnapshotTableKey)
	if err != nil {
		return nil, err
	}
	snap := &model.Snapshot{}
	err = pack.NewQuery("search_snapshot", table).
		WithLimit(1).
		AndEqual("cycle", cycle).
		AndEqual("index", idx).
		AndEqual("account_id", accId).
		Execute(ctx, snap)
	if err != nil {
		return nil, err
	}
	if snap.RowId == 0 {
		return nil, index.ErrNoSnapshotEntry
	}
	return snap, nil
}

func (m *Indexer) LookupBigmap(ctx context.Context, id int64, withLast bool) (*model.BigmapItem, *model.BigmapItem, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, nil, err
	}
	alloc := &model.BigmapItem{}
	err = pack.NewQuery("search_bigmap", table).
		WithLimit(1).
		AndEqual("bigmap_id", id).
		AndEqual("action", micheline.DiffActionAlloc).
		Execute(ctx, alloc)
	if err != nil {
		return nil, nil, err
	}
	if alloc.RowId == 0 {
		return nil, nil, index.ErrNoBigmapEntry
	}
	if !withLast {
		return alloc, nil, nil
	}
	last := &model.BigmapItem{}
	err = pack.NewQuery("search_bigmap", table).
		WithDesc().WithLimit(1).
		AndEqual("bigmap_id", id).
		Execute(ctx, last)
	return alloc, last, err
}

func (m *Indexer) ListBigmapKeys(ctx context.Context, id, height int64, keyhash tezos.ExprHash, offset, limit uint) ([]*model.BigmapItem, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_bigmap", table).
		AndEqual("bigmap_id", id).
		AndEqual("action", micheline.DiffActionUpdate)
	if height == 0 {
		// rely on flags to quickly find latest state
		q = q.AndEqual("is_replaced", false).AndEqual("is_deleted", false)
	} else {
		// time-warp: ignore flags and future updates after height
		q = q.AndLte("height", height)
	}
	if keyhash.IsValid() {
		// assume hash collisions
		q = q.WithDesc().AndEqual("key_id", model.GetKeyId(id, keyhash))
	}
	items := make([]*model.BigmapItem, 0)
	err = q.Stream(ctx, func(r pack.Row) error {
		var b *model.BigmapItem
		if height > 0 {
			// time-warp check requires to decode first
			b = model.AllocBigmapItem()
			if err := r.Decode(b); err != nil {
				return err
			}
			// skip values that were updated before height
			// FIXME: when the database supports OR conditions, this can be
			// done more efficiently with a condtion updated.eq=0 || updated.gt=height
			if b.Updated > 0 && b.Updated <= height {
				b.Free()
				return nil
			}
			// skip values that were removed
			if b.IsDeleted {
				b.Free()
				return nil
			}
			// skip matches when offset is used
			if offset > 0 {
				offset--
				b.Free()
				return nil
			}
		} else {
			// for non-time-warp it's more efficient to skip before decoding
			if offset > 0 {
				offset--
				return nil
			}
			b = model.AllocBigmapItem()
			if err := r.Decode(b); err != nil {
				b.Free()
				return err
			}
		}

		// skip hash collisions on key_id
		if keyhash.IsValid() && !keyhash.Equal(b.GetKeyHash()) {
			return nil
		}

		// log.Infof("Found item %s %d %d key %x", b.Action, b.BigMapId, b.RowId, b.Key)
		items = append(items, b)
		if len(items) == int(limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}

func (m *Indexer) ListBigmapUpdates(ctx context.Context, id, minHeight, maxHeight int64, keyhash tezos.ExprHash, offset, limit uint) ([]*model.BigmapItem, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_bigmap", table).
		AndEqual("bigmap_id", id).
		AndNotIn("action", []uint8{
			uint8(micheline.DiffActionAlloc),
			uint8(micheline.DiffActionCopy),
		})
	if minHeight > 0 {
		q = q.AndGte("height", minHeight)
	}
	if maxHeight > 0 {
		q = q.AndLte("height", maxHeight)
	}
	if keyhash.IsValid() {
		q = q.AndEqual("key_id", model.GetKeyId(id, keyhash))
	}
	items := make([]*model.BigmapItem, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		b := model.AllocBigmapItem()
		if err := r.Decode(b); err != nil {
			return err
		}
		// skip hash collisions on key_id
		if keyhash.IsValid() && !keyhash.Equal(b.GetKeyHash()) {
			return nil
		}
		items = append(items, b)
		if len(items) == int(limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}

// luck, performance, contribution (reliability)
func (m *Indexer) BakerPerformance(ctx context.Context, id model.AccountID, fromCycle, toCycle int64) ([3]int64, error) {
	perf := [3]int64{}
	table, err := m.Table(index.IncomeTableKey)
	if err != nil {
		return perf, err
	}
	q := pack.NewQuery("baker_income", table).
		WithoutCache().
		AndEqual("account_id", id).
		AndRange("cycle", fromCycle, toCycle)
	var count int64
	income := &model.Income{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(income); err != nil {
			return err
		}
		perf[0] += income.LuckPct
		perf[1] += income.PerformancePct
		perf[2] += income.ContributionPct
		count++
		return nil
	})
	if err != nil {
		return perf, err
	}
	if count > 0 {
		perf[0] /= count
		perf[1] /= count
		perf[2] /= count
	}
	return perf, nil
}

func (m *Indexer) UpdateMetadata(ctx context.Context, md *model.Metadata) error {
	if md == nil {
		return nil
	}
	table, err := m.Table(index.MetadataTableKey)
	if err != nil {
		return err
	}
	if md.RowId > 0 {
		return table.Update(ctx, md)
	}
	err = pack.NewQuery("api.metadata.search", table).
		WithoutCache().
		WithLimit(1).
		AndEqual("address", md.Address.Bytes22()).
		AndEqual("asset_id", md.AssetId).
		Stream(ctx, func(r pack.Row) error {
			m := &model.Metadata{}
			if err := r.Decode(m); err != nil {
				return err
			}
			md.RowId = m.RowId
			return nil
		})
	if err != nil {
		return err
	}
	return table.Update(ctx, md)
}

func (m *Indexer) RemoveMetadata(ctx context.Context, md *model.Metadata) error {
	if md == nil {
		return nil
	}
	table, err := m.Table(index.MetadataTableKey)
	if err != nil {
		return err
	}
	if md.RowId > 0 {
		return table.DeleteIds(ctx, []uint64{md.RowId})
	}
	q := pack.NewQuery("api.metadata.delete", table).
		AndEqual("address", md.Address.Bytes22()).
		AndEqual("asset_id", md.AssetId).
		AndEqual("is_asset", md.IsAsset)
	_, err = table.Delete(ctx, q)
	return err
}

func (m *Indexer) PurgeMetadata(ctx context.Context) error {
	table, err := m.Table(index.MetadataTableKey)
	if err != nil {
		return err
	}
	q := pack.NewQuery("api.metadata.purge", table).AndGte("row_id", 0)
	_, err = table.Delete(ctx, q)
	return err
}

func (m *Indexer) UpsertMetadata(ctx context.Context, entries []*model.Metadata) error {
	table, err := m.Table(index.MetadataTableKey)
	if err != nil {
		return err
	}

	// copy slice ptrs
	match := make([]*model.Metadata, len(entries))
	copy(match, entries)

	// find existing metadata entries for update
	upd := make([]pack.Item, 0)
	md := &model.Metadata{}
	err = pack.NewQuery("api.metadata.upsert", table).
		WithoutCache().
		WithFields("row_id", "address", "asset_id", "is_asset").
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(md); err != nil {
				return err
			}

			// find next match; each address/asset combination is unique
			idx := -1
			for i := range match {
				if match[i].IsAsset != md.IsAsset {
					continue
				}
				if match[i].AssetId != md.AssetId {
					continue
				}
				if !match[i].Address.Equal(md.Address) {
					continue
				}
				idx = i
				break
			}

			// not found, ignore this table row
			if idx < 0 {
				return nil
			}

			// found, use row_id and remove from match set
			match[idx].RowId = md.RowId
			upd = append(upd, match[idx])
			match = append(match[:idx], match[idx+1:]...)
			if len(match) == 0 {
				return io.EOF
			}
			return nil
		})
	if err != nil && err != io.EOF {
		return err
	}

	// update
	if len(upd) > 0 {
		if err := table.Update(ctx, upd); err != nil {
			return err
		}
	}

	// insert remaining matches
	if len(match) > 0 {
		ins := make([]pack.Item, len(match))
		for i, v := range match {
			ins[i] = v
		}
		if err := table.Insert(ctx, ins); err != nil {
			return err
		}
	}

	return nil
}
