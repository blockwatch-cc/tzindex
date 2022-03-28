// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"io"
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

type ListRequest struct {
	Account     *model.Account
	Mode        pack.FilterMode
	Typs        model.OpTypeList
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
	BigmapId    int64
	BigmapKey   tezos.ExprHash
	OpId        model.OpID
}

func (r ListRequest) WithDelegation() bool {
	if r.Mode == pack.FilterModeEqual || r.Mode == pack.FilterModeIn {
		for _, t := range r.Typs {
			if t == model.OpTypeDelegation {
				return true
			}
		}
		return false
	} else {
		for _, t := range r.Typs {
			if t == model.OpTypeDelegation {
				return false
			}
		}
		return true
	}
}

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
	NewAccounts     int64
	NewContracts    int64
	ClearedAccounts int64
	FundedAccounts  int64
}

func (m *Indexer) GrowthByDuration(ctx context.Context, to time.Time, d time.Duration) (*Growth, error) {
	table, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	type XBlock struct {
		NewAccounts     int64 `pack:"A"`
		NewContracts    int64 `pack:"C"`
		ClearedAccounts int64 `pack:"E"`
		FundedAccounts  int64 `pack:"J"`
	}
	from := to.Add(-d)
	x := &XBlock{}
	g := &Growth{}
	err = pack.NewQuery("aggregate_growth", table).
		WithFields("A", "C", "E", "J").
		AndRange("time", from, to). // search for timestamp
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(x); err != nil {
				return err
			}
			g.NewAccounts += x.NewAccounts
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
	err = pack.NewQuery("block_by_id", blocks).
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
	err = pack.NewQuery("block_by_height", blocks).
		AndEqual("height", height).
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
	q := pack.NewQuery("block_by_hash", blocks).WithLimit(1).WithDesc()
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

func (m *Indexer) LookupBlockId(ctx context.Context, blockIdent string) (tezos.BlockHash, int64, error) {
	switch true {
	case blockIdent == "head":
		b, err := m.BlockByHeight(ctx, m.tips[index.BlockTableKey].Height)
		if err != nil {
			return tezos.BlockHash{}, 0, err
		}
		return b.Hash, b.Height, nil
	case len(blockIdent) == tezos.HashTypeBlock.Base58Len() || tezos.HashTypeBlock.MatchPrefix(blockIdent):
		// assume it's a hash
		var blockHash tezos.BlockHash
		blockHash, err := tezos.ParseBlockHash(blockIdent)
		if err != nil {
			return tezos.BlockHash{}, 0, index.ErrInvalidBlockHash
		}
		b, err := m.BlockByHash(ctx, blockHash, 0, 0)
		return b.Hash, b.Height, nil
	default:
		// try parsing as height
		var blockHeight int64
		blockHeight, err := strconv.ParseInt(blockIdent, 10, 64)
		if err != nil {
			return tezos.BlockHash{}, 0, index.ErrInvalidBlockHeight
		}
		// from cache
		return m.LookupBlockHash(ctx, blockHeight), blockHeight, nil
	}
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

func (m *Indexer) LookupLastBakedBlock(ctx context.Context, bkr *model.Baker) (*model.Block, error) {
	if bkr.BlocksBaked == 0 {
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
		AndRange("height", bkr.Account.FirstSeen, bkr.Account.LastSeen).
		AndEqual("proposer_id", bkr.AccountId).
		Execute(ctx, b)
	if err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	return b, nil
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

func (m *Indexer) LookupBaker(ctx context.Context, addr tezos.Address) (*model.Baker, error) {
	if !addr.IsValid() {
		return nil, ErrInvalidHash
	}
	table, err := m.Table(index.BakerTableKey)
	if err != nil {
		return nil, err
	}
	bkr := &model.Baker{}
	err = pack.NewQuery("baker_by_hash", table).
		AndEqual("address", addr.Bytes22()).
		Execute(ctx, bkr)
	if bkr.RowId == 0 {
		err = index.ErrNoBakerEntry
	}
	if err != nil {
		return nil, err
	}
	acc, err := m.LookupAccountId(ctx, bkr.AccountId)
	if err != nil {
		return nil, err
	}
	bkr.Account = acc
	return bkr, nil
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

func (m *Indexer) LookupContractType(ctx context.Context, id model.AccountID) (micheline.Type, micheline.Type, error) {
	elem, ok := m.contract_types.Get(id)
	if !ok {
		cc, err := m.LookupContractId(ctx, id)
		if err != nil {
			return micheline.Type{}, micheline.Type{}, err
		}
		elem = m.contract_types.Add(cc)
	}
	return elem.ParamType, elem.StorageType, nil
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

func (m *Indexer) LookupBakerId(ctx context.Context, id model.AccountID) (*model.Baker, error) {
	acc, err := m.LookupAccountId(ctx, id)
	if err != nil {
		return nil, err
	}
	table, err := m.Table(index.BakerTableKey)
	if err != nil {
		return nil, err
	}
	bkr := &model.Baker{}
	err = pack.NewQuery("baker_by_id", table).
		AndEqual("account_id", id).
		Execute(ctx, bkr)
	if bkr.RowId == 0 {
		err = index.ErrNoAccountEntry
	}
	if err != nil {
		return nil, err
	}
	bkr.Account = acc
	return bkr, nil
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

func (m *Indexer) ListBakers(ctx context.Context, activeOnly bool) ([]*model.Baker, error) {
	bakers, err := m.Table(index.BakerTableKey)
	if err != nil {
		return nil, err
	}
	bkrs := make([]*model.Baker, 0)
	q := pack.NewQuery("list_bakers", bakers)
	if activeOnly {
		q = q.AndEqual("is_active", true)
	}
	err = q.Execute(ctx, &bkrs)
	if err != nil {
		return nil, err
	}
	bkrMap := make(map[model.AccountID]*model.Baker)
	accIds := make([]uint64, 0)
	for _, v := range bkrs {
		bkrMap[v.AccountId] = v
		accIds = append(accIds, v.AccountId.Value())
	}
	accounts, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	res, err := accounts.Lookup(ctx, accIds)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	err = res.Walk(func(r pack.Row) error {
		acc := &model.Account{}
		if err := r.Decode(acc); err != nil {
			return err
		}
		bkr, ok := bkrMap[acc.RowId]
		if ok {
			bkr.Account = acc
		}
		return nil
	})
	return bkrs, err
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
		// try parsing as event id
		eventId, err := strconv.ParseUint(opIdent, 10, 64)
		if err != nil {
			return nil, index.ErrInvalidOpID
		}
		q = q.AndEqual("height", int64(eventId>>16)).AndEqual("op_n", int64(eventId&0xFFFF))
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

func (m *Indexer) LookupOpHash(ctx context.Context, opid model.OpID) tezos.OpHash {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return tezos.OpHash{}
	}
	type XOp struct {
		Hash tezos.OpHash `pack:"H"`
	}
	o := &XOp{}
	err = pack.NewQuery("find_tx", table).AndEqual("I", opid).Execute(ctx, o)
	if err != nil {
		return tezos.OpHash{}
	}
	return o.Hash
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
		AndEqual("type", model.OpTypeActivation).
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
		AndEqual("type", model.OpTypeDelegation). // type
		AndEqual("sender_id", id).                // search for sender account id
		AndNotEqual("baker_id", 0).               // delegate id
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
		AndEqual("type", model.OpTypeOrigination). // type
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
		opn := int64(r.Cursor & 0xFFFF)
		if r.Order == pack.OrderDesc {
			q = q.AndLt("op_n", opn)
		} else {
			q = q.AndGt("op_n", opn)
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

	// check if we should list delegations, consider different query modes
	withDelegation := r.WithDelegation()
	onlyDelegation := withDelegation && len(r.Typs) == 1

	// list all ops where this address is any of
	// - sender
	// - receiver
	// - delegate (only for delegation type)
	q := pack.NewQuery("list_account_ops", table).
		WithOrder(r.Order).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset))

	switch {
	case r.SenderId > 0: // anything received by us from this sender
		if onlyDelegation {
			q = q.Or(
				// regular delegation is del + delegate set
				// internal delegation are tx + delegate set
				// regular origination + delegation is orig + delegate set
				// internal origination + delegation is orig + delegate set
				pack.And(
					pack.In("type", []model.OpType{model.OpTypeDelegation, model.OpTypeOrigination}),
					pack.Equal("baker_id", r.Account.RowId),
				),
				// regular un/re-delegation is del + receiver set
				// internal un/re-delegation is del + receiver set
				pack.And(
					pack.Equal("type", model.OpTypeDelegation),
					pack.Equal("receiver_id", r.Account.RowId),
				),
			)
			r.Typs = nil
		} else if withDelegation {
			q = q.Or(
				pack.Equal("receiver_id", r.Account.RowId),
				pack.Equal("baker_id", r.Account.RowId),
			)
		} else {
			q = q.AndEqual("receiver_id", r.Account.RowId)
		}
		q = q.AndEqual("sender_id", r.SenderId)
	case r.ReceiverId > 0: // anything sent by us to this receiver
		if onlyDelegation {
			q = q.Or(
				// regular delegation is del + delegate set
				// internal delegation are tx + delegate set
				// regular origination + delegation is orig + delegate set
				// internal origination + delegation is orig + delegate set
				pack.And(
					pack.In("type", []model.OpType{model.OpTypeDelegation, model.OpTypeOrigination}),
					pack.Equal("baker_id", r.ReceiverId),
				),
				// regular un/re-delegation is del + receiver set
				// internal un/re-delegation is del + receiver set
				pack.And(
					pack.Equal("type", model.OpTypeDelegation),
					pack.Equal("receiver_id", r.ReceiverId),
				),
			)
			r.Typs = nil
		} else if withDelegation {
			q = q.Or(
				pack.Equal("receiver_id", r.ReceiverId),
				pack.Equal("baker_id", r.ReceiverId),
			)
		} else {
			q = q.AndEqual("receiver_id", r.ReceiverId)
		}
		q = q.AndEqual("sender_id", r.Account.RowId)
	default: // anything sent or received by us
		if withDelegation {
			q = q.Or(
				pack.Equal("sender_id", r.Account.RowId),
				pack.Equal("receiver_id", r.Account.RowId),
				pack.Equal("baker_id", r.Account.RowId),
			)
		} else {
			q = q.Or(
				pack.Equal("sender_id", r.Account.RowId),
				pack.Equal("receiver_id", r.Account.RowId),
			)
		}
	}

	if r.Cursor > 0 {
		height := int64(r.Cursor >> 16)
		opn := int64(r.Cursor & 0xFFFF)
		if r.Order == pack.OrderDesc {
			q = q.Or(
				pack.Lt("height", height),
				pack.And(
					pack.Equal("height", height),
					pack.Lt("op_n", opn),
				),
			)
		} else {
			q = q.Or(
				pack.Gt("height", height),
				pack.And(
					pack.Equal("height", height),
					pack.Gt("op_n", opn),
				),
			)
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

	// check if we should list delegations, consider different query modes
	withDelegation := r.WithDelegation()
	onlyDelegation := withDelegation && len(r.Typs) == 1

	// list all ops where this address is any of
	// - sender
	// - receiver
	// - delegate
	q := pack.NewQuery("list_account_ops", table).
		WithOrder(r.Order)

	switch {
	case r.SenderId > 0:
		if onlyDelegation {
			q = q.Or(
				// regular delegation is del + delegate set
				// internal delegation are tx + delegate set
				// regular origination + delegation is orig + delegate set
				// internal origination + delegation is orig + delegate set
				pack.And(
					pack.In("type", []model.OpType{model.OpTypeDelegation, model.OpTypeOrigination}),
					pack.Equal("baker_id", r.Account.RowId),
				),
				// regular un/re-delegation is del + receiver set
				// internal un/re-delegation is del + receiver set
				pack.And(
					pack.Equal("type", model.OpTypeDelegation),
					pack.Equal("receiver_id", r.Account.RowId),
				),
			)
			r.Typs = nil
		} else if withDelegation {
			q = q.Or(
				pack.Equal("receiver_id", r.Account.RowId),
				pack.Equal("baker_id", r.Account.RowId),
			)
		} else {
			q = q.AndEqual("receiver_id", r.Account.RowId)
		}
		q = q.AndEqual("sender_id", r.SenderId)

	case r.ReceiverId > 0:
		if onlyDelegation {
			q = q.Or(
				// regular delegation is del + delegate set
				// internal delegation are tx + delegate set
				// regular origination + delegation is orig + delegate set
				// internal origination + delegation is orig + delegate set
				pack.And(
					pack.In("type", []model.OpType{model.OpTypeDelegation, model.OpTypeOrigination}),
					pack.Equal("baker_id", r.ReceiverId),
				),
				// regular un/re-delegation is del + receiver set
				// internal un/re-delegation is del + receiver set
				pack.And(
					pack.Equal("type", model.OpTypeDelegation),
					pack.Equal("receiver_id", r.ReceiverId),
				),
			)
			r.Typs = nil
		} else if withDelegation {
			q = q.Or(
				pack.Equal("receiver_id", r.ReceiverId),
				pack.Equal("baker_id", r.ReceiverId),
			)
		} else {
			q = q.AndEqual("receiver_id", r.ReceiverId)
		}
		q = q.AndEqual("sender_id", r.Account.RowId)
	default:
		if withDelegation {
			q = q.Or(
				pack.Equal("sender_id", r.Account.RowId),
				pack.Equal("receiver_id", r.Account.RowId),
				pack.Equal("baker_id", r.Account.RowId),
			)
		} else {
			q = q.Or(
				pack.Equal("sender_id", r.Account.RowId),
				pack.Equal("receiver_id", r.Account.RowId),
			)
		}
	}

	// FIXME:
	// - if S/R is only in one internal op, pull the entire op group
	ops := make([]*model.Op, 0)

	if r.Cursor > 0 {
		height := int64(r.Cursor >> 16)
		opn := int64(r.Cursor & 0xFFFF)
		if r.Order == pack.OrderDesc {
			q = q.Or(
				pack.Lt("height", height),
				pack.And(
					pack.Equal("height", height),
					pack.Lt("op_n", opn),
				),
			)
		} else {
			q = q.Or(
				pack.Gt("height", height),
				pack.And(
					pack.Equal("height", height),
					pack.Gt("op_n", opn),
				),
			)
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
		lastP      int = -1
		lastHeight int64
		count      int
	)
	err = q.Stream(ctx, func(rx pack.Row) error {
		op := model.AllocOp()
		if err := rx.Decode(op); err != nil {
			return err
		}
		// detect next op group (works in both directions)
		isFirst := lastP < 0
		isNext := op.OpP != lastP || op.Height != lastHeight
		lastP, lastHeight = op.OpP, op.Height

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

	// list all successful tx (calls) received by this contract
	q := pack.NewQuery("list_calls_recv", table).
		WithOrder(r.Order).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset)).
		AndEqual("receiver_id", r.Account.RowId).
		AndEqual("type", model.OpTypeTransaction).
		AndEqual("is_success", true)

	if r.SenderId > 0 {
		q = q.AndEqual("sender_id", r.SenderId)
	}

	// add entrypoint filter
	switch len(r.Entrypoints) {
	case 0:
		// none, search op type
	case 1:
		// any single
		q = q.AndCondition("entrypoint_id", r.Mode, r.Entrypoints[0]) // entrypoint_id
	default:
		// in/nin
		q = q.AndCondition("entrypoint_id", r.Mode, r.Entrypoints) // entrypoint_ids
	}

	if r.Cursor > 0 {
		height := int64(r.Cursor >> 16)
		opn := int64(r.Cursor & 0xFFFF)
		if r.Order == pack.OrderDesc {
			q = q.Or(
				pack.Lt("height", height),
				pack.And(
					pack.Equal("height", height),
					pack.Lt("op_n", opn),
				),
			)
		} else {
			q = q.Or(
				pack.Gt("height", height),
				pack.And(
					pack.Equal("height", height),
					pack.Gt("op_n", opn),
				),
			)
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
		AndEqual("type", model.OpTypeTransaction).
		AndEqual("is_contract", true).
		AndEqual("is_success", true).
		Execute(ctx, op)
	if err != nil {
		return nil, err
	}
	if op.RowId == 0 {
		return nil, index.ErrNoOpEntry
	}
	return op, nil
}

func (m *Indexer) ListContractBigmapIds(ctx context.Context, acc model.AccountID) ([]int64, error) {
	table, err := m.Table(index.BigmapAllocTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_bigmaps", table).AndEqual("account_id", acc)
	ids := make([]int64, 0)
	alloc := &model.BigmapAlloc{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(alloc); err != nil {
			return err
		}
		ids = append(ids, alloc.BigmapId)
		return nil
	})
	if err != nil {
		return nil, err
	}
	vec.Int64Sorter(ids).Sort()
	return ids, nil
}

func (m *Indexer) LookupBigmapAlloc(ctx context.Context, id int64) (*model.BigmapAlloc, error) {
	table, err := m.Table(index.BigmapAllocTableKey)
	if err != nil {
		return nil, err
	}
	alloc := &model.BigmapAlloc{}
	err = pack.NewQuery("search_bigmap", table).
		AndEqual("bigmap_id", id).
		Execute(ctx, alloc)
	if err != nil {
		return nil, err
	}
	if alloc.RowId == 0 {
		return nil, index.ErrNoBigmapAlloc
	}
	return alloc, nil
}

// only type info is relevant
func (m *Indexer) LookupBigmapType(ctx context.Context, id int64) (*model.BigmapAlloc, error) {
	alloc, ok := m.bigmap_types.GetType(id)
	if ok {
		return alloc, nil
	}
	table, err := m.Table(index.BigmapAllocTableKey)
	if err != nil {
		return nil, err
	}
	alloc = &model.BigmapAlloc{}
	err = pack.NewQuery("search_bigmap", table).
		AndEqual("bigmap_id", id).
		Execute(ctx, alloc)
	if err != nil {
		return nil, err
	}
	if alloc.RowId == 0 {
		return nil, index.ErrNoBigmapAlloc
	}
	m.bigmap_types.Add(alloc)
	return alloc, nil
}

func (m *Indexer) ListHistoricBigmapKeys(ctx context.Context, r ListRequest) ([]*model.BigmapKV, error) {
	hist, ok := m.bigmap_values.Get(r.BigmapId, r.Since)
	if !ok {
		start := time.Now()
		table, err := m.Table(index.BigmapUpdateTableKey)
		if err != nil {
			return nil, err
		}

		// check if we have any previous bigmap state cached
		prev, ok := m.bigmap_values.GetBest(r.BigmapId, r.Since)
		if ok {
			// update from existing cache
			hist, err = m.bigmap_values.Update(ctx, prev, table, r.Since)
			if err != nil {
				return nil, err
			}
			log.Debugf("Updated history cache for bigmap %d from height %d to height %d with %d entries in %s",
				r.BigmapId, prev.Height, r.Since, hist.Len(), time.Since(start))

		} else {
			// build a new cache
			hist, err = m.bigmap_values.Build(ctx, table, r.BigmapId, r.Since)
			if err != nil {
				return nil, err
			}
			log.Debugf("Built history cache for bigmap %d at height %d with %d entries in %s",
				r.BigmapId, r.Since, hist.Len(), time.Since(start))
		}
	}

	// cursor and offset are mutually exclusive, we use offset below
	// Note that cursor starts at 1
	if r.Cursor > 0 {
		r.Offset = uint(r.Cursor - 1)
	}
	var from, to int
	if r.Order == pack.OrderAsc {
		from, to = int(r.Offset), int(r.Offset+r.Limit)
	} else {
		l := hist.Len()
		from = util.Max(0, l-int(r.Offset-r.Limit))
		to = from + int(r.Limit)
	}

	// get from cache
	var items []*model.BigmapKV
	if r.BigmapKey.IsValid() {
		if item := hist.Get(r.BigmapKey); item != nil {
			items = []*model.BigmapKV{item}
		}
	} else {
		items = hist.Range(from, to)
	}

	// maybe reverse order
	if r.Order == pack.OrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	return items, nil
}

func (m *Indexer) ListBigmapKeys(ctx context.Context, r ListRequest) ([]*model.BigmapKV, error) {
	table, err := m.Table(index.BigmapValueTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_bigmap", table).
		WithOrder(r.Order).
		AndEqual("bigmap_id", r.BigmapId)
	if r.Cursor > 0 {
		r.Offset = 0
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.BigmapKey.IsValid() {
		// assume hash collisions
		q = q.WithDesc().AndEqual("key_id", model.GetKeyId(r.BigmapId, r.BigmapKey))
	}
	items := make([]*model.BigmapKV, 0)
	err = q.Stream(ctx, func(row pack.Row) error {
		// skip before decoding
		if r.Offset > 0 {
			r.Offset--
			return nil
		}
		b := &model.BigmapKV{}
		if err := row.Decode(b); err != nil {
			return err
		}

		// skip hash collisions on key_id
		if r.BigmapKey.IsValid() && !r.BigmapKey.Equal(b.GetKeyHash()) {
			// log.Infof("Skip hash collision for item %s %d %d key %x", b.Action, b.BigmapId, b.RowId, b.Key)
			return nil
		}

		// log.Infof("Found item %s %d %d key %x", b.Action, b.BigmapId, b.RowId, b.Key)
		items = append(items, b)
		if len(items) == int(r.Limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}

func (m *Indexer) ListBigmapUpdates(ctx context.Context, r ListRequest) ([]*model.BigmapUpdate, error) {
	table, err := m.Table(index.BigmapUpdateTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("list_bigmap", table).WithOrder(r.Order)
	if r.BigmapId > 0 {
		q = q.AndEqual("bigmap_id", r.BigmapId)
	}
	if r.OpId > 0 {
		q = q.AndEqual("op_id", r.OpId)
	}
	if r.Cursor > 0 {
		r.Offset = 0
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	if r.Since > 0 {
		q = q.AndGte("height", r.Since)
	}
	if r.Until > 0 {
		q = q.AndLte("height", r.Until)
	}
	if r.BigmapKey.IsValid() {
		q = q.AndEqual("key_id", model.GetKeyId(r.BigmapId, r.BigmapKey))
	}
	items := make([]*model.BigmapUpdate, 0)
	err = table.Stream(ctx, q, func(row pack.Row) error {
		if r.Offset > 0 {
			r.Offset--
			return nil
		}
		b := &model.BigmapUpdate{}
		if err := row.Decode(b); err != nil {
			return err
		}
		// skip hash collisions on key_id
		if r.BigmapKey.IsValid() && !r.BigmapKey.Equal(b.GetKeyHash()) {
			return nil
		}
		items = append(items, b)
		if len(items) == int(r.Limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
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
	if _, err := table.Delete(ctx, q); err != nil {
		return err
	}
	return table.Flush(ctx)
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

func (m *Indexer) LookupConstant(ctx context.Context, hash tezos.ExprHash) (*model.Constant, error) {
	if !hash.IsValid() {
		return nil, ErrInvalidHash
	}
	table, err := m.Table(index.ConstantTableKey)
	if err != nil {
		return nil, err
	}
	cc := &model.Constant{}
	err = pack.NewQuery("constant_by_hash", table).
		AndEqual("address", hash.Bytes()).
		Execute(ctx, cc)
	if err != nil {
		return nil, err
	}
	if cc.RowId == 0 {
		return nil, index.ErrNoConstantEntry
	}
	return cc, nil
}
