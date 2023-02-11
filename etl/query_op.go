// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "io"
    "strconv"
    "sync"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/packdb/util"
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) LookupOp(ctx context.Context, opIdent string, r ListRequest) ([]*model.Op, error) {
    table, err := m.Table(index.OpTableKey)
    if err != nil {
        return nil, err
    }
    q := pack.NewQuery("find_tx").WithTable(table)
    switch {
    case len(opIdent) == tezos.HashTypeOperation.Base58Len() || tezos.HashTypeOperation.MatchPrefix(opIdent):
        // assume it's a hash
        oh, err := tezos.ParseOpHash(opIdent)
        if err != nil {
            return nil, ErrInvalidHash
        }
        q = q.AndEqual("hash", oh.Hash.Hash)
    default:
        // try parsing as event id
        eventId, err := strconv.ParseUint(opIdent, 10, 64)
        if err != nil {
            return nil, index.ErrInvalidOpID
        }
        q = q.AndEqual("height", int64(eventId>>16)).AndEqual("op_n", int64(eventId&0xFFFF))
    }
    ops := make([]*model.Op, 0)
    err = q.Execute(ctx, &ops)
    if err != nil {
        return nil, err
    }
    if len(ops) == 0 {
        return nil, index.ErrNoOpEntry
    }

    if r.WithStorage {
        m.joinStorage(ctx, ops)
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
    err = pack.NewQuery("find_tx").
        WithTable(table).
        AndEqual("I", opid).Execute(ctx, o)
    if err != nil {
        return tezos.OpHash{}
    }
    return o.Hash
}

func (m *Indexer) LookupEndorsement(ctx context.Context, opIdent string) ([]*model.Op, error) {
    table, err := m.Table(index.EndorseOpTableKey)
    if err != nil {
        return nil, err
    }
    q := pack.NewQuery("find_endorsement").WithTable(table)
    switch {
    case len(opIdent) == tezos.HashTypeOperation.Base58Len() || tezos.HashTypeOperation.MatchPrefix(opIdent):
        // assume it's a hash
        oh, err := tezos.ParseOpHash(opIdent)
        if err != nil {
            return nil, ErrInvalidHash
        }
        q = q.AndEqual("hash", oh.Hash.Hash)
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
        ed := &model.Endorsement{}
        if err := r.Decode(ed); err != nil {
            return err
        }
        ops = append(ops, ed.ToOp())
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

// these are row_id's (!)
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
    q := pack.NewQuery("list_block_ops").
        WithTable(table).
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
    if r.Account != nil {
        q = q.OrCondition(
            pack.Equal("sender_id", r.Account.RowId),
            pack.Equal("receiver_id", r.Account.RowId),
            pack.Equal("baker_id", r.Account.RowId),
            pack.Equal("creator_id", r.Account.RowId),
        )
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
            q = q.And("type", r.Mode, r.Typs[0])
        } else {
            q = q.And("type", r.Mode, r.Typs)
        }
    }
    ops := make([]*model.Op, 0, r.Limit)
    if err = q.Execute(ctx, &ops); err != nil {
        return nil, err
    }
    if r.WithStorage {
        m.joinStorage(ctx, ops)
    }
    return ops, nil
}

func (m *Indexer) ListBlockEndorsements(ctx context.Context, r ListRequest) ([]*model.Endorsement, error) {
    table, err := m.Table(index.EndorseOpTableKey)
    if err != nil {
        return nil, err
    }

    // cursor and offset are mutually exclusive
    if r.Cursor > 0 {
        r.Offset = 0
    }

    q := pack.NewQuery("list_block_endorse").
        WithTable(table).
        WithOrder(r.Order).
        AndEqual("height", r.Since).
        WithLimit(int(r.Limit)).
        WithOffset(int(r.Offset))

    if r.Cursor > 0 {
        opn := int64(r.Cursor & 0xFFFF)
        if r.Order == pack.OrderDesc {
            q = q.AndLt("op_n", opn)
        } else {
            q = q.AndGt("op_n", opn)
        }
    }
    if r.SenderId > 0 {
        q = q.AndEqual("sender_id", r.SenderId)
    }
    if r.Account != nil {
        q = q.AndEqual("sender_id", r.Account.RowId)
    }
    endorse := make([]*model.Endorsement, 0)
    err = q.Execute(ctx, &endorse)
    if err != nil {
        return nil, err
    }
    return endorse, nil
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
    q := pack.NewQuery("list_account_ops").
        WithTable(table).
        WithOrder(r.Order).
        WithLimit(int(r.Limit)).
        WithOffset(int(r.Offset))

    switch {
    case r.SenderId > 0: // anything received by us from this sender
        switch {
        case onlyDelegation:
            q = q.OrCondition(
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
        case withDelegation:
            q = q.OrCondition(
                pack.Equal("receiver_id", r.Account.RowId),
                pack.Equal("baker_id", r.Account.RowId),
            )
        default:
            q = q.AndEqual("receiver_id", r.Account.RowId)
        }
        q = q.AndEqual("sender_id", r.SenderId)
    case r.ReceiverId > 0: // anything sent by us to this receiver
        switch {
        case onlyDelegation:
            q = q.OrCondition(
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
        case withDelegation:
            q = q.OrCondition(
                pack.Equal("receiver_id", r.ReceiverId),
                pack.Equal("baker_id", r.ReceiverId),
            )
        default:
            q = q.AndEqual("receiver_id", r.ReceiverId)
        }
        q = q.AndEqual("sender_id", r.Account.RowId)
    default: // anything sent or received by us
        if withDelegation {
            q = q.OrCondition(
                pack.Equal("sender_id", r.Account.RowId),
                pack.Equal("receiver_id", r.Account.RowId),
                pack.Equal("baker_id", r.Account.RowId),
            )
        } else {
            q = q.OrCondition(
                pack.Equal("sender_id", r.Account.RowId),
                pack.Equal("receiver_id", r.Account.RowId),
            )
        }
    }

    if r.Cursor > 0 {
        height := int64(r.Cursor >> 16)
        opn := int64(r.Cursor & 0xFFFF)
        if r.Order == pack.OrderDesc {
            q = q.OrCondition(
                pack.Lt("height", height),
                pack.And(
                    pack.Equal("height", height),
                    pack.Lt("op_n", opn),
                ),
            )
        } else {
            q = q.OrCondition(
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
            q = q.And("type", r.Mode, r.Typs[0])
        } else {
            q = q.And("type", r.Mode, r.Typs)
        }
    }

    ops := make([]*model.Op, 0)
    if err := q.Execute(ctx, &ops); err != nil {
        return nil, err
    }

    if r.WithStorage {
        m.joinStorage(ctx, ops)
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
    q := pack.NewQuery("list_account_ops").
        WithTable(table).
        WithOrder(r.Order)

    switch {
    case r.SenderId > 0:
        switch {
        case onlyDelegation:
            q = q.OrCondition(
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
        case withDelegation:
            q = q.OrCondition(
                pack.Equal("receiver_id", r.Account.RowId),
                pack.Equal("baker_id", r.Account.RowId),
            )
        default:
            q = q.AndEqual("receiver_id", r.Account.RowId)
        }
        q = q.AndEqual("sender_id", r.SenderId)

    case r.ReceiverId > 0:
        switch {
        case onlyDelegation:
            q = q.OrCondition(
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
        case withDelegation:
            q = q.OrCondition(
                pack.Equal("receiver_id", r.ReceiverId),
                pack.Equal("baker_id", r.ReceiverId),
            )
        default:
            q = q.AndEqual("receiver_id", r.ReceiverId)
        }
        q = q.AndEqual("sender_id", r.Account.RowId)
    default:
        if withDelegation {
            q = q.OrCondition(
                pack.Equal("sender_id", r.Account.RowId),
                pack.Equal("receiver_id", r.Account.RowId),
                pack.Equal("baker_id", r.Account.RowId),
            )
        } else {
            q = q.OrCondition(
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
            q = q.OrCondition(
                pack.Lt("height", height),
                pack.And(
                    pack.Equal("height", height),
                    pack.Lt("op_n", opn),
                ),
            )
        } else {
            q = q.OrCondition(
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
            q = q.And("type", r.Mode, r.Typs[0])
        } else {
            q = q.And("type", r.Mode, r.Typs)
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

    if r.WithStorage {
        m.joinStorage(ctx, ops)
    }

    return ops, nil
}

func (m *Indexer) ListBakerEndorsements(ctx context.Context, r ListRequest) ([]*model.Op, error) {
    table, err := m.Table(index.EndorseOpTableKey)
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

    q := pack.NewQuery("list_baker_endorsements").
        WithTable(table).
        WithOrder(r.Order).
        WithLimit(int(r.Limit)).
        WithOffset(int(r.Offset)).
        AndEqual("sender_id", r.Account.RowId)

    if r.Cursor > 0 {
        height := int64(r.Cursor >> 16)
        opn := int64(r.Cursor & 0xFFFF)
        if r.Order == pack.OrderDesc {
            q = q.OrCondition(
                pack.Lt("height", height),
                pack.And(
                    pack.Equal("height", height),
                    pack.Lt("op_n", opn),
                ),
            )
        } else {
            q = q.OrCondition(
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

    ops := make([]*model.Op, 0)
    var end model.Endorsement
    if err := q.Stream(ctx, func(row pack.Row) error {
        if err := row.Decode(&end); err != nil {
            return err
        }
        ops = append(ops, end.ToOp())
        return nil
    }); err != nil {
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
    q := pack.NewQuery("list_calls_recv").
        WithTable(table).
        WithOrder(r.Order).
        WithLimit(int(r.Limit)).
        WithOffset(int(r.Offset)).
        AndEqual("receiver_id", r.Account.RowId).
        AndEqual("is_success", true)

    if r.Account.Address.IsContract() {
        q = q.AndEqual("type", model.OpTypeTransaction)
    } else if r.Account.Address.IsRollup() {
        q = q.AndIn("type", []model.OpType{
            model.OpTypeTransaction,
            model.OpTypeRollupOrigination,
            model.OpTypeRollupTransaction,
        })
    }

    if r.SenderId > 0 {
        q = q.AndEqual("sender_id", r.SenderId)
    }

    // add entrypoint filter
    switch len(r.Entrypoints) {
    case 0:
        // none, search op type
    case 1:
        // any single
        q = q.And("entrypoint_id", r.Mode, r.Entrypoints[0]) // entrypoint_id
    default:
        // in/nin
        q = q.And("entrypoint_id", r.Mode, r.Entrypoints) // entrypoint_ids
    }

    if r.Cursor > 0 {
        height := int64(r.Cursor >> 16)
        opn := int64(r.Cursor & 0xFFFF)
        if r.Order == pack.OrderDesc {
            q = q.OrCondition(
                pack.Lt("height", height),
                pack.And(
                    pack.Equal("height", height),
                    pack.Lt("op_n", opn),
                ),
            )
        } else {
            q = q.OrCondition(
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

    if r.WithStorage {
        m.joinStorage(ctx, ops)
    }

    return ops, nil
}

func (m *Indexer) FindLastCall(ctx context.Context, acc model.AccountID, from, to int64) (*model.Op, error) {
    table, err := m.Table(index.OpTableKey)
    if err != nil {
        return nil, err
    }
    q := pack.NewQuery("last_call").
        WithTable(table).
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

func (m *Indexer) FindLatestDelegation(ctx context.Context, id model.AccountID, height int64) (*model.Op, error) {
    table, err := m.Table(index.OpTableKey)
    if err != nil {
        return nil, err
    }
    o := &model.Op{}
    err = pack.NewQuery("find_last_delegation").
        WithTable(table).
        WithoutCache().
        WithDesc().
        WithLimit(1).
        AndEqual("type", model.OpTypeDelegation). // type
        AndEqual("sender_id", id).                // search for sender account id
        AndNotEqual("baker_id", 0).               // delegate id
        AndLt("height", height).                  // must be in a previous block
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
    err = pack.NewQuery("find_origination").
        WithTable(table).
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

// Optimized concurrent lookup for many ops (500)
func (m *Indexer) joinStorage(ctx context.Context, ops []*model.Op) {
    // load and merge storage updates, bigmap diffs, events and ticket updates
    opRowIds := make([]uint64, 0, len(ops))
    opIds := make([]uint64, 0, len(ops))
    for _, v := range ops {
        if !v.IsSuccess || !v.IsContract {
            continue
        }
        if v.Type != model.OpTypeTransaction && v.Type != model.OpTypeOrigination && v.Type != model.OpTypeSubsidy {
            continue
        }
        opRowIds = append(opRowIds, v.RowId.Value())
        opIds = append(opIds, v.Id())
    }

    var wg sync.WaitGroup

    // storage
    wg.Add(1)
    go func() {
        defer wg.Done()
        table, _ := m.Table(index.StorageTableKey)
        for _, v := range ops {
            if !v.IsSuccess || !v.IsContract {
                continue
            }
            if v.Type != model.OpTypeTransaction && v.Type != model.OpTypeOrigination && v.Type != model.OpTypeSubsidy {
                continue
            }
            store := &model.Storage{}
            err := pack.NewQuery("api.storage.lookup").
                WithTable(table).
                WithDesc(). // search in reverse order to find latest update
                AndLte("height", v.Height).
                AndEqual("account_id", v.ReceiverId).
                AndEqual("hash", v.StorageHash).
                Execute(ctx, store)
            if err == nil && store.RowId > 0 {
                v.Storage = store.Storage
            }
        }
    }()

    // bigmaps
    upd := make([]model.BigmapUpdate, 0)
    wg.Add(1)
    go func() {
        defer wg.Done()
        table, err := m.Table(index.BigmapUpdateTableKey)
        if err == nil {
            _ = pack.NewQuery("api.list_bigmap").
                WithTable(table).
                AndIn("op_id", opRowIds).
                Execute(ctx, &upd)
        }
    }()

    // events
    events := make([]*model.Event, 0)
    wg.Add(1)
    go func() {
        defer wg.Done()
        table, err := m.Table(index.EventTableKey)
        if err == nil {
            _ = pack.NewQuery("api.list_events").
                WithTable(table).
                AndIn("op_id", opIds).
                Execute(ctx, &events)
        }
    }()

    // ticket updates
    tickets := make([]*model.TicketUpdate, 0)
    wg.Add(1)
    go func() {
        defer wg.Done()
        table, err := m.Table(index.TicketUpdateTableKey)
        if err == nil {
            _ = pack.NewQuery("api.list_ticket_updates").
                WithTable(table).
                AndIn("op_id", opIds).
                Execute(ctx, &tickets)
        }
    }()

    // wait
    wg.Wait()

    // assign
    var bmIdx, evIdx, tiIdx int
    for _, v := range ops {
        if !v.IsSuccess || !v.IsContract {
            continue
        }
        if v.Type != model.OpTypeTransaction && v.Type != model.OpTypeOrigination {
            continue
        }
        // skip if necessary
        for bmIdx < len(upd) && upd[bmIdx].OpId < v.RowId {
            bmIdx++
        }
        // assign
        for bmIdx < len(upd) && upd[bmIdx].OpId == v.RowId {
            v.BigmapUpdates = append(v.BigmapUpdates, upd[bmIdx])
            bmIdx++
        }
        // skip if necessary
        for evIdx < len(events) && events[evIdx].OpId < v.Id() {
            evIdx++
        }
        // assign
        for evIdx < len(events) && events[evIdx].OpId == v.Id() {
            v.Events = append(v.Events, events[evIdx])
            evIdx++
        }
        // skip if necessary
        for tiIdx < len(tickets) && tickets[tiIdx].OpId < v.Id() {
            tiIdx++
        }
        // assign
        for tiIdx < len(tickets) && tickets[tiIdx].OpId == v.Id() {
            v.TicketUpdates = append(v.TicketUpdates, tickets[tiIdx])
            tiIdx++
        }
    }
}
