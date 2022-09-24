// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "fmt"
    "strconv"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) BlockByID(ctx context.Context, id uint64) (*model.Block, error) {
    if id == 0 {
        return nil, index.ErrNoBlockEntry
    }
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return nil, err
    }
    b := &model.Block{}
    err = pack.NewQuery("block_by_parent_id").
        WithTable(table).
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
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return nil, err
    }
    b := &model.Block{}
    err = pack.NewQuery("block_by_parent_id").
        WithTable(table).
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
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return b.Hash, err
    }
    err = pack.NewQuery("block_hash_by_height").
        WithTable(table).
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
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return b.Hash, err
    }
    err = pack.NewQuery("block_hash_by_id").
        WithTable(table).
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
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return nil, err
    }
    b := &model.Block{}
    err = pack.NewQuery("block_by_height").
        WithTable(table).
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
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return nil, err
    }
    q := pack.NewQuery("block_by_hash").
        WithTable(table).
        WithLimit(1).
        WithDesc()
    if from > 0 {
        q = q.AndGte("height", from)
    }
    if to > 0 {
        q = q.AndLte("height", to)
    }
    // most expensive condition last
    q = q.AndEqual("hash", h.Hash.Hash)
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
    var err error
    switch {
    case blockIdent == "head":
        if b, err2 := m.BlockByHeight(ctx, m.tips[index.BlockTableKey].Height); err2 == nil {
            return b.Hash, b.Height, nil
        } else {
            err = err2
        }
    case len(blockIdent) == tezos.HashTypeBlock.Base58Len() || tezos.HashTypeBlock.MatchPrefix(blockIdent):
        // assume it's a hash
        if blockHash, err2 := tezos.ParseBlockHash(blockIdent); err2 == nil {
            if b, err3 := m.BlockByHash(ctx, blockHash, 0, 0); err3 == nil {
                return b.Hash, b.Height, nil
            } else {
                err = err3
            }
        } else {
            err = err2
        }
    default:
        // try parsing as height
        if blockHeight, err2 := strconv.ParseInt(blockIdent, 10, 64); err2 == nil {
            // from cache
            return m.LookupBlockHash(ctx, blockHeight), blockHeight, nil
        }
        err = index.ErrInvalidBlockHeight
    }
    return tezos.BlockHash{}, 0, err
}

func (m *Indexer) LookupBlock(ctx context.Context, blockIdent string) (*model.Block, error) {
    var (
        b   *model.Block
        err error
    )
    switch {
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
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return nil, err
    }
    b := &model.Block{}
    err = pack.NewQuery("last_baked_block").
        WithTable(table).
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

func (m *Indexer) LookupLastEndorsedBlock(ctx context.Context, bkr *model.Baker) (*model.Block, error) {
    if bkr.SlotsEndorsed == 0 {
        return nil, index.ErrNoBlockEntry
    }
    table, err := m.Table(index.EndorseOpTableKey)
    if err != nil {
        return nil, err
    }
    var ed model.Endorsement
    err = pack.NewQuery("last_endorse_op").
        WithTable(table).
        WithFields("h").
        WithLimit(1).
        WithDesc().
        AndRange("height", bkr.Account.FirstSeen, bkr.Account.LastSeen).
        AndEqual("sender_id", bkr.AccountId).
        Execute(ctx, &ed)
    if err != nil {
        return nil, err
    }
    if ed.Height == 0 {
        return nil, index.ErrNoBlockEntry
    }
    return m.BlockByHeight(ctx, ed.Height)
}

func (m *Indexer) ListBlockRights(ctx context.Context, height int64, typ tezos.RightType) ([]model.BaseRight, error) {
    table, err := m.Table(index.RightsTableKey)
    if err != nil {
        return nil, err
    }
    p := m.ParamsByHeight(height)
    q := pack.NewQuery("list_rights").
        WithTable(table).
        AndEqual("cycle", p.CycleFromHeight(height))
    if typ.IsValid() {
        q = q.AndEqual("type", typ)
    }
    resp := make([]model.BaseRight, 0)
    right := model.Right{}
    start := p.CycleStartHeight(p.CycleFromHeight(height))
    pos := int(height - start)
    err = q.Stream(ctx, func(r pack.Row) error {
        if err := r.Decode(&right); err != nil {
            return err
        }
        switch typ {
        case tezos.RightTypeBaking:
            if r, ok := right.ToBase(pos, tezos.RightTypeBaking); ok {
                resp = append(resp, r)
            }
        case tezos.RightTypeEndorsing:
            if r, ok := right.ToBase(pos, tezos.RightTypeEndorsing); ok {
                resp = append(resp, r)
            }
        default:
            if r, ok := right.ToBase(pos, tezos.RightTypeBaking); ok {
                resp = append(resp, r)
            }
            if r, ok := right.ToBase(pos, tezos.RightTypeEndorsing); ok {
                resp = append(resp, r)
            }
        }
        return nil
    })
    if err != nil {
        return nil, err
    }
    return resp, nil
}
