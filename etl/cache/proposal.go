// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cache

import (
    "context"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzindex/etl/model"
)

// a cache of on-chain addresses id->hash
type ProposalCache struct {
    props map[model.ProposalID]tezos.ProtocolHash
    stats Stats
}

func NewProposalCache() *ProposalCache {
    return &ProposalCache{
        props: make(map[model.ProposalID]tezos.ProtocolHash),
    }
}

func (c ProposalCache) Stats() Stats {
    s := c.stats.Get()
    s.Size = c.Len()
    s.Bytes = int64(c.Size())
    return s
}

func (c ProposalCache) Len() int {
    return len(c.props)
}

func (c ProposalCache) Size() int {
    return len(c.props) * (8 + tezos.HashTypeProtocol.Len())
}

func (c *ProposalCache) GetHash(id model.ProposalID) tezos.ProtocolHash {
    h, ok := c.props[id]
    if ok {
        c.stats.CountHits(1)
        return h
    }
    c.stats.CountMisses(1)
    return tezos.ProtocolHash{}
}

func (c *ProposalCache) Build(ctx context.Context, table *pack.Table) error {
    type XProposal struct {
        RowId model.ProposalID   `pack:"I,pk"`
        Hash  tezos.ProtocolHash `pack:"H"`
    }
    c.stats.CountUpdates(1)
    p := XProposal{}
    return pack.NewQuery("init_cache", table).
        WithoutCache().
        WithFields("row_id", "hash").
        Stream(ctx, func(r pack.Row) error {
            if err := r.Decode(&p); err != nil {
                return err
            }
            c.props[p.RowId] = p.Hash.Clone()
            return nil
        })
}
