// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"io"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) UpdateMetadata(ctx context.Context, md *model.Metadata) error {
	if md == nil {
		return nil
	}
	table, err := m.Table(model.MetadataTableKey)
	if err != nil {
		return err
	}
	if md.RowId > 0 {
		return table.Update(ctx, md)
	}
	err = pack.NewQuery("api.metadata.search").
		WithTable(table).
		WithoutCache().
		WithLimit(1).
		AndEqual("address", md.Address[:]).
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
	table, err := m.Table(model.MetadataTableKey)
	if err != nil {
		return err
	}
	if md.RowId > 0 {
		return table.DeleteIds(ctx, []uint64{md.RowId})
	}
	q := pack.NewQuery("api.metadata.delete").
		WithTable(table).
		AndEqual("address", md.Address[:]).
		AndEqual("asset_id", md.AssetId).
		AndEqual("is_asset", md.IsAsset)
	_, err = table.Delete(ctx, q)
	return err
}

func (m *Indexer) PurgeMetadata(ctx context.Context) error {
	table, err := m.Table(model.MetadataTableKey)
	if err != nil {
		return err
	}
	q := pack.NewQuery("api.metadata.purge").
		WithTable(table).
		AndGte("row_id", 0)
	if _, err := table.Delete(ctx, q); err != nil {
		return err
	}
	return table.Flush(ctx)
}

func (m *Indexer) UpsertMetadata(ctx context.Context, entries []*model.Metadata) error {
	table, err := m.Table(model.MetadataTableKey)
	if err != nil {
		return err
	}

	// copy slice ptrs
	match := make([]*model.Metadata, len(entries))
	copy(match, entries)

	// find existing metadata entries for update
	upd := make([]pack.Item, 0)
	md := &model.Metadata{}
	err = pack.NewQuery("api.metadata.upsert").
		WithTable(table).
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
				if match[i].Address != md.Address {
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
