// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	. "blockwatch.cc/tzindex/etl/model"
)

const (
	MetadataPackSizeLog2         = 11 // 2048
	MetadataJournalSizeLog2      = 12 // 4096
	MetadataCacheSize            = 4
	MetadataFillLevel            = 100
	MetadataIndexPackSizeLog2    = 11 // 2048
	MetadataIndexJournalSizeLog2 = 12 // 4096
	MetadataIndexCacheSize       = 4
	MetadataIndexFillLevel       = 90
	MetadataIndexKey             = "metadata"
	MetadataTableKey             = "metadata"
)

var (
	ErrNoMetadataEntry = errors.New("metadata not found")
)

type MetadataIndex struct {
	db    *pack.DB
	opts  pack.Options
	iopts pack.Options
	table *pack.Table
}

var _ BlockIndexer = (*MetadataIndex)(nil)

func NewMetadataIndex(opts, iopts pack.Options) *MetadataIndex {
	return &MetadataIndex{
		opts:  opts,
		iopts: iopts,
	}
}

func (idx *MetadataIndex) DB() *pack.DB {
	return idx.db
}

func (idx *MetadataIndex) Tables() []*pack.Table {
	return []*pack.Table{idx.table}
}

func (idx *MetadataIndex) Key() string {
	return MetadataIndexKey
}

func (idx *MetadataIndex) Name() string {
	return MetadataIndexKey + " index"
}

func (idx *MetadataIndex) Create(path, label string, opts interface{}) error {
	fields, err := pack.Fields(Metadata{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
	}
	defer db.Close()

	table, err := db.CreateTableIfNotExists(
		MetadataTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, MetadataPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, MetadataJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, MetadataCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, MetadataFillLevel),
		})
	if err != nil {
		return err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // account hash field (21/22 byte hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.iopts.PackSizeLog2, MetadataIndexPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, MetadataIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, MetadataIndexCacheSize),
			FillLevel:       util.NonZero(idx.iopts.FillLevel, MetadataIndexFillLevel),
		})
	if err != nil {
		return err
	}

	return nil
}

func (idx *MetadataIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.table, err = idx.db.Table(
		MetadataTableKey,
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, MetadataJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, MetadataCacheSize),
		},
		pack.Options{
			JournalSizeLog2: util.NonZero(idx.iopts.JournalSizeLog2, MetadataIndexJournalSizeLog2),
			CacheSize:       util.NonZero(idx.iopts.CacheSize, MetadataIndexCacheSize),
		})
	if err != nil {
		idx.Close()
		return err
	}

	return nil
}

func (idx *MetadataIndex) Close() error {
	if idx.table != nil {
		if err := idx.table.Close(); err != nil {
			log.Errorf("Closing %s: %v", idx.Name(), err)
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

func (idx *MetadataIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// noop
	return nil
}

func (idx *MetadataIndex) DeleteBlock(ctx context.Context, height int64) error {
	// noop
	return nil
}
