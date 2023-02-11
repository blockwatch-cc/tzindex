// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
    "context"
    "errors"
    "fmt"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/packdb/util"

    "blockwatch.cc/tzindex/etl/model"
)

const (
    StoragePackSizeLog2    = 10  // 1k packs because storage can be very large
    StorageJournalSizeLog2 = 10  // 1k journal to flush often
    StorageCacheSize       = 2   // very small cache, fixed
    StorageFillLevel       = 100 // append only
    StorageIndexKey        = "storage"
    StorageTableKey        = "storage"
)

var (
    ErrNoStorageEntry = errors.New("storage not indexed")
)

type StorageIndex struct {
    db       *pack.DB
    opts     pack.Options
    storages *pack.Table
}

var _ model.BlockIndexer = (*StorageIndex)(nil)

func NewStorageIndex(opts pack.Options) *StorageIndex {
    return &StorageIndex{opts: opts}
}

func (idx *StorageIndex) DB() *pack.DB {
    return idx.db
}

func (idx *StorageIndex) Tables() []*pack.Table {
    return []*pack.Table{idx.storages}
}

func (idx *StorageIndex) Key() string {
    return StorageIndexKey
}

func (idx *StorageIndex) Name() string {
    return StorageIndexKey + " index"
}

func (idx *StorageIndex) Create(path, label string, opts interface{}) error {
    fields, err := pack.Fields(model.Storage{})
    if err != nil {
        return err
    }
    db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
    if err != nil {
        return fmt.Errorf("creating %s database: %v", idx.Key(), err)
    }
    defer db.Close()

    _, err = db.CreateTableIfNotExists(
        StorageTableKey,
        fields,
        pack.Options{
            PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, StoragePackSizeLog2),
            JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, StorageJournalSizeLog2),
            CacheSize:       util.NonZero(idx.opts.CacheSize, StorageCacheSize),
            FillLevel:       util.NonZero(idx.opts.FillLevel, StorageFillLevel),
        })
    return err
}

func (idx *StorageIndex) Init(path, label string, opts interface{}) error {
    var err error
    idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
    if err != nil {
        return err
    }
    idx.storages, err = idx.db.Table(
        StorageTableKey,
        pack.Options{
            JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, StorageJournalSizeLog2),
            CacheSize:       util.NonZero(idx.opts.CacheSize, StorageCacheSize),
        })
    if err != nil {
        idx.Close()
        return err
    }
    return nil
}

func (idx *StorageIndex) FinalizeSync(ctx context.Context) error {
    return nil
}

func (idx *StorageIndex) Close() error {
    for _, v := range idx.Tables() {
        if v != nil {
            if err := v.Close(); err != nil {
                log.Errorf("Closing %s table: %s", v.Name(), err)
            }
        }
    }
    idx.storages = nil
    if idx.db != nil {
        if err := idx.db.Close(); err != nil {
            return err
        }
        idx.db = nil
    }
    return nil
}

func (idx *StorageIndex) ConnectBlock(ctx context.Context, block *model.Block, b model.BlockBuilder) error {
    ins := make([]pack.Item, 0)
    for _, op := range block.Ops {
        // don't process failed or unrelated ops
        if !op.IsStorageUpdate || !op.IsSuccess {
            continue
        }

        store := &model.Storage{
            AccountId: op.Contract.AccountId,
            Hash:      op.StorageHash,
            Height:    block.Height,
            Storage:   op.Storage,
        }
        ins = append(ins, store)
    }

    // insert, will generate unique row ids
    return idx.storages.Insert(ctx, ins)
}

func (idx *StorageIndex) DisconnectBlock(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
    return idx.DeleteBlock(ctx, block.Height)
}

func (idx *StorageIndex) DeleteBlock(ctx context.Context, height int64) error {
    _, err := pack.NewQuery("etl.op.delete").
        WithTable(idx.storages).
        AndEqual("height", height).
        Delete(ctx)
    return err
}

func (idx *StorageIndex) DeleteCycle(ctx context.Context, cycle int64) error {
    return nil
}

func (idx *StorageIndex) Flush(ctx context.Context) error {
    for _, v := range idx.Tables() {
        if err := v.Flush(ctx); err != nil {
            return err
        }
    }
    return nil
}
