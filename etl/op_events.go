// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"

    "blockwatch.cc/tzindex/etl/model"
)

func (b *Builder) AppendInvoiceOp(ctx context.Context, acc *model.Account, amount int64, p int) error {
    id := model.OpRef{
        Kind: model.OpTypeInvoice,
        N:    b.block.NextN(),
        L:    model.OPL_PROTOCOL_UPGRADE,
        P:    p,
    }
    b.block.Flows = append(b.block.Flows, b.NewInvoiceFlow(acc, amount, id))
    op := model.NewEventOp(b.block, acc.RowId, id)
    op.SenderId = acc.RowId
    op.Reward = amount
    b.block.Ops = append(b.block.Ops, op)
    return nil
}

func (b *Builder) AppendAirdropOp(ctx context.Context, acc *model.Account, amount int64, p int) error {
    id := model.OpRef{
        Kind: model.OpTypeAirdrop,
        N:    b.block.NextN(),
        L:    model.OPL_PROTOCOL_UPGRADE,
        P:    p,
    }
    b.block.Flows = append(b.block.Flows, b.NewAirdropFlow(acc, amount, id))
    op := model.NewEventOp(b.block, acc.RowId, id)
    op.Reward = amount
    b.block.Ops = append(b.block.Ops, op)
    return nil
}

func (b *Builder) AppendMagicBakerRegistrationOp(ctx context.Context, bkr *model.Baker, p int) error {
    id := model.OpRef{
        Kind: model.OpTypeDelegation,
        N:    b.block.NextN(),
        L:    model.OPL_PROTOCOL_UPGRADE,
        P:    p,
    }
    op := model.NewEventOp(b.block, 0, id)
    op.SenderId = bkr.AccountId
    op.BakerId = bkr.AccountId
    b.block.Ops = append(b.block.Ops, op)
    return nil
}

func (b *Builder) AppendContractMigrationOp(ctx context.Context, acc *model.Account, c *model.Contract, p int) error {
    id := model.OpRef{
        Kind: model.OpTypeMigration,
        N:    b.block.NextN(),
        L:    model.OPL_PROTOCOL_UPGRADE,
        P:    p,
    }
    op := model.NewEventOp(b.block, acc.RowId, id)
    op.IsContract = true
    op.Storage = c.Storage
    op.StorageHash = c.StorageHash
    op.IsStorageUpdate = true
    op.Contract = c
    b.block.Ops = append(b.block.Ops, op)
    return nil
}
