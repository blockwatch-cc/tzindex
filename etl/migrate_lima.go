// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"

	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) MigrateLima(ctx context.Context, oldparams, params *rpc.Params) error {
	// nothing to do in light mode or when chain starts with this proto
	if b.idx.lightMode || b.block.Height <= 2 {
		return nil
	}

	// fetch and build rights + income for future 5 cycles
	if err := b.RebuildFutureRightsAndIncome(ctx, params); err != nil {
		return err
	}

	log.Infof("Migrate v%03d: complete", params.Version)
	return nil
}
