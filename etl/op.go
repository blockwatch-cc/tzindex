// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"

	"blockwatch.cc/tzindex/etl/model"
)

func (b *Builder) AppendRegularBlockOps(ctx context.Context, rollback bool) error {
	for op_l, ol := range b.block.TZ.Block.Operations {
		for op_p, oh := range ol {
			for op_c, o := range oh.Contents {
				var err error
				id := model.OpRef{
					Hash: oh.Hash,
					Kind: model.MapOpType(o.Kind()),
					N:    b.block.NextN(),
					L:    op_l,
					P:    op_p,
					C:    op_c,
					I:    0,
					Raw:  o,
				}
				switch id.Kind {
				case model.OpTypeEndorsement, model.OpTypePreendorsement:
					err = b.AppendEndorsementOp(ctx, oh, id, rollback)
				case model.OpTypeTransaction:
					err = b.AppendTransactionOp(ctx, oh, id, rollback)
				case model.OpTypeDelegation:
					err = b.AppendDelegationOp(ctx, oh, id, rollback)
				case model.OpTypeReveal:
					err = b.AppendRevealOp(ctx, oh, id, rollback)
				case model.OpTypeNonceRevelation:
					err = b.AppendSeedNonceOp(ctx, oh, id, rollback)
				case model.OpTypeOrigination:
					err = b.AppendOriginationOp(ctx, oh, id, rollback)
				case model.OpTypeActivation:
					err = b.AppendActivationOp(ctx, oh, id, rollback)
				case model.OpTypeProposal:
					err = b.AppendProposalOp(ctx, oh, id, rollback)
				case model.OpTypeBallot:
					err = b.AppendBallotOp(ctx, oh, id, rollback)
				case model.OpTypeDoubleBaking:
					err = b.AppendDoubleBakingOp(ctx, oh, id, rollback)
				case model.OpTypeDoubleEndorsement, model.OpTypeDoublePreendorsement:
					err = b.AppendDoubleEndorsingOp(ctx, oh, id, rollback)
				case model.OpTypeRegisterConstant:
					err = b.AppendRegisterConstantOp(ctx, oh, id, rollback)
				case model.OpTypeDepositsLimit:
					err = b.AppendDepositsLimitOp(ctx, oh, id, rollback)
				case model.OpTypeRollupOrigination:
					err = b.AppendRollupOriginationOp(ctx, oh, id, rollback)
				case model.OpTypeRollupTransaction:
					err = b.AppendRollupTransactionOp(ctx, oh, id, rollback)
				case model.OpTypeVdfRevelation:
					err = b.AppendVdfRevelationOp(ctx, oh, id, rollback)
				case model.OpTypeIncreasePaidStorage:
					err = b.AppendStorageLimitOp(ctx, oh, id, rollback)
				case model.OpTypeDrainDelegate:
					err = b.AppendDrainDelegateOp(ctx, oh, id, rollback)
				case model.OpTypeUpdateConsensusKey:
					err = b.AppendUpdateConsensusKeyOp(ctx, oh, id, rollback)
				}
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
