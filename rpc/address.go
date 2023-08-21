// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"fmt"

	"blockwatch.cc/tzgo/tezos"
)

func (b *Block) CollectAddresses(addUnique func(tezos.Address)) error {
	// collect from block-level balance updates if invoice is found
	if inv, ok := b.Invoices(); ok {
		for _, v := range inv {
			addUnique(v.Address())
		}
	}
	addUnique(b.Metadata.ProposerConsensusKey)
	addUnique(b.Metadata.BakerConsensusKey)

	// collect from ops
	for _, oll := range b.Operations {
		for _, oh := range oll {
			// check for pruned metadata
			if len(oh.Metadata) > 0 {
				return fmt.Errorf("metadata %s! Your archive node has pruned metadata and must be rebuilt. Always run Octez v12.x with --metadata-size-limit unlimited. Sorry for your trouble.", oh.Metadata)
			}
			// parse operations
			for _, op := range oh.Contents {
				switch tx := op.(type) {
				case *Activation:
					// need to search for blinded key
					// account may already be activated and this is a second claim
					// don't look for and allocate new account, this happens in
					// op processing
					bkey, err := tezos.BlindAddress(tx.Pkh, tx.Secret)
					if err != nil {
						return fmt.Errorf("%s %s: blinded address creation failed: %w", op.Kind(), oh.Hash, err)
					}
					addUnique(bkey)

				case *Ballot:
					addUnique(tx.Source)

				case *Proposals:
					addUnique(tx.Source)

				case *Delegation:
					addUnique(tx.Source)
					addUnique(tx.Delegate)

				case *Origination:
					addUnique(tx.Source)
					addUnique(tx.ManagerAddress())
					res := tx.Result()
					for _, v := range res.OriginatedContracts {
						addUnique(v)
					}

					// add addresses found in storage and bigmap updates
					// to identify ghost accounts
					if res.IsSuccess() {
						tx.AddEmbeddedAddresses(addUnique)
					}

				case *Reveal:
					addUnique(tx.Source)

				case *Transaction:
					addUnique(tx.Source)
					addUnique(tx.Destination)
					for _, res := range tx.Metadata.InternalResults {
						addUnique(res.Destination)
						addUnique(res.Delegate)
						for _, v := range res.Result.OriginatedContracts {
							addUnique(v)
						}
					}
					if tx.Result().IsSuccess() {
						tx.AddEmbeddedAddresses(addUnique)
					}

				case *ConstantRegistration:
					addUnique(tx.Source)

				case *SetDepositsLimit:
					addUnique(tx.Source)

				case *IncreasePaidStorage:
					addUnique(tx.Source)
					addUnique(tx.Destination)

				case *TransferTicket:
					addUnique(tx.Source)
					addUnique(tx.Destination)
					addUnique(tx.Ticketer)
					for _, res := range tx.Metadata.InternalResults {
						addUnique(res.Destination)
						addUnique(res.Delegate)
						for _, v := range res.Result.OriginatedContracts {
							addUnique(v)
						}
					}
					if tx.Result().IsSuccess() {
						tx.AddEmbeddedAddresses(addUnique)
					}

				case *TxRollup:
					addUnique(tx.Source)
					addUnique(tx.Target())
					addUnique(tx.Result().OriginatedRollup)

					// find offender who gets slashed from balance updates
					if op.Kind() == tezos.OpTypeTxRollupRejection {
						if bal := tx.Result().BalanceUpdates; len(bal) > 0 {
							addUnique(bal[0].Address()) // offender
						}
					}

				case *UpdateConsensusKey:
					addUnique(tx.Source)

				case *DrainDelegate:
					addUnique(tx.ConsensusKey)
					addUnique(tx.Delegate)
					addUnique(tx.Destination)

				case *SmartRollupOriginate:
					addUnique(tx.Source)
					addUnique(*tx.Result().Address)

				case *SmartRollupAddMessages:
					addUnique(tx.Source)

				case *SmartRollupCement:
					addUnique(tx.Source)
					addUnique(tx.Rollup)

				case *SmartRollupPublish:
					addUnique(tx.Source)
					addUnique(tx.Rollup)

				case *SmartRollupRefute:
					addUnique(tx.Source)
					addUnique(tx.Rollup)
					addUnique(tx.Opponent)

				case *SmartRollupTimeout:
					addUnique(tx.Source)
					addUnique(tx.Rollup)
					addUnique(tx.Stakers.Alice)
					addUnique(tx.Stakers.Bob)

				case *SmartRollupExecuteOutboxMessage:
					addUnique(tx.Source)
					addUnique(tx.Rollup)
					for _, res := range tx.Metadata.InternalResults {
						addUnique(res.Destination)
						addUnique(res.Delegate)
						for _, v := range res.Result.OriginatedContracts {
							addUnique(v)
						}
					}

				case *SmartRollupRecoverBond:
					addUnique(tx.Source)
					addUnique(tx.Rollup)
					addUnique(tx.Staker)

					// TODO
					// tezos.OpTypeDalSlotAvailability,
					// tezos.OpTypeDalPublishSlotHeader:

					// No address info
					// - SeedNonceRevelation
					// - VdfRevelation
					// - DoubleBakingEvidence
					// - DoubleEndorsementEvidence
					// - DoublePreendorsementEvidence
					// - Endorsement
					// - EndorsementWithSlot
				}
			}
		}
	}

	// collect from implicit block ops
	for i, op := range b.Metadata.ImplicitOperationsResults {
		switch op.Kind {
		case tezos.OpTypeOrigination:
			for _, v := range op.OriginatedContracts {
				addUnique(v)
			}
		case tezos.OpTypeTransaction:
			for _, v := range op.BalanceUpdates {
				addUnique(v.Address())
			}
		default:
			return fmt.Errorf("implicit block op [%d]: unsupported op %s", i, op.Kind)
		}
	}
	return nil
}
