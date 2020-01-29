// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"time"

	"blockwatch.cc/packdb/pack"
)

type Chain struct {
	RowId              uint64    `pack:"I,pk,snappy"   json:"row_id"`                             // unique id
	Height             int64     `pack:"h,snappy"      json:"height"`                             // bc: block height (also for orphans)
	Cycle              int64     `pack:"c,snappy"      json:"cycle"`                              // bc: block cycle (tezos specific)
	Timestamp          time.Time `pack:"T,snappy"      json:"time"`                               // bc: block creation time
	TotalAccounts      int64     `pack:"A,snappy"      json:"total_accounts"`                     // default accounts
	TotalImplicit      int64     `pack:"J,snappy"      json:"total_implicit"`                     // implicit accounts
	TotalManaged       int64     `pack:"M,snappy"      json:"total_managed"`                      // managed/originated accounts for delegation (KT1 without code)
	TotalContracts     int64     `pack:"C,snappy"      json:"total_contracts"`                    // smart contracts (KT1 with code)
	TotalOps           int64     `pack:"O,snappy"      json:"total_ops"`                          //
	TotalContractOps   int64     `pack:"G,snappy"      json:"total_contract_ops"`                 // ops on KT1 contracts
	TotalActivations   int64     `pack:"t,snappy"      json:"total_activations"`                  // fundraiser accounts activated
	TotalSeedNonces    int64     `pack:"N,snappy"      json:"total_seed_nonce_revelations"`       //
	TotalEndorsements  int64     `pack:"E,snappy"      json:"total_endorsements"`                 //
	TotalDoubleBake    int64     `pack:"X,snappy"      json:"total_double_baking_evidences"`      //
	TotalDoubleEndorse int64     `pack:"Y,snappy"      json:"total_double_endorsement_evidences"` //
	TotalDelegations   int64     `pack:"D,snappy"      json:"total_delegations"`                  //
	TotalReveals       int64     `pack:"R,snappy"      json:"total_reveals"`                      //
	TotalOriginations  int64     `pack:"g,snappy"      json:"total_originations"`                 //
	TotalTransactions  int64     `pack:"x,snappy"      json:"total_transactions"`                 //
	TotalProposals     int64     `pack:"p,snappy"      json:"total_proposals"`                    //
	TotalBallots       int64     `pack:"b,snappy"      json:"total_ballots"`                      //
	TotalStorageBytes  int64     `pack:"S,snappy"      json:"total_storage_bytes"`                //
	TotalPaidBytes     int64     `pack:"P,snappy"      json:"total_paid_bytes"`                   // storage paid for KT1 and contract store
	TotalUsedBytes     int64     `pack:"B,snappy"      json:"total_used_bytes"`                   // ? (used by txscan.io)
	TotalOrphans       int64     `pack:"U,snappy"      json:"total_orphans"`                      // alternative block headers
	FundedAccounts     int64     `pack:"f,snappy"      json:"funded_accounts"`                    // total number of accounts qith non-zero balance
	UnclaimedAccounts  int64     `pack:"u,snappy"      json:"unclaimed_accounts"`                 // fundraiser accounts unclaimed
	TotalDelegators    int64     `pack:"2,snappy"      json:"total_delegators"`                   // count of all non-zero balance delegators
	ActiveDelegators   int64     `pack:"3,snappy"      json:"active_delegators"`                  // KT1 delegating to active delegates
	InactiveDelegators int64     `pack:"4,snappy"      json:"inactive_delegators"`                // KT1 delegating to inactive delegates
	TotalDelegates     int64     `pack:"5,snappy"      json:"total_delegates"`                    // count of all delegates (active and inactive)
	ActiveDelegates    int64     `pack:"6,snappy"      json:"active_delegates"`                   // tz* active delegates
	InactiveDelegates  int64     `pack:"7,snappy"      json:"inactive_delegates"`                 // tz* inactive delegates
	ZeroDelegates      int64     `pack:"z,snappy"      json:"zero_delegates"`                     // tz* delegate with zero staking balance
	SelfDelegates      int64     `pack:"s,snappy"      json:"self_delegates"`                     // tz* delegate with no incoming delegation
	SingleDelegates    int64     `pack:"1,snappy"      json:"single_delegates"`                   // tz* delegate with 1 incoming delegation
	MultiDelegates     int64     `pack:"m,snappy"      json:"multi_delegates"`                    // tz* delegate with >1 incoming delegations (delegation services)
	Rolls              int64     `pack:"r,snappy"      json:"rolls"`                              // total sum of rolls (delegated_balance/10,000)
	RollOwners         int64     `pack:"o,snappy"      json:"roll_owners"`                        // distinct delegates
}

// Ensure Chain implements the pack.Item interface.
var _ pack.Item = (*Chain)(nil)

func (c *Chain) ID() uint64 {
	return c.RowId
}

func (c *Chain) SetID(id uint64) {
	c.RowId = id
}

func (c *Chain) Update(b *Block, delegates map[AccountID]*Account) {
	c.RowId = 0 // force allocating new id
	c.Height = b.Height
	c.Cycle = b.Cycle
	c.Timestamp = b.Timestamp
	c.TotalAccounts += int64(b.NewAccounts)
	c.TotalImplicit += int64(b.NewImplicitAccounts)
	c.TotalManaged += int64(b.NewManagedAccounts)
	c.TotalContracts += int64(b.NewContracts)
	c.TotalOps += int64(b.NOps)
	c.TotalContractOps += int64(b.NOpsContract)
	c.TotalActivations += int64(b.NActivation)
	c.UnclaimedAccounts -= int64(b.NActivation)
	c.TotalSeedNonces += int64(b.NSeedNonce)
	c.TotalEndorsements += int64(b.NEndorsement)
	c.TotalDoubleBake += int64(b.N2Baking)
	c.TotalDoubleEndorse += int64(b.N2Endorsement)
	c.TotalDelegations += int64(b.NDelegation)
	c.TotalReveals += int64(b.NReveal)
	c.TotalOriginations += int64(b.NOrigination)
	c.TotalTransactions += int64(b.NTx)
	c.TotalProposals += int64(b.NProposal)
	c.TotalBallots += int64(b.NBallot)
	c.TotalStorageBytes += int64(b.StorageSize)
	for _, op := range b.Ops {
		c.TotalPaidBytes += op.StoragePaid
	}
	c.FundedAccounts += int64(b.FundedAccounts - b.ClearedAccounts)

	// from delegates
	c.TotalDelegators = 0
	c.ActiveDelegators = 0
	c.InactiveDelegators = 0
	c.TotalDelegates = 0
	c.ActiveDelegates = 0
	c.InactiveDelegates = 0
	c.ZeroDelegates = 0
	c.SelfDelegates = 0
	c.SingleDelegates = 0
	c.MultiDelegates = 0
	c.Rolls = 0
	c.RollOwners = 0
	for _, acc := range delegates {
		// sanity checks
		if !acc.IsDelegate {
			continue
		}

		// all delegators, except when they have zero balance
		c.TotalDelegators += int64(acc.ActiveDelegations)
		c.TotalDelegates++

		// count only active delegates going forward
		if !acc.IsActiveDelegate {
			c.InactiveDelegates++
			c.InactiveDelegators += int64(acc.ActiveDelegations)
			continue
		}
		c.ActiveDelegates++
		c.ActiveDelegators += int64(acc.ActiveDelegations)

		switch acc.ActiveDelegations {
		case 0:
			c.SelfDelegates++
		case 1:
			c.SingleDelegates++
		default:
			c.MultiDelegates++
		}

		// only active delegates get rolls
		bal := acc.StakingBalance()
		if bal == 0 {
			c.ZeroDelegates++
		} else if bal >= b.Params.TokensPerRoll {
			c.Rolls += bal / b.Params.TokensPerRoll
			c.RollOwners++
		}
	}

	if b.IsOrphan {
		c.TotalOrphans++
	}

	// TODO
	// s.TotalUsedBytes +=
}

func (c *Chain) Rollback(b *Block) {
	// update identity only
	c.Height = b.Height
	c.Cycle = b.Cycle
}
