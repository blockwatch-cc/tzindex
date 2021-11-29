// Copyright (c) 2020-2021 Blockwatch Data Inc.
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
	TotalContracts     int64     `pack:"C,snappy"      json:"total_contracts"`                    // smart contracts (KT1 with code)
	TotalOps           int64     `pack:"O,snappy"      json:"total_ops"`                          //
	TotalContractOps   int64     `pack:"G,snappy"      json:"total_contract_ops"`                 // ops on KT1 contracts
	TotalContractCalls int64     `pack:"a,snappy"      json:"total_contract_calls"`               // calls from EOA to KT1 contracts with params
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
	TotalConstants     int64     `pack:"n,snappy"      json:"total_constants"`                    // registered global constants
	TotalStorageBytes  int64     `pack:"S,snappy"      json:"total_storage_bytes"`                //
	TotalPaidBytes     int64     `pack:"P,snappy"      json:"total_paid_bytes"`                   // storage paid for KT1 and contract store
	FundedAccounts     int64     `pack:"f,snappy"      json:"funded_accounts"`                    // total number of accounts qith non-zero balance
	DustAccounts       int64     `pack:"d,snappy"      json:"dust_accounts"`                      // accounts with a balance < 1 tez
	UnclaimedAccounts  int64     `pack:"u,snappy"      json:"unclaimed_accounts"`                 // fundraiser accounts unclaimed
	TotalDelegators    int64     `pack:"2,snappy"      json:"total_delegators"`                   // count of all non-zero balance delegators
	ActiveDelegators   int64     `pack:"3,snappy"      json:"active_delegators"`                  // KT1 delegating to active delegates
	InactiveDelegators int64     `pack:"4,snappy"      json:"inactive_delegators"`                // KT1 delegating to inactive delegates
	DustDelegators     int64     `pack:"8,snappy"      json:"dust_delegators"`                    // KT1 delegating to inactive delegates
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

// be compatible with time series interface
func (c Chain) Time() time.Time {
	return c.Timestamp
}

func (c *Chain) Update(b *Block, accounts, delegates map[AccountID]*Account) {
	c.RowId = 0 // force allocating new id
	c.Height = b.Height
	c.Cycle = b.Cycle
	c.Timestamp = b.Timestamp
	c.TotalAccounts += int64(b.NewAccounts)
	c.TotalContracts += int64(b.NewContracts)
	c.TotalOps += int64(b.NOps)
	c.TotalContractOps += int64(b.NOpsContract)
	c.TotalContractCalls += int64(b.NContractCalls)
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
	c.TotalConstants += int64(b.NRegister)
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

	for _, acc := range accounts {
		// sanity checks
		if acc.IsDelegate {
			continue
		}

		// update dust metrics
		if acc.WasDust != acc.IsDust() {
			if acc.WasDust {
				c.DustAccounts--
				if acc.IsDelegated {
					c.DustDelegators--
				}
			} else {
				c.DustAccounts++
				if acc.IsDelegated {
					c.DustDelegators++
				}
			}
		}
	}

	for _, acc := range delegates {
		// sanity checks
		if !acc.IsDelegate {
			continue
		}

		// update dust metrics
		if acc.WasDust != acc.IsDust() {
			if acc.WasDust {
				c.DustAccounts--
			} else {
				c.DustAccounts++
			}
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
		} else {
			// if we're at the last block of a cycle we need to adjust down the
			// balance by unfrozen rewards since Tezos snapshots rolls before unfreeze
			if b.Params.IsCycleEnd(b.Height) {
				// find the reward unfreeze flow for this baker
				for _, flow := range b.Flows {
					if flow.AccountId != acc.RowId {
						continue
					}
					if flow.Category != FlowCategoryRewards {
						continue
					}
					if flow.Operation != FlowTypeInternal {
						continue
					}
					bal -= flow.AmountOut
					break
				}
			}
			if bal >= b.Params.TokensPerRoll {
				c.Rolls += bal / b.Params.TokensPerRoll
				c.RollOwners++
			}
		}
	}
}

func (c *Chain) Rollback(b *Block) {
	// update identity only
	c.Height = b.Height
	c.Cycle = b.Cycle
}
