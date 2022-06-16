// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"time"

	"blockwatch.cc/packdb/pack"
)

type Chain struct {
	RowId                uint64    `pack:"I,pk,snappy"   json:"row_id"`                    // unique id
	Height               int64     `pack:"h,snappy"      json:"height"`                    // bc: block height (also for orphans)
	Cycle                int64     `pack:"c,snappy"      json:"cycle"`                     // bc: block cycle (tezos specific)
	Timestamp            time.Time `pack:"T,snappy"      json:"time"`                      // bc: block creation time
	TotalAccounts        int64     `pack:"A,snappy"      json:"total_accounts"`            // default accounts
	TotalContracts       int64     `pack:"C,snappy"      json:"total_contracts"`           // smart contracts (KT1 with code)
	TotalRollups         int64     `pack:"U,snappy"      json:"total_rollups"`             // rollups (any kind)
	TotalOps             int64     `pack:"O,snappy"      json:"total_ops"`                 //
	TotalOpsFailed       int64     `pack:"F,snappy"      json:"total_ops_failed"`          //
	TotalContractOps     int64     `pack:"G,snappy"      json:"total_contract_ops"`        // ops on KT1 contracts
	TotalContractCalls   int64     `pack:"a,snappy"      json:"total_contract_calls"`      // calls from EOA to KT1 contracts with params
	TotalRollupCalls     int64     `pack:"l,snappy"      json:"total_rollup_calls"`        // calls from EOA or KT1 to rollups
	TotalActivations     int64     `pack:"t,snappy"      json:"total_activations"`         // fundraiser accounts activated
	TotalSeedNonces      int64     `pack:"N,snappy"      json:"total_nonce_revelations"`   //
	TotalEndorsements    int64     `pack:"E,snappy"      json:"total_endorsements"`        //
	TotalPreendorsements int64     `pack:"P,snappy"      json:"total_preendorsements"`     //
	TotalDoubleBake      int64     `pack:"X,snappy"      json:"total_double_bakings"`      //
	TotalDoubleEndorse   int64     `pack:"Y,snappy"      json:"total_double_endorsements"` //
	TotalDelegations     int64     `pack:"D,snappy"      json:"total_delegations"`         //
	TotalReveals         int64     `pack:"R,snappy"      json:"total_reveals"`             //
	TotalOriginations    int64     `pack:"g,snappy"      json:"total_originations"`        //
	TotalTransactions    int64     `pack:"x,snappy"      json:"total_transactions"`        //
	TotalProposals       int64     `pack:"p,snappy"      json:"total_proposals"`           //
	TotalBallots         int64     `pack:"b,snappy"      json:"total_ballots"`             //
	TotalConstants       int64     `pack:"n,snappy"      json:"total_constants"`           // registered global constants
	TotalSetLimits       int64     `pack:"L,snappy"      json:"total_set_limits"`          // registered global constants
	TotalStorageBytes    int64     `pack:"S,snappy"      json:"total_storage_bytes"`       //
	FundedAccounts       int64     `pack:"f,snappy"      json:"funded_accounts"`           // total number of accounts qith non-zero balance
	DustAccounts         int64     `pack:"d,snappy"      json:"dust_accounts"`             // accounts with a balance < 1 tez
	UnclaimedAccounts    int64     `pack:"u,snappy"      json:"unclaimed_accounts"`        // fundraiser accounts unclaimed
	TotalDelegators      int64     `pack:"2,snappy"      json:"total_delegators"`          // count of all non-zero balance delegators
	ActiveDelegators     int64     `pack:"3,snappy"      json:"active_delegators"`         // KT1 delegating to active delegates
	InactiveDelegators   int64     `pack:"4,snappy"      json:"inactive_delegators"`       // KT1 delegating to inactive delegates
	DustDelegators       int64     `pack:"8,snappy"      json:"dust_delegators"`           // KT1 delegating to inactive delegates
	TotalBakers          int64     `pack:"5,snappy"      json:"total_bakers"`              // count of all delegates (active and inactive)
	ActiveBakers         int64     `pack:"6,snappy"      json:"active_bakers"`             // tz* active delegates
	InactiveBakers       int64     `pack:"7,snappy"      json:"inactive_bakers"`           // tz* inactive delegates
	ZeroBakers           int64     `pack:"z,snappy"      json:"zero_bakers"`               // tz* delegate with zero staking balance
	SelfBakers           int64     `pack:"s,snappy"      json:"self_bakers"`               // tz* delegate with no incoming delegation
	SingleBakers         int64     `pack:"1,snappy"      json:"single_bakers"`             // tz* delegate with 1 incoming delegation
	MultiBakers          int64     `pack:"m,snappy"      json:"multi_bakers"`              // tz* delegate with >1 incoming delegations (delegation services)
	Rolls                int64     `pack:"r,snappy"      json:"rolls"`                     // total sum of rolls (delegated_balance/10,000)
	RollOwners           int64     `pack:"o,snappy"      json:"roll_owners"`               // distinct delegates
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

func (c *Chain) Update(b *Block, accounts map[AccountID]*Account, bakers map[AccountID]*Baker) {
	c.RowId = 0 // force allocating new id
	c.Height = b.Height
	c.Cycle = b.Cycle
	c.Timestamp = b.Timestamp
	c.TotalAccounts += int64(b.NewAccounts)
	c.TotalContracts += int64(b.NewContracts)
	c.TotalOps += int64(b.NOpsApplied)
	c.TotalOpsFailed += int64(b.NOpsFailed)
	c.TotalContractCalls += int64(b.NContractCalls)
	c.TotalRollupCalls += int64(b.NRollupCalls)
	c.TotalStorageBytes += int64(b.StoragePaid)
	c.FundedAccounts += int64(b.FundedAccounts - b.ClearedAccounts)

	for _, op := range b.Ops {
		if !op.IsSuccess {
			continue
		}
		if op.IsContract {
			c.TotalContractOps++
		}
		switch op.Type {
		case OpTypeActivation:
			c.TotalActivations++
			c.UnclaimedAccounts--
		case OpTypeNonceRevelation:
			c.TotalSeedNonces++
		case OpTypeEndorsement:
			c.TotalEndorsements++
		case OpTypePreendorsement:
			c.TotalPreendorsements++
		case OpTypeDoubleBaking:
			c.TotalDoubleBake++
		case OpTypeDoubleEndorsement, OpTypeDoublePreendorsement:
			c.TotalDoubleEndorse++
		case OpTypeDelegation:
			c.TotalDelegations++
		case OpTypeReveal:
			c.TotalReveals++
		case OpTypeOrigination:
			c.TotalOriginations++
		case OpTypeTransaction:
			c.TotalTransactions++
		case OpTypeProposal:
			c.TotalProposals++
		case OpTypeBallot:
			c.TotalBallots++
		case OpTypeRegisterConstant:
			c.TotalConstants++
		case OpTypeDepositsLimit:
			c.TotalSetLimits++
		case OpTypeRollupOrigination:
			c.TotalRollups++
		}
	}

	// from bakers
	c.TotalDelegators = 0
	c.ActiveDelegators = 0
	c.InactiveDelegators = 0
	c.TotalBakers = 0
	c.ActiveBakers = 0
	c.InactiveBakers = 0
	c.ZeroBakers = 0
	c.SelfBakers = 0
	c.SingleBakers = 0
	c.MultiBakers = 0
	c.Rolls = 0
	c.RollOwners = 0

	for _, acc := range accounts {
		// sanity checks
		if acc.IsBaker {
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

	for _, bkr := range bakers {
		acc := bkr.Account
		// sanity checks
		if !acc.IsBaker {
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
		c.TotalDelegators += int64(bkr.ActiveDelegations)
		c.TotalBakers++

		// count only active bakers below
		if !bkr.IsActive {
			c.InactiveBakers++
			c.InactiveDelegators += int64(bkr.ActiveDelegations)
			continue
		}
		c.ActiveBakers++
		c.ActiveDelegators += int64(bkr.ActiveDelegations)

		switch bkr.ActiveDelegations {
		case 0:
			c.SelfBakers++
		case 1:
			c.SingleBakers++
		default:
			c.MultiBakers++
		}

		// only active delegates get rolls
		bal := bkr.StakingBalance()
		if bal == 0 {
			c.ZeroBakers++
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
