// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"time"

	"blockwatch.cc/packdb/pack"
)

const ChainTableKey = "chain"

var (
	// ErrNoChain is an error that indicates a requested entry does
	// not exist in the chain table.
	ErrNoChain = errors.New("chain state not indexed")
)

type Chain struct {
	RowId                uint64    `pack:"I,pk,snappy"       json:"row_id"`                    // unique id
	Height               int64     `pack:"a,i32,snappy"      json:"height"`                    // bc: block height (also for orphans)
	Cycle                int64     `pack:"b,i16,snappy"      json:"cycle"`                     // bc: block cycle (tezos specific)
	Timestamp            time.Time `pack:"c,snappy"          json:"time"`                      // bc: block creation time
	TotalAccounts        int64     `pack:"d,i32,snappy"      json:"total_accounts"`            // default accounts
	TotalContracts       int64     `pack:"e,i32,snappy"      json:"total_contracts"`           // smart contracts (KT1 with code)
	TotalRollups         int64     `pack:"f,i32,snappy"      json:"total_rollups"`             // rollups (any kind)
	TotalOps             int64     `pack:"g,i32,snappy"      json:"total_ops"`                 //
	TotalOpsFailed       int64     `pack:"h,i32,snappy"      json:"total_ops_failed"`          //
	TotalContractOps     int64     `pack:"i,i32,snappy"      json:"total_contract_ops"`        // ops on KT1 contracts
	TotalContractCalls   int64     `pack:"j,i32,snappy"      json:"total_contract_calls"`      // calls from EOA to KT1 contracts with params
	TotalRollupCalls     int64     `pack:"k,i32,snappy"      json:"total_rollup_calls"`        // calls from EOA or KT1 to rollups
	TotalActivations     int64     `pack:"l,i32,snappy"      json:"total_activations"`         // fundraiser accounts activated
	TotalSeedNonces      int64     `pack:"m,i32,snappy"      json:"total_nonce_revelations"`   //
	TotalEndorsements    int64     `pack:"n,i32,snappy"      json:"total_endorsements"`        //
	TotalPreendorsements int64     `pack:"o,i32,snappy"      json:"total_preendorsements"`     //
	TotalDoubleBake      int64     `pack:"p,i32,snappy"      json:"total_double_bakings"`      //
	TotalDoubleEndorse   int64     `pack:"q,i32,snappy"      json:"total_double_endorsements"` //
	TotalDelegations     int64     `pack:"r,i32,snappy"      json:"total_delegations"`         //
	TotalReveals         int64     `pack:"s,i32,snappy"      json:"total_reveals"`             //
	TotalOriginations    int64     `pack:"t,i32,snappy"      json:"total_originations"`        //
	TotalTransactions    int64     `pack:"u,i32,snappy"      json:"total_transactions"`        //
	TotalProposals       int64     `pack:"v,i32,snappy"      json:"total_proposals"`           //
	TotalBallots         int64     `pack:"w,i32,snappy"      json:"total_ballots"`             //
	TotalConstants       int64     `pack:"x,i32,snappy"      json:"total_constants"`           // registered global constants
	TotalSetLimits       int64     `pack:"y,i32,snappy"      json:"total_set_limits"`          // registered global constants
	TotalStorageBytes    int64     `pack:"z,snappy"          json:"total_storage_bytes"`       //
	TotalTicketTransfers int64     `pack:"1,i32,snappy"      json:"total_ticket_transfers"`    // transfer ticket
	FundedAccounts       int64     `pack:"2,i32,snappy"      json:"funded_accounts"`           // total number of accounts qith non-zero balance
	DustAccounts         int64     `pack:"3,i32,snappy"      json:"dust_accounts"`             // accounts with a balance < 1 tez
	GhostAccounts        int64     `pack:"4,i32,snappy"      json:"ghost_accounts"`            // unfunded L2 accounts (who own tokens, but cannot move them)
	UnclaimedAccounts    int64     `pack:"5,i16,snappy"      json:"unclaimed_accounts"`        // fundraiser accounts unclaimed
	TotalDelegators      int64     `pack:"6,i32,snappy"      json:"total_delegators"`          // count of all non-zero balance delegators
	ActiveDelegators     int64     `pack:"7,i32,snappy"      json:"active_delegators"`         // KT1 delegating to active bakers
	InactiveDelegators   int64     `pack:"8,i32,snappy"      json:"inactive_delegators"`       // KT1 delegating to inactive bakers
	DustDelegators       int64     `pack:"9,i32,snappy"      json:"dust_delegators"`           // KT1 delegating to inactive bakers
	TotalBakers          int64     `pack:"0,i16,snappy"      json:"total_bakers"`              // count of all bakers (active and inactive)
	EligibleBakers       int64     `pack:"A,i16,snappy"      json:"eligible_bakers"`           // bakers above minimum
	ActiveBakers         int64     `pack:"B,i16,snappy"      json:"active_bakers"`             // tz* active bakers
	InactiveBakers       int64     `pack:"C,i16,snappy"      json:"inactive_bakers"`           // tz* inactive bakers
	ZeroBakers           int64     `pack:"D,i16,snappy"      json:"zero_bakers"`               // tz* delegate with zero staking balance
	SelfBakers           int64     `pack:"E,i16,snappy"      json:"self_bakers"`               // tz* delegate with no incoming delegation
	SingleBakers         int64     `pack:"F,i16,snappy"      json:"single_bakers"`             // tz* delegate with 1 incoming delegation
	MultiBakers          int64     `pack:"G,i16,snappy"      json:"multi_bakers"`              // tz* delegate with >1 incoming delegations (delegation services)
	TotalStakers         int64     `pack:"H,i32,snappy"      json:"total_stakers"`             // wallets staking
	ActiveStakers        int64     `pack:"J,i32,snappy"      json:"active_stakers"`            // wallets staking with active bakers
	InactiveStakers      int64     `pack:"K,i32,snappy"      json:"inactive_stakers"`          // wallets staking with inactive bakers
}

// Ensure Chain implements the pack.Item interface.
var _ pack.Item = (*Chain)(nil)

func (c *Chain) ID() uint64 {
	return c.RowId
}

func (c *Chain) SetID(id uint64) {
	c.RowId = id
}

func (m Chain) TableKey() string {
	return ChainTableKey
}

func (m Chain) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 15,
		CacheSize:       2,   // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m Chain) IndexOpts(key string) pack.Options {
	return pack.NoOptions
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
	c.TotalStorageBytes += b.StoragePaid
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
		case OpTypeTransferTicket:
			c.TotalTicketTransfers++
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
	c.EligibleBakers = 0
	c.TotalStakers = 0
	c.ActiveStakers = 0
	c.InactiveStakers = 0

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

		// count ghosts
		if acc.Address.IsEOA() {
			if acc.IsNew && !acc.IsFunded {
				// log.Infof("%5d NEW GHOST %s new=%t funded=%t->%t first=%d bal=%d",
				// 	b.Height, acc.Address, acc.IsNew, acc.WasFunded, acc.IsFunded, acc.FirstIn, acc.Balance())
				c.GhostAccounts++
			}
			if !acc.IsNew && !acc.IsActivated && acc.IsFunded && !acc.WasFunded && acc.FirstIn == b.Height {
				c.GhostAccounts--
				// log.Infof("%5d OLD GHOST %s new=%t funded=%t->%t first=%d bal=%d",
				// 	b.Height, acc.Address, acc.IsNew, acc.WasFunded, acc.IsFunded, acc.FirstIn, acc.Balance())
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
		c.TotalDelegators += bkr.ActiveDelegations
		c.TotalStakers += bkr.ActiveStakers
		c.TotalBakers++

		// count only active bakers below
		if !bkr.IsActive {
			c.InactiveBakers++
			c.InactiveDelegators += bkr.ActiveDelegations
			c.InactiveStakers += bkr.ActiveStakers
			continue
		}
		c.ActiveBakers++
		c.ActiveDelegators += bkr.ActiveDelegations
		c.ActiveStakers += bkr.ActiveStakers

		switch bkr.ActiveDelegations + bkr.ActiveStakers {
		case 0:
			c.SelfBakers++
		case 1:
			c.SingleBakers++
		default:
			c.MultiBakers++
		}

		// only active bakers with minimal stake get rights
		bal := bkr.StakingBalance()
		if bal == 0 {
			c.ZeroBakers++
		} else {
			// if we're at the last block of a cycle we need to adjust down the
			// balance by unfrozen rewards since Tezos snapshots stake before unfreeze
			if b.TZ.IsCycleEnd() {
				// if b.Params.IsCycleEnd(b.Height) {
				// find the reward unfreeze flow for this baker
				for _, flow := range b.Flows {
					if flow.AccountId != acc.RowId {
						continue
					}
					if flow.Kind != FlowKindRewards {
						continue
					}
					if flow.Type != FlowTypeInternal {
						continue
					}
					bal -= flow.AmountOut
					break
				}
			}
			if bal >= b.Params.MinimalStake {
				c.EligibleBakers++
			}
		}
	}
}

func (c *Chain) Rollback(b *Block) {
	// update identity only
	c.Height = b.Height
	c.Cycle = b.Cycle
}
