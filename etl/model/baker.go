// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
    "encoding/binary"
    "fmt"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/packdb/util"
    "blockwatch.cc/tzgo/tezos"
)

type BakerID uint64

func (id BakerID) Value() uint64 {
    return uint64(id)
}

// Baker is an up-to-date snapshot of the current status of active or inactive bakers.
// For history look at Op and Flow (balance updates), for generic info look
// at Account.
type Baker struct {
    RowId              BakerID       `pack:"I,pk,snappy" json:"row_id"`
    AccountId          AccountID     `pack:"A,snappy"    json:"account_id"`
    Address            tezos.Address `pack:"H,snappy"    json:"address"`
    IsActive           bool          `pack:"v,snappy"    json:"is_active"`
    BakerSince         int64         `pack:"*,snappy"    json:"baker_since"`
    BakerUntil         int64         `pack:"/,snappy"    json:"baker_until"`
    TotalRewardsEarned int64         `pack:"W,snappy"    json:"total_rewards_earned"`
    TotalFeesEarned    int64         `pack:"E,snappy"    json:"total_fees_earned"`
    TotalLost          int64         `pack:"L,snappy"    json:"total_lost"`
    FrozenDeposits     int64         `pack:"z,snappy"    json:"frozen_deposits"`
    FrozenRewards      int64         `pack:"Z,snappy"    json:"frozen_rewards"`
    FrozenFees         int64         `pack:"Y,snappy"    json:"frozen_fees"`
    DelegatedBalance   int64         `pack:"~,snappy"    json:"delegated_balance"`
    DepositsLimit      int64         `pack:"T,snappy"    json:"deposit_limit"`
    TotalDelegations   int64         `pack:">,snappy"    json:"total_delegations"`
    ActiveDelegations  int64         `pack:"a,snappy"    json:"active_delegations"`
    BlocksBaked        int64         `pack:"b,snappy"    json:"blocks_baked"`
    BlocksProposed     int64         `pack:"P,snappy"    json:"blocks_proposed"`
    SlotsEndorsed      int64         `pack:"e,snappy"    json:"slots_endorsed"`
    NBakerOps          int64         `pack:"1,snappy"    json:"n_baker_ops"`
    NProposal          int64         `pack:"2,snappy"    json:"n_proposals"`
    NBallot            int64         `pack:"3,snappy"    json:"n_ballots"`
    NEndorsement       int64         `pack:"4,snappy"    json:"n_endorsements"`
    NPreendorsement    int64         `pack:"5,snappy"    json:"n_preendorsements"`
    NSeedNonce         int64         `pack:"6,snappy"    json:"n_nonce_revelations"`
    N2Baking           int64         `pack:"7,snappy"    json:"n_double_bakings"`
    N2Endorsement      int64         `pack:"8,snappy"    json:"n_double_endorsements"`
    NSetDepositsLimit  int64         `pack:"9,snappy"    json:"n_set_limits"`
    NAccusations       int64         `pack:"0,i32"       json:"n_accusations"`
    GracePeriod        int64         `pack:"G,snappy"    json:"grace_period"`
    Version            uint32        `pack:"N,snappy"    json:"baker_version"`

    Account *Account `pack:"-" json:"-"` // related account
    IsNew   bool     `pack:"-" json:"-"` // first seen this block
    IsDirty bool     `pack:"-" json:"-"` // indicates an update happened
}

// Ensure Baker implements the pack.Item interface.
var _ pack.Item = (*Baker)(nil)

func NewBaker(acc *Account) *Baker {
    return &Baker{
        AccountId:     acc.RowId,
        Address:       acc.Address.Clone(),
        Account:       acc,
        DepositsLimit: -1, // unset
        IsNew:         true,
        IsDirty:       true,
    }
}

func (b Baker) ID() uint64 {
    return uint64(b.RowId)
}

func (b *Baker) SetID(id uint64) {
    b.RowId = BakerID(id)
}

func (b Baker) String() string {
    return b.Address.String()
}

func (b Baker) Balance() int64 {
    return b.FrozenBalance() + b.Account.SpendableBalance
}

func (b Baker) FrozenBalance() int64 {
    return b.FrozenDeposits + b.FrozenFees + b.FrozenRewards
}

func (b Baker) TotalBalance() int64 {
    return b.Account.SpendableBalance + b.FrozenDeposits + b.FrozenFees
}

// own balance plus frozen deposits+fees (NOT REWARDS!) plus
// all delegated balances (this is self-delegation safe)
func (b Baker) StakingBalance() int64 {
    return b.FrozenDeposits + b.FrozenFees + b.Account.SpendableBalance + b.DelegatedBalance
}

// <v12 staking balance capped by capacity
// v12+ locked deposits
func (b Baker) ActiveStake(p *tezos.Params, netRolls int64) int64 {
    if p.Version < 12 {
        return util.Min64(b.StakingBalance(), b.StakingCapacity(p, netRolls))
    }
    deposit_cap := util.Min64(b.DepositsLimit, b.TotalBalance())
    if deposit_cap < 0 {
        deposit_cap = b.TotalBalance()
    }
    return util.Min64(b.StakingBalance(), deposit_cap*100/int64(p.FrozenDepositsPercentage))
}

func (b Baker) Rolls(p *tezos.Params) int64 {
    if p.TokensPerRoll == 0 {
        return 0
    }
    return b.StakingBalance() / p.TokensPerRoll
}

func (b Baker) StakingCapacity(p *tezos.Params, netRolls int64) int64 {
    if p.Version < 12 {
        blockDeposits := p.BlockSecurityDeposit + p.EndorsementSecurityDeposit*int64(p.EndorsersPerBlock)
        netBond := blockDeposits * p.BlocksPerCycle * (p.PreservedCycles + 1)
        netStake := netRolls * p.TokensPerRoll
        // numeric overflow is likely
        return int64(float64(b.TotalBalance()) * float64(netStake) / float64(netBond))
    }

    // 10% of staking balance must be locked
    // deposit = stake * frozen_deposits_percentage / 100
    // max_stake = total_balance / frozen_deposits_percentage * 100
    deposit_cap := util.Min64(b.DepositsLimit, b.TotalBalance())
    if deposit_cap < 0 {
        deposit_cap = b.TotalBalance()
    }
    return deposit_cap * 100 / int64(p.FrozenDepositsPercentage)
}

func (b *Baker) SetVersion(nonce uint64) {
    b.Version = uint32(nonce >> 32)
}

func (b *Baker) GetVersionBytes() []byte {
    var buf [4]byte
    binary.BigEndian.PutUint32(buf[:], b.Version)
    return buf[:]
}

func (b *Baker) Reset() {
    b.RowId = 0
    b.AccountId = 0
    b.Address = tezos.Address{}
    b.IsActive = false
    b.BakerSince = 0
    b.BakerUntil = 0
    b.TotalRewardsEarned = 0
    b.TotalFeesEarned = 0
    b.TotalLost = 0
    b.FrozenDeposits = 0
    b.FrozenRewards = 0
    b.FrozenFees = 0
    b.DelegatedBalance = 0
    b.DepositsLimit = 0
    b.TotalDelegations = 0
    b.ActiveDelegations = 0
    b.BlocksBaked = 0
    b.BlocksProposed = 0
    b.SlotsEndorsed = 0
    b.NBakerOps = 0
    b.NProposal = 0
    b.NBallot = 0
    b.NEndorsement = 0
    b.NPreendorsement = 0
    b.NSeedNonce = 0
    b.N2Baking = 0
    b.N2Endorsement = 0
    b.NSetDepositsLimit = 0
    b.NAccusations = 0
    b.GracePeriod = 0
    b.Version = 0
    b.Account = nil
    b.IsNew = false
    b.IsDirty = false
}

func (b *Baker) UpdateBalanceN(flows []*Flow) error {
    for _, f := range flows {
        if err := b.UpdateBalance(f); err != nil {
            return err
        }
    }
    return nil
}

func (b *Baker) UpdateBalance(f *Flow) error {
    b.IsDirty = true
    if err := b.Account.UpdateBalance(f); err != nil {
        return err
    }

    switch f.Category {
    case FlowCategoryRewards:
        if b.FrozenRewards < f.AmountOut-f.AmountIn {
            log.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
                "outgoing amount %d", b.RowId, b, b.FrozenRewards, f.AmountOut-f.AmountIn)
            // this happens on Ithaca upgrade due to a seed nonce slash bug earlier
            b.FrozenRewards = f.AmountOut - f.AmountIn
            // return fmt.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
            //     "outgoing amount %d", b.RowId, b, b.FrozenRewards, f.AmountOut-f.AmountIn)
        }
        b.TotalRewardsEarned += f.AmountIn - f.AmountOut
        b.FrozenRewards += f.AmountIn - f.AmountOut
        if f.Operation == FlowTypePenalty {
            b.TotalLost += f.AmountOut
        }

    case FlowCategoryDeposits:
        if b.FrozenDeposits < f.AmountOut-f.AmountIn {
            log.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
                "outgoing amount %d", b.RowId, b, b.FrozenDeposits, f.AmountOut-f.AmountIn)
            // return fmt.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
            //     "outgoing amount %d", b.RowId, b, b.FrozenDeposits, f.AmountOut-f.AmountIn)
        }
        b.FrozenDeposits += f.AmountIn - f.AmountOut
        if f.Operation == FlowTypePenalty {
            b.TotalLost += f.AmountOut
        }

    case FlowCategoryFees:
        if b.FrozenFees < f.AmountOut-f.AmountIn {
            log.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
                "outgoing amount %d", b.RowId, b, b.FrozenFees, f.AmountOut-f.AmountIn)
            // this happens on Ithaca upgrade due to a seed nonce slash bug earlier
            b.FrozenFees = f.AmountOut - f.AmountIn
            // return fmt.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
            //     "outgoing amount %d", b.RowId, b, b.FrozenFees, f.AmountOut-f.AmountIn)
        }
        b.TotalFeesEarned += f.AmountIn
        if f.Operation == FlowTypePenalty {
            b.TotalLost += f.AmountOut
        }
        b.FrozenFees += f.AmountIn - f.AmountOut

    case FlowCategoryBalance:
        // post-Ithaca some rewards are paid immediately
        // Note: careful do not double-count frozen deposits/rewards from pre-Ithaca
        // on baking/endorsing/nonce types
        if !f.IsFrozen && !f.IsUnfrozen {
            switch f.Operation {
            case FlowTypeReward:
                // reward can be given or burned
                b.TotalRewardsEarned += f.AmountIn - f.AmountOut
                b.TotalLost += f.AmountOut

            case FlowTypeNonceRevelation:
                // reward can be given or burned
                b.TotalRewardsEarned += f.AmountIn - f.AmountOut
                b.TotalLost += f.AmountOut

            case FlowTypeBaking, FlowTypeBonus:
                // Ithaca+ rewards and fees are paid directly
                if f.IsFee {
                    b.TotalFeesEarned += f.AmountIn
                } else {
                    b.TotalRewardsEarned += f.AmountIn
                }
            }
        }

    case FlowCategoryDelegation:
        if b.DelegatedBalance < f.AmountOut-f.AmountIn {
            return fmt.Errorf("baker.update id %d %s delegated balance %d is smaller than "+
                "outgoing amount %d", b.RowId, b, b.DelegatedBalance, f.AmountOut-f.AmountIn)
        }
        b.DelegatedBalance += f.AmountIn - f.AmountOut
    }
    return nil
}

func (b *Baker) RollbackBalanceN(flows []*Flow) error {
    for _, f := range flows {
        if err := b.RollbackBalance(f); err != nil {
            return err
        }
    }
    return nil
}

func (b *Baker) RollbackBalance(f *Flow) error {
    b.IsDirty = true
    switch f.Category {
    case FlowCategoryRewards:
        // pre-Ithaca only
        if b.FrozenRewards < f.AmountIn-f.AmountOut {
            log.Errorf("baker.update id %d %s frozen rewards %d is smaller than "+
                "reversed incoming amount %d", b.RowId, b, b.FrozenRewards, f.AmountIn-f.AmountOut)
        }
        b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
        b.FrozenRewards -= f.AmountIn - f.AmountOut
        if f.Operation == FlowTypePenalty {
            b.TotalLost -= f.AmountOut
            b.TotalRewardsEarned += f.AmountOut
        }

    case FlowCategoryDeposits:
        // pre/post-Ithaca
        if b.FrozenDeposits < f.AmountIn-f.AmountOut {
            log.Errorf("baker.update id %d %s frozen deposits %d is smaller than "+
                "reversed incoming amount %d", b.RowId, b, b.FrozenDeposits, f.AmountIn-f.AmountOut)
        }
        b.FrozenDeposits -= f.AmountIn - f.AmountOut
        if f.Operation == FlowTypePenalty {
            b.TotalLost -= f.AmountOut
        }

    case FlowCategoryFees:
        // pre-Ithaca only
        if b.FrozenFees < f.AmountIn-f.AmountOut {
            log.Errorf("baker.update id %d %s frozen fees %d is smaller than "+
                "reversed incoming amount %d", b.RowId, b, b.FrozenFees, f.AmountIn-f.AmountOut)
        }
        b.TotalFeesEarned -= f.AmountIn
        if f.Operation == FlowTypePenalty {
            b.TotalLost -= f.AmountOut
        }
        b.FrozenFees -= f.AmountIn - f.AmountOut

    case FlowCategoryBalance:
        // post-Ithaca some rewards are paid immediately
        // Note: careful do not double-count frozen deposits/rewards from pre-Ithaca
        // on baking/endorsing/nonce types
        if !f.IsFrozen {
            switch f.Operation {
            case FlowTypeReward:
                // reward can be given or burned
                b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
                b.TotalLost -= f.AmountOut

            case FlowTypeNonceRevelation:
                // reward can be given or burned
                b.TotalRewardsEarned -= f.AmountIn - f.AmountOut
                b.TotalLost -= f.AmountOut

            case FlowTypeBaking, FlowTypeBonus:
                // Ithaca+ rewards and fees are paid directly
                if f.IsFee {
                    b.TotalFeesEarned -= f.AmountIn
                } else {
                    b.TotalRewardsEarned -= f.AmountIn
                }
            }
        }

    case FlowCategoryDelegation:
        if b.DelegatedBalance < f.AmountIn-f.AmountOut {
            return fmt.Errorf("baker.update id %d %s delegated balance %d is smaller than "+
                "reversed incoming amount %d", b.RowId, b, b.DelegatedBalance, f.AmountIn-f.AmountOut)
        }
        b.DelegatedBalance -= f.AmountIn - f.AmountOut
    }
    if err := b.Account.RollbackBalance(f); err != nil {
        return err
    }
    return nil
}

// init 11 cycles ahead of current cycle
func (b *Baker) InitGracePeriod(cycle int64, params *tezos.Params) {
    b.GracePeriod = cycle + 2*params.PreservedCycles + 1 // (11)
    b.IsDirty = true
}

// keep initial (+11) max grace period, otherwise cycle + 6
func (b *Baker) UpdateGracePeriod(cycle int64, params *tezos.Params) {
    if b.IsActive {
        b.GracePeriod = util.Max64(cycle+params.PreservedCycles+1, b.GracePeriod)
    } else {
        // reset after inactivity
        b.IsActive = true
        b.BakerUntil = 0
        b.InitGracePeriod(cycle, params)
    }
    b.IsDirty = true
}
