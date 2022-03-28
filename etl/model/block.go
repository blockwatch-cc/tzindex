// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

var (
	blockPool *sync.Pool
)

func init() {
	blockPool = &sync.Pool{
		New: func() interface{} { return new(Block) },
	}
}

// Block contains extracted and translated data describing a Tezos block. Block also
// contains raw data and translations for related types such as operations, chain totals
// rights, etc. that is used by indexers and reporters
type Block struct {
	RowId            uint64                 `pack:"I,pk,snappy" json:"row_id"`
	ParentId         uint64                 `pack:"P,snappy"    json:"parent_id"`
	Hash             tezos.BlockHash        `pack:"H,snappy"    json:"hash"`
	Height           int64                  `pack:"h,snappy"    json:"height"`
	Cycle            int64                  `pack:"c,snappy"    json:"cycle"`
	IsCycleSnapshot  bool                   `pack:"o,snappy"    json:"is_cycle_snapshot"`
	Timestamp        time.Time              `pack:"T,snappy"    json:"time"`
	Solvetime        int                    `pack:"d,snappy"    json:"solvetime"`
	Version          int                    `pack:"v,snappy"    json:"version"`
	Round            int                    `pack:"p,snappy"    json:"round"`
	Nonce            uint64                 `pack:"n,snappy"    json:"nonce"`
	VotingPeriodKind tezos.VotingPeriodKind `pack:"k,snappy"    json:"voting_period_kind"`
	BakerId          AccountID              `pack:"B,snappy"    json:"baker_id"`
	ProposerId       AccountID              `pack:"X,snappy"    json:"proposer_id"`
	NSlotsEndorsed   int                    `pack:"e,snappy"    json:"n_endorsed_slots"`
	NOpsApplied      int                    `pack:"1,snappy"    json:"n_ops_applied"`
	NOpsFailed       int                    `pack:"2,snappy"    json:"n_ops_failed"`
	NContractCalls   int                    `pack:"3,snappy"    json:"n_calls"`
	NEvents          int                    `pack:"4,snappy"    json:"n_events"`
	Volume           int64                  `pack:"V,snappy"    json:"volume"`
	Fee              int64                  `pack:"F,snappy"    json:"fee"`
	Reward           int64                  `pack:"R,snappy"    json:"reward"`
	Deposit          int64                  `pack:"D,snappy"    json:"deposit"`
	ActivatedSupply  int64                  `pack:"S,snappy"    json:"activated_supply"`
	BurnedSupply     int64                  `pack:"b,snappy"    json:"burned_supply"`
	MintedSupply     int64                  `pack:"m,snappy"    json:"minted_supply"`
	SeenAccounts     int                    `pack:"a,snappy"    json:"n_accounts"`
	NewAccounts      int                    `pack:"A,snappy"    json:"n_new_accounts"`
	NewContracts     int                    `pack:"C,snappy"    json:"n_new_contracts"`
	ClearedAccounts  int                    `pack:"E,snappy"    json:"n_cleared_accounts"`
	FundedAccounts   int                    `pack:"J,snappy"    json:"n_funded_accounts"`
	GasLimit         int64                  `pack:"L,snappy"    json:"gas_limit"`
	GasUsed          int64                  `pack:"G,snappy"    json:"gas_used"`
	StoragePaid      int64                  `pack:"Y,snappy"    json:"storage_paid"`
	LbEscapeVote     bool                   `pack:"O,snappy"    json:"lb_esc_vote"`
	LbEscapeEma      int64                  `pack:"M,snappy"    json:"lb_esc_ema"`

	// other tz or extracted/translated data for processing
	TZ           *rpc.Bundle   `pack:"-" json:"-"`
	Params       *tezos.Params `pack:"-" json:"-"`
	Chain        *Chain        `pack:"-" json:"-"`
	Supply       *Supply       `pack:"-" json:"-"`
	Ops          []*Op         `pack:"-" json:"-"`
	Flows        []*Flow       `pack:"-" json:"-"`
	Baker        *Baker        `pack:"-" json:"-"`
	Proposer     *Baker        `pack:"-" json:"-"`
	Parent       *Block        `pack:"-" json:"-"`
	HasProposals bool          `pack:"-" json:"-"`
	HasBallots   bool          `pack:"-" json:"-"`
	HasSeeds     bool          `pack:"-" json:"-"`
}

// Ensure Block implements the pack.Item interface.
var _ pack.Item = (*Block)(nil)

func (b Block) ID() uint64 {
	return b.RowId
}

func (b *Block) SetID(id uint64) {
	b.RowId = id
}

// be compatible with time series interface
func (b Block) Time() time.Time {
	return b.Timestamp
}

func AllocBlock() *Block {
	return blockPool.Get().(*Block)
}

func NewBlock(tz *rpc.Bundle, parent *Block) (*Block, error) {
	b := AllocBlock()
	if tz == nil || tz.Block == nil {
		return nil, fmt.Errorf("block init: missing rpc block")
	}

	b.TZ = tz
	b.Params = tz.Params
	b.Chain = &Chain{}
	b.Supply = &Supply{}
	if b.Ops == nil {
		b.Ops = make([]*Op, 0)
	}
	if b.Flows == nil {
		b.Flows = make([]*Flow, 0)
	}

	// init block model from rpc block and local data (expecing defaults for unset fields)
	b.Height = tz.Block.GetLevel()
	b.Cycle = tz.Block.GetCycle()
	b.Timestamp = tz.Block.GetTimestamp()
	b.Hash = tz.Block.Hash
	b.Version = tz.Block.GetVersion()

	head := tz.Block.Header
	b.Round = head.Priority + head.PayloadRound
	b.LbEscapeVote = head.LiquidityBakingEscapeVote
	b.LbEscapeEma = tz.Block.Metadata.LiquidityBakingEscapeEma
	if len(head.ProofOfWorkNonce) >= 8 {
		b.Nonce = binary.BigEndian.Uint64(head.ProofOfWorkNonce)
	}

	// adjust protocol version number for genesis and bootstrap blocks
	if b.Height <= 1 {
		b.Version--
	}

	// be robust against missing voting period (like on block 0 and 1)
	b.VotingPeriodKind = tz.Block.GetVotingPeriodKind()
	if !b.VotingPeriodKind.IsValid() {
		if parent != nil {
			b.VotingPeriodKind = parent.VotingPeriodKind
		} else {
			b.VotingPeriodKind = tezos.VotingPeriodProposal
		}
	}

	// parent info
	if parent != nil {
		b.ParentId = parent.RowId
		b.Parent = parent
		b.Solvetime = util.Max(0, int(b.Timestamp.Sub(parent.Timestamp)/time.Second))
		if parent.Chain != nil {
			*b.Chain = *parent.Chain // copy
		}
		if parent.Supply != nil {
			*b.Supply = *parent.Supply // copy
		}
	}

	return b, nil
}

func (b Block) Clone() *Block {
	clone := b
	clone.TZ = nil
	clone.Params = nil
	clone.Chain = nil
	clone.Supply = nil
	clone.Ops = nil
	clone.Flows = nil
	clone.Baker = nil
	clone.Proposer = nil
	clone.Parent = nil
	return &clone
}

func (b *Block) FetchRPC(ctx context.Context, c *rpc.Client) error {
	if !b.Hash.IsValid() {
		return fmt.Errorf("invalid block hash on block id %d", b.RowId)
	}
	var err error
	if b.TZ == nil {
		b.TZ = &rpc.Bundle{}
	}
	if b.TZ.Block == nil {
		b.TZ.Block, err = c.GetBlock(ctx, b.Hash)
		if err != nil {
			return err
		}
	}
	if b.Params == nil {
		// fetch params from chain
		if b.Height > 0 {
			cons, err := c.GetConstants(ctx, rpc.BlockLevel(b.Height))
			if err != nil {
				return fmt.Errorf("block init: %w", err)
			}
			b.Params = cons.MapToChainParams()
		} else {
			b.Params = tezos.NewParams()
		}
		b.Params = b.Params.
			ForNetwork(b.TZ.Block.ChainId).
			ForProtocol(b.TZ.Block.Metadata.Protocol)
		b.Params.Deployment = b.TZ.Block.Header.Proto
	}
	b.TZ.Params = b.Params
	return nil
}

func (b *Block) IsProtocolUpgrade() bool {
	if b.Parent == nil || b.Parent.TZ == nil || b.TZ == nil {
		return false
	}
	return !b.Parent.TZ.Block.Metadata.Protocol.Equal(b.TZ.Block.Metadata.Protocol)
}

func (b *Block) GetOpId(opn, opc, opi int) (OpID, bool) {
	if opn < 0 {
		return 0, false
	}
	for _, o := range b.Ops {
		// ops are ordered
		if o.OpN < opn || o.OpC < opc || o.OpI < opi {
			continue
		}
		return o.RowId, true
	}
	return 0, false
}

func (b *Block) NextN() int {
	n := 0
	if l := len(b.Ops); l > 0 {
		n = b.Ops[l-1].OpN + 1
	}
	return n
}

// used for token age in flows and ops
func (b *Block) Age(height int64) int64 {
	// instead of real time we use block offsets and the target time
	// between blocks as time diff
	return (b.Height - height) * int64(b.Params.BlockTime()/time.Second)
}

func (b *Block) Free() {
	b.Reset()
	blockPool.Put(b)
}

func (b *Block) Clean() {
	if b.Ops != nil {
		for _, op := range b.Ops {
			op.Free()
		}
		b.Ops = b.Ops[:0]
	}
	if b.Flows != nil {
		for _, f := range b.Flows {
			f.Free()
		}
		b.Flows = b.Flows[:0]
	}
	if b.TZ != nil {
		b.TZ.Block.Operations = b.TZ.Block.Operations[:0]
	}
}

func (b *Block) Reset() {
	b.RowId = 0
	b.ParentId = 0
	b.Hash = tezos.BlockHash{Hash: tezos.InvalidHash}
	b.Height = 0
	b.Cycle = 0
	b.IsCycleSnapshot = false
	b.Timestamp = time.Time{}
	b.Solvetime = 0
	b.Version = 0
	b.Round = 0
	b.Nonce = 0
	b.VotingPeriodKind = 0
	b.BakerId = 0
	b.ProposerId = 0
	b.NSlotsEndorsed = 0
	b.NOpsApplied = 0
	b.NOpsFailed = 0
	b.NContractCalls = 0
	b.NEvents = 0
	b.Volume = 0
	b.Fee = 0
	b.Reward = 0
	b.Deposit = 0
	b.ActivatedSupply = 0
	b.BurnedSupply = 0
	b.MintedSupply = 0
	b.SeenAccounts = 0
	b.NewAccounts = 0
	b.NewContracts = 0
	b.ClearedAccounts = 0
	b.FundedAccounts = 0
	b.GasLimit = 0
	b.GasUsed = 0
	b.StoragePaid = 0
	b.LbEscapeVote = false
	b.LbEscapeEma = 0
	b.TZ = nil
	b.Params = nil
	b.Chain = nil
	b.Supply = nil
	b.Baker = nil
	b.Proposer = nil
	b.Parent = nil
	if b.Ops != nil {
		for _, o := range b.Ops {
			o.Free()
		}
		b.Ops = b.Ops[:0]
	}
	if b.Flows != nil {
		for _, f := range b.Flows {
			f.Free()
		}
		b.Flows = b.Flows[:0]
	}
	b.HasProposals = false
	b.HasBallots = false
	b.HasSeeds = false
}

func (b *Block) Update(accounts map[AccountID]*Account, bakers map[AccountID]*Baker) {
	// initial state
	b.NOpsApplied = 0
	b.NOpsFailed = 0
	b.NEvents = 0
	b.Volume = 0
	b.Fee = 0
	b.Reward = 0
	b.Deposit = 0
	b.ActivatedSupply = 0
	b.BurnedSupply = 0
	b.MintedSupply = 0
	b.SeenAccounts = 0
	b.NewAccounts = 0
	b.NewContracts = 0
	b.ClearedAccounts = 0
	b.FundedAccounts = 0
	b.GasLimit = 0
	b.GasUsed = 0
	b.StoragePaid = 0

	var endorsedSlots int

	for _, op := range b.Ops {
		b.GasLimit += op.GasLimit
		b.GasUsed += op.GasUsed
		b.StoragePaid += op.StoragePaid

		if op.IsSuccess {
			if op.IsEvent {
				b.NEvents++
			} else {
				b.NOpsApplied++
			}
		} else {
			b.NOpsFailed++
		}

		switch op.Type {
		case OpTypeBake:
			// only bake ops count against block deposit and reward
			b.MintedSupply += op.Reward
			b.Reward += op.Reward
			b.Deposit += op.Deposit

		case OpTypeBonus:
			// post-Ithaca extra bonus counts also against block reward
			b.MintedSupply += op.Reward
			b.Reward += op.Reward

		case OpTypeReward:
			// post-Ithaca endorsing rewards are minted and directly paid
			// at end of cycle unless participation was too low
			b.MintedSupply += op.Reward
			b.BurnedSupply += op.Burned

		case OpTypeEndorsement:
			// pre-Ithace endorsements pay deposit and mint frozen rewards
			endorsedSlots += op.Raw.Meta().EndorsementPower()
			b.MintedSupply += op.Reward

		case OpTypeNonceRevelation:
			b.HasSeeds = true
			b.MintedSupply += op.Reward

		case OpTypeAirdrop, OpTypeInvoice, OpTypeSubsidy:
			b.MintedSupply += op.Reward

		case OpTypeActivation:
			if op.IsSuccess {
				b.ActivatedSupply += op.Volume
				b.Volume += op.Volume
			}
		case OpTypeDoubleBaking, OpTypeDoubleEndorsement, OpTypeDoublePreendorsement:
			b.BurnedSupply += op.Burned

		case OpTypeSeedSlash:
			// pre-Ithaca seed slash burns from already minted frozen rewards
			// and collected fees
			b.BurnedSupply += op.Burned

			// post-Ithaca seed slash mints and directly burns
			if b.Params.Version >= 12 {
				b.MintedSupply += op.Reward
			}

		case OpTypeTransaction:
			b.Fee += op.Fee
			b.BurnedSupply += op.Burned
			b.MintedSupply += op.Reward // LB subsidy
			if op.IsSuccess && !op.IsEvent {
				// txo, _ := b.GetRpcOp(op.OpL, op.OpP, op.OpC)
				tx, _ := op.Raw.(*rpc.Transaction)
				if tezos.ZeroAddress.Equal(tx.Destination) {
					b.BurnedSupply += op.Volume
				} else {
					b.Volume += op.Volume
				}
				// only count calls with params
				if tx.Source.IsEOA() && tx.Destination.IsContract() && len(op.Parameters) > 0 {
					b.NContractCalls++
				}
			}

		case OpTypeOrigination:
			b.Fee += op.Fee
			b.BurnedSupply += op.Burned
			if op.IsSuccess {
				b.Volume += op.Volume
			}

		case OpTypeDelegation, OpTypeReveal, OpTypeDepositsLimit:
			b.Fee += op.Fee

		case OpTypeRegisterConstant:
			b.Fee += op.Fee
			b.BurnedSupply += op.Burned
		case OpTypeProposal:
			b.HasProposals = true
		case OpTypeBallot:
			b.HasBallots = true
		}
	}

	if b.Parent != nil {
		b.Parent.NSlotsEndorsed = endorsedSlots
	}

	// count account changes
	for _, acc := range accounts {
		if acc.IsNew && acc.IsDirty {
			if acc.IsBaker {
				// see below
				continue
			}
			if acc.IsContract {
				b.NewContracts++
			} else {
				b.NewAccounts++
			}
		}
		if acc.LastSeen == b.Height {
			b.SeenAccounts++
			if !acc.IsFunded {
				if acc.WasFunded {
					b.ClearedAccounts++
				}
			} else {
				if !acc.WasFunded {
					b.FundedAccounts++
				}
			}
		}
	}

	// handle (new/updated) bakers separate since they are kept separate
	for _, bkr := range bakers {
		acc := bkr.Account
		if acc.IsNew {
			b.NewAccounts++
		}
		if acc.LastSeen == b.Height {
			b.SeenAccounts++
			if !acc.IsFunded {
				if acc.WasFunded {
					b.ClearedAccounts++
				}
			} else {
				if !acc.WasFunded {
					b.FundedAccounts++
				}
			}
		}
	}
}

func (b *Block) Rollback(accounts map[AccountID]*Account, bakers map[AccountID]*Baker) {
	// block will be deleted, nothing to do
}
