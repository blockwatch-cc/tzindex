// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

const BlockTableKey = "block"

var (
	blockPool = &sync.Pool{
		New: func() interface{} { return new(Block) },
	}

	// ErrNoBlock is an error that indicates a requested entry does
	// not exist in the block bucket.
	ErrNoBlock = errors.New("block not indexed")

	// ErrInvalidBlockHeight
	ErrInvalidBlockHeight = errors.New("invalid block height")

	// ErrInvalidBlockHash
	ErrInvalidBlockHash = errors.New("invalid block hash")
)

// Block contains extracted and translated data describing a Tezos block. Block also
// contains raw data and translations for related types such as operations, chain totals
// rights, etc. that is used by indexers and reporters
type Block struct {
	RowId                  uint64                 `pack:"I,pk"             json:"row_id"`
	ParentId               uint64                 `pack:"P"                json:"parent_id"`
	Hash                   tezos.BlockHash        `pack:"H,snappy,bloom=3" json:"hash"`
	Height                 int64                  `pack:"h,i32"            json:"height"`
	Cycle                  int64                  `pack:"c,i16"            json:"cycle"`
	IsCycleSnapshot        bool                   `pack:"o,snappy"         json:"is_cycle_snapshot"`
	Timestamp              time.Time              `pack:"T"                json:"time"`
	Solvetime              int                    `pack:"d,i16"            json:"solvetime"`
	Version                int                    `pack:"v,i8"             json:"version"`
	Round                  int                    `pack:"p,i8"             json:"round"`
	Nonce                  uint64                 `pack:"n,snappy"         json:"nonce"`
	VotingPeriodKind       tezos.VotingPeriodKind `pack:"k,u8"             json:"voting_period_kind"`
	BakerId                AccountID              `pack:"B,u32"            json:"baker_id"`
	ProposerId             AccountID              `pack:"X,u32"            json:"proposer_id"`
	NSlotsEndorsed         int                    `pack:"e,i16"            json:"n_endorsed_slots"`
	NOpsApplied            int                    `pack:"1,i16"            json:"n_ops_applied"`
	NOpsFailed             int                    `pack:"2,i16"            json:"n_ops_failed"`
	NContractCalls         int                    `pack:"3,i16"            json:"n_calls"`
	NRollupCalls           int                    `pack:"6,i16"            json:"n_rollup_calls"`
	NEvents                int                    `pack:"4,i16"            json:"n_events"`
	NTx                    int                    `pack:"5,i16"            json:"n_tx"`
	NTickets               int                    `pack:"7,i16"            json:"n_tickets"`
	Volume                 int64                  `pack:"V"                json:"volume"`
	Fee                    int64                  `pack:"F"                json:"fee"`
	Reward                 int64                  `pack:"R"                json:"reward"`
	Deposit                int64                  `pack:"D"                json:"deposit"`
	ActivatedSupply        int64                  `pack:"S"                json:"activated_supply"`
	BurnedSupply           int64                  `pack:"b"                json:"burned_supply"`
	MintedSupply           int64                  `pack:"m"                json:"minted_supply"`
	SeenAccounts           int                    `pack:"a,i16"            json:"n_accounts"`
	NewAccounts            int                    `pack:"A,i16"            json:"n_new_accounts"`
	NewContracts           int                    `pack:"C,i16"            json:"n_new_contracts"`
	ClearedAccounts        int                    `pack:"E,i16"            json:"n_cleared_accounts"`
	FundedAccounts         int                    `pack:"J,i16"            json:"n_funded_accounts"`
	GasLimit               int64                  `pack:"L,i32"            json:"gas_limit"`
	GasUsed                int64                  `pack:"G,i32"            json:"gas_used"`
	StoragePaid            int64                  `pack:"Y,i32"            json:"storage_paid"`
	LbVote                 tezos.FeatureVote      `pack:"O,snappy"         json:"lb_vote"`
	LbEma                  int64                  `pack:"M,i32"            json:"lb_ema"`
	AiVote                 tezos.FeatureVote      `pack:"z,snappy"         json:"ai_vote"`
	AiEma                  int64                  `pack:"i,i32"            json:"ai_ema"`
	ProposerConsensusKeyId AccountID              `pack:"x"                json:"proposer_consensus_key_id"`
	BakerConsensusKeyId    AccountID              `pack:"y"                json:"baker_consensus_key_id"`

	// other tz or extracted/translated data for processing
	TZ               *rpc.Bundle `pack:"-" json:"-"`
	Params           *rpc.Params `pack:"-" json:"-"`
	Chain            *Chain      `pack:"-" json:"-"`
	Supply           *Supply     `pack:"-" json:"-"`
	Ops              []*Op       `pack:"-" json:"-"`
	Flows            []*Flow     `pack:"-" json:"-"`
	Baker            *Baker      `pack:"-" json:"-"`
	Proposer         *Baker      `pack:"-" json:"-"`
	Parent           *Block      `pack:"-" json:"-"`
	HasProposals     bool        `pack:"-" json:"-"`
	HasBallots       bool        `pack:"-" json:"-"`
	HasSeeds         bool        `pack:"-" json:"-"`
	OfflineBaker     AccountID   `pack:"-" json:"-"`
	OfflineEndorsers []AccountID `pack:"-" json:"-"`

	// annotated ops
	// BakeOps             BakingOpList           `pack:"-" json:"-"`
	// EndorseOps          EndorsementOpList      `pack:"-" json:"-"`
	// PreendorseOps       PreendorsementOpList   `pack:"-" json:"-"`
	// BallotOps           BallotOpList           `pack:"-" json:"-"`
	// ProposalOps         ProposalOpList         `pack:"-" json:"-"`
	// ActivationOps       ActivationOpList       `pack:"-" json:"-"`
	// DoubleBakeOps       DoubleBakingOpList     `pack:"-" json:"-"`
	// DoubleEndorsingOps  DoubleEndorsingOpList  `pack:"-" json:"-"`
	// NonceRevelationOps  NonceRevelationOpList  `pack:"-" json:"-"`
	// RevealOps           RevealOpList           `pack:"-" json:"-"`
	// DelegationOps       DelegationOpList       `pack:"-" json:"-"`
	// OriginationOps      OriginationOpList      `pack:"-" json:"-"`
	// TransactionOps      TransactionOpList      `pack:"-" json:"-"`
	// RegisterConstantOps RegisterConstantOpList `pack:"-" json:"-"`
	// DepositsLimitOps    DepositsLimitOpList    `pack:"-" json:"-"`
	// Events              EventOpList            `pack:"-" json:"-"`
}

// Ensure Block implements the pack.Item interface.
var _ pack.Item = (*Block)(nil)

func (b Block) ID() uint64 {
	return b.RowId
}

func (b *Block) SetID(id uint64) {
	b.RowId = id
}

func (m Block) TableKey() string {
	return BlockTableKey
}

func (m Block) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 16,
		CacheSize:       128,
		FillLevel:       100,
	}
}

func (m Block) IndexOpts(key string) pack.Options {
	return pack.NoOptions
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
	b.LbVote = head.LbVote()
	b.LbEma = tz.Block.Metadata.GetLbEma()
	b.AiVote = tz.Block.Header.AdaptiveIssuanceVote
	b.AiEma = tz.Block.Metadata.AdaptiveIssuanceEma
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
	clone.HasProposals = false
	clone.HasBallots = false
	clone.HasSeeds = false
	clone.OfflineBaker = 0
	clone.OfflineEndorsers = nil
	return &clone
}

func (b *Block) FetchRPC(ctx context.Context, c *rpc.Client) error {
	if !b.Hash.IsValid() {
		return fmt.Errorf("invalid hash for block id %d", b.RowId)
	}
	if b.TZ != nil {
		return nil
	}
	bundle, err := c.GetLightBundle(ctx, b.Hash, b.Params)
	if err != nil {
		return err
	}
	b.TZ = bundle
	return nil
}

func (b *Block) IsProtocolUpgrade() bool {
	if b.Parent == nil || b.Parent.TZ == nil || b.TZ == nil {
		return false
	}
	return b.Parent.TZ.Protocol() != b.TZ.Protocol()
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
		b.TZ.Baking = b.TZ.Baking[:0]
		b.TZ.Endorsing = b.TZ.Endorsing[:0]
		b.TZ.Block.Operations[0] = b.TZ.Block.Operations[0][:0]
		b.TZ.Block.Operations[1] = b.TZ.Block.Operations[1][:0]
		b.TZ.Block.Operations[2] = b.TZ.Block.Operations[2][:0]
		b.TZ.Block.Operations[3] = b.TZ.Block.Operations[3][:0]
	}
	b.OfflineEndorsers = nil
}

func (b *Block) Reset() {
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
	*b = Block{}
}

func (b *Block) Update(accounts map[AccountID]*Account, bakers map[AccountID]*Baker) {
	// initial state
	b.NOpsApplied = 0
	b.NOpsFailed = 0
	b.NEvents = 0
	b.NContractCalls = 0
	b.NRollupCalls = 0
	b.NTx = 0
	b.NTickets = 0
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
			b.MintedSupply += op.Reward + op.Volume
			b.Reward += op.Reward + op.Volume
			b.Deposit += op.Deposit

		case OpTypeBonus:
			// post-Ithaca extra bonus counts also against block reward
			b.MintedSupply += op.Reward + op.Volume
			b.Reward += op.Reward + op.Volume

		case OpTypeReward:
			// post-Ithaca endorsing rewards are minted and directly paid
			// at end of cycle unless participation was too low
			b.MintedSupply += op.Reward + op.Volume
			b.BurnedSupply += op.Burned

		case OpTypeEndorsement:
			// pre-Ithace endorsements pay deposit and mint frozen rewards
			endorsedSlots += op.Raw.Meta().Power()
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
		case OpTypeDoubleBaking,
			OpTypeDoubleEndorsement,
			OpTypeDoublePreendorsement,
			OpTypeStakeSlash:

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
				if op.IsBurnAddress {
					b.BurnedSupply += op.Volume
				} else {
					b.Volume += op.Volume
				}
				// only count external calls with params
				if !op.IsInternal && op.IsContract && len(op.Parameters) > 0 {
					b.NContractCalls++
				}
				// only count external tx
				if !op.IsInternal {
					b.NTx++
				}
				if len(op.RawTicketUpdates) > 0 {
					b.NTickets++
				}
			}

		case OpTypeOrigination, OpTypeRollupOrigination:
			b.Fee += op.Fee
			b.BurnedSupply += op.Burned
			if op.IsSuccess {
				b.Volume += op.Volume
				if len(op.RawTicketUpdates) > 0 {
					b.NTickets++
				}
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

		case OpTypeTransferTicket:
			b.Fee += op.Fee
			b.BurnedSupply += op.Burned
			if op.IsSuccess {
				b.NTickets++
			}

		case OpTypeRollupTransaction:
			b.Fee += op.Fee
			b.BurnedSupply += op.Burned
			if op.IsSuccess {
				b.Volume += op.Volume
				b.NRollupCalls++
				if len(op.RawTicketUpdates) > 0 {
					b.NTickets++
				}
			}

		case OpTypeStake, OpTypeUnstake, OpTypeFinalizeUnstake, OpTypeSetDelegateParameters:
			b.Fee += op.Fee
			// no other action required, i.e. don't count stake volume
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
