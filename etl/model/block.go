// Copyright (c) 2020-2021 Blockwatch Data Inc.
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
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"
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
	RowId            uint64                 `pack:"I,pk,snappy"   json:"row_id"`                         // internal: id, not height!
	ParentId         uint64                 `pack:"P,snappy"      json:"parent_id"`                      // internal: parent block id
	Hash             tezos.BlockHash        `pack:"H"             json:"hash"`                           // bc: block hash
	IsOrphan         bool                   `pack:"Z,snappy"      json:"is_orphan,omitempty"`            // internal: valid or orphan state
	Height           int64                  `pack:"h,snappy"      json:"height"`                         // bc: block height (also for orphans)
	Cycle            int64                  `pack:"c,snappy"      json:"cycle"`                          // bc: block cycle (tezos specific)
	IsCycleSnapshot  bool                   `pack:"o,snappy"      json:"is_cycle_snapshot"`              // will be set when snapshot index is determined
	Timestamp        time.Time              `pack:"T,snappy"      json:"time"`                           // bc: block creation time
	Solvetime        int                    `pack:"d,snappy"      json:"solvetime"`                      // stats: time to solve this block in seconds
	Version          int                    `pack:"v,snappy"      json:"version"`                        // bc: block version (mapped from tezos protocol version)
	Validation       int                    `pack:"l,snappy"      json:"validation_pass"`                // bc: tezos validation pass
	Fitness          uint64                 `pack:"f,snappy"      json:"fitness"`                        // bc: block fitness bits
	Priority         int                    `pack:"p,snappy"      json:"priority"`                       // bc: baker priority
	Nonce            uint64                 `pack:"n,snappy"      json:"nonce"`                          // bc: block nonce
	VotingPeriodKind tezos.VotingPeriodKind `pack:"k,snappy"      json:"voting_period_kind"`             // bc: tezos voting period (enum)
	BakerId          AccountID              `pack:"B,snappy"      json:"baker_id"`                       // bc: block baker (address id)
	SlotsEndorsed    []byte                 `pack:"s,snappy"      json:"endorsed_slots"`                 // bc: slots that were endorsed by the following block
	NSlotsEndorsed   int                    `pack:"e,snappy"      json:"n_endorsed_slots"`               // stats: successful endorsed slots
	NOps             int                    `pack:"1,snappy"      json:"n_ops"`                          // stats: successful operation count
	NOpsFailed       int                    `pack:"2,snappy"      json:"n_ops_failed"`                   // stats: failed operation coiunt
	NOpsContract     int                    `pack:"3,snappy"      json:"n_ops_contract"`                 // stats: successful contract operation count
	NTx              int                    `pack:"4,snappy"      json:"n_tx"`                           // stats: number of Tx operations
	NActivation      int                    `pack:"5,snappy"      json:"n_activation"`                   // stats: number of Activations operations
	NSeedNonce       int                    `pack:"6,snappy"      json:"n_seed_nonce_revelation"`        // stats: number of Nonce Revelations operations
	N2Baking         int                    `pack:"7,snappy"      json:"n_double_baking_evidence"`       // stats: number of 2Baking operations
	N2Endorsement    int                    `pack:"8,snappy"      json:"n_double_endorsement_evidence"`  // stats: number of 2Endorsement operations
	NEndorsement     int                    `pack:"9,snappy"      json:"n_endorsement"`                  // stats: number of Endorsements operations
	NDelegation      int                    `pack:"0,snappy"      json:"n_delegation"`                   // stats: number of Delegations operations
	NReveal          int                    `pack:"w,snappy"      json:"n_reveal"`                       // stats: number of Reveals operations
	NOrigination     int                    `pack:"x,snappy"      json:"n_origination"`                  // stats: number of Originations operations
	NProposal        int                    `pack:"y,snappy"      json:"n_proposal"`                     // stats: number of Proposals operations
	NBallot          int                    `pack:"z,snappy"      json:"n_ballot"`                       // stats: number of Ballots operations
	NRegister        int                    `pack:"r,snappy"      json:"n_register_constant"`            // stats: number of register_global_constant operations
	NContractCalls   int                    `pack:"N,snappy"      json:"n_contract_calls"`               // stats: number of EOA to KT1 calls
	Volume           int64                  `pack:"V,snappy"      json:"volume"`                         // stats: sum of transacted amount
	Fee              int64                  `pack:"F,snappy"      json:"fee"`                            // stats: sum of transaction fees
	Reward           int64                  `pack:"R,snappy"      json:"reward"`                         // stats: baking and endorsement rewards
	Deposit          int64                  `pack:"D,snappy"      json:"deposit"`                        // stats: bond deposits for baking and endorsement
	UnfrozenFees     int64                  `pack:"u,snappy"      json:"unfrozen_fees"`                  // stats: total unfrozen fees
	UnfrozenRewards  int64                  `pack:"U,snappy"      json:"unfrozen_rewards"`               // stats: total unfrozen rewards
	UnfrozenDeposits int64                  `pack:"X,snappy"      json:"unfrozen_deposits"`              // stats: total unfrozen deposits
	ActivatedSupply  int64                  `pack:"S,snappy"      json:"activated_supply"`               // stats: new activated supply
	BurnedSupply     int64                  `pack:"b,snappy"      json:"burned_supply"`                  // stats: burned tezos
	SeenAccounts     int                    `pack:"a,snappy"      json:"n_accounts"`                     // stats: count of unique accounts
	NewAccounts      int                    `pack:"A,snappy"      json:"n_new_accounts"`                 // stats: count of new accounts
	NewContracts     int                    `pack:"C,snappy"      json:"n_new_contracts"`                // stats: count of new contracts (KT1 with code)
	ClearedAccounts  int                    `pack:"E,snappy"      json:"n_cleared_accounts"`             // stats: count of zero balance acc at end of block that were funded before
	FundedAccounts   int                    `pack:"J,snappy"      json:"n_funded_accounts"`              // stats: count of (re)funded acc at end of block (new or previously cleared aacc)
	GasLimit         int64                  `pack:"L,snappy"      json:"gas_limit"`                      // stats: total gas limit this block
	GasUsed          int64                  `pack:"G,snappy"      json:"gas_used"`                       // stats: total gas used this block
	GasPrice         float64                `pack:"g,convert,precision=5,snappy"  json:"gas_price"`      // stats: gas price in tezos per unit gas
	StorageSize      int64                  `pack:"Y,snappy"      json:"storage_size"`                   // stats: total new storage size allocated
	TDD              float64                `pack:"t,convert,precision=5,snappy"  json:"days_destroyed"` // stats: token days destroyed (from last-in time to spend)
	NOpsImplicit     int                    `pack:"j,snappy"      json:"n_ops_implicit"`                 // stats: number of implicit operations
	LbEscapeVote     bool                   `pack:"O,snappy"      json:"lb_esc_vote"`                    // stats: liquidity baking vote
	LbEscapeEma      int64                  `pack:"M,snappy"      json:"lb_esc_ema"`                     // stats: liquidity baking disable moving average

	// other tz or extracted/translated data for processing
	TZ     *Bundle       `pack:"-" json:"-"`
	Params *tezos.Params `pack:"-" json:"-"`
	Chain  *Chain        `pack:"-" json:"-"`
	Supply *Supply       `pack:"-" json:"-"`
	Ops    []*Op         `pack:"-" json:"-"`
	Flows  []*Flow       `pack:"-" json:"-"`
	Baker  *Account      `pack:"-" json:"-"`
	Parent *Block        `pack:"-" json:"-"`
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

func NewBlock(tz *Bundle, parent *Block) (*Block, error) {
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
	b.Validation = tz.Block.Header.ValidationPass
	b.Priority = tz.Block.Header.Priority
	b.LbEscapeVote = tz.Block.Header.LiquidityBakingEscapeVote
	b.LbEscapeEma = tz.Block.Metadata.LiquidityBakingEscapeEma
	if len(tz.Block.Header.ProofOfWorkNonce) >= 8 {
		b.Nonce = binary.BigEndian.Uint64(tz.Block.Header.ProofOfWorkNonce)
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

	// take the longest and highest hex value which represents the max height + #(endorsements) + 1
	if l := len(tz.Block.Header.Fitness); l > 0 {
		b.Fitness = binary.BigEndian.Uint64(tz.Block.Header.Fitness[l-1])
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
	clone.Parent = nil
	return &clone
}

func (b *Block) FetchRPC(ctx context.Context, c *rpc.Client) error {
	if !b.Hash.IsValid() {
		return fmt.Errorf("invalid block hash on block id %d", b.RowId)
	}
	var err error
	if b.TZ == nil {
		b.TZ = &Bundle{}
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
			cons, err := c.GetConstantsHeight(ctx, b.Height)
			if err != nil {
				return fmt.Errorf("block init: %w", err)
			}
			b.Params = cons.MapToChainParams()
		} else {
			b.Params = tezos.NewParams()
		}
		b.Params = b.Params.
			ForNetwork(b.TZ.Block.ChainId).
			ForProtocol(b.TZ.Block.Protocol)
		b.Params.Deployment = b.TZ.Block.Header.Proto
	}
	b.TZ.Params = b.Params
	// start fetching more rights at cycle 2 (look-ahead is 5)
	if b.Height >= b.Params.CycleStartHeight(2) && b.Params.IsCycleStart(b.Height) {
		// snapshot index and rights for future cycle N; the snapshot index
		// refers to a snapshot block taken in cycle N-7 and randomness
		// collected from seed_nonce_revelations during cycle N-6; N is the
		// farthest future cycle that exists.
		//
		// Note that for consistency and due to an off-by-one error in Tezos RPC
		// nodes we fetch snapshot index and rights at the start of cycle N-5 even
		// though they are created at the end of N-6!
		cycle := b.Cycle + b.Params.PreservedCycles
		b.TZ.Baking, err = c.GetBakingRightsCycle(ctx, b.Height, cycle)
		if err != nil {
			return fmt.Errorf("fetching baking rights for cycle %d: %w", cycle, err)
		}
		b.TZ.Endorsing, err = c.GetEndorsingRightsCycle(ctx, b.Height, cycle)
		if err != nil {
			return fmt.Errorf("fetching endorsing rights for cycle %d: %w", cycle, err)
		}
		b.TZ.Snapshot, err = c.GetSnapshotIndexCycle(ctx, b.Height, cycle)
		if err != nil {
			return fmt.Errorf("fetching snapshot for cycle %d: %w", cycle, err)
		}
	}
	return nil
}

func (b *Block) IsProtocolUpgrade() bool {
	if b.Parent == nil || b.Parent.TZ == nil || b.TZ == nil {
		return false
	}
	return !b.Parent.TZ.Block.Metadata.Protocol.Equal(b.TZ.Block.Metadata.Protocol)
}

func (b *Block) GetRpcOp(l, p, c int) (rpc.Operation, bool) {
	if l >= 0 && (len(b.TZ.Block.Operations) < l ||
		len(b.TZ.Block.Operations[l]) < p ||
		len(b.TZ.Block.Operations[l][p].Contents) < c) {
		return nil, false
	}
	return b.TZ.Block.Operations[l][p].Contents[c], true
}

func (b *Block) GetRpcOpHeader(l, p int) (*rpc.OperationHeader, bool) {
	if len(b.TZ.Block.Operations) < l ||
		len(b.TZ.Block.Operations[l]) < p {
		return nil, false
	}
	return b.TZ.Block.Operations[l][p], true
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

func (b *Block) Age(height int64) int64 {
	// instead of real time we use block offsets and the target time
	// between blocks as time diff
	return (b.Height - height) * int64(b.Params.BlockTime()/time.Second)
}

func (b *Block) BlockReward(p *tezos.Params) int64 {
	blockReward := p.BlockReward
	if b.Cycle < p.NoRewardCycles {
		blockReward = 0
	}

	if p.Version < 5 {
		return blockReward
	}

	// count number of included endorsements
	var nEndorsements int
	for _, op := range b.Ops {
		if op.Type != tezos.OpTypeEndorsement {
			continue
		}
		eop, _ := b.GetRpcOp(op.OpL, op.OpP, op.OpC)
		nEndorsements += len(eop.(*rpc.EndorsementOp).Metadata.Slots)
	}

	if p.Version == 5 {
		// in v5
		// The baking reward is now calculated w.r.t a given priority [p] and a
		// number [e] of included endorsements as follows:
		endorseFactor := 0.8 + 0.2*float64(nEndorsements)/float64(p.EndorsersPerBlock)
		blockReward = int64(float64(blockReward) / float64(b.Priority+1) * endorseFactor)

	} else {
		// starting at v6
		// baking_reward_per_endorsement represent the reward you get (per endorsement
		// included) for baking at priority 0 (1st elem of the list) and the reward
		// for prio 1 and more (2nd elem). endorsement_reward is the same : reward for
		// endorsing blocks of prio 0 and 1+
		baseReward := p.BlockRewardV6[0]
		if b.Priority > 0 {
			baseReward = p.BlockRewardV6[1]
		}
		blockReward = int64(nEndorsements) * baseReward
	}
	return blockReward
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
		b.TZ.Block.Operations = b.TZ.Block.Operations[:0]
	}
}

func (b *Block) Reset() {
	b.RowId = 0
	b.ParentId = 0
	b.Hash = tezos.BlockHash{Hash: tezos.InvalidHash}
	b.IsOrphan = false
	b.Height = 0
	b.Cycle = 0
	b.IsCycleSnapshot = false
	b.Timestamp = time.Time{}
	b.Solvetime = 0
	b.Version = 0
	b.Validation = 0
	b.Fitness = 0
	b.Priority = 0
	b.Nonce = 0
	b.VotingPeriodKind = 0
	b.BakerId = 0
	b.SlotsEndorsed = nil
	b.NSlotsEndorsed = 0
	b.NOps = 0
	b.NOpsFailed = 0
	b.NOpsContract = 0
	b.NTx = 0
	b.NActivation = 0
	b.NSeedNonce = 0
	b.N2Baking = 0
	b.N2Endorsement = 0
	b.NEndorsement = 0
	b.NDelegation = 0
	b.NReveal = 0
	b.NOrigination = 0
	b.NProposal = 0
	b.NBallot = 0
	b.NRegister = 0
	b.NContractCalls = 0
	b.Volume = 0
	b.Fee = 0
	b.Reward = 0
	b.Deposit = 0
	b.UnfrozenFees = 0
	b.UnfrozenRewards = 0
	b.UnfrozenDeposits = 0
	b.ActivatedSupply = 0
	b.BurnedSupply = 0
	b.SeenAccounts = 0
	b.NewAccounts = 0
	b.NewContracts = 0
	b.ClearedAccounts = 0
	b.FundedAccounts = 0
	b.GasLimit = 0
	b.GasUsed = 0
	b.GasPrice = 0
	b.StorageSize = 0
	b.TDD = 0
	b.NOpsImplicit = 0
	b.LbEscapeVote = false
	b.LbEscapeEma = 0
	b.TZ = nil
	b.Params = nil
	b.Chain = nil
	b.Supply = nil
	b.Baker = nil
	b.Parent = nil
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
}

func (b *Block) Update(accounts, delegates map[AccountID]*Account) {
	// initial state
	// b.IsOrphan = false // don't set here to reuse this function for side-chain blocks
	b.NOps = 0
	b.NOpsFailed = 0
	b.NTx = 0
	b.NActivation = 0
	b.NReveal = 0
	b.N2Baking = 0
	b.N2Endorsement = 0
	b.NEndorsement = 0
	b.NDelegation = 0
	b.NReveal = 0
	b.NOrigination = 0
	b.NProposal = 0
	b.NBallot = 0
	b.NRegister = 0
	b.NContractCalls = 0
	b.Volume = 0
	b.Fee = 0
	b.Reward = 0
	b.Deposit = 0
	b.UnfrozenFees = 0
	b.UnfrozenRewards = 0
	b.UnfrozenDeposits = 0
	b.ActivatedSupply = 0
	b.BurnedSupply = 0
	b.SeenAccounts = 0
	b.NewAccounts = 0
	b.NewContracts = 0
	b.ClearedAccounts = 0
	b.FundedAccounts = 0
	b.GasLimit = 0
	b.GasUsed = 0
	b.GasPrice = 0
	b.StorageSize = 0
	b.TDD = 0
	b.NOpsImplicit = 0
	slotsEndorsed := vec.NewBitSet(b.Params.EndorsersPerBlock)

	for _, op := range b.Ops {
		b.BurnedSupply += op.Burned
		b.TDD += op.TDD
		b.GasLimit += op.GasLimit
		b.GasUsed += op.GasUsed
		b.StorageSize += op.StoragePaid

		if op.IsContract {
			b.NOpsContract++
		}
		if !op.IsSuccess {
			b.NOpsFailed++
		}
		if op.IsImplicit {
			b.NOpsImplicit++
		}
		switch op.Type {
		case tezos.OpTypeActivateAccount:
			b.NOps++
			b.NActivation++
			if op.IsSuccess {
				b.ActivatedSupply += op.Volume
				b.Volume += op.Volume
			}
		case tezos.OpTypeDoubleBakingEvidence:
			b.NOps++
			b.N2Baking++
		case tezos.OpTypeDoubleEndorsementEvidence:
			b.NOps++
			b.N2Endorsement++
		case tezos.OpTypeSeedNonceRevelation:
			b.NOps++
			b.NSeedNonce++
		case tezos.OpTypeTransaction:
			b.NOps++
			b.NTx++
			b.Fee += op.Fee
			if op.IsSuccess && op.OpL >= 0 {
				txo, _ := b.GetRpcOp(op.OpL, op.OpP, op.OpC)
				tx, _ := txo.(*rpc.TransactionOp)
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
		case tezos.OpTypeOrigination:
			b.NOps++
			b.NOrigination++
			b.Fee += op.Fee
			if op.IsSuccess {
				b.Volume += op.Volume
			}
		case tezos.OpTypeDelegation:
			b.NOps++
			b.NDelegation++
			b.Fee += op.Fee
		case tezos.OpTypeReveal:
			b.NOps++
			b.NReveal++
			b.Fee += op.Fee
		case tezos.OpTypeEndorsement:
			b.NOps++
			b.NEndorsement++
			eop, _ := b.GetRpcOp(op.OpL, op.OpP, op.OpC)
			for _, v := range eop.(*rpc.EndorsementOp).Metadata.Slots {
				slotsEndorsed.Set(v)
			}
		case tezos.OpTypeProposals:
			b.NOps++
			b.NProposal++
		case tezos.OpTypeBallot:
			b.NOps++
			b.NBallot++
		case tezos.OpTypeRegisterConstant:
			b.NOps++
			b.NRegister++
		}
	}

	if b.Parent != nil {
		b.Parent.SlotsEndorsed = slotsEndorsed.Bytes()
		b.Parent.NSlotsEndorsed = int(slotsEndorsed.Count())
	}

	// mean gas price for this block
	if b.GasUsed > 0 && b.Fee > 0 {
		b.GasPrice = float64(b.Fee) / float64(b.GasUsed)
	}

	// some updates are not reflected in operations (e.g. baking, airdrops) so we
	// have to look at flows too
	for _, f := range b.Flows {
		switch f.Operation {
		case FlowTypeBaking:
			switch f.Category {
			case FlowCategoryRewards:
				b.Reward += f.AmountIn
			case FlowCategoryDeposits:
				b.Deposit += f.AmountIn
			}
		case FlowTypeInternal:
			if f.IsUnfrozen {
				switch f.Category {
				case FlowCategoryDeposits:
					b.UnfrozenDeposits += f.AmountOut
				case FlowCategoryRewards:
					b.UnfrozenRewards += f.AmountOut
				case FlowCategoryFees:
					b.UnfrozenFees += f.AmountOut
				}
			}
		case FlowTypeNonceRevelation:
			// seed nonce burn is no operation, but still creates a flow
			if f.IsBurned {
				// technically this happends at the end of parent block
				// but we have no process in place to run updates after
				// all indexers have run
				b.BurnedSupply += f.AmountOut
			}
		}
	}

	// count account changes
	for _, acc := range accounts {
		if acc.IsNew && acc.IsDirty {
			if acc.IsDelegate {
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

	// handle (new/updated) delegates separate
	// delegates are not in accMap even if they are part of a regular op
	for _, acc := range delegates {
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

func (b *Block) Rollback(accounts, delegates map[AccountID]*Account) {
	// block will be stored as orphan
	b.Update(accounts, delegates)
	b.IsOrphan = true
}
