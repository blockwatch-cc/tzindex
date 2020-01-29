// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerBlock{})
}

var _ RESTful = (*ExplorerBlock)(nil)

type ExplorerBlock struct {
	Hash                chain.BlockHash        `json:"hash"`
	ParentHash          chain.BlockHash        `json:"predecessor"`
	FollowerHash        chain.BlockHash        `json:"successor"`
	Baker               chain.Address          `json:"baker"`
	IsOrphan            bool                   `json:"is_orphan,omitempty"`
	Height              int64                  `json:"height"`
	Cycle               int64                  `json:"cycle"`
	IsCycleSnapshot     bool                   `json:"is_cycle_snapshot"`
	Timestamp           time.Time              `json:"time"`
	Solvetime           int                    `json:"solvetime"`
	Version             int                    `json:"version"`
	Validation          int                    `json:"validation_pass"`
	Fitness             uint64                 `json:"fitness"`
	Priority            int                    `json:"priority"`
	Nonce               uint64                 `json:"nonce"`
	VotingPeriodKind    chain.VotingPeriodKind `json:"voting_period_kind"`
	SlotsEndorsed       uint32                 `json:"endorsed_slots"`
	NSlotsEndorsed      int                    `json:"n_endorsed_slots"`
	NOps                int                    `json:"n_ops"`
	NOpsFailed          int                    `json:"n_ops_failed"`
	NOpsContract        int                    `json:"n_ops_contract"`
	NTx                 int                    `json:"n_tx"`
	NActivation         int                    `json:"n_activation"`
	NSeedNonce          int                    `json:"n_seed_nonce_revelations"`
	N2Baking            int                    `json:"n_double_baking_evidences"`
	N2Endorsement       int                    `json:"n_double_endorsement_evidences"`
	NEndorsement        int                    `json:"n_endorsement"`
	NDelegation         int                    `json:"n_delegation"`
	NReveal             int                    `json:"n_reveal"`
	NOrigination        int                    `json:"n_origination"`
	NProposal           int                    `json:"n_proposal"`
	NBallot             int                    `json:"n_ballot"`
	Volume              float64                `json:"volume"`
	Fees                float64                `json:"fees"`
	Rewards             float64                `json:"rewards"`
	Deposits            float64                `json:"deposits"`
	UnfrozenFees        float64                `json:"unfrozen_fees"`
	UnfrozenRewards     float64                `json:"unfrozen_rewards"`
	UnfrozenDeposits    float64                `json:"unfrozen_deposits"`
	ActivatedSupply     float64                `json:"activated_supply"`
	BurnedSupply        float64                `json:"burned_supply"`
	SeenAccounts        int                    `json:"n_accounts"`
	NewAccounts         int                    `json:"n_new_accounts"`
	NewImplicitAccounts int                    `json:"n_new_implicit"`
	NewManagedAccounts  int                    `json:"n_new_managed"`
	NewContracts        int                    `json:"n_new_contracts"`
	ClearedAccounts     int                    `json:"n_cleared_accounts"`
	FundedAccounts      int                    `json:"n_funded_accounts"`
	GasLimit            int64                  `json:"gas_limit"`
	GasUsed             int64                  `json:"gas_used"`
	GasPrice            float64                `json:"gas_price"`
	StorageSize         int64                  `json:"storage_size"`
	TDD                 float64                `json:"days_destroyed"`
	PctAccountsReused   float64                `json:"pct_account_reuse"`
	Ops                 *[]*ExplorerOp         `json:"ops,omitempty"`
	Endorsers           []chain.Address        `json:"endorsers,omitempty"`
	expires             time.Time              `json:"-"`
}

func NewExplorerBlock(ctx *ApiContext, block *model.Block, p *chain.Params) *ExplorerBlock {
	b := &ExplorerBlock{
		Hash:                block.Hash,
		IsOrphan:            block.IsOrphan,
		Baker:               lookupAddress(ctx, block.BakerId),
		Height:              block.Height,
		Cycle:               block.Cycle,
		IsCycleSnapshot:     block.IsCycleSnapshot,
		Timestamp:           block.Timestamp,
		Solvetime:           block.Solvetime,
		Version:             block.Version,
		Validation:          block.Validation,
		Fitness:             block.Fitness,
		Priority:            block.Priority,
		Nonce:               block.Nonce,
		VotingPeriodKind:    block.VotingPeriodKind,
		SlotsEndorsed:       block.SlotsEndorsed,
		NSlotsEndorsed:      block.NSlotsEndorsed,
		NOps:                block.NOps,
		NOpsFailed:          block.NOpsFailed,
		NOpsContract:        block.NOpsContract,
		NTx:                 block.NTx,
		NActivation:         block.NActivation,
		NSeedNonce:          block.NSeedNonce,
		N2Baking:            block.N2Baking,
		N2Endorsement:       block.N2Endorsement,
		NEndorsement:        block.NEndorsement,
		NDelegation:         block.NDelegation,
		NReveal:             block.NReveal,
		NOrigination:        block.NOrigination,
		NProposal:           block.NProposal,
		NBallot:             block.NBallot,
		Volume:              p.ConvertValue(block.Volume),
		Fees:                p.ConvertValue(block.Fees),
		Rewards:             p.ConvertValue(block.Rewards),
		Deposits:            p.ConvertValue(block.Deposits),
		UnfrozenFees:        p.ConvertValue(block.UnfrozenFees),
		UnfrozenRewards:     p.ConvertValue(block.UnfrozenRewards),
		UnfrozenDeposits:    p.ConvertValue(block.UnfrozenDeposits),
		ActivatedSupply:     p.ConvertValue(block.ActivatedSupply),
		BurnedSupply:        p.ConvertValue(block.BurnedSupply),
		SeenAccounts:        block.SeenAccounts,
		NewAccounts:         block.NewAccounts,
		NewImplicitAccounts: block.NewImplicitAccounts,
		NewManagedAccounts:  block.NewManagedAccounts,
		NewContracts:        block.NewContracts,
		ClearedAccounts:     block.ClearedAccounts,
		FundedAccounts:      block.FundedAccounts,
		GasLimit:            block.GasLimit,
		GasUsed:             block.GasUsed,
		GasPrice:            block.GasPrice,
		StorageSize:         block.StorageSize,
		TDD:                 block.TDD,
	}
	nowHeight := ctx.Crawler.Height()
	if block.SeenAccounts > 0 {
		b.PctAccountsReused = float64(block.SeenAccounts-block.NewAccounts) / float64(block.SeenAccounts) * 100
	}
	prev, err := ctx.Indexer.BlockByID(ctx.Context, block.ParentId)
	if err != nil {
		log.Errorf("explorer block: cannot resolve parent block id %d: %v", block.ParentId, err)
	} else {
		b.ParentHash = prev.Hash
	}
	if nowHeight > block.Height {
		next, err := ctx.Indexer.BlockByParentId(ctx.Context, block.RowId)
		if err != nil {
			log.Errorf("explorer block: cannot resolve successor for block id %d: %v", block.RowId, err)
		} else {
			b.FollowerHash = next.Hash
		}
	}
	rights, err := ctx.Indexer.ListBlockEndorsingRights(ctx.Context, block.Height)
	if err != nil {
		log.Errorf("explorer block: cannot resolve rights for block %d: %v", block.Height, err)
	} else {
		b.Endorsers = make([]chain.Address, len(rights))
		for i, v := range rights {
			b.Endorsers[i] = lookupAddress(ctx, v.AccountId)
		}
	}
	if b.Height == nowHeight {
		// cache most recent block only until next block
		b.expires = b.Timestamp.Add(p.TimeBetweenBlocks[0])
	} else if b.Height+chain.MaxBranchDepth >= nowHeight {
		// cache blocks in the reorg safety zone only until next block is expected
		b.expires = ctx.Crawler.Time().Add(p.TimeBetweenBlocks[0])
	}
	return b
}

func (b ExplorerBlock) LastModified() time.Time {
	return b.Timestamp
}

func (b ExplorerBlock) Expires() time.Time {
	return b.expires
}

func (b ExplorerBlock) RESTPrefix() string {
	return "/explorer/block"
}

func (b ExplorerBlock) RESTPath(r *mux.Router) string {
	path, _ := r.Get("block").URLPath("ident", b.Hash.String())
	return path.String()
}

func (b ExplorerBlock) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b ExplorerBlock) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadBlock)).Methods("GET").Name("block")
	r.HandleFunc("/{ident}/op", C(ReadBlockOps)).Methods("GET")
	return nil

}

func loadBlock(ctx *ApiContext) *model.Block {
	if blockIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || blockIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing block identifier", nil))
	} else {
		block, err := ctx.Indexer.LookupBlock(ctx, blockIdent)
		if err != nil {
			switch err {
			case index.ErrNoBlockEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such block", err))
			case index.ErrInvalidBlockHeight:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case index.ErrInvalidBlockHash:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid block hash", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return block
	}
}

func ReadBlock(ctx *ApiContext) (interface{}, int) {
	block := loadBlock(ctx)
	params := ctx.Crawler.ParamsByHeight(block.Height)
	return NewExplorerBlock(ctx, block, params), http.StatusOK
}

func ReadBlockOps(ctx *ApiContext) (interface{}, int) {
	args := &ExplorerOpsRequest{}
	ctx.ParseRequestArgs(args)
	block := loadBlock(ctx)
	params := ctx.Crawler.ParamsByHeight(block.Height)
	b := NewExplorerBlock(ctx, block, params)
	ops, err := ctx.Indexer.ListBlockOps(ctx, block.Height, args.Type, args.Offset, ctx.Cfg.ClampExplore(args.Limit))
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read block operations", err))
	}

	// FIXME: collect account and op lookup into only two queries
	eops := make([]*ExplorerOp, len(ops))
	for i, v := range ops {
		eops[i] = NewExplorerOp(ctx, v, block, nil, params, nil)
	}
	b.Ops = &eops
	return b, http.StatusOK
}
