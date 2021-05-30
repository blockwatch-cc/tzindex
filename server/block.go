// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerBlock{})
}

var _ RESTful = (*ExplorerBlock)(nil)

type ExplorerBlock struct {
	Hash                tezos.BlockHash              `json:"hash"`
	ParentHash          tezos.BlockHash              `json:"predecessor"`
	FollowerHash        tezos.BlockHash              `json:"successor"`
	Baker               tezos.Address                `json:"baker"`
	IsOrphan            bool                         `json:"is_orphan,omitempty"`
	Height              int64                        `json:"height"`
	Cycle               int64                        `json:"cycle"`
	IsCycleSnapshot     bool                         `json:"is_cycle_snapshot"`
	Timestamp           time.Time                    `json:"time"`
	Solvetime           int                          `json:"solvetime"`
	Version             int                          `json:"version"`
	Validation          int                          `json:"validation_pass"`
	Fitness             uint64                       `json:"fitness"`
	Priority            int                          `json:"priority"`
	Nonce               string                       `json:"nonce"`
	VotingPeriodKind    tezos.VotingPeriodKind       `json:"voting_period_kind"`
	SlotsEndorsed       uint32                       `json:"endorsed_slots"`
	NSlotsEndorsed      int                          `json:"n_endorsed_slots"`
	NOps                int                          `json:"n_ops"`
	NOpsFailed          int                          `json:"n_ops_failed"`
	NOpsContract        int                          `json:"n_ops_contract"`
	NTx                 int                          `json:"n_tx"`
	NActivation         int                          `json:"n_activation"`
	NSeedNonce          int                          `json:"n_seed_nonce_revelations"`
	N2Baking            int                          `json:"n_double_baking_evidences"`
	N2Endorsement       int                          `json:"n_double_endorsement_evidences"`
	NEndorsement        int                          `json:"n_endorsement"`
	NDelegation         int                          `json:"n_delegation"`
	NReveal             int                          `json:"n_reveal"`
	NOrigination        int                          `json:"n_origination"`
	NProposal           int                          `json:"n_proposal"`
	NBallot             int                          `json:"n_ballot"`
	Volume              float64                      `json:"volume"`
	Fee                 float64                      `json:"fee"`
	Reward              float64                      `json:"reward"`
	Deposit             float64                      `json:"deposit"`
	UnfrozenFees        float64                      `json:"unfrozen_fees"`
	UnfrozenRewards     float64                      `json:"unfrozen_rewards"`
	UnfrozenDeposits    float64                      `json:"unfrozen_deposits"`
	ActivatedSupply     float64                      `json:"activated_supply"`
	BurnedSupply        float64                      `json:"burned_supply"`
	SeenAccounts        int                          `json:"n_accounts"`
	NewAccounts         int                          `json:"n_new_accounts"`
	NewImplicitAccounts int                          `json:"n_new_implicit"`
	NewManagedAccounts  int                          `json:"n_new_managed"`
	NewContracts        int                          `json:"n_new_contracts"`
	ClearedAccounts     int                          `json:"n_cleared_accounts"`
	FundedAccounts      int                          `json:"n_funded_accounts"`
	GasLimit            int64                        `json:"gas_limit"`
	GasUsed             int64                        `json:"gas_used"`
	GasPrice            float64                      `json:"gas_price"`
	StorageSize         int64                        `json:"storage_size"`
	TDD                 float64                      `json:"days_destroyed"`
	PctAccountsReused   float64                      `json:"pct_account_reuse"`
	NOpsImplicit        int                          `json:"n_ops_implicit"`
	Metadata            map[string]*ExplorerMetadata `json:"metadata,omitempty"`
	Rights              []ExplorerRight              `json:"rights,omitempty"`

	// LEGACY
	Ops *ExplorerOpList `json:"ops,omitempty"`

	// caching
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

type ExplorerRight struct {
	Type       tezos.RightType `json:"type"`
	Priority   *int            `json:"priority,omitempty"`
	Slot       *int            `json:"slot,omitempty"`
	AccountId  model.AccountID `json:"-"`
	Address    tezos.Address   `json:"address"`
	IsUsed     *bool           `json:"is_used,omitempty"`
	IsLost     *bool           `json:"is_lost,omitempty"`
	IsStolen   *bool           `json:"is_stolen,omitempty"`
	IsMissed   *bool           `json:"is_missed,omitempty"`
	IsBondMiss *bool           `json:"is_bond_miss,omitempty"`
}

func NewExplorerRight(ctx *ApiContext, r model.Right) ExplorerRight {
	er := ExplorerRight{
		Type:      r.Type,
		AccountId: r.AccountId,
		Address:   ctx.Indexer.LookupAddress(ctx, r.AccountId),
	}
	if r.Type == tezos.RightTypeBaking {
		er.Priority = IntPtr(r.Priority)
	} else {
		er.Slot = IntPtr(r.Priority)
	}
	if r.IsUsed {
		er.IsUsed = BoolPtr(true)
	}
	if r.IsLost {
		er.IsLost = BoolPtr(true)
	}
	if r.IsStolen {
		er.IsStolen = BoolPtr(true)
	}
	if r.IsMissed {
		er.IsMissed = BoolPtr(true)
	}
	if r.IsBondMiss {
		er.IsBondMiss = BoolPtr(true)
	}
	return er
}

func NewExplorerBlock(ctx *ApiContext, block *model.Block, args Options) *ExplorerBlock {
	p := ctx.Params
	if !p.ContainsHeight(block.Height) {
		p = ctx.Crawler.ParamsByHeight(block.Height)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], block.Nonce)
	b := &ExplorerBlock{
		Hash:                block.Hash,
		IsOrphan:            block.IsOrphan,
		Baker:               ctx.Indexer.LookupAddress(ctx, block.BakerId),
		Height:              block.Height,
		Cycle:               block.Cycle,
		IsCycleSnapshot:     block.IsCycleSnapshot,
		Timestamp:           block.Timestamp,
		Solvetime:           block.Solvetime,
		Version:             block.Version,
		Validation:          block.Validation,
		Fitness:             block.Fitness,
		Priority:            block.Priority,
		Nonce:               hex.EncodeToString(buf[:]),
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
		Fee:                 p.ConvertValue(block.Fee),
		Reward:              p.ConvertValue(block.Reward),
		Deposit:             p.ConvertValue(block.Deposit),
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
		NOpsImplicit:        block.NOpsImplicit,
	}
	nowHeight := ctx.Tip.BestHeight
	if block.SeenAccounts > 0 {
		b.PctAccountsReused = float64(block.SeenAccounts-block.NewAccounts) / float64(block.SeenAccounts) * 100
	}

	// use database lookups here (not cache) to correctly link orphans
	if prev, err := ctx.Indexer.BlockByID(ctx.Context, block.ParentId); err == nil {
		b.ParentHash = prev.Hash
	}
	if nowHeight > block.Height {
		next, err := ctx.Indexer.BlockByParentId(ctx.Context, block.RowId)
		if err != nil {
			log.Errorf("explorer: cannot resolve successor for block id %d: %v", block.RowId, err)
		} else {
			b.FollowerHash = next.Hash
		}
	}

	if args.WithRights() {
		rights, err := ctx.Indexer.ListBlockRights(ctx.Context, block.Height, 0)
		if err != nil {
			log.Errorf("explorer: cannot resolve rights for block %d: %v", block.Height, err)
		} else {
			b.Rights = make([]ExplorerRight, 0)
			for _, v := range rights {
				switch v.Type {
				case tezos.RightTypeEndorsing:
					b.Rights = append(b.Rights, NewExplorerRight(ctx, v))
				case tezos.RightTypeBaking:
					if v.Priority <= block.Priority {
						b.Rights = append(b.Rights, NewExplorerRight(ctx, v))
					}
				}
			}
		}
	}

	if args.WithMeta() {
		// add metadata for baker and rights holders
		b.Metadata = make(map[string]*ExplorerMetadata)

		// add baker
		if md, ok := lookupMetadataById(ctx, block.BakerId, 0, false); ok {
			b.Metadata[b.Baker.String()] = md
		}

		// add rightholders
		for _, v := range b.Rights {
			if _, ok := b.Metadata[v.Address.String()]; ok {
				continue
			}
			if md, ok := lookupMetadataById(ctx, v.AccountId, 0, false); ok {
				b.Metadata[v.Address.String()] = md
			}
		}
	}

	if b.Height == nowHeight {
		// cache most recent block only until next block and endorsements are due
		b.expires = b.Timestamp.Add(p.TimeBetweenBlocks[0])
		b.lastmod = b.Timestamp
	} else if b.Height+tezos.MaxBranchDepth >= nowHeight {
		// cache blocks in the reorg safety zone only until next block is expected
		b.expires = ctx.Tip.BestTime.Add(p.TimeBetweenBlocks[0])
		b.lastmod = b.Timestamp.Add(p.TimeBetweenBlocks[0])
	} else {
		b.expires = b.Timestamp.Add(maxCacheExpires)
		b.lastmod = b.Timestamp
	}
	return b
}

func (b ExplorerBlock) LastModified() time.Time {
	return b.lastmod
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
	r.HandleFunc("/{ident}/operations", C(ListBlockOps)).Methods("GET")

	// LEGACY
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

type BlockRequest struct {
	Meta   bool `schema:"meta"`   // include account metadata
	Rights bool `schema:"rights"` // include rights
}

func (r *BlockRequest) WithPrim() bool     { return false }
func (r *BlockRequest) WithUnpack() bool   { return false }
func (r *BlockRequest) WithHeight() int64  { return 0 }
func (r *BlockRequest) WithMeta() bool     { return r != nil && r.Meta }
func (r *BlockRequest) WithRights() bool   { return r != nil && r.Rights }
func (r *BlockRequest) WithCollapse() bool { return false }

func ReadBlock(ctx *ApiContext) (interface{}, int) {
	args := &BlockRequest{}
	ctx.ParseRequestArgs(args)
	block := loadBlock(ctx)
	return NewExplorerBlock(ctx, block, args), http.StatusOK
}

// LEGACY
func ReadBlockOps(ctx *ApiContext) (interface{}, int) {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	block := loadBlock(ctx)
	b := NewExplorerBlock(ctx, block, args)
	fn := ctx.Indexer.ListBlockOps
	if args.WithCollapse() {
		fn = ctx.Indexer.ListBlockOpsCollapsed
	}

	r := etl.ListRequest{
		Mode:   args.TypeMode,
		Typs:   args.TypeList,
		Since:  block.Height,
		Until:  block.Height,
		Offset: args.Offset,
		Limit:  ctx.Cfg.ClampExplore(args.Limit),
		Cursor: args.Cursor,
		Order:  args.Order,
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
		}
	}

	ops, err := fn(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read block operations", err))
	}
	b.Ops = &ExplorerOpList{
		list: make([]*ExplorerOp, 0),
	}
	cache := make(map[int64]interface{})
	for _, v := range ops {
		b.Ops.Append(NewExplorerOp(ctx, v, block, nil, args, cache), args.WithCollapse())
	}
	return b, http.StatusOK
}

func ListBlockOps(ctx *ApiContext) (interface{}, int) {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	block := loadBlock(ctx)
	fn := ctx.Indexer.ListBlockOps
	if args.WithCollapse() {
		fn = ctx.Indexer.ListBlockOpsCollapsed
	}

	r := etl.ListRequest{
		Mode:   args.TypeMode,
		Typs:   args.TypeList,
		Since:  block.Height,
		Until:  block.Height,
		Offset: args.Offset,
		Limit:  ctx.Cfg.ClampExplore(args.Limit),
		Cursor: args.Cursor,
		Order:  args.Order,
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
		}
	}

	ops, err := fn(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read block operations", err))
	}
	resp := &ExplorerOpList{
		list:    make([]*ExplorerOp, 0),
		expires: ctx.Now.Add(maxCacheExpires),
	}
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewExplorerOp(ctx, v, block, nil, args, cache), args.WithCollapse())
	}

	return resp, http.StatusOK
}
