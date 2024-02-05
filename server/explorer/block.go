// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"net/http"
	"sort"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Block{})
}

var _ server.RESTful = (*Block)(nil)
var _ server.Resource = (*Block)(nil)

type Block struct {
	Hash                 tezos.BlockHash           `json:"hash"`
	ParentHash           string                    `json:"predecessor,omitempty"`
	FollowerHash         string                    `json:"successor,omitempty"`
	Protocol             tezos.ProtocolHash        `json:"protocol"`
	Baker                tezos.Address             `json:"baker"`
	Proposer             tezos.Address             `json:"proposer"`
	BakerConsensusKey    tezos.Address             `json:"baker_consensus_key"`
	ProposerConsensusKey tezos.Address             `json:"proposer_consensus_key"`
	Height               int64                     `json:"height"`
	Cycle                int64                     `json:"cycle"`
	IsCycleSnapshot      bool                      `json:"is_cycle_snapshot"`
	Timestamp            time.Time                 `json:"time"`
	Solvetime            int                       `json:"solvetime"`
	Version              int                       `json:"version"`
	Round                int                       `json:"round"`
	Nonce                string                    `json:"nonce"`
	VotingPeriodKind     tezos.VotingPeriodKind    `json:"voting_period_kind"`
	NSlotsEndorsed       int                       `json:"n_endorsed_slots"`
	NOps                 int                       `json:"n_ops_applied"`
	NOpsFailed           int                       `json:"n_ops_failed"`
	NContractCalls       int                       `json:"n_calls"`
	NRollupCalls         int                       `json:"n_rollup_calls"`
	NTx                  int                       `json:"n_tx"`
	NEvents              int                       `json:"n_events"`
	NTickets             int                       `json:"n_tickets"`
	Volume               float64                   `json:"volume"`
	Fee                  float64                   `json:"fee"`
	Reward               float64                   `json:"reward"`
	Deposit              float64                   `json:"deposit"`
	ActivatedSupply      float64                   `json:"activated_supply"`
	MintedSupply         float64                   `json:"minted_supply"`
	BurnedSupply         float64                   `json:"burned_supply"`
	SeenAccounts         int                       `json:"n_accounts"`
	NewAccounts          int                       `json:"n_new_accounts"`
	NewContracts         int                       `json:"n_new_contracts"`
	ClearedAccounts      int                       `json:"n_cleared_accounts"`
	FundedAccounts       int                       `json:"n_funded_accounts"`
	GasLimit             int64                     `json:"gas_limit"`
	GasUsed              int64                     `json:"gas_used"`
	StoragePaid          int64                     `json:"storage_paid"`
	PctAccountsReused    float64                   `json:"pct_account_reuse"`
	LbVote               tezos.FeatureVote         `json:"lb_vote"`
	LbEma                int64                     `json:"lb_ema"`
	AiVote               tezos.FeatureVote         `json:"ai_vote"`
	AiEma                int64                     `json:"ai_ema"`
	Metadata             map[string]*ShortMetadata `json:"metadata,omitempty"`
	Rights               []Right                   `json:"rights,omitempty"`

	// LEGACY
	Ops OpList `json:"ops,omitempty"`

	// caching
	expires time.Time
	lastmod time.Time
}

type Right struct {
	Type           tezos.RightType `json:"type"`
	AccountId      model.AccountID `json:"-"`
	Address        tezos.Address   `json:"address"`
	Round          *int            `json:"round,omitempty"`
	IsUsed         *bool           `json:"is_used,omitempty"`
	IsLost         *bool           `json:"is_lost,omitempty"`
	IsStolen       *bool           `json:"is_stolen,omitempty"`
	IsMissed       *bool           `json:"is_missed,omitempty"`
	IsSeedRequired *bool           `json:"is_seed_required,omitempty"`
	IsSeedRevealed *bool           `json:"is_seed_revealed,omitempty"`
}

func NewRight(ctx *server.Context, r model.BaseRight, addFlags bool) Right {
	er := Right{
		Type:      r.Type,
		AccountId: r.AccountId,
		Address:   ctx.Indexer.LookupAddress(ctx, r.AccountId),
	}
	if r.Type == tezos.RightTypeBaking {
		var round int
		er.Round = IntPtr(round)
	}
	if addFlags {
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
		if r.IsSeedRequired {
			er.IsSeedRequired = BoolPtr(true)
		}
		if r.IsSeedRevealed {
			er.IsSeedRevealed = BoolPtr(true)
		}
	}
	return er
}

func NewBlock(ctx *server.Context, block *model.Block, args server.Options) *Block {
	p := ctx.Params
	if p == nil || !p.ContainsHeight(block.Height) {
		p = ctx.Crawler.ParamsByHeight(block.Height)
	}
	b := &Block{
		Hash:                 block.Hash,
		Baker:                ctx.Indexer.LookupAddress(ctx, block.BakerId),
		Proposer:             ctx.Indexer.LookupAddress(ctx, block.ProposerId),
		Height:               block.Height,
		Cycle:                block.Cycle,
		IsCycleSnapshot:      block.IsCycleSnapshot,
		Timestamp:            block.Timestamp,
		Solvetime:            block.Solvetime,
		Version:              block.Version,
		Round:                block.Round,
		Nonce:                util.U64String(block.Nonce).Hex(),
		VotingPeriodKind:     block.VotingPeriodKind,
		NSlotsEndorsed:       block.NSlotsEndorsed,
		NOps:                 model.Int16Correct(block.NOpsApplied),
		NOpsFailed:           model.Int16Correct(block.NOpsFailed),
		NContractCalls:       model.Int16Correct(block.NContractCalls),
		NRollupCalls:         model.Int16Correct(block.NRollupCalls),
		NTx:                  model.Int16Correct(block.NTx),
		NEvents:              model.Int16Correct(block.NEvents),
		NTickets:             model.Int16Correct(block.NTickets),
		Volume:               p.ConvertValue(block.Volume),
		Fee:                  p.ConvertValue(block.Fee),
		Reward:               p.ConvertValue(block.Reward),
		Deposit:              p.ConvertValue(block.Deposit),
		ActivatedSupply:      p.ConvertValue(block.ActivatedSupply),
		MintedSupply:         p.ConvertValue(block.MintedSupply),
		BurnedSupply:         p.ConvertValue(block.BurnedSupply),
		SeenAccounts:         model.Int16Correct(block.SeenAccounts),
		NewAccounts:          model.Int16Correct(block.NewAccounts),
		NewContracts:         model.Int16Correct(block.NewContracts),
		ClearedAccounts:      model.Int16Correct(block.ClearedAccounts),
		FundedAccounts:       model.Int16Correct(block.FundedAccounts),
		GasLimit:             block.GasLimit,
		GasUsed:              block.GasUsed,
		StoragePaid:          block.StoragePaid,
		LbVote:               block.LbVote,
		LbEma:                block.LbEma,
		AiVote:               block.AiVote,
		AiEma:                block.AiEma,
		Protocol:             p.Protocol,
		BakerConsensusKey:    ctx.Indexer.LookupAddress(ctx, block.BakerConsensusKeyId),
		ProposerConsensusKey: ctx.Indexer.LookupAddress(ctx, block.ProposerConsensusKeyId),
	}
	nowHeight := ctx.Tip.BestHeight
	if b.SeenAccounts != 0 {
		b.PctAccountsReused = float64(b.SeenAccounts-b.NewAccounts) / float64(b.SeenAccounts) * 100
	}

	// use database lookups here (not cache) to correctly link orphans
	if prev, err := ctx.Indexer.BlockByID(ctx.Context, block.ParentId); err == nil && prev.Hash.IsValid() {
		b.ParentHash = prev.Hash.String()
	}
	if nowHeight > block.Height {
		next, err := ctx.Indexer.BlockByParentId(ctx.Context, block.RowId)
		if err != nil {
			log.Errorf("explorer: cannot resolve successor for block id %d: %v", block.RowId, err)
		} else if next.Hash.IsValid() {
			b.FollowerHash = next.Hash.String()
		}
	}

	if args.WithRights() {
		rights, err := ctx.Indexer.ListBlockRights(ctx.Context, block.Height, 0)
		if err != nil {
			log.Errorf("explorer: cannot resolve rights for block %d: %v", block.Height, err)
		} else {
			b.Rights = make([]Right, 0)
			for _, v := range rights {
				switch v.Type {
				case tezos.RightTypeEndorsing:
					b.Rights = append(b.Rights, NewRight(ctx, v, block.Height < ctx.Tip.BestHeight))
				case tezos.RightTypeBaking:
					b.Rights = append(b.Rights, NewRight(ctx, v, block.Height < ctx.Tip.BestHeight))
				}
			}
		}
	}

	if args.WithMeta() {
		// add metadata for baker and rights holders
		b.Metadata = make(map[string]*ShortMetadata)

		// add baker
		if md, ok := lookupAddressIdMetadata(ctx, block.BakerId); ok {
			b.Metadata[b.Baker.String()] = md.Short()
		}

		// add rightholders
		for _, v := range b.Rights {
			if _, ok := b.Metadata[v.Address.String()]; ok {
				continue
			}
			if md, ok := lookupAddressIdMetadata(ctx, v.AccountId); ok {
				b.Metadata[v.Address.String()] = md.Short()
			}
		}
	}

	switch {
	case b.Height == nowHeight:
		// cache most recent block only until next block and endorsements are due
		b.expires = ctx.Expires
		b.lastmod = b.Timestamp
	case b.Height+p.MaxOperationsTTL >= nowHeight:
		// cache blocks in the reorg safety zone only until next block is expected
		b.expires = ctx.Expires
		b.lastmod = b.Timestamp
	default:
		b.expires = b.Timestamp.Add(ctx.Cfg.Http.CacheMaxExpires)
		b.lastmod = b.Timestamp
	}
	return b
}

func (b Block) LastModified() time.Time {
	return b.lastmod
}

func (b Block) Expires() time.Time {
	return b.expires
}

func (b Block) RESTPrefix() string {
	return "/explorer/block"
}

func (b Block) RESTPath(r *mux.Router) string {
	path, _ := r.Get("block").URLPath("ident", b.Hash.String())
	return path.String()
}

func (b Block) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Block) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadBlock)).Methods("GET").Name("block")
	r.HandleFunc("/{ident}/operations", server.C(ListBlockOps)).Methods("GET")

	// LEGACY
	r.HandleFunc("/{ident}/op", server.C(ReadBlockOps)).Methods("GET")
	return nil
}

func loadBlock(ctx *server.Context) *model.Block {
	if blockIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || blockIdent == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing block identifier", nil))
	} else {
		block, err := ctx.Indexer.LookupBlock(ctx, blockIdent)
		if err != nil {
			switch err {
			case model.ErrNoBlock:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such block", err))
			case model.ErrInvalidBlockHeight:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case model.ErrInvalidBlockHash:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block hash", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return block
	}
}

type BlockRequest struct {
	Meta   bool `schema:"meta"`   // include account metadata
	Rights bool `schema:"rights"` // include rights
}

func (r *BlockRequest) WithPrim() bool    { return false }
func (r *BlockRequest) WithUnpack() bool  { return false }
func (r *BlockRequest) WithHeight() int64 { return 0 }
func (r *BlockRequest) WithMeta() bool    { return r != nil && r.Meta }
func (r *BlockRequest) WithRights() bool  { return r != nil && r.Rights }
func (r *BlockRequest) WithMerge() bool   { return false }
func (r *BlockRequest) WithStorage() bool { return false }

func ReadBlock(ctx *server.Context) (interface{}, int) {
	args := &BlockRequest{}
	ctx.ParseRequestArgs(args)
	block := loadBlock(ctx)
	return NewBlock(ctx, block, args), http.StatusOK
}

// LEGACY
func ReadBlockOps(ctx *server.Context) (interface{}, int) {
	args := &BlockRequest{}
	ctx.ParseRequestArgs(args)
	b := NewBlock(ctx, loadBlock(ctx), args)
	b.Ops = listBlockOps(ctx)
	return b, http.StatusOK
}

func ListBlockOps(ctx *server.Context) (interface{}, int) {
	return listBlockOps(ctx), http.StatusOK
}

func listBlockOps(ctx *server.Context) OpList {
	args := &OpsRequest{}
	ctx.ParseRequestArgs(args)
	block := loadBlock(ctx)

	// don't use offset/limit because we mix in endorsements
	r := etl.ListRequest{
		Mode:        args.TypeMode,
		Typs:        args.TypeList,
		Since:       block.Height,
		Until:       block.Height,
		Cursor:      args.Cursor,
		Order:       args.Order,
		WithStorage: args.WithStorage(),
	}

	if args.Sender.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Sender); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender account", err))
		} else {
			r.SenderId = a.RowId
		}
	}
	if args.Receiver.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Receiver); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such receiver account", err))
		} else {
			r.ReceiverId = a.RowId
		}
	}
	if args.Address.IsValid() {
		if a, err := ctx.Indexer.LookupAccount(ctx.Context, args.Address); err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such account", err))
		} else {
			r.Account = a
		}
	}

	var (
		endorse []*model.Endorsement
		err     error
	)
	if args.TypeList.IsEmpty() || args.TypeList.Contains(model.OpTypeEndorsement) {
		endorse, err = ctx.Indexer.ListBlockEndorsements(ctx, r)
		if err != nil {
			panic(server.EInternal(server.EC_DATABASE, "cannot read block endorsements", err))
		}
	}

	ops, err := ctx.Indexer.ListBlockOps(ctx, r)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read block operations", err))
	}
	resp := make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewOp(ctx, v, block, nil, args, cache), args.WithMerge())
	}
	if len(endorse) > 0 {
		for _, v := range endorse {
			resp.Append(NewOp(ctx, v.ToOp(), block, nil, args, cache), args.WithMerge())
		}
		sort.Slice(resp, func(i, j int) bool { return resp[i].OpN < resp[j].OpN })
	}

	// apply offset/limit
	if args.Offset > 0 || args.Limit > 0 {
		resp = resp[util.Min(len(resp), int(args.Offset)):]
		resp = resp[:util.Min(len(resp), int(args.Limit))]
	}

	return resp
}
