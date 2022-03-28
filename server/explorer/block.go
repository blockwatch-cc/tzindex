// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Block{})
}

var _ server.RESTful = (*Block)(nil)
var _ server.Resource = (*Block)(nil)

type Block struct {
	Hash              tezos.BlockHash        `json:"hash"`
	ParentHash        tezos.BlockHash        `json:"predecessor"`
	FollowerHash      tezos.BlockHash        `json:"successor"`
	Baker             tezos.Address          `json:"baker"`
	Proposer          tezos.Address          `json:"proposer"`
	Height            int64                  `json:"height"`
	Cycle             int64                  `json:"cycle"`
	IsCycleSnapshot   bool                   `json:"is_cycle_snapshot"`
	Timestamp         time.Time              `json:"time"`
	Solvetime         int                    `json:"solvetime"`
	Version           int                    `json:"version"`
	Round             int                    `json:"round"`
	Nonce             string                 `json:"nonce"`
	VotingPeriodKind  tezos.VotingPeriodKind `json:"voting_period_kind"`
	NSlotsEndorsed    int                    `json:"n_endorsed_slots"`
	NOps              int                    `json:"n_ops_applied"`
	NOpsFailed        int                    `json:"n_ops_failed"`
	NEvents           int                    `json:"n_events"`
	NContractCalls    int                    `json:"n_contract_calls"`
	Volume            float64                `json:"volume"`
	Fee               float64                `json:"fee"`
	Reward            float64                `json:"reward"`
	Deposit           float64                `json:"deposit"`
	ActivatedSupply   float64                `json:"activated_supply"`
	MintedSupply      float64                `json:"minted_supply"`
	BurnedSupply      float64                `json:"burned_supply"`
	SeenAccounts      int                    `json:"n_accounts"`
	NewAccounts       int                    `json:"n_new_accounts"`
	NewContracts      int                    `json:"n_new_contracts"`
	ClearedAccounts   int                    `json:"n_cleared_accounts"`
	FundedAccounts    int                    `json:"n_funded_accounts"`
	GasLimit          int64                  `json:"gas_limit"`
	GasUsed           int64                  `json:"gas_used"`
	StoragePaid       int64                  `json:"storage_paid"`
	PctAccountsReused float64                `json:"pct_account_reuse"`
	LbEscapeVote      bool                   `json:"lb_esc_vote"`
	LbEscapeEma       int64                  `json:"lb_esc_ema"`
	Metadata          map[string]*Metadata   `json:"metadata,omitempty"`
	Protocol          tezos.ProtocolHash     `json:"protocol"`

	// LEGACY
	Ops OpList `json:"ops,omitempty"`

	// caching
	expires time.Time `json:"-"`
	lastmod time.Time `json:"-"`
}

func NewBlock(ctx *server.Context, block *model.Block, args server.Options) *Block {
	p := ctx.Params
	if !p.ContainsHeight(block.Height) {
		p = ctx.Crawler.ParamsByHeight(block.Height)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], block.Nonce)
	b := &Block{
		Hash:             block.Hash,
		Baker:            ctx.Indexer.LookupAddress(ctx, block.BakerId),
		Proposer:         ctx.Indexer.LookupAddress(ctx, block.ProposerId),
		Height:           block.Height,
		Cycle:            block.Cycle,
		IsCycleSnapshot:  block.IsCycleSnapshot,
		Timestamp:        block.Timestamp,
		Solvetime:        block.Solvetime,
		Version:          block.Version,
		Round:            block.Round,
		Nonce:            hex.EncodeToString(buf[:]),
		VotingPeriodKind: block.VotingPeriodKind,
		NSlotsEndorsed:   block.NSlotsEndorsed,
		NOps:             model.Int16Correct(block.NOpsApplied),
		NOpsFailed:       model.Int16Correct(block.NOpsFailed),
		NEvents:          model.Int16Correct(block.NEvents),
		NContractCalls:   model.Int16Correct(block.NContractCalls),
		Volume:           p.ConvertValue(block.Volume),
		Fee:              p.ConvertValue(block.Fee),
		Reward:           p.ConvertValue(block.Reward),
		Deposit:          p.ConvertValue(block.Deposit),
		ActivatedSupply:  p.ConvertValue(block.ActivatedSupply),
		MintedSupply:     p.ConvertValue(block.MintedSupply),
		BurnedSupply:     p.ConvertValue(block.BurnedSupply),
		SeenAccounts:     model.Int16Correct(block.SeenAccounts),
		NewAccounts:      model.Int16Correct(block.NewAccounts),
		NewContracts:     model.Int16Correct(block.NewContracts),
		ClearedAccounts:  model.Int16Correct(block.ClearedAccounts),
		FundedAccounts:   model.Int16Correct(block.FundedAccounts),
		GasLimit:         block.GasLimit,
		GasUsed:          block.GasUsed,
		StoragePaid:      block.StoragePaid,
		LbEscapeVote:     block.LbEscapeVote,
		LbEscapeEma:      block.LbEscapeEma,
		Protocol:         p.Protocol,
	}
	nowHeight := ctx.Tip.BestHeight
	if b.SeenAccounts != 0 {
		b.PctAccountsReused = float64(b.SeenAccounts-b.NewAccounts) / float64(b.SeenAccounts) * 100
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

	if args.WithMeta() {
		// add metadata for baker and rights holders
		b.Metadata = make(map[string]*Metadata)

		// add baker
		if md, ok := lookupMetadataById(ctx, block.BakerId, 0, false); ok {
			b.Metadata[b.Baker.String()] = md
		}
	}

	if b.Height == nowHeight {
		// cache most recent block only until next block and endorsements are due
		b.expires = b.Timestamp.Add(p.BlockTime())
		b.lastmod = b.Timestamp
	} else if b.Height+p.MaxOperationsTTL >= nowHeight {
		// cache blocks in the reorg safety zone only until next block is expected
		b.expires = ctx.Tip.BestTime.Add(p.BlockTime())
		b.lastmod = b.Timestamp.Add(p.BlockTime())
	} else {
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
			case index.ErrNoBlockEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such block", err))
			case index.ErrInvalidBlockHeight:
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
			case index.ErrInvalidBlockHash:
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
		Mode:   args.TypeMode,
		Typs:   args.TypeList,
		Since:  block.Height,
		Until:  block.Height,
		Cursor: args.Cursor,
		Order:  args.Order,
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

	ops, err := ctx.Indexer.ListBlockOps(ctx, r)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot read block operations", err))
	}
	resp := make(OpList, 0)
	cache := make(map[int64]interface{})
	for _, v := range ops {
		resp.Append(NewOp(ctx, v, block, nil, args, cache), args.WithMerge())
	}

	// apply offset/limit
	if args.Offset > 0 || args.Limit > 0 {
		resp = resp[util.Min(len(resp), int(args.Offset)):]
		resp = resp[:util.Min(len(resp), int(args.Limit))]
	}

	return resp
}
