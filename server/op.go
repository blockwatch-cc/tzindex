// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"

	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

func init() {
	register(ExplorerOp{})
}

var (
	_ RESTful = (*ExplorerOp)(nil)
	_ RESTful = (*ExplorerOpList)(nil)
)

type ExplorerOpList []*ExplorerOp

func (t ExplorerOpList) LastModified() time.Time {
	return t[0].Timestamp
}

func (t ExplorerOpList) Expires() time.Time {
	return time.Time{}
}

func (t ExplorerOpList) RESTPrefix() string {
	return ""
}

func (t ExplorerOpList) RESTPath(r *mux.Router) string {
	return ""
}

func (t ExplorerOpList) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t ExplorerOpList) RegisterRoutes(r *mux.Router) error {
	return nil
}

type ExplorerOp struct {
	Hash         chain.OperationHash   `json:"hash"`
	Type         chain.OpType          `json:"type"`
	BlockHash    chain.BlockHash       `json:"block"`
	Timestamp    time.Time             `json:"time"`
	Height       int64                 `json:"height"`
	Cycle        int64                 `json:"cycle"`
	Counter      int64                 `json:"counter"`
	OpN          int                   `json:"op_n"`
	OpC          int                   `json:"op_c"`
	OpI          int                   `json:"op_i"`
	Status       string                `json:"status"`
	IsSuccess    bool                  `json:"is_success"`
	IsContract   bool                  `json:"is_contract"`
	GasLimit     int64                 `json:"gas_limit"`
	GasUsed      int64                 `json:"gas_used"`
	GasPrice     float64               `json:"gas_price"`
	StorageLimit int64                 `json:"storage_limit"`
	StorageSize  int64                 `json:"storage_size"`
	StoragePaid  int64                 `json:"storage_paid"`
	Volume       float64               `json:"volume"`
	Fee          float64               `json:"fee"`
	Reward       float64               `json:"reward"`
	Deposit      float64               `json:"deposit"`
	Burned       float64               `json:"burned"`
	IsInternal   bool                  `json:"is_internal"`
	HasData      bool                  `json:"has_data"`
	TDD          float64               `json:"days_destroyed"`
	Parameters   *micheline.Parameters `json:"parameters,omitempty"`
	Storage      *micheline.Prim       `json:"storage,omitempty"`
	Data         json.RawMessage       `json:"data,omitempty"`
	BigMapDiff   micheline.BigMapDiff  `json:"big_map_diff,omitempty"`
	Errors       json.RawMessage       `json:"errors,omitempty"`
	Sender       *chain.Address        `json:"sender,omitempty"`
	Receiver     *chain.Address        `json:"receiver,omitempty"`
	Manager      *chain.Address        `json:"manager,omitempty"`
	Delegate     *chain.Address        `json:"delegate,omitempty"`
}

func NewExplorerOp(ctx *ApiContext, op *model.Op, block *model.Block, p *chain.Params) *ExplorerOp {
	t := &ExplorerOp{
		Hash:         op.Hash,
		Type:         op.Type,
		Timestamp:    op.Timestamp,
		Height:       op.Height,
		Cycle:        op.Cycle,
		Counter:      op.Counter,
		OpN:          op.OpN,
		OpC:          op.OpC,
		OpI:          op.OpI,
		Status:       op.Status.String(),
		IsSuccess:    op.IsSuccess,
		IsContract:   op.IsContract,
		GasLimit:     op.GasLimit,
		GasUsed:      op.GasUsed,
		GasPrice:     op.GasPrice,
		StorageLimit: op.StorageLimit,
		StorageSize:  op.StorageSize,
		StoragePaid:  op.StoragePaid,
		Volume:       p.ConvertValue(op.Volume),
		Fee:          p.ConvertValue(op.Fee),
		Reward:       p.ConvertValue(op.Reward),
		Deposit:      p.ConvertValue(op.Deposit),
		Burned:       p.ConvertValue(op.Burned),
		IsInternal:   op.IsInternal,
		HasData:      op.HasData,
		TDD:          op.TDD,
	}
	if op.HasData {
		switch op.Type {
		case chain.OpTypeDoubleBakingEvidence, chain.OpTypeDoubleEndorsementEvidence:
			t.Data = json.RawMessage{}
			if err := json.Unmarshal([]byte(op.Data), &t.Data); err != nil {
				t.Data = nil
				log.Errorf("Unmarshal %s data: %v", op.Type, err)
			}
		default:
			if op.Data != "" {
				t.Data = json.RawMessage(strconv.Quote(op.Data))
			}
		}
	}

	// set params
	if len(op.Parameters) > 0 {
		t.Parameters = &micheline.Parameters{}
		if err := t.Parameters.UnmarshalBinary(op.Parameters); err != nil {
			log.Errorf("Unmarshal %s params: %v", op.Type, err)
		}
	}
	if len(op.Storage) > 0 {
		t.Storage = &micheline.Prim{}
		if err := t.Storage.UnmarshalBinary(op.Storage); err != nil {
			log.Errorf("Unmarshal %s storage: %v", op.Type, err)
		}
	}
	if len(op.BigMapDiff) > 0 {
		t.BigMapDiff = make(micheline.BigMapDiff, 0)
		if err := t.BigMapDiff.UnmarshalBinary(op.BigMapDiff); err != nil {
			log.Errorf("Unmarshal %s bigmap: %v", op.Type, err)
		}
	}

	if op.Errors != "" {
		t.Errors = json.RawMessage(op.Errors)
	}

	// set block hash
	if block != nil {
		t.BlockHash = block.Hash
	} else {
		b, err := ctx.Indexer.BlockByHeight(ctx.Context, op.Height)
		if err == nil {
			t.BlockHash = b.Hash
		}
	}

	// lookup accounts
	accs, err := ctx.Indexer.LookupAccountIds(ctx.Context,
		vec.UniqueUint64Slice([]uint64{
			op.SenderId.Value(),
			op.ReceiverId.Value(),
			op.ManagerId.Value(),
			op.DelegateId.Value(),
		}))
	if err != nil {
		log.Errorf("explorer op: cannot resolve accounts: %v", err)
	}
	for _, acc := range accs {
		if acc.RowId == op.SenderId {
			t.Sender = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
		if acc.RowId == op.ReceiverId {
			t.Receiver = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
		if acc.RowId == op.ManagerId {
			t.Manager = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
		if acc.RowId == op.DelegateId {
			t.Delegate = &chain.Address{
				Type: acc.Type,
				Hash: acc.Hash,
			}
		}
	}
	return t
}

func (t ExplorerOp) LastModified() time.Time {
	return t.Timestamp
}

func (t ExplorerOp) Expires() time.Time {
	return time.Time{}
}

func (t ExplorerOp) RESTPrefix() string {
	return "/explorer/op"
}

func (t ExplorerOp) RESTPath(r *mux.Router) string {
	path, _ := r.Get("op").URLPath("ident", t.Hash.String())
	return path.String()
}

func (t ExplorerOp) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (t ExplorerOp) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadOp)).Methods("GET").Name("op")
	return nil

}

// used when listing ops in block/account/contract context
type ExplorerOpsRequest struct {
	ExplorerListRequest
	Type chain.OpType `schema:"type"`
}

func loadOps(ctx *ApiContext) []*model.Op {
	if opIdent, ok := mux.Vars(ctx.Request)["ident"]; !ok || opIdent == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing operation hash", nil))
	} else {
		ops, err := ctx.Indexer.LookupOp(ctx, opIdent)
		if err != nil {
			switch err {
			case index.ErrNoOpEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no such operation", err))
			case etl.ErrInvalidHash:
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid operation hash", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return ops
	}
}

func ReadOp(ctx *ApiContext) (interface{}, int) {
	ops := loadOps(ctx)
	resp := make(ExplorerOpList, 0, len(ops))
	var params *chain.Params
	for _, v := range ops {
		if params == nil {
			params = ctx.Crawler.ParamsByHeight(v.Height)
		}
		resp = append(resp, NewExplorerOp(ctx, v, nil, params))
	}
	return resp, http.StatusOK
}
