// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

func (m *Indexer) ParamsByHeight(height int64) *chain.Params {
	return m.reg.GetParamsByHeight(height)
}

func (m *Indexer) ParamsByProtocol(proto chain.ProtocolHash) (*chain.Params, error) {
	return m.reg.GetParams(proto)
}

func (m *Indexer) ParamsByDeployment(v int) (*chain.Params, error) {
	return m.reg.GetParamsByDeployment(v)
}

func (m *Indexer) Table(key string) (*pack.Table, error) {
	t, ok := m.tables[key]
	if !ok {
		return nil, ErrNoTable
	}
	return t, nil
}

func (m *Indexer) TableStats() map[string]pack.TableStats {
	stats := make(map[string]pack.TableStats)
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			stats[t.Name()] = t.Stats()
		}
	}
	return stats
}

func (m *Indexer) MemStats() map[string]pack.TableSizeStats {
	stats := make(map[string]pack.TableSizeStats)
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			stats[t.Name()] = t.Size()
		}
	}
	return stats
}

func (m *Indexer) ChainByHeight(ctx context.Context, height int64) (*model.Chain, error) {
	table, err := m.Table(index.ChainTableKey)
	if err != nil {
		return nil, err
	}
	res, err := table.Query(ctx, pack.Query{
		Name: "api.search_chain_height",
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("h"), // search for block height
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Rows() == 0 {
		return nil, index.ErrNoChainEntry
	}
	c := &model.Chain{}
	err = res.DecodeAt(0, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Indexer) SupplyByHeight(ctx context.Context, height int64) (*model.Supply, error) {
	table, err := m.Table(index.SupplyTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.search_supply_height",
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("h"), // search for block height
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	var count int
	s := &model.Supply{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		count++
		return r.Decode(s)
	})
	if count == 0 {
		return nil, index.ErrNoSupplyEntry
	}
	return s, nil
}

func (m *Indexer) SupplyByTime(ctx context.Context, t time.Time) (*model.Supply, error) {
	table, err := m.Table(index.SupplyTableKey)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	from, to := t, now
	if from.After(to) {
		from, to = to, from
	}
	q := pack.Query{
		Name: "api.search_supply_time",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("T"), // search for timestamp
				Mode:  pack.FilterModeRange,
				From:  from,
				To:    to,
			},
			pack.Condition{
				Field: table.Fields().Find("h"), // height larger than supply init block 1
				Mode:  pack.FilterModeGte,
				Value: int64(1),
			}},
		Limit: 1,
	}

	var count int
	s := &model.Supply{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		count++
		return r.Decode(s)
	})
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoSupplyEntry
	}
	return s, nil
}

type Growth struct {
	NewAccounts         int64
	NewImplicitAccounts int64
	NewManagedAccounts  int64
	NewContracts        int64
	ClearedAccounts     int64
	FundedAccounts      int64
}

func (m *Indexer) GrowthByDuration(ctx context.Context, to time.Time, d time.Duration) (*Growth, error) {
	table, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	type XBlock struct {
		NewAccounts         int64 `pack:"A,snappy"`
		NewImplicitAccounts int64 `pack:"i,snappy"`
		NewManagedAccounts  int64 `pack:"m,snappy"`
		NewContracts        int64 `pack:"C,snappy"`
		ClearedAccounts     int64 `pack:"E,snappy"`
		FundedAccounts      int64 `pack:"J,snappy"`
	}
	from := to.Add(-d)
	q := pack.Query{
		Name:   "api.aggregate_growth",
		Fields: table.Fields().Select("A", "i", "m", "C", "E", "J"),
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("T"), // search for timestamp
			Mode:  pack.FilterModeRange,
			From:  from,
			To:    to,
		}},
	}
	x := &XBlock{}
	g := &Growth{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(x); err != nil {
			return err
		}
		g.NewAccounts += x.NewAccounts
		g.NewImplicitAccounts += x.NewImplicitAccounts
		g.NewManagedAccounts += x.NewManagedAccounts
		g.NewContracts += x.NewContracts
		g.ClearedAccounts += x.ClearedAccounts
		g.FundedAccounts += x.FundedAccounts
		return nil
	})
	if err != nil {
		return nil, err
	}
	return g, nil
}

// called concurrently from API consumers, uses read-mostly cache
func (m *Indexer) BlockTime(ctx context.Context, height int64) time.Time {
	av := m.times.Load()
	if av == nil {
		// lazy init
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again after aquiring the lock
		av = m.times.Load()
		if av == nil {
			times, err := m.buildBlockTimes(ctx)
			if err != nil {
				log.Errorf("init block time cache: %v", err)
				return time.Time{}
			}
			m.times.Store(times)
			av = times
		}
	}
	times := av.([]uint32)
	if height > 0 && len(times) > int(height) {
		return time.Unix(int64(times[int(height)])+int64(times[0]), 0).UTC()
	}
	return time.Time{}
}

func (m *Indexer) BlockTimeMs(ctx context.Context, height int64) int64 {
	if height == 0 {
		return 0
	}
	tm := m.BlockTime(ctx, height)
	return tm.Unix() * 1000
}

func (m *Indexer) BlockHeightFromTime(ctx context.Context, tm time.Time) int64 {
	av := m.times.Load()
	if av == nil {
		// lazy init
		m.mu.Lock()
		defer m.mu.Unlock()
		// check again after aquiring the lock
		av = m.times.Load()
		if av == nil {
			times, err := m.buildBlockTimes(ctx)
			if err != nil {
				log.Errorf("init block time cache: %v", err)
				return 0
			}
			m.times.Store(times)
			av = times
		}
	}
	times := av.([]uint32)
	if len(times) == 0 || !tm.After(time.Unix(int64(times[0]), 0)) {
		return 0
	}
	tsdiff := uint32(tm.Unix() - int64(times[0]))
	l := len(times)
	i := sort.Search(l, func(i int) bool { return times[i] >= tsdiff })
	if i == l {
		return int64(l - 1)
	}
	if times[i] == tsdiff {
		return int64(i)
	}
	return int64(i - 1)
}

func (m *Indexer) BlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	res, err := blocks.Lookup(ctx, []uint64{id})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Rows() == 0 {
		return nil, index.ErrNoBlockEntry
	}
	b := model.AllocBlock()
	err = res.DecodeAt(0, b)
	if err != nil {
		b.Free()
		return nil, err
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

// find a block's canonical successor (non-orphan)
func (m *Indexer) BlockByParentId(ctx context.Context, id uint64) (*model.Block, error) {
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := model.AllocBlock()
	err = blocks.Stream(ctx, pack.Query{
		Name:  "api.search_block_by_parent",
		Limit: 1,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: blocks.Fields().Find("P"), // search for parent id
				Mode:  pack.FilterModeEqual,
				Value: id,
			},
			pack.Condition{
				Field: blocks.Fields().Find("Z"), // non-orphan
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(b)
	})
	if err != nil {
		b.Free()
		return nil, err
	}
	if b.RowId == 0 {
		b.Free()
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

func (m *Indexer) BlockHashByHeight(ctx context.Context, height int64) (chain.BlockHash, error) {
	type XBlock struct {
		Hash chain.BlockHash `pack:"H"`
	}
	b := &XBlock{}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return b.Hash, err
	}
	err = blocks.Stream(ctx, pack.Query{
		Name:   "api.search_block_height",
		Fields: blocks.Fields().Select("H"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: blocks.Fields().Find("h"), // search for block height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
			pack.Condition{
				Field: blocks.Fields().Find("Z"), // search for non-orphan blocks
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(b)
	})
	if err != nil {
		return b.Hash, err
	}
	if !b.Hash.IsValid() {
		return b.Hash, index.ErrNoBlockEntry
	}
	return b.Hash, nil
}

func (m *Indexer) BlockHashById(ctx context.Context, id uint64) (chain.BlockHash, error) {
	type XBlock struct {
		Hash chain.BlockHash `pack:"H"`
	}
	b := &XBlock{}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return b.Hash, err
	}
	err = blocks.Stream(ctx, pack.Query{
		Name:   "api.search_block_height",
		Fields: blocks.Fields().Select("H"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: blocks.Fields().Find("I"), // search for pk
				Mode:  pack.FilterModeEqual,
				Value: id,
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(b)
	})
	if err != nil {
		return b.Hash, err
	}
	if !b.Hash.IsValid() {
		return b.Hash, index.ErrNoBlockEntry
	}
	return b.Hash, nil
}

func (m *Indexer) BlockByHeight(ctx context.Context, height int64) (*model.Block, error) {
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := model.AllocBlock()
	err = blocks.Stream(ctx, pack.Query{
		Name: "api.search_block_height",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: blocks.Fields().Find("h"), // search for block height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
			pack.Condition{
				Field: blocks.Fields().Find("Z"), // search for non-orphan blocks
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(b)
	})
	if err != nil {
		b.Free()
		return nil, err
	}
	if b.RowId == 0 {
		b.Free()
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

func (m *Indexer) BlockByHash(ctx context.Context, h chain.BlockHash) (*model.Block, error) {
	if !h.IsValid() {
		return nil, fmt.Errorf("invalid block hash %s", h)
	}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := model.AllocBlock()
	err = blocks.Stream(ctx, pack.Query{
		Name: "api.search_block_hash",
		Conditions: pack.ConditionList{pack.Condition{
			Field: blocks.Fields().Find("H"), // search for block hash
			Mode:  pack.FilterModeEqual,
			Value: h.Hash.Hash[:],
		}},
	}, func(r pack.Row) error {
		return r.Decode(b)
	})
	if err != nil {
		b.Free()
		return nil, err
	}
	if b.RowId == 0 {
		b.Free()
		return nil, index.ErrNoBlockEntry
	}
	b.Params, _ = m.reg.GetParamsByDeployment(b.Version)
	return b, nil
}

func (m *Indexer) LookupBlock(ctx context.Context, blockIdent string) (*model.Block, error) {
	var (
		b   *model.Block
		err error
	)
	switch true {
	case blockIdent == "head":
		b, err = m.BlockByHeight(ctx, m.tips[index.BlockTableKey].Height)
	case len(blockIdent) == chain.HashTypeBlock.Base58Len() || chain.HashTypeBlock.MatchPrefix(blockIdent):
		// assume it's a hash
		var blockHash chain.BlockHash
		blockHash, err = chain.ParseBlockHash(blockIdent)
		if err != nil {
			return nil, index.ErrInvalidBlockHash
		}
		b, err = m.BlockByHash(ctx, blockHash)
	default:
		// try parsing as height
		var blockHeight int64
		blockHeight, err = strconv.ParseInt(blockIdent, 10, 64)
		if err != nil {
			return nil, index.ErrInvalidBlockHeight
		}
		b, err = m.BlockByHeight(ctx, blockHeight)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (m *Indexer) LookupLastBakedBlock(ctx context.Context, a *model.Account) (*model.Block, error) {
	if a.BlocksBaked == 0 {
		return nil, index.ErrNoBlockEntry
	}
	blocks, err := m.Table(index.BlockTableKey)
	if err != nil {
		return nil, err
	}
	b := model.AllocBlock()
	err = blocks.Stream(ctx, pack.Query{
		Name:  "api.search_last_baked",
		Order: pack.OrderDesc,
		Limit: 1,
		Conditions: pack.ConditionList{pack.Condition{
			Field: blocks.Fields().Find("h"), // from block height
			Mode:  pack.FilterModeGte,
			Value: a.FirstSeen,
		}, pack.Condition{
			Field: blocks.Fields().Find("h"), // to block height
			Mode:  pack.FilterModeLte,
			Value: a.LastSeen,
		}, pack.Condition{
			Field: blocks.Fields().Find("B"), // baker id
			Mode:  pack.FilterModeEqual,
			Value: a.RowId.Value(),
		}},
	}, func(r pack.Row) error {
		return r.Decode(b)
	})
	if err != nil {
		return nil, err
	}
	if b.RowId == 0 {
		return nil, index.ErrNoBlockEntry
	}
	return b, nil
}

func (m *Indexer) LookupLastEndorsedBlock(ctx context.Context, a *model.Account) (*model.Block, error) {
	if a.BlocksBaked == 0 {
		return nil, index.ErrNoBlockEntry
	}
	ops, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	var op model.Op
	err = ops.Stream(ctx, pack.Query{
		Name:   "api.search_last_endorsed",
		Fields: ops.Fields().Select("h", "S", "t"),
		Order:  pack.OrderDesc,
		Limit:  1,
		Conditions: pack.ConditionList{pack.Condition{
			Field: ops.Fields().Find("h"), // from block height
			Mode:  pack.FilterModeGte,
			Value: a.FirstSeen,
		}, pack.Condition{
			Field: ops.Fields().Find("h"), // to block height
			Mode:  pack.FilterModeLte,
			Value: a.LastSeen,
		}, pack.Condition{
			Field: ops.Fields().Find("S"), // sender id
			Mode:  pack.FilterModeEqual,
			Value: a.RowId.Value(),
		}, pack.Condition{
			Field: ops.Fields().Find("t"), // op type
			Mode:  pack.FilterModeEqual,
			Value: int64(chain.OpTypeEndorsement),
		}},
	}, func(r pack.Row) error {
		return r.Decode(&op)
	})
	if err != nil {
		return nil, err
	}
	if op.Height == 0 {
		return nil, index.ErrNoBlockEntry
	}
	return m.BlockByHeight(ctx, op.Height)
}

func (m *Indexer) LookupNextRight(ctx context.Context, a *model.Account, height int64, typ chain.RightType, prio int64) (*model.Right, error) {
	rights, err := m.Table(index.RightsTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:   "api.search_next_baking",
		Fields: rights.Fields().Select("h", "t", "A", "p"),
		Conditions: pack.ConditionList{pack.Condition{
			Field: rights.Fields().Find("h"), // from block height
			Mode:  pack.FilterModeGt,
			Value: height,
		}, pack.Condition{
			Field: rights.Fields().Find("t"), // right type
			Mode:  pack.FilterModeEqual,
			Value: int64(typ),
		}, pack.Condition{
			Field: rights.Fields().Find("A"), // delegate id
			Mode:  pack.FilterModeEqual,
			Value: a.RowId.Value(),
		}},
		Limit: 1,
	}
	if prio >= 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: rights.Fields().Find("p"), // priority
			Mode:  pack.FilterModeEqual,
			Value: int64(0),
		})
	}
	right := &model.Right{}
	err = rights.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(right)
	})
	if err != nil {
		return nil, err
	}
	if right.RowId == 0 {
		return nil, index.ErrNoRightsEntry
	}
	return right, nil
}

func (m *Indexer) ListBlockEndorsingRights(ctx context.Context, height int64) ([]model.Right, error) {
	rights, err := m.Table(index.RightsTableKey)
	if err != nil {
		return nil, err
	}
	resp := make([]model.Right, 0, 32)
	right := model.Right{}
	err = rights.Stream(ctx, pack.Query{
		Name:   "api.search_block_endorsing",
		Fields: rights.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: rights.Fields().Find("h"), // from block height
				Mode:  pack.FilterModeEqual,
				Value: height,
			}, pack.Condition{
				Field: rights.Fields().Find("t"), // type == endorsing
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.RightTypeEndorsing),
			}},
	}, func(r pack.Row) error {
		if err := r.Decode(&right); err != nil {
			return err
		}
		resp = append(resp, right)
		return nil
	})
	return resp, nil
}

func (m *Indexer) LookupAccount(ctx context.Context, addr chain.Address) (*model.Account, error) {
	if !addr.IsValid() {
		return nil, ErrInvalidHash
	}

	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}

	res, err := table.Query(ctx, pack.Query{
		Name: "api.search_account_hash",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("H"), // hash
				Mode:  pack.FilterModeEqual,
				Value: addr.Hash, // must be []byte
			},
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(addr.Type), // must be int64
			}},
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Rows() == 0 {
		return nil, index.ErrNoAccountEntry
	}
	acc := model.AllocAccount()
	if err := res.DecodeAt(0, acc); err != nil {
		acc.Free()
		return nil, err
	}
	return acc, nil
}

func (m *Indexer) LookupContract(ctx context.Context, addr chain.Address) (*model.Contract, error) {
	if !addr.IsValid() {
		return nil, ErrInvalidHash
	}

	table, err := m.Table(index.ContractTableKey)
	if err != nil {
		return nil, err
	}

	// use hash and type to protect against duplicates
	cc := model.AllocContract()
	err = table.Stream(ctx, pack.Query{
		Name: "api.search_contract_hash",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("H"), // hash
				Mode:  pack.FilterModeEqual,
				Value: addr.Hash, // must be []byte
			}},
	}, func(r pack.Row) error {
		return r.Decode(cc)
	})
	if err != nil {
		cc.Free()
		return nil, err
	}
	if cc.RowId == 0 {
		cc.Free()
		// try account lookup and stitch manager.tz for pre-babylon KT1's
		if acc, err := m.LookupAccount(ctx, addr); err == nil {
			if c, err := acc.ManagerContract(); err == nil {
				cc = c
			}
		} else {
			return nil, index.ErrNoContractEntry
		}
	}
	return cc, nil
}

func (m *Indexer) LookupContractId(ctx context.Context, id model.AccountID) (*model.Contract, error) {
	table, err := m.Table(index.ContractTableKey)
	if err != nil {
		return nil, err
	}

	// use hash and type to protect against duplicates
	cc := model.AllocContract()
	err = table.Stream(ctx, pack.Query{
		Name: "api.search_contract_hash",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("A"), // account_id
				Mode:  pack.FilterModeEqual,
				Value: id.Value(),
			}},
	}, func(r pack.Row) error {
		return r.Decode(cc)
	})
	if err != nil {
		cc.Free()
		return nil, err
	}
	if cc.RowId == 0 {
		cc.Free()
		// try account lookup and stitch manager.tz for pre-babylon KT1's
		if acc, err := m.LookupAccountId(ctx, id); err == nil {
			if c, err := acc.ManagerContract(); err == nil {
				cc = c
			}
		} else {
			return nil, index.ErrNoContractEntry
		}
	}
	return cc, nil
}

func (m *Indexer) LookupAccountId(ctx context.Context, id model.AccountID) (*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	a := model.AllocAccount()
	err = table.StreamLookup(ctx, []uint64{id.Value()}, func(r pack.Row) error {
		return r.Decode(a)
	})
	if err != nil {
		a.Free()
		return nil, err
	}
	if a.RowId == 0 {
		return nil, index.ErrNoAccountEntry
	}
	return a, nil
}

func (m *Indexer) LookupAccountIds(ctx context.Context, ids []uint64) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	accs := make([]*model.Account, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(accs) {
			return io.EOF
		}
		a := model.AllocAccount()
		if err := r.Decode(a); err != nil {
			a.Free()
			return err
		}
		accs[count] = a
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoAccountEntry
	}
	accs = accs[:count]
	return accs, nil
}

func (m *Indexer) ListAllDelegates(ctx context.Context) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.list_all_delegates",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("d"), // is_delegate
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
	accs := make([]*model.Account, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		acc := model.AllocAccount()
		if err := r.Decode(acc); err != nil {
			acc.Free()
			return err
		}
		accs = append(accs, acc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return accs, nil
}

func (m *Indexer) ListActiveDelegates(ctx context.Context) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.list_active_delegates",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("d"), // is_delegate
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
			pack.Condition{
				Field: table.Fields().Find("v"), // is_active_delegate
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
	accs := make([]*model.Account, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		acc := model.AllocAccount()
		if err := r.Decode(acc); err != nil {
			acc.Free()
			return err
		}
		accs = append(accs, acc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return accs, nil
}

func (m *Indexer) ListManaged(ctx context.Context, id model.AccountID, offset, limit uint) ([]*model.Account, error) {
	table, err := m.Table(index.AccountTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.list_managed",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("M"), // manager id
				Mode:  pack.FilterModeEqual,
				Value: id.Value(),
			},
		},
	}
	accs := make([]*model.Account, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		acc := model.AllocAccount()
		if err := r.Decode(acc); err != nil {
			acc.Free()
			return err
		}
		accs = append(accs, acc)
		if limit > 0 && len(accs) >= int(limit) {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return accs, nil
}

func (m *Indexer) LookupOp(ctx context.Context, ophash string) ([]*model.Op, error) {
	oh, err := chain.ParseOperationHash(ophash)
	if err != nil {
		return nil, ErrInvalidHash
	}
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.search_tx_hash",
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("H"), // search for hash
			Mode:  pack.FilterModeEqual,
			Value: oh.Hash.Hash[:],
		}},
	}
	ops := make([]*model.Op, 0, 2)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		op := model.AllocOp()
		if err := r.Decode(op); err != nil {
			op.Free()
			return err
		}
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, index.ErrNoOpEntry
	}
	return ops, nil
}

func (m *Indexer) FindActivatedAccount(ctx context.Context, addr chain.Address) (*model.Account, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	type Xop struct {
		SenderId  model.AccountID `pack:"S,snappy"`
		ManagerId model.AccountID `pack:"M,snappy"`
		Data      string          `pack:"a,snappy"`
	}
	q := pack.Query{
		Name:    "api.search_activation",
		NoCache: true,
		Fields:  table.Fields().Select("S", "M", "a"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.OpTypeActivateAccount),
			},
		},
	}
	var o Xop
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(&o); err != nil {
			return err
		}
		// data contains hex(secret),blinded_address
		data := strings.Split(o.Data, ",")
		if len(data) != 2 {
			// skip broken records
			return nil
		}
		ba, err := chain.DecodeBlindedAddress(data[1])
		if err != nil {
			// skip broken records
			return nil
		}
		if addr.IsEqual(ba) {
			return io.EOF // found
		}
		return nil
	})
	if err != io.EOF {
		if err == nil {
			err = index.ErrNoAccountEntry
		}
		return nil, err
	}
	if o.ManagerId != 0 {
		return m.LookupAccountId(ctx, o.ManagerId)
	}
	return m.LookupAccountId(ctx, o.SenderId)
}

func (m *Indexer) FindLatestDelegation(ctx context.Context, id model.AccountID) (*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:    "api.search_delegation",
		NoCache: true,
		Fields:  table.Fields(),
		Order:   pack.OrderDesc,
		Limit:   1,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.OpTypeDelegation),
			},
			pack.Condition{
				Field: table.Fields().Find("S"), // search for sender account id
				Mode:  pack.FilterModeEqual,
				Value: id.Value(),
			},
			pack.Condition{
				Field: table.Fields().Find("D"), // delegate id
				Mode:  pack.FilterModeNotEqual,
				Value: uint64(0),
			},
		},
	}
	o := &model.Op{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(o)
	})
	if err != nil {
		return nil, err
	}
	if o.RowId == 0 {
		return nil, index.ErrNoOpEntry
	}
	return o, nil
}

func (m *Indexer) FindOrigination(ctx context.Context, id model.AccountID) (*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:    "api.search_origination",
		NoCache: true,
		Fields:  table.Fields(),
		Limit:   1,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.OpTypeOrigination),
			},
			pack.Condition{
				Field: table.Fields().Find("R"), // search for account id
				Mode:  pack.FilterModeEqual,
				Value: id.Value(),
			},
		},
	}
	o := &model.Op{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(o)
	})
	if err != nil {
		return nil, err
	}
	if o.RowId == 0 {
		return nil, index.ErrNoOpEntry
	}
	return o, nil
}

func (m *Indexer) LookupOpIds(ctx context.Context, ids []uint64) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	ops := make([]*model.Op, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(ops) {
			return io.EOF
		}
		op := model.AllocOp()
		if err := r.Decode(op); err != nil {
			op.Free()
			return err
		}
		ops[count] = op
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoOpEntry
	}
	ops = ops[:count]
	return ops, nil
}

// Note: offset and limit count in transactions
func (m *Indexer) ListBlockOps(ctx context.Context, height int64, typ chain.OpType, offset, limit uint) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.list_block_ops",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("h"), // search for block height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		},
		Limit: int(offset + limit),
	}
	if typ.IsValid() {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("t"), // search op type
			Mode:  pack.FilterModeEqual,
			Value: int64(typ), // must be int64 type
		})
	}
	ops := make([]*model.Op, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		op := model.AllocOp()
		if err := r.Decode(op); err != nil {
			op.Free()
			return err
		}
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ops, nil
}

// Note:
// - OR queries are not supported by pack table yet!
// - order is defined by funding or spending operation
// - offset and limit counts in ops
func (m *Indexer) ListAccountOps(ctx context.Context, accId model.AccountID, typ chain.OpType, since, until int64, offset, limit uint, order pack.OrderType) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	// list all ops where this address is sender OR receiver (high traffic addresses
	// may have many, so we use query limits)
	q := pack.Query{
		Name:   "api.list_account_ops_sent",
		Order:  order,
		Fields: table.Fields(),
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("S"), // search for sender account id
			Mode:  pack.FilterModeEqual,
			Value: accId.Value(),
		}},
		Limit: int(offset + limit),
	}
	if since > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeGt,
			Value: since,
		})
	}
	if until > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeLte,
			Value: until,
		})
	}
	if typ.IsValid() {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("t"), // search op type
			Mode:  pack.FilterModeEqual,
			Value: int64(typ), // must be int64 type
		})
	}
	ops := make([]*model.Op, 0, util.NonZero(2*int(offset+limit), 512))
	err = table.Stream(ctx, q, func(r pack.Row) error {
		op := model.AllocOp()
		if err := r.Decode(op); err != nil {
			op.Free()
			return err
		}
		ops = append(ops, op)
		if len(ops) == int(limit+offset) {
			return io.EOF
		}
		if len(ops)%512 == 0 {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	// same for receivers
	q = pack.Query{
		Name:   "api.list_account_ops_recv",
		Fields: table.Fields(),
		Order:  order,
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("R"), // search for receiver account id
			Mode:  pack.FilterModeEqual,
			Value: accId.Value(),
		}},
		Limit: int(offset + limit),
	}
	if since > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeGt,
			Value: since,
		})
	}
	if until > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeLte,
			Value: until,
		})
	}
	if typ.IsValid() {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("t"), // search op type
			Mode:  pack.FilterModeEqual,
			Value: int64(typ), // must be int64 type
		})
	}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		op := model.AllocOp()
		if err := r.Decode(op); err != nil {
			op.Free()
			return err
		}
		ops = append(ops, op)
		if len(ops) == 2*int(limit+offset) {
			return io.EOF
		}
		if len(ops)%512 == 0 {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	// sort
	if order == pack.OrderAsc {
		sort.Slice(ops, func(i, j int) bool { return ops[i].RowId < ops[j].RowId })
	} else {
		sort.Slice(ops, func(i, j int) bool { return ops[i].RowId > ops[j].RowId })
	}
	// cut offset and limit
	for i := 0; i < len(ops) && i < int(offset); i++ {
		ops[i].Free()
	}
	for i := int(offset + limit); i < len(ops); i++ {
		ops[i].Free()
	}
	ops = ops[util.Min(int(offset), len(ops)):util.Min(int(offset+limit), len(ops))]
	return ops, nil
}

func (m *Indexer) ListContractCalls(ctx context.Context, accId model.AccountID, since, until int64, offset, limit uint, order pack.OrderType) ([]*model.Op, error) {
	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	// list all tx (calls) received by this address
	q := pack.Query{
		Name:   "api.list_contract_calls",
		Fields: table.Fields(),
		Order:  order,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("R"), // search for receiver account id
				Mode:  pack.FilterModeEqual,
				Value: accId.Value(),
			},
			pack.Condition{
				Field: table.Fields().Find("t"), // search op type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.OpTypeTransaction), // must be int64 type
			},
			// list originations and non-parameter transactions
			// pack.Condition{
			// 	Field: table.Fields().Find("w"), // must have data to be a call
			// 	Mode:  pack.FilterModeEqual,
			// 	Value: true,
			// },
		},
		Limit: int(offset + limit),
	}
	if since > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeGt,
			Value: since,
		})
	}
	if until > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeLte,
			Value: until,
		})
	}
	ops := make([]*model.Op, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		op := model.AllocOp()
		if err := r.Decode(op); err != nil {
			op.Free()
			return err
		}
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ops, nil
}

func (m *Indexer) FindLastCall(ctx context.Context, acc model.AccountID, height int64) (*model.Op, error) {
	// load account for last-seen optimization
	a, err := m.LookupAccountId(ctx, acc)
	if err != nil {
		return nil, err
	}
	height = util.Min64(height, a.LastSeen)

	table, err := m.Table(index.OpTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:  "api.search_last_call",
		Limit: 1,
		Order: pack.OrderDesc,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("R"), // receiver_id
				Mode:  pack.FilterModeEqual,
				Value: acc.Value(),
			},
			pack.Condition{
				Field: table.Fields().Find("t"), // type
				Mode:  pack.FilterModeEqual,
				Value: int64(chain.OpTypeTransaction),
			},
			pack.Condition{
				Field: table.Fields().Find("C"), // is_contract
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
			pack.Condition{
				Field: table.Fields().Find("!"), // is_success
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
			pack.Condition{
				Field: table.Fields().Find("w"), // has_data
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
	if height > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeLte,
			Value: height,
		})
	}
	op := model.AllocOp()
	err = table.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(op)
	})
	if err != nil {
		op.Free()
		return nil, err
	}
	if op.RowId == 0 {
		op.Free()
		return nil, index.ErrNoOpEntry
	}
	return op, nil
}

func (m *Indexer) ListContractBigMapIds(ctx context.Context, acc model.AccountID) ([]int64, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name: "api.search_bigmaps",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("A"), // account_id
				Mode:  pack.FilterModeEqual,
				Value: acc.Value(),
			},
			pack.Condition{
				Field: table.Fields().Find("a"), // action
				Mode:  pack.FilterModeEqual,
				Value: uint64(micheline.BigMapDiffActionAlloc), // byte -> uint
			},
		},
	}
	ids := make([]int64, 0)
	bmi := &model.BigMapItem{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(bmi); err != nil {
			return err
		}
		ids = append(ids, bmi.BigMapId)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (m *Indexer) Flush(ctx context.Context) error {
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			log.Debugf("Flushing %s.", t.Name())
			if err := t.Flush(ctx); err != nil {
				return err
			}
		}
		if err := m.storeTip(idx.Key()); err != nil {
			return err
		}
	}
	return nil
}

func (m *Indexer) FlushJournals(ctx context.Context) error {
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			log.Debugf("Flushing %s.", t.Name())
			if err := t.FlushJournal(ctx); err != nil {
				return err
			}
		}
		if err := m.storeTip(idx.Key()); err != nil {
			return err
		}
	}
	return nil
}

func (m *Indexer) GC(ctx context.Context, ratio float64) error {
	if err := m.Flush(ctx); err != nil {
		return err
	}
	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}
	for _, idx := range m.indexes {
		for _, t := range idx.Tables() {
			log.Infof("Compacting %s.", t.Name())
			if err := t.Compact(ctx); err != nil {
				return err
			}
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
		db := idx.DB()
		log.Infof("Garbage collecting %s (%s).", idx.Name(), db.Path())
		if err := db.GC(ctx, ratio); err != nil {
			return err
		}
		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}
	}
	return nil
}

func (m *Indexer) ElectionByHeight(ctx context.Context, height int64) (*model.Election, error) {
	table, err := m.Table(index.ElectionTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:    "api.search_election_height",
		NoCache: true,
		Fields:  table.Fields(),
		Order:   pack.OrderDesc,
		Limit:   1,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("H"), // start height
				Mode:  pack.FilterModeLte,
				Value: height,
			},
		},
	}
	election := &model.Election{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(election)
	})
	if err != nil {
		return nil, err
	}
	if election.RowId == 0 {
		return nil, index.ErrNoElectionEntry
	}
	return election, nil
}

func (m *Indexer) ElectionById(ctx context.Context, eid model.ElectionID) (*model.Election, error) {
	table, err := m.Table(index.ElectionTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:    "api.search_election_id",
		NoCache: true,
		Fields:  table.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("I"), // row id
				Mode:  pack.FilterModeEqual,
				Value: eid.Value(),
			},
		},
	}
	election := &model.Election{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(election)
	})
	if err != nil {
		return nil, err
	}
	if election.RowId == 0 {
		return nil, index.ErrNoElectionEntry
	}
	return election, nil
}

func (m *Indexer) VotesByElection(ctx context.Context, eid model.ElectionID) ([]*model.Vote, error) {
	table, err := m.Table(index.VoteTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:    "api.list_votes",
		NoCache: true,
		Fields:  table.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("E"), // election id
				Mode:  pack.FilterModeEqual,
				Value: eid.Value(),
			},
		},
	}
	votes := make([]*model.Vote, 0, 4)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		vote := &model.Vote{}
		err := r.Decode(vote)
		votes = append(votes, vote)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(votes) == 0 {
		return nil, index.ErrNoVoteEntry
	}
	return votes, nil
}

func (m *Indexer) ProposalsByElection(ctx context.Context, eid model.ElectionID) ([]*model.Proposal, error) {
	table, err := m.Table(index.ProposalTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:    "api.list_proposals",
		NoCache: true,
		Fields:  table.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("E"), // election id
				Mode:  pack.FilterModeEqual,
				Value: eid.Value(),
			},
		},
	}
	proposals := make([]*model.Proposal, 0, 20)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		p := &model.Proposal{}
		err := r.Decode(p)
		proposals = append(proposals, p)
		return err
	})
	if err != nil {
		return nil, err
	}
	return proposals, nil
}

func (m *Indexer) LookupProposal(ctx context.Context, proto chain.ProtocolHash) (*model.Proposal, error) {
	if !proto.IsValid() {
		return nil, ErrInvalidHash
	}

	table, err := m.Table(index.ProposalTableKey)
	if err != nil {
		return nil, err
	}

	res, err := table.Query(ctx, pack.Query{
		Name: "api.search_proposal_hash",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("H"), // hash
				Mode:  pack.FilterModeEqual,
				Value: proto.Hash.Hash, // must be []byte
			}},
	})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Rows() == 0 {
		return nil, index.ErrNoProposalEntry
	}
	prop := &model.Proposal{}
	if err := res.DecodeAt(0, prop); err != nil {
		return nil, err
	}
	return prop, nil
}

func (m *Indexer) LookupProposalIds(ctx context.Context, ids []uint64) ([]*model.Proposal, error) {
	table, err := m.Table(index.ProposalTableKey)
	if err != nil {
		return nil, err
	}
	props := make([]*model.Proposal, len(ids))
	var count int
	err = table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(props) {
			return io.EOF
		}
		p := &model.Proposal{}
		if err := r.Decode(p); err != nil {
			return err
		}
		props[count] = p
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, index.ErrNoProposalEntry
	}
	props = props[:count]
	return props, nil
}

func (m *Indexer) ListAccountBallots(ctx context.Context, accId model.AccountID, offset, limit uint) ([]*model.Ballot, error) {
	table, err := m.Table(index.BallotTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:   "api.search_account_ballots",
		Fields: table.Fields(),
		Conditions: pack.ConditionList{pack.Condition{
			Field: table.Fields().Find("S"), // search for source account id
			Mode:  pack.FilterModeEqual,
			Value: accId.Value(),
		}},
	}
	ballots := make([]*model.Ballot, 0, util.NonZero(int(limit), 512))
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		b := &model.Ballot{}
		if err := r.Decode(b); err != nil {
			return err
		}
		ballots = append(ballots, b)
		if len(ballots) == int(limit) {
			return io.EOF
		}
		if len(ballots)%512 == 0 {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return ballots, nil
}

func (m *Indexer) LookupSnapshot(ctx context.Context, accId model.AccountID, cycle, idx int64) (*model.Snapshot, error) {
	table, err := m.Table(index.SnapshotTableKey)
	if err != nil {
		return nil, err
	}

	snap := model.NewSnapshot()
	err = table.Stream(ctx, pack.Query{
		Name: "api.search_snapshot",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("c"), // cycle
				Mode:  pack.FilterModeEqual,
				Value: cycle,
			},
			pack.Condition{
				Field: table.Fields().Find("i"), // index
				Mode:  pack.FilterModeEqual,
				Value: idx,
			},
			pack.Condition{
				Field: table.Fields().Find("a"), // account id
				Mode:  pack.FilterModeEqual,
				Value: accId.Value(),
			},
		},
		Limit: 1,
	}, func(r pack.Row) error {
		return r.Decode(snap)
	})
	if err != nil {
		return nil, err
	}
	if snap.RowId == 0 {
		return nil, index.ErrNoSnapshotEntry
	}
	return snap, nil
}

func (m *Indexer) LookupBigmap(ctx context.Context, id int64, withLast bool) (*model.BigMapItem, *model.BigMapItem, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, nil, err
	}
	alloc := model.AllocBigMapItem()
	err = table.Stream(ctx, pack.Query{
		Name:  "api.search_bigmap",
		Limit: 1,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("B"), // id
				Mode:  pack.FilterModeEqual,
				Value: id,
			},
			pack.Condition{
				Field: table.Fields().Find("a"), // alloc
				Mode:  pack.FilterModeEqual,
				Value: uint64(micheline.BigMapDiffActionAlloc),
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(alloc)
	})
	if err != nil {
		return nil, nil, err
	}
	if alloc.RowId == 0 {
		return nil, nil, index.ErrNoBigMapEntry
	}
	if !withLast {
		return alloc, nil, nil
	}
	last := model.AllocBigMapItem()
	err = table.Stream(ctx, pack.Query{
		Name:  "api.search_bigmap",
		Order: pack.OrderDesc,
		Limit: 1,
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("B"), // id
				Mode:  pack.FilterModeEqual,
				Value: id,
			},
		},
	}, func(r pack.Row) error {
		return r.Decode(last)
	})
	return alloc, last, err
}

func (m *Indexer) ListBigMapKeys(ctx context.Context, id, height int64, keyhash chain.ExprHash, key []byte, offset, limit uint) ([]*model.BigMapItem, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:   "api.search_bigmap",
		Fields: table.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("B"), // id
				Mode:  pack.FilterModeEqual,
				Value: id,
			},
			pack.Condition{
				Field: table.Fields().Find("a"), // action
				Mode:  pack.FilterModeNotIn,
				Value: []uint64{
					uint64(micheline.BigMapDiffActionAlloc),
					uint64(micheline.BigMapDiffActionCopy),
				},
			},
		},
	}
	if height == 0 {
		// rely on flags to quickly find latest state
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("r"), // is_replaced
			Mode:  pack.FilterModeEqual,
			Value: false,
		})
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("d"), // is_deleted
			Mode:  pack.FilterModeEqual,
			Value: false,
		})
	} else {
		// time-warp: ignore flags and future updates after height
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeLte,
			Value: height,
		})
	}
	if keyhash.IsValid() {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("H"), // hash
			Mode:  pack.FilterModeEqual,
			Value: keyhash.Hash.Hash,
		})
	} else if len(key) > 0 {
		// log.Infof("searching key %x", key)
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("k"), // key
			Mode:  pack.FilterModeEqual,
			Value: key,
		})
	}
	items := make([]*model.BigMapItem, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		var b *model.BigMapItem
		if height > 0 {
			// time-warp check requires to decode first
			b = model.AllocBigMapItem()
			if err := r.Decode(b); err != nil {
				return err
			}
			// skip values that were updated before height
			// FIXME: when the database supports OR conditions, this can be
			// done more efficiently with a condtion updated.eq=0 || updated.gt=height
			if b.Updated > 0 && b.Updated <= height {
				b.Free()
				return nil
			}
			// skip values that were removed
			if b.IsDeleted {
				b.Free()
				return nil
			}
			// skip matches when offset is used
			if offset > 0 {
				offset--
				b.Free()
				return nil
			}
		} else {
			// for non-time-warp it's more efficient to skip before decoding
			if offset > 0 {
				offset--
				return nil
			}
			b = model.AllocBigMapItem()
			if err := r.Decode(b); err != nil {
				b.Free()
				return err
			}
		}

		// log.Infof("Found item %s %d %d key %x", b.Action, b.BigMapId, b.RowId, b.Key)
		items = append(items, b)
		if len(items) == int(limit) {
			return io.EOF
		}
		if len(items)%128 == 0 {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}

func (m *Indexer) ListBigMapUpdates(ctx context.Context, id, minHeight, maxHeight int64, keyhash chain.ExprHash, key []byte, offset, limit uint) ([]*model.BigMapItem, error) {
	table, err := m.Table(index.BigMapTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.Query{
		Name:   "api.search_bigmap",
		Fields: table.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("B"), // id
				Mode:  pack.FilterModeEqual,
				Value: id,
			},
			pack.Condition{
				Field: table.Fields().Find("a"), // action
				Mode:  pack.FilterModeNotIn,
				Value: []uint64{
					uint64(micheline.BigMapDiffActionAlloc),
					uint64(micheline.BigMapDiffActionCopy),
				},
			},
		},
	}
	if minHeight > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeGte,
			Value: minHeight,
		})
	}
	if maxHeight > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("h"), // height
			Mode:  pack.FilterModeLte,
			Value: maxHeight,
		})
	}
	if keyhash.IsValid() {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("H"), // hash
			Mode:  pack.FilterModeEqual,
			Value: keyhash.Hash.Hash,
		})
	} else if len(key) > 0 {
		q.Conditions = append(q.Conditions, pack.Condition{
			Field: table.Fields().Find("k"), // key
			Mode:  pack.FilterModeEqual,
			Value: key,
		})
	}
	items := make([]*model.BigMapItem, 0)
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if offset > 0 {
			offset--
			return nil
		}
		b := model.AllocBigMapItem()
		if err := r.Decode(b); err != nil {
			return err
		}
		items = append(items, b)
		if len(items) == int(limit) {
			return io.EOF
		}
		if len(items)%128 == 0 {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return items, nil
}

func (m *Indexer) LookupRanking(ctx context.Context, id model.AccountID) (*AccountRankingEntry, bool) {
	if id == 0 {
		return nil, false
	}
	ranks, err := m.GetRanking(ctx, time.Now().UTC())
	if err != nil {
		log.Errorf("ranking failed: %v", err)
		return nil, false
	}
	r, ok := ranks.GetAccount(id)
	return r, ok
}

func (m *Indexer) TopRich(ctx context.Context, n int) ([]*AccountRankingEntry, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid negative top value %d", n)
	}
	ranks, err := m.GetRanking(ctx, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return ranks.TopRich(n), nil
}

func (m *Indexer) TopTraffic(ctx context.Context, n int) ([]*AccountRankingEntry, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid negative top value %d", n)
	}
	ranks, err := m.GetRanking(ctx, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return ranks.TopTraffic(n), nil
}

func (m *Indexer) TopFlows(ctx context.Context, n int) ([]*AccountRankingEntry, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid negative top value %d", n)
	}
	ranks, err := m.GetRanking(ctx, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return ranks.TopFlows(n), nil
}
