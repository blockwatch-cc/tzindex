// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

var (
	blockSeriesNames = util.StringList([]string{
		"time",
		"count",
		"n_ops",
		"n_ops_failed",
		"n_ops_contract",
		"n_ops_implicit",
		"n_contract_calls",
		"n_tx",
		"n_activation",
		"n_seed_nonce_revelation",
		"n_double_baking_evidence",
		"n_double_endorsement_evidence",
		"n_endorsement",
		"n_delegation",
		"n_reveal",
		"n_origination",
		"n_proposal",
		"n_ballot",
		"n_register_constant",
		"volume",
		"fee",
		"reward",
		"deposit",
		"unfrozen_fees",
		"unfrozen_rewards",
		"unfrozen_deposits",
		"activated_supply",
		"burned_supply",
		"n_new_accounts",
		"n_new_contracts",
		"n_cleared_accounts",
		"n_funded_accounts",
		"gas_used",
		"storage_size",
		"days_destroyed",
		"n_endorsed_slots",
	})
)

// Only use fields that can be summed over time
// configurable marshalling helper
type BlockSeries struct {
	Timestamp        time.Time `json:"time"`
	Count            int       `json:"count"`
	NSlotsEndorsed   int64     `json:"n_endorsed_slots"`
	NOps             int64     `json:"n_ops"`
	NOpsFailed       int64     `json:"n_ops_failed"`
	NOpsContract     int64     `json:"n_ops_contract"`
	NContractCalls   int64     `json:"n_contract_calls"`
	NTx              int64     `json:"n_tx"`
	NActivation      int64     `json:"n_activation"`
	NSeedNonce       int64     `json:"n_seed_nonce_revelation"`
	N2Baking         int64     `json:"n_double_baking_evidence"`
	N2Endorsement    int64     `json:"n_double_endorsement_evidence"`
	NEndorsement     int64     `json:"n_endorsement"`
	NDelegation      int64     `json:"n_delegation"`
	NReveal          int64     `json:"n_reveal"`
	NOrigination     int64     `json:"n_origination"`
	NProposal        int64     `json:"n_proposal"`
	NBallot          int64     `json:"n_ballot"`
	NRegister        int64     `json:"n_register_constant"`
	Volume           int64     `json:"volume"`
	Fee              int64     `json:"fee"`
	Reward           int64     `json:"reward"`
	Deposit          int64     `json:"deposit"`
	UnfrozenFees     int64     `json:"unfrozen_fees"`
	UnfrozenRewards  int64     `json:"unfrozen_rewards"`
	UnfrozenDeposits int64     `json:"unfrozen_deposits"`
	ActivatedSupply  int64     `json:"activated_supply"`
	BurnedSupply     int64     `json:"burned_supply"`
	NewAccounts      int64     `json:"n_new_accounts"`
	NewContracts     int64     `json:"n_new_contracts"`
	ClearedAccounts  int64     `json:"n_cleared_accounts"`
	FundedAccounts   int64     `json:"n_funded_accounts"`
	GasUsed          int64     `json:"gas_used"`
	StorageSize      int64     `json:"storage_size"`
	TDD              float64   `json:"days_destroyed"`
	NOpsImplicit     int64     `json:"n_ops_implicit"`

	columns util.StringList // cond. cols & order when brief
	params  *tezos.Params
	verbose bool
	null    bool
}

var _ SeriesBucket = (*BlockSeries)(nil)

func (s *BlockSeries) Init(params *tezos.Params, columns []string, verbose bool) {
	s.params = params
	s.columns = columns
	s.verbose = verbose
}

func (s *BlockSeries) IsEmpty() bool {
	return s.Count == 0
}

func (s *BlockSeries) Add(m SeriesModel) {
	b := m.(*model.Block)
	s.NSlotsEndorsed += int64(b.NSlotsEndorsed)
	s.NOps += int64(b.NOps)
	s.NOpsFailed += int64(b.NOpsFailed)
	s.NOpsContract += int64(b.NOpsContract)
	s.NContractCalls += int64(b.NContractCalls)
	s.NTx += int64(b.NTx)
	s.NActivation += int64(b.NActivation)
	s.NSeedNonce += int64(b.NSeedNonce)
	s.N2Baking += int64(b.N2Baking)
	s.N2Endorsement += int64(b.N2Endorsement)
	s.NEndorsement += int64(b.NEndorsement)
	s.NDelegation += int64(b.NDelegation)
	s.NReveal += int64(b.NReveal)
	s.NOrigination += int64(b.NOrigination)
	s.NProposal += int64(b.NProposal)
	s.NBallot += int64(b.NBallot)
	s.NRegister += int64(b.NRegister)
	s.Volume += int64(b.Volume)
	s.Fee += int64(b.Fee)
	s.Reward += int64(b.Reward)
	s.Deposit += int64(b.Deposit)
	s.UnfrozenFees += int64(b.UnfrozenFees)
	s.UnfrozenRewards += int64(b.UnfrozenRewards)
	s.UnfrozenDeposits += int64(b.UnfrozenDeposits)
	s.ActivatedSupply += int64(b.ActivatedSupply)
	s.BurnedSupply += int64(b.BurnedSupply)
	s.NewAccounts += int64(b.NewAccounts)
	s.NewContracts += int64(b.NewContracts)
	s.ClearedAccounts += int64(b.ClearedAccounts)
	s.FundedAccounts += int64(b.FundedAccounts)
	s.GasUsed += int64(b.GasUsed)
	s.StorageSize += int64(b.StorageSize)
	s.TDD += b.TDD
	s.NOpsImplicit += int64(b.NOpsImplicit)
	s.Count++
}

func (s *BlockSeries) Reset() {
	s.Timestamp = time.Time{}
	s.NSlotsEndorsed = 0
	s.NOps = 0
	s.NOpsFailed = 0
	s.NOpsContract = 0
	s.NContractCalls = 0
	s.NTx = 0
	s.NActivation = 0
	s.NSeedNonce = 0
	s.N2Baking = 0
	s.N2Endorsement = 0
	s.NEndorsement = 0
	s.NDelegation = 0
	s.NReveal = 0
	s.NOrigination = 0
	s.NProposal = 0
	s.NBallot = 0
	s.NRegister = 0
	s.Volume = 0
	s.Fee = 0
	s.Reward = 0
	s.Deposit = 0
	s.UnfrozenFees = 0
	s.UnfrozenRewards = 0
	s.UnfrozenDeposits = 0
	s.ActivatedSupply = 0
	s.BurnedSupply = 0
	s.NewAccounts = 0
	s.NewContracts = 0
	s.ClearedAccounts = 0
	s.FundedAccounts = 0
	s.GasUsed = 0
	s.StorageSize = 0
	s.TDD = 0
	s.NOpsImplicit = 0
	s.Count = 0
	s.null = false
}

func (s *BlockSeries) Null(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	s.null = true
	return s
}

func (s *BlockSeries) Zero(ts time.Time) SeriesBucket {
	s.Reset()
	s.Timestamp = ts
	return s
}

func (s *BlockSeries) SetTime(ts time.Time) SeriesBucket {
	s.Timestamp = ts
	return s
}

func (s *BlockSeries) Time() time.Time {
	return s.Timestamp
}

func (s *BlockSeries) Clone() SeriesBucket {
	return &BlockSeries{
		Timestamp:        s.Timestamp,
		NSlotsEndorsed:   s.NSlotsEndorsed,
		NOps:             s.NOps,
		NOpsFailed:       s.NOpsFailed,
		NOpsContract:     s.NOpsContract,
		NContractCalls:   s.NContractCalls,
		NTx:              s.NTx,
		NActivation:      s.NActivation,
		NSeedNonce:       s.NSeedNonce,
		N2Baking:         s.N2Baking,
		N2Endorsement:    s.N2Endorsement,
		NEndorsement:     s.NEndorsement,
		NDelegation:      s.NDelegation,
		NReveal:          s.NReveal,
		NOrigination:     s.NOrigination,
		NProposal:        s.NProposal,
		NBallot:          s.NBallot,
		NRegister:        s.NRegister,
		Volume:           s.Volume,
		Fee:              s.Fee,
		Reward:           s.Reward,
		Deposit:          s.Deposit,
		UnfrozenFees:     s.UnfrozenFees,
		UnfrozenRewards:  s.UnfrozenRewards,
		UnfrozenDeposits: s.UnfrozenDeposits,
		ActivatedSupply:  s.ActivatedSupply,
		BurnedSupply:     s.BurnedSupply,
		NewAccounts:      s.NewAccounts,
		NewContracts:     s.NewContracts,
		ClearedAccounts:  s.ClearedAccounts,
		FundedAccounts:   s.FundedAccounts,
		GasUsed:          s.GasUsed,
		StorageSize:      s.StorageSize,
		TDD:              s.TDD,
		NOpsImplicit:     s.NOpsImplicit,
		Count:            s.Count,
		columns:          s.columns,
		params:           s.params,
		verbose:          s.verbose,
		null:             s.null,
	}
}

func (s *BlockSeries) Interpolate(m SeriesBucket, ts time.Time) SeriesBucket {
	b := m.(*BlockSeries)
	weight := float64(ts.Sub(s.Timestamp)) / float64(b.Timestamp.Sub(s.Timestamp))
	if math.IsInf(weight, 1) {
		weight = 1
	}
	switch weight {
	case 0:
		return s
	default:
		return &BlockSeries{
			Timestamp:        ts,
			NSlotsEndorsed:   s.NSlotsEndorsed + int64(weight*float64(int64(b.NSlotsEndorsed)-s.NSlotsEndorsed)),
			NOps:             s.NOps + int64(weight*float64(int64(b.NOps)-s.NOps)),
			NOpsFailed:       s.NOpsFailed + int64(weight*float64(int64(b.NOpsFailed)-s.NOpsFailed)),
			NOpsContract:     s.NOpsContract + int64(weight*float64(int64(b.NOpsContract)-s.NOpsContract)),
			NContractCalls:   s.NContractCalls + int64(weight*float64(int64(b.NContractCalls)-s.NContractCalls)),
			NTx:              s.NTx + int64(weight*float64(int64(b.NTx)-s.NTx)),
			NActivation:      s.NActivation + int64(weight*float64(int64(b.NActivation)-s.NActivation)),
			NSeedNonce:       s.NSeedNonce + int64(weight*float64(int64(b.NSeedNonce)-s.NSeedNonce)),
			N2Baking:         s.N2Baking + int64(weight*float64(int64(b.N2Baking)-s.N2Baking)),
			N2Endorsement:    s.N2Endorsement + int64(weight*float64(int64(b.N2Endorsement)-s.N2Endorsement)),
			NEndorsement:     s.NEndorsement + int64(weight*float64(int64(b.NEndorsement)-s.NEndorsement)),
			NDelegation:      s.NDelegation + int64(weight*float64(int64(b.NDelegation)-s.NDelegation)),
			NReveal:          s.NReveal + int64(weight*float64(int64(b.NReveal)-s.NReveal)),
			NOrigination:     s.NOrigination + int64(weight*float64(int64(b.NOrigination)-s.NOrigination)),
			NProposal:        s.NProposal + int64(weight*float64(int64(b.NProposal)-s.NProposal)),
			NBallot:          s.NBallot + int64(weight*float64(int64(b.NBallot)-s.NBallot)),
			NRegister:        s.NRegister + int64(weight*float64(int64(b.NRegister)-s.NRegister)),
			Volume:           s.Volume + int64(weight*float64(int64(b.Volume)-s.Volume)),
			Fee:              s.Fee + int64(weight*float64(int64(b.Fee)-s.Fee)),
			Reward:           s.Reward + int64(weight*float64(int64(b.Reward)-s.Reward)),
			Deposit:          s.Deposit + int64(weight*float64(int64(b.Deposit)-s.Deposit)),
			UnfrozenFees:     s.UnfrozenFees + int64(weight*float64(int64(b.UnfrozenFees)-s.UnfrozenFees)),
			UnfrozenRewards:  s.UnfrozenRewards + int64(weight*float64(int64(b.UnfrozenRewards)-s.UnfrozenRewards)),
			UnfrozenDeposits: s.UnfrozenDeposits + int64(weight*float64(int64(b.UnfrozenDeposits)-s.UnfrozenDeposits)),
			ActivatedSupply:  s.ActivatedSupply + int64(weight*float64(int64(b.ActivatedSupply)-s.ActivatedSupply)),
			BurnedSupply:     s.BurnedSupply + int64(weight*float64(int64(b.BurnedSupply)-s.BurnedSupply)),
			NewAccounts:      s.NewAccounts + int64(weight*float64(int64(b.NewAccounts)-s.NewAccounts)),
			NewContracts:     s.NewContracts + int64(weight*float64(int64(b.NewContracts)-s.NewContracts)),
			ClearedAccounts:  s.ClearedAccounts + int64(weight*float64(int64(b.ClearedAccounts)-s.ClearedAccounts)),
			FundedAccounts:   s.FundedAccounts + int64(weight*float64(int64(b.FundedAccounts)-s.FundedAccounts)),
			GasUsed:          s.GasUsed + int64(weight*float64(int64(b.GasUsed)-s.GasUsed)),
			StorageSize:      s.StorageSize + int64(weight*float64(int64(b.StorageSize)-s.StorageSize)),
			TDD:              s.TDD + weight*b.TDD - s.TDD,
			NOpsImplicit:     s.NOpsImplicit + int64(weight*float64(int64(b.NOpsImplicit)-s.NOpsImplicit)),
			Count:            0,
			columns:          s.columns,
			params:           s.params,
			verbose:          s.verbose,
			null:             false,
		}
	}
}

func (s *BlockSeries) MarshalJSON() ([]byte, error) {
	if s.verbose {
		return s.MarshalJSONVerbose()
	} else {
		return s.MarshalJSONBrief()
	}
}

func (b *BlockSeries) MarshalJSONVerbose() ([]byte, error) {
	block := struct {
		Timestamp        time.Time `json:"time"`
		Count            int       `json:"count"`
		NSlotsEndorsed   int64     `json:"n_endorsed_slots"`
		NOps             int64     `json:"n_ops"`
		NOpsFailed       int64     `json:"n_ops_failed"`
		NOpsContract     int64     `json:"n_ops_contract"`
		NContractCalls   int64     `json:"n_contract_calls"`
		NTx              int64     `json:"n_tx"`
		NActivation      int64     `json:"n_activation"`
		NSeedNonce       int64     `json:"n_seed_nonce_revelation"`
		N2Baking         int64     `json:"n_double_baking_evidence"`
		N2Endorsement    int64     `json:"n_double_endorsement_evidence"`
		NEndorsement     int64     `json:"n_endorsement"`
		NDelegation      int64     `json:"n_delegation"`
		NReveal          int64     `json:"n_reveal"`
		NOrigination     int64     `json:"n_origination"`
		NProposal        int64     `json:"n_proposal"`
		NBallot          int64     `json:"n_ballot"`
		NRegister        int64     `json:"n_register_constant"`
		Volume           float64   `json:"volume"`
		Fee              float64   `json:"fee"`
		Reward           float64   `json:"reward"`
		Deposit          float64   `json:"deposit"`
		UnfrozenFees     float64   `json:"unfrozen_fees"`
		UnfrozenRewards  float64   `json:"unfrozen_rewards"`
		UnfrozenDeposits float64   `json:"unfrozen_deposits"`
		ActivatedSupply  float64   `json:"activated_supply"`
		BurnedSupply     float64   `json:"burned_supply"`
		NewAccounts      int64     `json:"n_new_accounts"`
		NewContracts     int64     `json:"n_new_contracts"`
		ClearedAccounts  int64     `json:"n_cleared_accounts"`
		FundedAccounts   int64     `json:"n_funded_accounts"`
		GasUsed          int64     `json:"gas_used"`
		StorageSize      int64     `json:"storage_size"`
		TDD              float64   `json:"days_destroyed"`
		NOpsImplicit     int64     `json:"n_ops_implicit"`
	}{
		Timestamp:        b.Timestamp,
		Count:            b.Count,
		NSlotsEndorsed:   b.NSlotsEndorsed,
		NOps:             b.NOps,
		NOpsFailed:       b.NOpsFailed,
		NOpsContract:     b.NOpsContract,
		NContractCalls:   b.NContractCalls,
		NTx:              b.NTx,
		NActivation:      b.NActivation,
		NSeedNonce:       b.NSeedNonce,
		N2Baking:         b.N2Baking,
		N2Endorsement:    b.N2Endorsement,
		NEndorsement:     b.NEndorsement,
		NDelegation:      b.NDelegation,
		NReveal:          b.NReveal,
		NOrigination:     b.NOrigination,
		NProposal:        b.NProposal,
		NBallot:          b.NBallot,
		NRegister:        b.NRegister,
		Volume:           b.params.ConvertValue(b.Volume),
		Fee:              b.params.ConvertValue(b.Fee),
		Reward:           b.params.ConvertValue(b.Reward),
		Deposit:          b.params.ConvertValue(b.Deposit),
		UnfrozenFees:     b.params.ConvertValue(b.UnfrozenFees),
		UnfrozenRewards:  b.params.ConvertValue(b.UnfrozenRewards),
		UnfrozenDeposits: b.params.ConvertValue(b.UnfrozenDeposits),
		ActivatedSupply:  b.params.ConvertValue(b.ActivatedSupply),
		BurnedSupply:     b.params.ConvertValue(b.BurnedSupply),
		NewAccounts:      b.NewAccounts,
		NewContracts:     b.NewContracts,
		ClearedAccounts:  b.ClearedAccounts,
		FundedAccounts:   b.FundedAccounts,
		GasUsed:          b.GasUsed,
		StorageSize:      b.StorageSize,
		TDD:              b.TDD,
		NOpsImplicit:     b.NOpsImplicit,
	}
	return json.Marshal(block)
}

func (b *BlockSeries) MarshalJSONBrief() ([]byte, error) {
	dec := b.params.Decimals
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for i, v := range b.columns {
		if b.null {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
			default:
				buf = append(buf, null...)
			}
		} else {
			switch v {
			case "time":
				buf = strconv.AppendInt(buf, util.UnixMilliNonZero(b.Timestamp), 10)
			case "count":
				buf = strconv.AppendInt(buf, int64(b.Count), 10)
			case "n_endorsed_slots":
				buf = strconv.AppendInt(buf, int64(b.NSlotsEndorsed), 10)
			case "n_ops":
				buf = strconv.AppendInt(buf, int64(b.NOps), 10)
			case "n_ops_failed":
				buf = strconv.AppendInt(buf, int64(b.NOpsFailed), 10)
			case "n_ops_contract":
				buf = strconv.AppendInt(buf, int64(b.NOpsContract), 10)
			case "n_contract_calls":
				buf = strconv.AppendInt(buf, int64(b.NContractCalls), 10)
			case "n_tx":
				buf = strconv.AppendInt(buf, int64(b.NTx), 10)
			case "n_activation":
				buf = strconv.AppendInt(buf, int64(b.NActivation), 10)
			case "n_seed_nonce_revelation":
				buf = strconv.AppendInt(buf, int64(b.NSeedNonce), 10)
			case "n_double_baking_evidence":
				buf = strconv.AppendInt(buf, int64(b.N2Baking), 10)
			case "n_double_endorsement_evidence":
				buf = strconv.AppendInt(buf, int64(b.N2Endorsement), 10)
			case "n_endorsement":
				buf = strconv.AppendInt(buf, int64(b.NEndorsement), 10)
			case "n_delegation":
				buf = strconv.AppendInt(buf, int64(b.NDelegation), 10)
			case "n_reveal":
				buf = strconv.AppendInt(buf, int64(b.NReveal), 10)
			case "n_origination":
				buf = strconv.AppendInt(buf, int64(b.NOrigination), 10)
			case "n_proposal":
				buf = strconv.AppendInt(buf, int64(b.NProposal), 10)
			case "n_ballot":
				buf = strconv.AppendInt(buf, int64(b.NBallot), 10)
			case "n_register_constant":
				buf = strconv.AppendInt(buf, int64(b.NRegister), 10)
			case "volume":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Volume), 'f', dec, 64)
			case "fee":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Fee), 'f', dec, 64)
			case "reward":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Reward), 'f', dec, 64)
			case "deposit":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.Deposit), 'f', dec, 64)
			case "unfrozen_fees":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.UnfrozenFees), 'f', dec, 64)
			case "unfrozen_rewards":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.UnfrozenRewards), 'f', dec, 64)
			case "unfrozen_deposits":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.UnfrozenDeposits), 'f', dec, 64)
			case "activated_supply":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.ActivatedSupply), 'f', dec, 64)
			case "burned_supply":
				buf = strconv.AppendFloat(buf, b.params.ConvertValue(b.BurnedSupply), 'f', dec, 64)
			case "n_new_accounts":
				buf = strconv.AppendInt(buf, int64(b.NewAccounts), 10)
			case "n_new_contracts":
				buf = strconv.AppendInt(buf, int64(b.NewContracts), 10)
			case "n_cleared_accounts":
				buf = strconv.AppendInt(buf, int64(b.ClearedAccounts), 10)
			case "n_funded_accounts":
				buf = strconv.AppendInt(buf, int64(b.FundedAccounts), 10)
			case "gas_used":
				buf = strconv.AppendInt(buf, b.GasUsed, 10)
			case "storage_size":
				buf = strconv.AppendInt(buf, b.StorageSize, 10)
			case "days_destroyed":
				buf = strconv.AppendFloat(buf, b.TDD, 'f', -1, 64)
			case "n_ops_implicit":
				buf = strconv.AppendInt(buf, int64(b.NOpsImplicit), 10)
			default:
				continue
			}
		}
		if i < len(b.columns)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ']')
	return buf, nil
}

func (b *BlockSeries) MarshalCSV() ([]string, error) {
	dec := b.params.Decimals
	res := make([]string, len(b.columns))
	for i, v := range b.columns {
		if b.null {
			switch v {
			case "time":
				res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
			default:
				continue
			}
		}
		switch v {
		case "time":
			res[i] = strconv.Quote(b.Timestamp.Format(time.RFC3339))
		case "count":
			res[i] = strconv.FormatInt(int64(b.Count), 10)
		case "n_endorsed_slots":
			res[i] = strconv.FormatInt(int64(b.NSlotsEndorsed), 10)
		case "n_ops":
			res[i] = strconv.FormatInt(int64(b.NOps), 10)
		case "n_ops_failed":
			res[i] = strconv.FormatInt(int64(b.NOpsFailed), 10)
		case "n_ops_contract":
			res[i] = strconv.FormatInt(int64(b.NOpsContract), 10)
		case "n_contract_calls":
			res[i] = strconv.FormatInt(int64(b.NContractCalls), 10)
		case "n_tx":
			res[i] = strconv.FormatInt(int64(b.NTx), 10)
		case "n_activation":
			res[i] = strconv.FormatInt(int64(b.NActivation), 10)
		case "n_seed_nonce_revelation":
			res[i] = strconv.FormatInt(int64(b.NSeedNonce), 10)
		case "n_double_baking_evidence":
			res[i] = strconv.FormatInt(int64(b.N2Baking), 10)
		case "n_double_endorsement_evidence":
			res[i] = strconv.FormatInt(int64(b.N2Endorsement), 10)
		case "n_endorsement":
			res[i] = strconv.FormatInt(int64(b.NEndorsement), 10)
		case "n_delegation":
			res[i] = strconv.FormatInt(int64(b.NDelegation), 10)
		case "n_reveal":
			res[i] = strconv.FormatInt(int64(b.NReveal), 10)
		case "n_origination":
			res[i] = strconv.FormatInt(int64(b.NOrigination), 10)
		case "n_proposal":
			res[i] = strconv.FormatInt(int64(b.NProposal), 10)
		case "n_ballot":
			res[i] = strconv.FormatInt(int64(b.NBallot), 10)
		case "n_register_constant":
			res[i] = strconv.FormatInt(int64(b.NRegister), 10)
		case "volume":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Volume), 'f', dec, 64)
		case "fee":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Fee), 'f', dec, 64)
		case "reward":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Reward), 'f', dec, 64)
		case "deposit":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.Deposit), 'f', dec, 64)
		case "unfrozen_fees":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.UnfrozenFees), 'f', dec, 64)
		case "unfrozen_rewards":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.UnfrozenRewards), 'f', dec, 64)
		case "unfrozen_deposits":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.UnfrozenDeposits), 'f', dec, 64)
		case "activated_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.ActivatedSupply), 'f', dec, 64)
		case "burned_supply":
			res[i] = strconv.FormatFloat(b.params.ConvertValue(b.BurnedSupply), 'f', dec, 64)
		case "n_new_accounts":
			res[i] = strconv.FormatInt(int64(b.NewAccounts), 10)
		case "n_new_contracts":
			res[i] = strconv.FormatInt(int64(b.NewContracts), 10)
		case "n_cleared_accounts":
			res[i] = strconv.FormatInt(int64(b.ClearedAccounts), 10)
		case "n_funded_accounts":
			res[i] = strconv.FormatInt(int64(b.FundedAccounts), 10)
		case "gas_used":
			res[i] = strconv.FormatInt(b.GasUsed, 10)
		case "storage_size":
			res[i] = strconv.FormatInt(b.StorageSize, 10)
		case "days_destroyed":
			res[i] = strconv.FormatFloat(b.TDD, 'f', -1, 64)
		case "n_ops_implicit":
			res[i] = strconv.FormatInt(int64(b.NOpsImplicit), 10)
		default:
			continue
		}
	}
	return res, nil
}

func (s *BlockSeries) BuildQuery(ctx *ApiContext, args *SeriesRequest) pack.Query {
	// use chain params at current height
	params := ctx.Params

	// access table
	table, err := ctx.Indexer.Table(args.Series)
	if err != nil {
		panic(ENotFound(EC_RESOURCE_NOTFOUND, fmt.Sprintf("cannot access table '%s'", args.Series), err))
	}

	// translate long column names to short names used in pack tables
	var srcNames []string
	// time is auto-added from parser
	if len(args.Columns) == 1 {
		// use all series columns
		args.Columns = blockSeriesNames
	}
	// resolve short column names
	srcNames = make([]string, 0, len(args.Columns))
	for _, v := range args.Columns {
		// ignore count column
		if v == "count" {
			continue
		}
		// ignore non-series columns
		if !blockSeriesNames.Contains(v) {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid time-series column '%s'", v), nil))
		}
		n, ok := blockSourceNames[v]
		if !ok {
			panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", v), nil))
		}
		if n != "-" {
			srcNames = append(srcNames, n)
		}
	}

	// build table query
	q := pack.NewQuery(ctx.RequestID, table).
		WithFields(srcNames...).
		WithOrder(args.Order).
		AndRange("time", args.From.Time(), args.To.Time()).
		AndEqual("is_orphan", false)

	// build dynamic filter conditions from query (will panic on error)
	for key, val := range ctx.Request.URL.Query() {
		keys := strings.Split(key, ".")
		prefix := keys[0]
		mode := pack.FilterModeEqual
		if len(keys) > 1 {
			mode = pack.ParseFilterMode(keys[1])
			if !mode.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s'", keys[1]), nil))
			}
		}
		switch prefix {
		case "columns", "collapse", "start_date", "end_date", "limit", "order", "verbose", "filename", "fill":
			// skip these fields
			continue

		case "baker":
			// parse baker address and lookup id
			// valid filter modes: eq, in
			// 1 resolve account_id from account table
			// 2 add eq/in cond: account_id
			// 3 cache result in map (for output)
			switch mode {
			case pack.FilterModeEqual, pack.FilterModeNotEqual:
				if val[0] == "" {
					// empty address matches id 0 (== missing baker)
					q.Conditions.AddAndCondition(&pack.Condition{
						Field: table.Fields().Find("B"), // baker id
						Mode:  mode,
						Value: 0,
						Raw:   val[0], // debugging aid
					})
				} else {
					// single-address lookup and compile condition
					addr, err := tezos.ParseAddress(val[0])
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", val[0]), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(err)
					}
					// Note: when not found we insert an always false condition
					if acc == nil || acc.RowId == 0 {
						q.Conditions.AddAndCondition(&pack.Condition{
							Field: table.Fields().Find("B"), // baker id
							Mode:  mode,
							Value: uint64(math.MaxUint64),
							Raw:   "account not found", // debugging aid
						})
					} else {
						// add addr id as extra fund_flow condition
						q.Conditions.AddAndCondition(&pack.Condition{
							Field: table.Fields().Find("B"), // baker id
							Mode:  mode,
							Value: acc.RowId,
							Raw:   val[0], // debugging aid
						})
					}
				}
			case pack.FilterModeIn, pack.FilterModeNotIn:
				// multi-address lookup and compile condition
				ids := make([]uint64, 0)
				for _, v := range strings.Split(val[0], ",") {
					addr, err := tezos.ParseAddress(v)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid address '%s'", v), err))
					}
					acc, err := ctx.Indexer.LookupAccount(ctx, addr)
					if err != nil && err != index.ErrNoAccountEntry {
						panic(err)
					}
					// skip not found account
					if acc == nil || acc.RowId == 0 {
						continue
					}
					// collect list of account ids
					ids = append(ids, acc.RowId.Value())
				}
				// Note: when list is empty (no accounts were found, the match will
				//       always be false and return no result as expected)
				q.Conditions.AddAndCondition(&pack.Condition{
					Field: table.Fields().Find("B"), // baker id
					Mode:  mode,
					Value: ids,
					Raw:   val[0], // debugging aid
				})
			default:
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s' for column '%s'", mode, prefix), nil))
			}

		case "voting_period_kind":
			// parse only the first value
			period := tezos.ParseVotingPeriod(val[0])
			if !period.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid voting period '%s'", val[0]), nil))
			}
			q.Conditions.AddAndCondition(&pack.Condition{
				Field: table.Fields().Find("p"),
				Mode:  mode,
				Value: period,
				Raw:   val[0], // debugging aid
			})
		default:
			// translate long column name used in query to short column name used in packs
			if short, ok := blockSourceNames[prefix]; !ok {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("unknown column '%s'", prefix), nil))
			} else {
				key = strings.Replace(key, prefix, short, 1)
			}

			// the same field name may appear multiple times, in which case conditions
			// are combined like any other condition with logical AND
			for _, v := range val {
				// convert amounts from float to int64
				switch prefix {
				case "volume", "reward", "fee", "deposit", "burned_supply",
					"unfrozen_fees", "unfrozen_rewards", "unfrozen_deposits",
					"activated_supply":
					fval, err := strconv.ParseFloat(v, 64)
					if err != nil {
						panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
					}
					v = strconv.FormatInt(params.ConvertAmount(fval), 10)
				}
				if cond, err := pack.ParseCondition(key, v, table.Fields()); err != nil {
					panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid %s filter value '%s'", key, v), err))
				} else {
					q.Conditions.AddAndCondition(&cond)
				}
			}
		}
	}

	return q
}
