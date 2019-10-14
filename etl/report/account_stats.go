// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"math"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

// maximum number of histogram bins (powers of ten)
const (
	maxHistBins = 19 // 1e0 .. 1e18
	maxTopBins  = 6  // N=1/10/100/1k/10k/100k
)

var topBucketSize int = int(math.Pow10(maxTopBins))

// max values for buckets
var exn = [maxHistBins]int64{
	1,    // 1 satoshi
	1e1,  // 10 satoshi
	1e2,  // 1 uBTC
	1e3,  // 10 uBTC
	1e4,  // 100 uBTC
	1e5,  // 1 mBTC
	1e6,  // 10 mBTC
	1e7,  // 100 mBTC
	1e8,  // 1 BTC
	1e9,  // 10
	1e10, // 100
	1e11, // 1k
	1e12, // 10k
	1e13, // 100k
	1e14, // 1M
	1e15, // 10M
	1e16, // 100M
	1e17, // 1B
	1e18, // 10B
}

type AccountStats struct {
	// request params
	From   time.Time
	To     time.Time
	Height int64

	// activity report
	Unique24h          int                 // num unique accounts seen in last 24 hours
	New24h             int                 // num new accounts created in last 24 hours
	Funded24h          int                 // num funded accounts last 24 hours
	Cleared24h         int                 // num cleared accounts last 24 hours
	ActiveBakers24h    int                 // num active bakers seen in last 24 hours
	ActiveEndorsers24h int                 // num active endorsers seen in last 24 hours
	Volume24h          float64             // sum of tx volume last 24h
	Tx24h              int                 // count of all non-smart contract tx last 24h
	ContractTx24h      int                 // count of all smart-contract tx last 24h
	TopVolume24h       [maxTopBins]float64 // sum tx volume for top N acc with most daily volume
	TopTraffic24h      [maxTopBins]int     // num tx for top N acc with most daily traffic

	// balance report
	TopBalance           [maxTopBins]float64  // total balance for top acc N=1/10/100/1k/10k/100k
	TopDelegated         [maxTopBins]float64  // total delegation balance for top acc N=1/10/100/1k/10k/100k
	NumBalanceHist       [maxHistBins]int     // num accounts per bin (10e13..10e-8 in value)
	SumBalanceHist       [maxHistBins]float64 // sum of balances in accounts per bin
	TotalFunded          int                  // count of all funded accounts
	TotalFundedImplicit  int                  // count of all funded tz1/2/3 implicit accounts
	TotalFundedManaged   int                  // count of all funded KT1 managed accounts (non smart contract)
	TotalFundedContracts int                  // count of all funded smart contracts
	SumFundedImplicit    float64              // funds at implicit accounts
	SumFundedManaged     float64              // funds at managed KT1 accounts
	SumFundedContracts   float64              // funds at smart contracts

	// age report
	NumDormantYrs  []int64   // by blockchain age in years
	SumDormantYrs  []float64 // by blockchain age in years
	NumDormantQtrs []int64   // by blockchain age in quarters
	SumDormantQtrs []float64 // by blockchain age in quarters
}

func (r *AccountReport) BuildStats(ctx context.Context, from, to time.Time, height int64) (*AccountStats, error) {
	stats := &AccountStats{
		From:   from,   // SOD
		To:     to,     // EOD
		Height: height, // last block of the day (adjusted from calling block)
	}
	start := time.Now()

	log.Debugf("Collecting account statistics at height %d time %s.", height, to)

	//
	// Part 1: Activity Stats
	//
	if err := stats.CollectActivityStats(ctx, r.crawler, from, to); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	//
	// Part 2: Funded Stats
	//
	// yrs := []int64{33803, 101712, 161337, 215792}
	if err := stats.CollectFundedStats(ctx, r.crawler); err != nil {
		return nil, err
	}

	if d := time.Since(start); d > 10*time.Second {
		log.Infof("Account statistics collection took %s.", d)
	} else {
		log.Debugf("Successfully collected account statistics in %s.", d)
	}
	return stats, nil
}

// stats collection helper for top N bucket
type TopAcc struct {
	Vol        float64
	NTx        int
	IsBaker    bool
	IsEndorser bool
}

func (s *AccountStats) CollectActivityStats(ctx context.Context, c model.BlockCrawler, from, to time.Time) error {
	start := time.Now()
	params := c.ParamsByHeight(-1)

	blocks, err := c.Table(index.BlockTableKey)
	if err != nil {
		return err
	}
	flows, err := c.Table(index.FlowTableKey)
	if err != nil {
		return err
	}

	// Step 1: count new/funded/emptied accounts
	q := pack.Query{
		Name:    "stats.blocks",
		NoCache: true,
		Fields:  blocks.Fields().Select("A", "i", "m", "C", "E", "J", "V", "3", "4"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: blocks.Fields().Find("T"), // filter by block timestamp
				Mode:  pack.FilterModeRange,
				From:  from,
				To:    to,
			},
			pack.Condition{
				Field: blocks.Fields().Find("Z"), // all non-orphan
				Mode:  pack.FilterModeEqual,
				Value: false,
			},
		},
	}
	type XBlock struct {
		NewAccounts         int   `pack:"A"`
		NewImplicitAccounts int   `pack:"i"`
		NewManagedAccounts  int   `pack:"m"`
		NewContracts        int   `pack:"C"`
		ClearedAccounts     int   `pack:"E"`
		FundedAccounts      int   `pack:"J"`
		Volume              int64 `pack:"V"`
		NOpsContract        int   `pack:"3"`
		NTx                 int   `pack:"4"`
	}
	b := &XBlock{}
	err = blocks.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(b); err != nil {
			return err
		}
		s.New24h += int(b.NewAccounts)
		s.Funded24h += int(b.FundedAccounts)
		s.Cleared24h += int(b.ClearedAccounts)
		s.Volume24h += params.ConvertValue(b.Volume)
		s.Tx24h += int(b.NTx)
		s.ContractTx24h += int(b.NOpsContract)
		return nil
	})
	if err != nil {
		return err
	}

	// Step 2: use flows to count unique active accounts, unique tx and volume per account
	//         ignoring all delegation, fees, rewards, deposits
	//
	// Unique24h          int                 // num unique accounts seen in last 24 hours
	// ActiveBakers24h    int                 // num active bakers seen in last 24 hours
	// ActiveEndorsers24h int                 // num active endorsers seen in last 24 hours
	// TopVolume24h    [maxTopBins]float64 // sum tx volume for top N acc with most daily volume
	// TopTraffic24h   [maxTopBins]int64   // sum tx for top N acc with most daily traffic
	//
	var accMap = make(map[uint64]TopAcc)
	type XFlow struct {
		AccountId uint64             `pack:"A"`
		Category  model.FlowCategory `pack:"C"`
		Operation model.FlowType     `pack:"O"`
		AmountIn  int64              `pack:"i"`
		AmountOut int64              `pack:"o"`
		IsFee     bool               `pack:"e,snappy"`
	}

	q = pack.Query{
		Name:    "stats.flows",
		NoCache: true,
		Fields:  flows.Fields().Select("A", "C", "O", "i", "o", "e"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: flows.Fields().Find("T"), // filter by flow timestamp
				Mode:  pack.FilterModeRange,
				From:  from,
				To:    to,
			},
		},
	}
	f := &XFlow{}
	err = flows.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(f); err != nil {
			return err
		}
		acc, _ := accMap[f.AccountId]
		// use tx, baking/endorsing flows only (anything else pays fees or rewards only)
		switch f.Operation {
		case model.FlowTypeBaking:
			// ignore deposits and rewards
			acc.IsBaker = true
		case model.FlowTypeEndorsement:
			// ignore deposits and rewards
			acc.IsEndorser = true
		case model.FlowTypeTransaction:
			if f.Category == model.FlowCategoryBalance {
				// for tx counts, count all balance updates (in and out)
				if !f.IsFee {
					acc.NTx++
				}
				// count in and out flows
				acc.Vol += params.ConvertValue(f.AmountIn + f.AmountOut)
			}
		}
		// store all accounts even if they had no tx volume (need this for unique 24h)
		accMap[f.AccountId] = acc
		return nil
	})
	if err != nil {
		return err
	}
	s.Unique24h = len(accMap)

	// Step 3: top N by volume
	topVol := vec.NewTopFloat64Heap(topBucketSize)
	for _, acc := range accMap {
		if acc.Vol > 0 {
			topVol.Add(acc.Vol)
		}
		if acc.IsBaker {
			s.ActiveBakers24h++
		}
		if acc.IsEndorser {
			s.ActiveEndorsers24h++
		}
	}
	for i := 0; i < maxTopBins; i++ {
		s.TopVolume24h[i] = topVol.SumN(int(math.Pow10(i)))
	}
	// free some resources
	topVol = nil

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Step 5: top N by tx
	topTraffic := vec.NewTopUint64Heap(topBucketSize)
	for _, acc := range accMap {
		if acc.NTx > 0 {
			topTraffic.Add(uint64(acc.NTx))
		}
	}
	for i := 0; i < maxTopBins; i++ {
		s.TopTraffic24h[i] = int(topTraffic.SumN(int(math.Pow10(i))))
	}
	// free some resources
	topTraffic = nil

	log.Debugf("Scanned %d unique accounts in %s\n", s.Unique24h, time.Since(start))
	return nil
}

func (s *AccountStats) CollectFundedStats(ctx context.Context, c model.BlockCrawler) error {
	tip := c.Tip()
	params := c.ParamsByHeight(tip.BestHeight)
	table, err := c.Table(index.AccountTableKey)
	if err != nil {
		return err
	}

	// Preparations
	// - we consider at most the top 100k accounts (N=1/10/100/1k/10k/100k)
	start := time.Now()
	yrs := tip.NYEveBlocks
	qtrs := tip.QuarterBlocks
	topBalance := vec.NewTopFloat64Heap(topBucketSize)
	topDelegated := vec.NewTopFloat64Heap(topBucketSize)
	s.NumDormantYrs = make([]int64, len(yrs))
	s.SumDormantYrs = make([]float64, len(yrs))
	s.NumDormantQtrs = make([]int64, len(qtrs))
	s.SumDormantQtrs = make([]float64, len(qtrs))

	// balance and age reports
	q := pack.Query{
		Name:    "stats.accounts",
		NoCache: true,
		Fields:  table.Fields().Select("I", "J", "O", "z", "Z", "Y", "s", "~", "t", "c"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("f"), // search funded acc only
				Mode:  pack.FilterModeEqual,
				Value: true,
			},
		},
	}
	// yrs is the first block number at the beginning of each blockchain year
	type XAccount struct {
		Id               uint64            `pack:"I"`
		LastIn           int64             `pack:"J"`
		LastOut          int64             `pack:"O"`
		FrozenDeposits   int64             `pack:"z"`
		FrozenRewards    int64             `pack:"Z"`
		FrozenFees       int64             `pack:"Y"`
		SpendableBalance int64             `pack:"s"`
		DelegatedBalance int64             `pack:"~"`
		Type             chain.AddressType `pack:"t"`
		IsContract       bool              `pack:"c"`
	}
	a := &XAccount{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(a); err != nil {
			return err
		}
		// stats use balance as float64 in COIN units
		// Note: rewards are not owned yet, but fees are
		sum := a.FrozenFees + a.FrozenDeposits + a.SpendableBalance
		balance := params.ConvertValue(sum)
		// add to topN bucket
		topBalance.Add(balance)
		topDelegated.Add(params.ConvertValue(a.DelegatedBalance))
		// add totals
		s.TotalFunded++
		switch a.Type {
		case chain.AddressTypeEd25519, chain.AddressTypeSecp256k1, chain.AddressTypeP256:
			s.TotalFundedImplicit++
			s.SumFundedImplicit += balance
		case chain.AddressTypeContract:
			if a.IsContract {
				s.TotalFundedContracts++
				s.SumFundedContracts += balance
			} else {
				s.TotalFundedManaged++
				s.SumFundedManaged += balance
			}
			// ignore blinded (unclaimed accounts)
		}

		// add to balance histograms
		for i := 0; i < len(exn); i++ {
			if sum <= exn[i] {
				s.NumBalanceHist[i]++
				s.SumBalanceHist[i] += balance
				break
			}
		}
		// dormant status
		lastTx := util.Max64(a.LastIn, a.LastOut)
		for i := 0; i < len(yrs); i++ {
			if lastTx < yrs[i] {
				s.NumDormantYrs[i]++
				s.SumDormantYrs[i] += balance
				break
			}
		}
		for i := 0; i < len(qtrs); i++ {
			if lastTx < qtrs[i] {
				s.NumDormantQtrs[i]++
				s.SumDormantQtrs[i] += balance
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < len(s.TopBalance); i++ {
		s.TopBalance[i] = topBalance.SumN(int(math.Pow10(i)))
	}
	for i := 0; i < len(s.TopDelegated); i++ {
		s.TopDelegated[i] = topDelegated.SumN(int(math.Pow10(i)))
	}

	log.Debugf("Scanned %d funded accounts in %s\n", s.TotalFunded, time.Since(start))
	return nil
}

func (r *AccountReport) LoadStats(ctx context.Context, at time.Time, height int64) (*AccountStats, error) {
	s := &AccountStats{
		To:             at,
		Height:         height,
		NumDormantYrs:  make([]int64, 0),
		SumDormantYrs:  make([]float64, 0),
		NumDormantQtrs: make([]int64, 0),
		SumDormantQtrs: make([]float64, 0),
	}
	// age
	q := pack.Query{
		Name: "stats.account_age",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: r.ageTable.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		},
	}
	found := false
	err := r.ageTable.Stream(ctx, q, func(row pack.Row) error {
		rep := AccountAgeReport{}
		if err := row.Decode(&rep); err != nil {
			return err
		}
		// FIXME: assuming result is sorted by year and quarter
		found = true
		if rep.Quarter == 0 {
			s.NumDormantYrs = append(s.NumDormantYrs, rep.NumDormant)
			s.SumDormantYrs = append(s.SumDormantYrs, rep.SumDormant)
		} else {
			s.NumDormantQtrs = append(s.NumDormantQtrs, rep.NumDormant)
			s.SumDormantQtrs = append(s.SumDormantQtrs, rep.SumDormant)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrReportNotFound
	}

	// activity
	q = pack.Query{
		Name: "stats.account_activity",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: r.activityTable.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		},
	}
	err = r.activityTable.Stream(ctx, q, func(row pack.Row) error {
		rep := AccountActivityReport{}
		if err := row.Decode(&rep); err != nil {
			return err
		}
		s.Unique24h = rep.Unique24h
		s.New24h = rep.New24h
		s.Funded24h = rep.Funded24h
		s.Cleared24h = rep.Cleared24h
		s.ActiveBakers24h = rep.ActiveBakers24h
		s.ActiveEndorsers24h = rep.ActiveEndorsers24h
		s.Volume24h = rep.Volume24h
		s.Tx24h = rep.Tx24h
		s.ContractTx24h = rep.ContractTx24h
		s.TopVolume24h[0] = rep.Volume24hTop1
		s.TopVolume24h[1] = rep.Volume24hTop10
		s.TopVolume24h[2] = rep.Volume24hTop100
		s.TopVolume24h[3] = rep.Volume24hTop1k
		s.TopVolume24h[4] = rep.Volume24hTop10k
		s.TopVolume24h[5] = rep.Volume24hTop100k
		s.TopTraffic24h[0] = rep.Traffic24hTop1
		s.TopTraffic24h[1] = rep.Traffic24hTop10
		s.TopTraffic24h[2] = rep.Traffic24hTop100
		s.TopTraffic24h[3] = rep.Traffic24hTop1k
		s.TopTraffic24h[4] = rep.Traffic24hTop10k
		s.TopTraffic24h[5] = rep.Traffic24hTop100k
		return nil
	})
	if err != nil {
		return nil, err
	}

	// balance
	q = pack.Query{
		Name: "stats.account_balance",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: r.balanceTable.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		},
	}
	err = r.balanceTable.Stream(ctx, q, func(row pack.Row) error {
		rep := AccountBalanceReport{}
		if err := row.Decode(&rep); err != nil {
			return err
		}
		s.TotalFunded = rep.TotalFunded
		s.TotalFundedImplicit = rep.TotalFundedImplicit
		s.TotalFundedManaged = rep.TotalFundedManaged
		s.TotalFundedContracts = rep.TotalFundedContracts
		s.SumFundedImplicit = rep.SumFundedImplicit
		s.SumFundedManaged = rep.SumFundedManaged
		s.SumFundedContracts = rep.SumFundedContracts
		s.TopBalance[0] = rep.BalanceTop1
		s.TopBalance[1] = rep.BalanceTop10
		s.TopBalance[2] = rep.BalanceTop100
		s.TopBalance[3] = rep.BalanceTop1k
		s.TopBalance[4] = rep.BalanceTop10k
		s.TopBalance[5] = rep.BalanceTop100k
		s.TopDelegated[0] = rep.DelegatedTop1
		s.TopDelegated[1] = rep.DelegatedTop10
		s.TopDelegated[2] = rep.DelegatedTop100
		s.TopDelegated[3] = rep.DelegatedTop1k
		s.TopDelegated[4] = rep.DelegatedTop10k
		s.TopDelegated[5] = rep.DelegatedTop100k
		s.NumBalanceHist[0] = rep.NumBalanceHist1E0
		s.NumBalanceHist[1] = rep.NumBalanceHist1E1
		s.NumBalanceHist[2] = rep.NumBalanceHist1E2
		s.NumBalanceHist[3] = rep.NumBalanceHist1E3
		s.NumBalanceHist[4] = rep.NumBalanceHist1E4
		s.NumBalanceHist[5] = rep.NumBalanceHist1E5
		s.NumBalanceHist[6] = rep.NumBalanceHist1E6
		s.NumBalanceHist[7] = rep.NumBalanceHist1E7
		s.NumBalanceHist[8] = rep.NumBalanceHist1E8
		s.NumBalanceHist[9] = rep.NumBalanceHist1E9
		s.NumBalanceHist[10] = rep.NumBalanceHist1E10
		s.NumBalanceHist[11] = rep.NumBalanceHist1E11
		s.NumBalanceHist[12] = rep.NumBalanceHist1E12
		s.NumBalanceHist[13] = rep.NumBalanceHist1E13
		s.NumBalanceHist[14] = rep.NumBalanceHist1E14
		s.NumBalanceHist[15] = rep.NumBalanceHist1E15
		s.NumBalanceHist[16] = rep.NumBalanceHist1E16
		s.NumBalanceHist[17] = rep.NumBalanceHist1E17
		s.NumBalanceHist[18] = rep.NumBalanceHist1E18
		s.SumBalanceHist[0] = rep.SumBalanceHist1E0
		s.SumBalanceHist[1] = rep.SumBalanceHist1E1
		s.SumBalanceHist[2] = rep.SumBalanceHist1E2
		s.SumBalanceHist[3] = rep.SumBalanceHist1E3
		s.SumBalanceHist[4] = rep.SumBalanceHist1E4
		s.SumBalanceHist[5] = rep.SumBalanceHist1E5
		s.SumBalanceHist[6] = rep.SumBalanceHist1E6
		s.SumBalanceHist[7] = rep.SumBalanceHist1E7
		s.SumBalanceHist[8] = rep.SumBalanceHist1E8
		s.SumBalanceHist[9] = rep.SumBalanceHist1E9
		s.SumBalanceHist[10] = rep.SumBalanceHist1E10
		s.SumBalanceHist[11] = rep.SumBalanceHist1E11
		s.SumBalanceHist[12] = rep.SumBalanceHist1E12
		s.SumBalanceHist[13] = rep.SumBalanceHist1E13
		s.SumBalanceHist[14] = rep.SumBalanceHist1E14
		s.SumBalanceHist[15] = rep.SumBalanceHist1E15
		s.SumBalanceHist[16] = rep.SumBalanceHist1E16
		s.SumBalanceHist[17] = rep.SumBalanceHist1E17
		s.SumBalanceHist[18] = rep.SumBalanceHist1E18
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (r *AccountReport) StoreStats(ctx context.Context, stats *AccountStats) error {
	// age
	for i, _ := range stats.NumDormantYrs {
		rep := &AccountAgeReport{
			Timestamp:  stats.To,
			Height:     stats.Height,
			Year:       i,
			Quarter:    0,
			NumDormant: stats.NumDormantYrs[i],
			SumDormant: stats.SumDormantYrs[i],
		}
		if err := r.ageTable.Insert(ctx, rep); err != nil {
			return err
		}
	}
	for i, _ := range stats.NumDormantQtrs {
		rep := &AccountAgeReport{
			Timestamp:  stats.To,
			Height:     stats.Height,
			Year:       i / 4,
			Quarter:    (i + 1) % 4, // start counting at 1
			NumDormant: stats.NumDormantQtrs[i],
			SumDormant: stats.SumDormantQtrs[i],
		}
		if err := r.ageTable.Insert(ctx, rep); err != nil {
			return err
		}
	}

	// activity
	actRep := &AccountActivityReport{
		Timestamp: stats.To,
		Height:    stats.Height,
	}
	actRep.Unique24h = stats.Unique24h
	actRep.New24h = stats.New24h
	actRep.Funded24h = stats.Funded24h
	actRep.Cleared24h = stats.Cleared24h
	actRep.ActiveBakers24h = stats.ActiveBakers24h
	actRep.ActiveEndorsers24h = stats.ActiveEndorsers24h
	actRep.Volume24h = stats.Volume24h
	actRep.Tx24h = stats.Tx24h
	actRep.ContractTx24h = stats.ContractTx24h
	actRep.Volume24hTop1 = stats.TopVolume24h[0]
	actRep.Volume24hTop10 = stats.TopVolume24h[1]
	actRep.Volume24hTop100 = stats.TopVolume24h[2]
	actRep.Volume24hTop1k = stats.TopVolume24h[3]
	actRep.Volume24hTop10k = stats.TopVolume24h[4]
	actRep.Volume24hTop100k = stats.TopVolume24h[5]
	actRep.Traffic24hTop1 = stats.TopTraffic24h[0]
	actRep.Traffic24hTop10 = stats.TopTraffic24h[1]
	actRep.Traffic24hTop100 = stats.TopTraffic24h[2]
	actRep.Traffic24hTop1k = stats.TopTraffic24h[3]
	actRep.Traffic24hTop10k = stats.TopTraffic24h[4]
	actRep.Traffic24hTop100k = stats.TopTraffic24h[5]
	if err := r.activityTable.Insert(ctx, actRep); err != nil {
		return err
	}

	// balance
	balRep := &AccountBalanceReport{
		Timestamp: stats.To,
		Height:    stats.Height,
	}
	balRep.TotalFunded = stats.TotalFunded
	balRep.TotalFundedImplicit = stats.TotalFundedImplicit
	balRep.TotalFundedManaged = stats.TotalFundedManaged
	balRep.TotalFundedContracts = stats.TotalFundedContracts
	balRep.SumFundedImplicit = stats.SumFundedImplicit
	balRep.SumFundedManaged = stats.SumFundedManaged
	balRep.SumFundedContracts = stats.SumFundedContracts
	balRep.BalanceTop1 = stats.TopBalance[0]
	balRep.BalanceTop10 = stats.TopBalance[1]
	balRep.BalanceTop100 = stats.TopBalance[2]
	balRep.BalanceTop1k = stats.TopBalance[3]
	balRep.BalanceTop10k = stats.TopBalance[4]
	balRep.BalanceTop100k = stats.TopBalance[5]
	balRep.DelegatedTop1 = stats.TopDelegated[0]
	balRep.DelegatedTop10 = stats.TopDelegated[1]
	balRep.DelegatedTop100 = stats.TopDelegated[2]
	balRep.DelegatedTop1k = stats.TopDelegated[3]
	balRep.DelegatedTop10k = stats.TopDelegated[4]
	balRep.DelegatedTop100k = stats.TopDelegated[5]
	balRep.NumBalanceHist1E0 = stats.NumBalanceHist[0]
	balRep.NumBalanceHist1E1 = stats.NumBalanceHist[1]
	balRep.NumBalanceHist1E2 = stats.NumBalanceHist[2]
	balRep.NumBalanceHist1E3 = stats.NumBalanceHist[3]
	balRep.NumBalanceHist1E4 = stats.NumBalanceHist[4]
	balRep.NumBalanceHist1E5 = stats.NumBalanceHist[5]
	balRep.NumBalanceHist1E6 = stats.NumBalanceHist[6]
	balRep.NumBalanceHist1E7 = stats.NumBalanceHist[7]
	balRep.NumBalanceHist1E8 = stats.NumBalanceHist[8]
	balRep.NumBalanceHist1E9 = stats.NumBalanceHist[9]
	balRep.NumBalanceHist1E10 = stats.NumBalanceHist[10]
	balRep.NumBalanceHist1E11 = stats.NumBalanceHist[11]
	balRep.NumBalanceHist1E12 = stats.NumBalanceHist[12]
	balRep.NumBalanceHist1E13 = stats.NumBalanceHist[13]
	balRep.NumBalanceHist1E14 = stats.NumBalanceHist[14]
	balRep.NumBalanceHist1E15 = stats.NumBalanceHist[15]
	balRep.NumBalanceHist1E16 = stats.NumBalanceHist[16]
	balRep.NumBalanceHist1E17 = stats.NumBalanceHist[17]
	balRep.NumBalanceHist1E18 = stats.NumBalanceHist[18]
	balRep.SumBalanceHist1E0 = stats.SumBalanceHist[0]
	balRep.SumBalanceHist1E1 = stats.SumBalanceHist[1]
	balRep.SumBalanceHist1E2 = stats.SumBalanceHist[2]
	balRep.SumBalanceHist1E3 = stats.SumBalanceHist[3]
	balRep.SumBalanceHist1E4 = stats.SumBalanceHist[4]
	balRep.SumBalanceHist1E5 = stats.SumBalanceHist[5]
	balRep.SumBalanceHist1E6 = stats.SumBalanceHist[6]
	balRep.SumBalanceHist1E7 = stats.SumBalanceHist[7]
	balRep.SumBalanceHist1E8 = stats.SumBalanceHist[8]
	balRep.SumBalanceHist1E9 = stats.SumBalanceHist[9]
	balRep.SumBalanceHist1E10 = stats.SumBalanceHist[10]
	balRep.SumBalanceHist1E11 = stats.SumBalanceHist[11]
	balRep.SumBalanceHist1E12 = stats.SumBalanceHist[12]
	balRep.SumBalanceHist1E13 = stats.SumBalanceHist[13]
	balRep.SumBalanceHist1E14 = stats.SumBalanceHist[14]
	balRep.SumBalanceHist1E15 = stats.SumBalanceHist[15]
	balRep.SumBalanceHist1E16 = stats.SumBalanceHist[16]
	balRep.SumBalanceHist1E17 = stats.SumBalanceHist[17]
	balRep.SumBalanceHist1E18 = stats.SumBalanceHist[18]
	if err := r.balanceTable.Insert(ctx, balRep); err != nil {
		return err
	}
	return nil
}

func (r *AccountReport) UpdateStats(ctx context.Context, stats *AccountStats) error {
	if err := r.DeleteStats(ctx, stats.Height, stats.To); err != nil {
		return err
	}
	if err := r.activityTable.Flush(ctx); err != nil {
		return err
	}
	if err := r.ageTable.Flush(ctx); err != nil {
		return err
	}
	if err := r.balanceTable.Flush(ctx); err != nil {
		return err
	}
	return r.StoreStats(ctx, stats)
}

func (r *AccountReport) DeleteStats(ctx context.Context, height int64, at time.Time) error {
	// age
	q := pack.Query{}
	if !at.IsZero() {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.ageTable.Fields().Find("T"), // time
				Mode:  pack.FilterModeEqual,
				Value: at,
			},
		}
	} else {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.ageTable.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		}
	}
	if _, err := r.ageTable.Delete(ctx, q); err != nil {
		return err
	}

	// activity (single entry)
	q = pack.Query{}
	if !at.IsZero() {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.activityTable.Fields().Find("T"), // time
				Mode:  pack.FilterModeEqual,
				Value: at,
			},
		}
	} else {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.activityTable.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		}
	}
	if _, err := r.activityTable.Delete(ctx, q); err != nil {
		return err
	}

	// balance (single entry)
	q = pack.Query{}
	if !at.IsZero() {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.balanceTable.Fields().Find("T"), // time
				Mode:  pack.FilterModeEqual,
				Value: at,
			},
		}
	} else {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.balanceTable.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		}
	}
	if _, err := r.balanceTable.Delete(ctx, q); err != nil {
		return err
	}

	return nil
}
