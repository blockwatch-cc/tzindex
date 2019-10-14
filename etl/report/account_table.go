// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl"
)

var (
	accountAgeTableKey      = "age"
	accountActivityTableKey = "activity"
	accountBalanceTableKey  = "balance"
)

// by blockchain age in years
type AccountAgeReport struct {
	RowId      uint64    `pack:"I,pk,snappy"`
	Timestamp  time.Time `pack:"T,snappy"`
	Height     int64     `pack:"h,snappy"`
	Year       int       `pack:"y,snappy"`
	Quarter    int       `pack:"q,snappy"`
	NumDormant int64     `pack:"a,snappy"`
	SumDormant float64   `pack:"f,snappy"`
}

var _ pack.Item = (*AccountAgeReport)(nil)

func (a AccountAgeReport) ID() uint64 {
	return a.RowId
}

func (a *AccountAgeReport) SetID(id uint64) {
	a.RowId = id
}

type AccountActivityReport struct {
	RowId              uint64    `pack:"I,pk,snappy"`
	Timestamp          time.Time `pack:"T,snappy"`
	Height             int64     `pack:"h,snappy"`
	Unique24h          int       `pack:"u,snappy"` // count of all accounts seen in last 24 hours
	New24h             int       `pack:"n,snappy"` // count of all new accounts seen in last 24 hours
	Funded24h          int       `pack:"f,snappy"` // count of all funded accounts last 24 hours
	Cleared24h         int       `pack:"e,snappy"` // count of all emptied accounts last 24 hours
	ActiveBakers24h    int       `pack:"B,snappy"` // num active bakers seen in last 24 hours
	ActiveEndorsers24h int       `pack:"E,snappy"` // num active endorsers seen in last 24 hours
	Volume24h          float64   `pack:"v,snappy"` // sum of tx volume last 24h
	Tx24h              int       `pack:"x,snappy"` // count of all spendable vout last 24h
	ContractTx24h      int       `pack:"o,snappy"` // count of all spendable vout last 24h
	Volume24hTop1      float64   `pack:"0,snappy"` // sum tx volume for top N account with most volume
	Volume24hTop10     float64   `pack:"1,snappy"`
	Volume24hTop100    float64   `pack:"2,snappy"`
	Volume24hTop1k     float64   `pack:"3,snappy"`
	Volume24hTop10k    float64   `pack:"4,snappy"`
	Volume24hTop100k   float64   `pack:"5,snappy"`
	Traffic24hTop1     int       `pack:"6,snappy"` // sum tx count for top N account with most traffic
	Traffic24hTop10    int       `pack:"7,snappy"`
	Traffic24hTop100   int       `pack:"8,snappy"`
	Traffic24hTop1k    int       `pack:"9,snappy"`
	Traffic24hTop10k   int       `pack:"a,snappy"`
	Traffic24hTop100k  int       `pack:"b,snappy"`
}

var _ pack.Item = (*AccountActivityReport)(nil)

func (a AccountActivityReport) ID() uint64 {
	return a.RowId
}

func (a *AccountActivityReport) SetID(id uint64) {
	a.RowId = id
}

type AccountBalanceReport struct {
	RowId                uint64    `pack:"I,pk,snappy"`
	Timestamp            time.Time `pack:"T,snappy"`
	Height               int64     `pack:"h,snappy"`
	TotalFunded          int       `pack:"t,snappy"`  // count of all funded accounts (for percent calculations)
	TotalFundedImplicit  int       `pack:"i,snappy"`  // count of all funded accounts (for percent calculations)
	TotalFundedManaged   int       `pack:"m,snappy"`  // count of all funded accounts (for percent calculations)
	TotalFundedContracts int       `pack:"c,snappy"`  // count of all funded accounts (for percent calculations)
	SumFundedImplicit    float64   `pack:"K,snappy"`  // sum account balances at type tz1/2/3
	SumFundedManaged     float64   `pack:"M,snappy"`  // sum account balances at type KT1 no-code
	SumFundedContracts   float64   `pack:"C,snappy"`  // sum account balances at type KT1 smart contract
	BalanceTop1          float64   `pack:"t0,snappy"` // total balance for top account N=1/10/100/1k/10k/100k
	BalanceTop10         float64   `pack:"t1,snappy"` // total balance for top account N=1/10/100/1k/10k/100k
	BalanceTop100        float64   `pack:"t2,snappy"` // total balance for top account N=1/10/100/1k/10k/100k
	BalanceTop1k         float64   `pack:"t3,snappy"` // total balance for top account N=1/10/100/1k/10k/100k
	BalanceTop10k        float64   `pack:"t4,snappy"` // total balance for top account N=1/10/100/1k/10k/100k
	BalanceTop100k       float64   `pack:"t5,snappy"` // total balance for top account N=1/10/100/1k/10k/100k
	DelegatedTop1        float64   `pack:"d0,snappy"` // total delegated balance for top delegate N=1/10/100/1k/10k/100k
	DelegatedTop10       float64   `pack:"d1,snappy"` // total delegated balance for top delegate N=1/10/100/1k/10k/100k
	DelegatedTop100      float64   `pack:"d2,snappy"` // total delegated balance for top delegate N=1/10/100/1k/10k/100k
	DelegatedTop1k       float64   `pack:"d3,snappy"` // total delegated balance for top delegate N=1/10/100/1k/10k/100k
	DelegatedTop10k      float64   `pack:"d4,snappy"` // total delegated balance for top delegate N=1/10/100/1k/10k/100k
	DelegatedTop100k     float64   `pack:"d5,snappy"` // total delegated balance for top delegate N=1/10/100/1k/10k/100k
	NumBalanceHist1E0    int       `pack:"n0,snappy"` // count of accounts per bin (10e8..10e-8 in value)
	NumBalanceHist1E1    int       `pack:"n1,snappy"`
	NumBalanceHist1E2    int       `pack:"n2,snappy"`
	NumBalanceHist1E3    int       `pack:"n3,snappy"`
	NumBalanceHist1E4    int       `pack:"n4,snappy"`
	NumBalanceHist1E5    int       `pack:"n5,snappy"`
	NumBalanceHist1E6    int       `pack:"n6,snappy"`
	NumBalanceHist1E7    int       `pack:"n7,snappy"`
	NumBalanceHist1E8    int       `pack:"n8,snappy"`
	NumBalanceHist1E9    int       `pack:"n9,snappy"`
	NumBalanceHist1E10   int       `pack:"n10,snappy"`
	NumBalanceHist1E11   int       `pack:"n11,snappy"`
	NumBalanceHist1E12   int       `pack:"n12,snappy"`
	NumBalanceHist1E13   int       `pack:"n13,snappy"`
	NumBalanceHist1E14   int       `pack:"n14,snappy"`
	NumBalanceHist1E15   int       `pack:"n15,snappy"`
	NumBalanceHist1E16   int       `pack:"n16,snappy"`
	NumBalanceHist1E17   int       `pack:"n17,snappy"`
	NumBalanceHist1E18   int       `pack:"n18,snappy"`
	NumBalanceHist1E19   int       `pack:"n19,snappy"`
	SumBalanceHist1E0    float64   `pack:"s0,snappy"` // sum of balances for accounts per bin
	SumBalanceHist1E1    float64   `pack:"s1,snappy"`
	SumBalanceHist1E2    float64   `pack:"s2,snappy"`
	SumBalanceHist1E3    float64   `pack:"s3,snappy"`
	SumBalanceHist1E4    float64   `pack:"s4,snappy"`
	SumBalanceHist1E5    float64   `pack:"s5,snappy"`
	SumBalanceHist1E6    float64   `pack:"s6,snappy"`
	SumBalanceHist1E7    float64   `pack:"s7,snappy"`
	SumBalanceHist1E8    float64   `pack:"s8,snappy"`
	SumBalanceHist1E9    float64   `pack:"s9,snappy"`
	SumBalanceHist1E10   float64   `pack:"s10,snappy"`
	SumBalanceHist1E11   float64   `pack:"s11,snappy"`
	SumBalanceHist1E12   float64   `pack:"s12,snappy"`
	SumBalanceHist1E13   float64   `pack:"s13,snappy"`
	SumBalanceHist1E14   float64   `pack:"s14,snappy"`
	SumBalanceHist1E15   float64   `pack:"s15,snappy"`
	SumBalanceHist1E16   float64   `pack:"s16,snappy"`
	SumBalanceHist1E17   float64   `pack:"s17,snappy"`
	SumBalanceHist1E18   float64   `pack:"s18,snappy"`
}

var _ pack.Item = (*AccountBalanceReport)(nil)

func (a AccountBalanceReport) ID() uint64 {
	return a.RowId
}

func (a *AccountBalanceReport) SetID(id uint64) {
	a.RowId = id
}

func (r *AccountReport) InitTable(db *pack.DB) error {
	opts := pack.Options{
		PackSizeLog2:    etl.ReportTablePackSizeLog2,
		JournalSizeLog2: etl.ReportTableJournalSizeLog2,
		CacheSize:       etl.ReportTableCacheSize,
		FillLevel:       etl.ReportTableFillLevel,
	}

	// activity
	fields, err := pack.Fields(AccountActivityReport{})
	if err != nil {
		return err
	}
	r.activityTable, err = db.CreateTableIfNotExists(accountActivityTableKey, fields, opts)
	if err != nil {
		return err
	}

	// age
	fields, err = pack.Fields(AccountAgeReport{})
	if err != nil {
		return err
	}
	r.ageTable, err = db.CreateTableIfNotExists(accountAgeTableKey, fields, opts)
	if err != nil {
		return err
	}

	// balance
	fields, err = pack.Fields(AccountBalanceReport{})
	if err != nil {
		return err
	}
	r.balanceTable, err = db.CreateTableIfNotExists(accountBalanceTableKey, fields, opts)
	if err != nil {
		return err
	}

	return nil
}

func (r *AccountReport) Tables() []*pack.Table {
	return []*pack.Table{
		r.ageTable,
		r.activityTable,
		r.balanceTable,
	}
}

func (r *AccountReport) Flush(ctx context.Context) error {
	if r.activityTable != nil {
		if err := r.activityTable.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", accountActivityTableKey, err)
		}
	}
	if r.ageTable != nil {
		if err := r.ageTable.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", accountAgeTableKey, err)
		}
	}
	if r.balanceTable != nil {
		if err := r.balanceTable.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", accountBalanceTableKey, err)
		}
	}
	return nil
}

func (r *AccountReport) FlushJournal(ctx context.Context) error {
	if r.activityTable != nil {
		if err := r.activityTable.FlushJournal(ctx); err != nil {
			log.Errorf("Flushing %s journal: %v", accountActivityTableKey, err)
		}
	}
	if r.ageTable != nil {
		if err := r.ageTable.FlushJournal(ctx); err != nil {
			log.Errorf("Flushing %s journal: %v", accountAgeTableKey, err)
		}
	}
	if r.balanceTable != nil {
		if err := r.balanceTable.FlushJournal(ctx); err != nil {
			log.Errorf("Flushing %s journal: %v", accountBalanceTableKey, err)
		}
	}
	return nil
}

func (r *AccountReport) CloseTable() error {
	if r.activityTable != nil {
		if err := r.activityTable.Close(); err != nil {
			log.Errorf("Closing %s table: %v", accountActivityTableKey, err)
		}
		r.activityTable = nil
	}
	if r.ageTable != nil {
		if err := r.ageTable.Close(); err != nil {
			log.Errorf("Closing %s table: %v", accountAgeTableKey, err)
		}
		r.ageTable = nil
	}
	if r.balanceTable != nil {
		if err := r.balanceTable.Close(); err != nil {
			log.Errorf("Closing %s table: %v", accountBalanceTableKey, err)
		}
		r.balanceTable = nil
	}
	return nil
}
