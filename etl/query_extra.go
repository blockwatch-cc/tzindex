// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "time"

    "blockwatch.cc/packdb/pack"
    // "blockwatch.cc/packdb/vec"
    "blockwatch.cc/tzindex/etl/index"
    "blockwatch.cc/tzindex/etl/model"
)

type Growth struct {
    NewAccounts     int64
    NewContracts    int64
    ClearedAccounts int64
    FundedAccounts  int64
}

func (m *Indexer) GrowthByDuration(ctx context.Context, to time.Time, d time.Duration) (*Growth, error) {
    table, err := m.Table(index.BlockTableKey)
    if err != nil {
        return nil, err
    }
    type XBlock struct {
        NewAccounts     int64 `pack:"A"`
        NewContracts    int64 `pack:"C"`
        ClearedAccounts int64 `pack:"E"`
        FundedAccounts  int64 `pack:"J"`
    }
    from := to.Add(-d)
    x := &XBlock{}
    g := &Growth{}
    err = pack.NewQuery("aggregate_growth").
        WithTable(table).
        WithFields("A", "C", "E", "J").
        AndRange("time", from, to). // search for timestamp
        Stream(ctx, func(r pack.Row) error {
            if err := r.Decode(x); err != nil {
                return err
            }
            g.NewAccounts += x.NewAccounts
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

// luck, performance, contribution (reliability)
func (m *Indexer) BakerPerformance(ctx context.Context, id model.AccountID, fromCycle, toCycle int64) ([3]int64, error) {
    perf := [3]int64{}
    table, err := m.Table(index.IncomeTableKey)
    if err != nil {
        return perf, err
    }
    q := pack.NewQuery("baker_income").
        WithTable(table).
        AndEqual("account_id", id).
        AndRange("cycle", fromCycle, toCycle)
    var count int64
    income := &model.Income{}
    err = q.Stream(ctx, func(r pack.Row) error {
        if err := r.Decode(income); err != nil {
            return err
        }
        perf[0] += income.LuckPct
        perf[1] += income.PerformancePct
        perf[2] += income.ContributionPct
        count++
        return nil
    })
    if err != nil {
        return perf, err
    }
    if count > 0 {
        perf[0] /= count
        perf[1] /= count
        perf[2] /= count
    }
    return perf, nil
}

// func (m *Indexer) GetLifetimeRewards(ctx context.Context, acc *model.Account, payoutMap map[uint64][]uint64) (total int64, last time.Time, err error) {
//     // get list of previous bakers from snapshot table
//     var snap, ops *pack.Table
//     snap, err = m.Table(index.SnapshotTableKey)
//     if err != nil {
//         return
//     }
//     bakers := make([]uint64, 0)
//     type XSnap struct {
//         DelegateId model.AccountID `pack:"d"`
//     }
//     xs := &XSnap{}
//     err = pack.NewQuery("api.list_historic_bakers").
//         WithTable(snap).
//         AndGte("height", acc.FirstSeen).
//         AndLte("height", acc.LastSeen).
//         AndEqual("account_id", acc.RowId).
//         WithFields("d").
//         Stream(ctx, func(r pack.Row) error {
//             if err := r.Decode(xs); err != nil {
//                 return err
//             }
//             bakers = vec.Uint64.AddUnique(bakers, xs.DelegateId.Value())
//             return nil
//         })
//     if err != nil {
//         return
//     }

//     // match payout addresses for these bakers from metadata, append payout addrs
//     for i, l := 0, len(bakers); i < l; i++ {
//         pay, ok := payoutMap[bakers[i]]
//         if !ok {
//             continue
//         }
//         for _, v := range pay {
//             bakers = vec.Uint64.AddUnique(bakers, v)
//         }
//     }

//     // accumulate transactions from baker and payout addresses to account
//     ops, err = m.Table(index.OpTableKey)
//     if err != nil {
//         return
//     }
//     type XOp struct {
//         Height int64 `pack:"h"`
//         Volume int64 `pack:"v"`
//     }
//     xo := &XOp{}
//     err = pack.NewQuery("api.list_historic_payouts").
//         WithTable(ops).
//         AndRange("height", acc.FirstSeen, acc.LastSeen).
//         AndEqual("receiver_id", acc.RowId).
//         AndEqual("is_success", true).
//         AndIn("sender_id", bakers).
//         WithFields("h", "v").
//         Stream(ctx, func(r pack.Row) error {
//             if err := r.Decode(xo); err != nil {
//                 return err
//             }
//             total += xo.Volume
//             return nil
//         })
//     if err != nil {
//         return
//     }
//     if xo.Height > 0 {
//         last = m.LookupBlockTime(ctx, xo.Height)
//     }
//     return
// }

// func (m *Indexer) GetEstimatedRewards(ctx context.Context, acc *model.Account) (int64, error) {
//     params, height := m.ParamsByHeight(-1), m.BestHeight()
//     if acc.BakerId == 0 {
//         return 0, nil
//     }

//     // estimate rewards based on baker income and own share in delegation

//     // include the current cycle and consider all future cycles for a baker (from income)
//     // then look at where the delegator owns a share of rewards (from snapshot)
//     startCycle := params.CycleFromHeight(height)
//     endCycle := params.CycleFromHeight(height) + params.PreservedCycles

//     // lookup current baker estimated income for current + 5 cycles from income
//     income, err := m.Table(index.IncomeTableKey)
//     if err != nil {
//         return 0, err
//     }
//     type ShareIncome struct {
//         Cycle     int64
//         Balance   int64 // own baker balance
//         Delegated int64 // total sum of delegations to baker
//         Income    int64 // eligible baker income
//         Share     int64 // amount owned by queried delegator
//     }
//     var (
//         ic       model.Income
//         sn       model.Snapshot
//         perCycle = make(map[int64]ShareIncome)
//     )
//     // log.Infof("Listing baker income for %s from cycle=%d--%d", acc, startCycle, endCycle)

//     // NOTE: this algorithm assumes a baker
//     // - shares all baking + endorsing + fee income and take a fixed commission
//     //   (not all bakers work like that)
//     // - accusation income and loss are unknown (unless from current/live cycle),
//     //   and not shared with delegators
//     err = pack.NewQuery("api.list_income").
//         WithTable(income).
//         AndEqual("account_id", acc.BakerId).
//         AndGte("cycle", startCycle).
//         WithFields("cycle", "balance", "delegated", "expected_income", "fees_income").
//         Stream(ctx, func(r pack.Row) error {
//             if err := r.Decode(&ic); err != nil {
//                 return err
//             }
//             perCycle[ic.Cycle] = ShareIncome{
//                 Cycle:     ic.Cycle,
//                 Balance:   ic.Balance,
//                 Delegated: ic.Delegated,
//                 Income:    ic.ExpectedIncome + ic.FeesIncome,
//             }
//             return nil
//         })
//     if err != nil {
//         return 0, err
//     }

//     // lookup current delegator share with baker over past 5 cycles from snapshot
//     // Note: the snapshot for rights leading to baker income is N-1 cycles before
//     // (Ithaca changed the diff to N-1, it used to be N-2 before)
//     offset := params.PreservedCycles + 1
//     snap, err := m.Table(index.SnapshotTableKey)
//     if err != nil {
//         return 0, err
//     }
//     // log.Infof("Listing delegator share for %s from snapshot cycles=%d--%d", acc, startCycle-offset, endCycle-offset)
//     err = pack.NewQuery("api.list_share").
//         WithTable(snap).
//         AndEqual("is_selected", true).
//         AndEqual("account_id", acc.RowId).
//         AndEqual("baker_id", acc.BakerId).
//         AndRange("cycle", startCycle-offset, endCycle-offset).
//         WithFields("cycle", "balance").
//         Stream(ctx, func(r pack.Row) error {
//             if err := r.Decode(&sn); err != nil {
//                 return err
//             }
//             // log.Infof("Snapshot balance at cycle %d is %d", sn.Cycle, sn.Balance)
//             si := perCycle[sn.Cycle+offset]
//             si.Share = sn.Balance
//             perCycle[sn.Cycle+offset] = si
//             return nil
//         })
//     if err != nil {
//         return 0, err
//     }

//     // aggregate income share per cycle
//     var total int64
//     for _, v := range perCycle {
//         pct := float64(v.Share) * 10e16 / float64(v.Delegated+v.Balance) / 10e16
//         // log.Infof("Cycle %d share=%d of=%d pct=%.16f%% amount=%.6f", v.Cycle, v.Share, v.Delegated+v.Balance, pct, float64(v.Income)*pct/1000000)
//         total += int64(float64(v.Income) * pct)
//     }
//     return total, nil
// }
