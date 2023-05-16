// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"io"
	"sort"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
)

func (m *Indexer) ElectionByHeight(ctx context.Context, height int64) (*model.Election, error) {
	table, err := m.Table(model.ElectionTableKey)
	if err != nil {
		return nil, err
	}
	// we are looking for the last election with start_height <= height
	e := &model.Election{}
	err = pack.NewQuery("api.election_height").
		WithTable(table).
		WithDesc().
		WithLimit(1).
		AndLte("start_height", height).
		Execute(ctx, e)
	if err != nil {
		return nil, err
	}
	if e.RowId == 0 {
		return nil, model.ErrNoElection
	}
	return e, nil
}

func (m *Indexer) ElectionById(ctx context.Context, id model.ElectionID) (*model.Election, error) {
	table, err := m.Table(model.ElectionTableKey)
	if err != nil {
		return nil, err
	}
	e := &model.Election{}
	err = pack.NewQuery("api.election_id").
		WithTable(table).
		WithLimit(1).
		AndEqual("I", id).
		Execute(ctx, e)
	if err != nil {
		return nil, err
	}
	if e.RowId == 0 {
		return nil, model.ErrNoElection
	}
	return e, nil
}

func (m *Indexer) VotesByElection(ctx context.Context, id model.ElectionID) ([]*model.Vote, error) {
	table, err := m.Table(model.VoteTableKey)
	if err != nil {
		return nil, err
	}
	votes := make([]*model.Vote, 0)
	err = pack.NewQuery("api.list_votes").
		WithTable(table).
		AndEqual("election_id", id).
		Execute(ctx, &votes)
	if err != nil {
		return nil, err
	}
	if len(votes) == 0 {
		return nil, model.ErrNoVote
	}
	return votes, nil
}

// r.Since is the true vote start block
func (m *Indexer) ListVoters(ctx context.Context, r ListRequest) ([]*model.Voter, error) {
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}

	// Step 1
	// collect voters from governance roll snapshot
	rollsTable, err := m.Table(model.StakeTableKey)
	if err != nil {
		return nil, err
	}
	q := pack.NewQuery("api.list_voters").
		WithTable(rollsTable).
		WithLimit(int(r.Limit)).
		WithOffset(int(r.Offset)).
		AndEqual("height", r.Since-1) // snapshots are made at end of previous vote

	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	voters := make(map[model.AccountID]*model.Voter)
	snap := &model.Stake{}
	err = q.Stream(ctx, func(r pack.Row) error {
		if err := r.Decode(snap); err != nil {
			return err
		}
		voters[snap.AccountId] = &model.Voter{
			RowId: snap.AccountId,
			Rolls: snap.Rolls,
			Stake: snap.Stake,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Step 2: list ballots
	ballotTable, err := m.Table(model.BallotTableKey)
	if err != nil {
		return nil, err
	}
	ballot := &model.Ballot{}
	err = pack.NewQuery("api.list_voters").
		WithTable(ballotTable).
		AndEqual("voting_period", r.Period).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(ballot); err != nil {
				return err
			}
			if voter, ok := voters[ballot.SourceId]; ok {
				voter.Ballot = ballot.Ballot
				voter.Time = ballot.Time
				voter.HasVoted = true
				found := false
				for _, v := range voter.Proposals {
					if v != ballot.ProposalId {
						continue
					}
					found = true
					break
				}
				if !found {
					voter.Proposals = append(voter.Proposals, ballot.ProposalId)
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	out := make([]*model.Voter, 0, len(voters))
	for _, v := range voters {
		out = append(out, v)
	}
	if r.Order == pack.OrderAsc {
		sort.Slice(out, func(i, j int) bool { return out[i].RowId < out[j].RowId })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].RowId > out[j].RowId })
	}
	return out, nil
}

func (m *Indexer) ProposalsByElection(ctx context.Context, id model.ElectionID) ([]*model.Proposal, error) {
	table, err := m.Table(model.ProposalTableKey)
	if err != nil {
		return nil, err
	}
	proposals := make([]*model.Proposal, 0)
	err = pack.NewQuery("api.list_proposals").
		WithTable(table).
		AndEqual("election_id", id).
		Execute(ctx, &proposals)
	if err != nil {
		return nil, err
	}
	return proposals, nil
}

func (m *Indexer) LookupProposal(ctx context.Context, proto tezos.ProtocolHash) (*model.Proposal, error) {
	if !proto.IsValid() {
		return nil, model.ErrInvalidProtocolHash
	}

	table, err := m.Table(model.ProposalTableKey)
	if err != nil {
		return nil, err
	}

	// use hash and type to protect against duplicates
	prop := &model.Proposal{}
	err = pack.NewQuery("api.proposal_by_hash").
		WithTable(table).
		AndEqual("hash", proto[:]).
		Execute(ctx, prop)
	if err != nil {
		return nil, err
	}
	if prop.RowId == 0 {
		return nil, model.ErrNoProposal
	}
	return prop, nil
}

func (m *Indexer) LookupProposalIds(ctx context.Context, ids []uint64) ([]*model.Proposal, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	table, err := m.Table(model.ProposalTableKey)
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
		return nil, model.ErrNoProposal
	}
	props = props[:count]
	return props, nil
}

func (m *Indexer) ListBallots(ctx context.Context, r ListRequest) ([]*model.Ballot, error) {
	table, err := m.Table(model.BallotTableKey)
	if err != nil {
		return nil, err
	}
	// cursor and offset are mutually exclusive
	if r.Cursor > 0 {
		r.Offset = 0
	}
	q := pack.NewQuery("api.list_ballots").
		WithTable(table).
		WithOrder(r.Order).
		WithOffset(int(r.Offset)).
		WithLimit(int(r.Limit))
	if r.Account != nil {
		// clamp time range to account lifetime
		r.Since = util.Max64(r.Since, r.Account.FirstSeen-1)
		r.Until = util.NonZeroMin64(r.Until, r.Account.LastSeen)
		q = q.AndEqual("source_id", r.Account.RowId)
	}
	if r.Period > 0 {
		q = q.AndEqual("voting_period", r.Period)
	}
	if r.Since > 0 {
		q = q.AndGt("height", r.Since)
	}
	if r.Until > 0 {
		q = q.AndLte("height", r.Until)
	}
	if r.Cursor > 0 {
		if r.Order == pack.OrderDesc {
			q = q.AndLt("I", r.Cursor)
		} else {
			q = q.AndGt("I", r.Cursor)
		}
	}
	ballots := make([]*model.Ballot, 0)
	if err := q.Execute(ctx, &ballots); err != nil {
		return nil, err
	}
	return ballots, nil
}
