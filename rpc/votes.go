// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"blockwatch.cc/tzindex/chain"
)

// Voter holds information about a vote listing
type Voter struct {
	Delegate chain.Address `json:"pkh"`
	Rolls    int64         `json:"rolls"`
}

// VoterList contains a list of voters
type VoterList []Voter

// Ballot holds information about a vote listing
type Ballot struct {
	Delegate chain.Address    `json:"pkh"`
	Ballot   chain.BallotVote `json:"ballot"`
}

// BallotList contains a list of voters
type BallotList []Ballot

// Ballots holds the current summary of a vote
type BallotSummary struct {
	Yay  int `json:"yay"`
	Nay  int `json:"nay"`
	Pass int `json:"pass"`
}

// Proposal holds information about a vote listing
type Proposal struct {
	Proposal chain.ProtocolHash
	Upvotes  int64
}

func (p *Proposal) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 || len(data) == 2 {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("rpc: proposal: expected JSON array")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return fmt.Errorf("rpc: proposal: %v", err)
	}
	if len(unpacked) != 2 {
		return fmt.Errorf("rpc: proposal: invalid JSON array")
	}
	if err := p.Proposal.UnmarshalText([]byte(unpacked[0].(string))); err != nil {
		return fmt.Errorf("rpc: proposal: %v", err)
	}
	p.Upvotes, err = strconv.ParseInt(unpacked[1].(json.Number).String(), 10, 64)
	if err != nil {
		return fmt.Errorf("rpc: proposal: %v", err)
	}
	return nil
}

// ProposalList contains a list of voters
type ProposalList []Proposal

// ListVoters returns information about all eligible voters for an election
// https://tezos.gitlab.io/api/rpc.html#rpcs-full-description
func (c *Client) ListVoters(ctx context.Context, height int64) (VoterList, error) {
	voters := make(VoterList, 0)
	u := fmt.Sprintf("chains/%s/blocks/%d/votes/listings", c.ChainID, height)
	if err := c.Get(ctx, u, &voters); err != nil {
		return nil, err
	}
	return voters, nil
}

// GetVoteQuorum returns information about the current voring quorum at a specific height
// Returned value is percent * 10000 i.e. 5820 for 58.20%
// https://tezos.gitlab.io/api/rpc.html#rpcs-full-description
func (c *Client) GetVoteQuorum(ctx context.Context, height int64) (int, error) {
	var quorum int
	u := fmt.Sprintf("chains/%s/blocks/%d/votes/current_quorum", c.ChainID, height)
	if err := c.Get(ctx, u, &quorum); err != nil {
		return 0, err
	}
	return quorum, nil
}

// GetVoteProposal returns the hash of the current voring proposal at a specific height
// https://tezos.gitlab.io/api/rpc.html#rpcs-full-description
func (c *Client) GetVoteProposal(ctx context.Context, height int64) (chain.ProtocolHash, error) {
	var proposal chain.ProtocolHash
	u := fmt.Sprintf("chains/%s/blocks/%d/votes/current_proposal", c.ChainID, height)
	err := c.Get(ctx, u, &proposal)
	return proposal, err
}

// ListBallots returns information about all eligible voters for an election
// https://tezos.gitlab.io/api/rpc.html#rpcs-full-description
func (c *Client) ListBallots(ctx context.Context, height int64) (BallotList, error) {
	ballots := make(BallotList, 0)
	u := fmt.Sprintf("chains/%s/blocks/%d/votes/ballot_list", c.ChainID, height)
	if err := c.Get(ctx, u, &ballots); err != nil {
		return nil, err
	}
	return ballots, nil
}

// GetVoteResult returns a summary of the current voting result at a specific height
// https://tezos.gitlab.io/api/rpc.html#rpcs-full-description
func (c *Client) GetVoteResult(ctx context.Context, height int64) (BallotSummary, error) {
	summary := BallotSummary{}
	u := fmt.Sprintf("chains/%s/blocks/%d/votes/ballots", c.ChainID, height)
	err := c.Get(ctx, u, &summary)
	return summary, err
}

// ListProposals returns a list of all submitted proposals and their upvote count
// This call only returns results when height is within a proposal vote period.
// https://tezos.gitlab.io/api/rpc.html#rpcs-full-description
func (c *Client) ListProposals(ctx context.Context, height int64) (ProposalList, error) {
	proposals := make(ProposalList, 0)
	u := fmt.Sprintf("chains/%s/blocks/%d/votes/proposals", c.ChainID, height)
	if err := c.Get(ctx, u, &proposals); err != nil {
		return nil, err
	}
	return proposals, nil
}
