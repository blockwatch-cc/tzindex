// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

import (
	"fmt"
)

type VotingPeriodKind int

const (
	VotingPeriodInvalid VotingPeriodKind = iota
	VotingPeriodProposal
	VotingPeriodTestingVote
	VotingPeriodTesting
	VotingPeriodPromotionVote
)

func (v VotingPeriodKind) IsValid() bool {
	return v != VotingPeriodInvalid
}

func (v *VotingPeriodKind) UnmarshalText(data []byte) error {
	vv := ParseVotingPeriod(string(data))
	if !vv.IsValid() {
		return fmt.Errorf("invalid voting period '%s'", string(data))
	}
	*v = vv
	return nil
}

func (v VotingPeriodKind) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v VotingPeriodKind) Num() int {
	switch v {
	case VotingPeriodProposal:
		return 1
	case VotingPeriodTestingVote:
		return 2
	case VotingPeriodTesting:
		return 3
	case VotingPeriodPromotionVote:
		return 4
	default:
		return 1
	}
}

func ToVotingPeriod(i int) VotingPeriodKind {
	switch i {
	case 2:
		return VotingPeriodTestingVote
	case 3:
		return VotingPeriodTesting
	case 4:
		return VotingPeriodPromotionVote
	default:
		return VotingPeriodProposal
	}
}

func ParseVotingPeriod(s string) VotingPeriodKind {
	switch s {
	case "proposal":
		return VotingPeriodProposal
	case "testing_vote":
		return VotingPeriodTestingVote
	case "testing":
		return VotingPeriodTesting
	case "promotion_vote":
		return VotingPeriodPromotionVote
	default:
		return VotingPeriodInvalid
	}
}

func (v VotingPeriodKind) String() string {
	switch v {
	case VotingPeriodProposal:
		return "proposal"
	case VotingPeriodTestingVote:
		return "testing_vote"
	case VotingPeriodTesting:
		return "testing"
	case VotingPeriodPromotionVote:
		return "promotion_vote"
	default:
		return ""
	}
}

type BallotVote int

const (
	BallotVoteInvalid BallotVote = iota
	BallotVoteYay
	BallotVoteNay
	BallotVotePass
)

func (v BallotVote) IsValid() bool {
	return v != BallotVoteInvalid
}

func (v *BallotVote) UnmarshalText(data []byte) error {
	vv := ParseBallotVote(string(data))
	if !vv.IsValid() {
		return fmt.Errorf("invalid ballot '%s'", string(data))
	}
	*v = vv
	return nil
}

func (v BallotVote) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func ParseBallotVote(s string) BallotVote {
	switch s {
	case "yay":
		return BallotVoteYay
	case "nay":
		return BallotVoteNay
	case "pass":
		return BallotVotePass
	default:
		return BallotVoteInvalid
	}
}

func (v BallotVote) String() string {
	switch v {
	case BallotVoteYay:
		return "yay"
	case BallotVoteNay:
		return "nay"
	case BallotVotePass:
		return "pass"
	default:
		return ""
	}
}
