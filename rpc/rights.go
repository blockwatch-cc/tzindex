// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
)

// BakingRight holds simplified information about the right to bake a specific Tezos block
type BakingRight struct {
	Delegate string
	Level    int64
	Round    int
}

func (r BakingRight) Address() tezos.Address {
	a, _ := tezos.ParseAddress(r.Delegate)
	return a
}

func (r *BakingRight) UnmarshalJSON(data []byte) error {
	type FullBakingRight struct {
		Delegate string `json:"delegate"`
		Level    int64  `json:"level"`
		Priority int    `json:"priority"` // until v011
		Round    int    `json:"round"`    // v012+
	}
	var rr FullBakingRight
	err := json.Unmarshal(data, &rr)
	r.Delegate = rr.Delegate
	r.Level = rr.Level
	r.Round = rr.Priority + rr.Round
	return err
}

// EndorsingRight holds simplified information about the right to endorse
// a specific Tezos block
type EndorsingRight struct {
	Delegate string
	Level    int64
	Power    int
}

func (r EndorsingRight) Address() tezos.Address {
	a, _ := tezos.ParseAddress(r.Delegate)
	return a
}

type StakeInfo struct {
	ActiveStake int64         `json:"active_stake,string"`
	Baker       tezos.Address `json:"baker"`
}

type SnapshotInfo struct {
	LastRoll     []string    `json:"last_roll"`
	Nonces       []string    `json:"nonces"`
	RandomSeed   string      `json:"random_seed"`
	RollSnapshot int         `json:"roll_snapshot"`                         // until v011
	Cycle        int64       `json:"cycle"`                                 // added, not part of RPC response
	BakerStake   []StakeInfo `json:"selected_stake_distribution,omitempty"` // v012+
	TotalStake   int64       `json:"total_active_stake,string"`             // v012+
	// Slashed []??? "slashed_deposits"
}

type SnapshotIndex struct {
	Cycle int64 // the requested cycle that contains rights from the snapshot
	Base  int64 // the cycle where the snapshot happened
	Index int   // the index inside base where snapshot happened
}

// ListBakingRights returns information about baking rights at block id.
// Use max to set a max block priority (before Ithaca) or a max round (after Ithaca).
func (c *Client) ListBakingRights(ctx context.Context, id BlockID, max int, p *tezos.Params) ([]BakingRight, error) {
	maxSelector := "max_priority=%d"
	if p.Version >= 12 {
		maxSelector = "max_round=%d"
	}
	if p.Version < 6 {
		max++
	}
	rights := make([]BakingRight, 0)
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/baking_rights?all=true&"+maxSelector, id, max)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// ListBakingRightsCycle returns information about baking rights for an entire cycle
// as seen from block id. Note block and cycle must be no further than preserved cycles
// away from each other. Use max to set a max block priority (before Ithaca) or a max
// round (after Ithaca).
func (c *Client) ListBakingRightsCycle(ctx context.Context, id BlockID, cycle int64, max int, p *tezos.Params) ([]BakingRight, error) {
	maxSelector := "max_round=%d"
	if p.Version < 12 {
		maxSelector = "max_priority=%d"
	}
	if p.Version < 6 {
		max++
	}
	rights := make([]BakingRight, 0)
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/baking_rights?all=true&cycle=%d&"+maxSelector, id, cycle, max)
	if err := c.Get(ctx, u, &rights); err != nil {
		return nil, err
	}
	return rights, nil
}

// ListEndorsingRights returns information about block endorsing rights.
func (c *Client) ListEndorsingRights(ctx context.Context, id BlockID, p *tezos.Params) ([]EndorsingRight, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/endorsing_rights?all=true", id)
	rights := make([]EndorsingRight, 0)
	// Note: future cycles are seen from current protocol (!)
	if p.Version >= 12 {
		type V12Rights struct {
			Level     int64 `json:"level"`
			Delegates []struct {
				Delegate string `json:"delegate"`
				Power    int    `json:"endorsing_power"`
			} `json:"delegates"`
		}
		v12rights := make([]V12Rights, 0)
		if err := c.Get(ctx, u, &v12rights); err != nil {
			return nil, err
		}
		for _, v := range v12rights {
			for _, r := range v.Delegates {
				rights = append(rights, EndorsingRight{
					Level:    v.Level,
					Delegate: r.Delegate,
					Power:    r.Power,
				})
			}
		}
	} else {
		type Rights struct {
			Level    int64  `json:"level"`
			Delegate string `json:"delegate"`
			Slots    []int  `json:"slots"`
		}
		list := make([]Rights, 0)
		if err := c.Get(ctx, u, &list); err != nil {
			return nil, err
		}
		for _, r := range list {
			rights = append(rights, EndorsingRight{
				Level:    r.Level,
				Delegate: r.Delegate,
				Power:    len(r.Slots),
			})
		}
	}
	return rights, nil
}

// ListEndorsingRightsCycle returns information about endorsing rights for an entire cycle
// as seen from block id. Note block and cycle must be no further than preserved cycles
// away.
func (c *Client) ListEndorsingRightsCycle(ctx context.Context, id BlockID, cycle int64, p *tezos.Params) ([]EndorsingRight, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/endorsing_rights?all=true&cycle=%d", id, cycle)
	rights := make([]EndorsingRight, 0)
	// Note: future cycles are seen from current protocol (!)
	if p.Version >= 12 {
		type V12Rights struct {
			Level     int64 `json:"level"`
			Delegates []struct {
				Delegate string `json:"delegate"`
				Power    int    `json:"endorsing_power"`
			} `json:"delegates"`
		}
		v12rights := make([]V12Rights, 0)
		if err := c.Get(ctx, u, &v12rights); err != nil {
			return nil, err
		}
		for _, v := range v12rights {
			for _, r := range v.Delegates {
				rights = append(rights, EndorsingRight{
					Level:    v.Level,
					Delegate: r.Delegate,
					Power:    r.Power,
				})
			}
		}
	} else {
		type Rights struct {
			Level    int64  `json:"level"`
			Delegate string `json:"delegate"`
			Slots    []int  `json:"slots"`
		}
		list := make([]Rights, 0)
		if err := c.Get(ctx, u, &list); err != nil {
			return nil, err
		}
		for _, r := range list {
			rights = append(rights, EndorsingRight{
				Level:    r.Level,
				Delegate: r.Delegate,
				Power:    len(r.Slots),
			})
		}
	}
	return rights, nil
}

// GetSnapshotInfoCycle returns information about a roll snapshot as seen from block id.
// Note block and cycle must be no further than preserved cycles away.
func (c *Client) GetSnapshotInfoCycle(ctx context.Context, id BlockID, cycle int64) (*SnapshotInfo, error) {
	idx := &SnapshotInfo{
		Cycle:        cycle,
		RollSnapshot: -1,
	}
	u := fmt.Sprintf("chains/main/blocks/%s/context/raw/json/cycle/%d", id, cycle)
	if err := c.Get(ctx, u, idx); err != nil {
		return nil, err
	}
	if idx.RandomSeed == "" {
		return nil, fmt.Errorf("missing snapshot for cycle %d at block %s", cycle, id)
	}
	return idx, nil
}

// GetSnapshotIndexCycle returns information about a roll snapshot as seen from block id.
// Note block and cycle must be no further than preserved cycles away.
func (c *Client) GetSnapshotIndexCycle(ctx context.Context, id BlockID, cycle int64, p *tezos.Params) (*SnapshotIndex, error) {
	idx := &SnapshotIndex{}
	if p.Version >= 12 {
		idx.Cycle = cycle
		idx.Base = p.SnapshotBaseCycle(cycle)
		idx.Index = -1
		if cycle >= p.PreservedCycles+1 {
			u := fmt.Sprintf("chains/main/blocks/%s/context/selected_snapshot?cycle=%d", id, cycle)
			if err := c.Get(ctx, u, &idx.Index); err != nil {
				return nil, err
			}
		} else {
			log.Debugf("No snapshot for cycle %d", cycle)
		}
	} else {
		// pre-Ithaca we can at most look PRESERVED_CYCLES into the future since
		// the snapshot happened 2 cycles back from the block we're looking from.
		var info SnapshotInfo
		u := fmt.Sprintf("chains/main/blocks/%s/context/raw/json/cycle/%d", id, cycle)
		if err := c.Get(ctx, u, &info); err != nil {
			return nil, err
		}
		if info.RandomSeed == "" {
			return nil, fmt.Errorf("missing snapshot for cycle %d at block %s", cycle, id)
		}
		idx.Cycle = cycle
		idx.Base = p.SnapshotBaseCycle(cycle)
		idx.Index = info.RollSnapshot
	}
	return idx, nil
}

func (c *Client) FetchRightsByCycle(ctx context.Context, height, cycle int64, bundle *Bundle) error {
	level := BlockLevel(height)
	if bundle.Params == nil {
		p, err := c.GetParams(ctx, level)
		if err != nil {
			return err
		}
		bundle.Params = p
		log.Debugf("Using fresh params with version=%d", p.Version)
	} else {
		log.Debugf("Using passed params with version=%d", bundle.Params.Version)
	}
	br, err := c.ListBakingRightsCycle(ctx, level, cycle, 0, bundle.Params)
	if err != nil {
		return err
	}
	if len(br) == 0 {
		return fmt.Errorf("empty baking rights, make sure your Tezos node runs in archive mode")
	}
	log.Debugf("Fetched %d baking rights for cycle %d at height %d", len(br), cycle, height)
	bundle.Baking = append(bundle.Baking, br)

	er, err := c.ListEndorsingRightsCycle(ctx, level, cycle, bundle.Params)
	if err != nil {
		return err
	}
	if len(er) == 0 {
		return fmt.Errorf("empty endorsing rights, make sure your Tezos node runs in archive mode")
	}
	log.Debugf("Fetched %d endorsing rights for cycle %d at height %d", len(er), cycle, height)
	bundle.Endorsing = append(bundle.Endorsing, er)

	// unavailable on genesis
	if height > 1 {
		prev, err := c.ListEndorsingRights(ctx, BlockLevel(height-1), bundle.Params)
		if err != nil {
			return err
		}
		if len(prev) == 0 {
			return fmt.Errorf("empty endorsing rights from last cycle end, make sure your Tezos node runs in archive mode")
		}
		bundle.PrevEndorsing = prev
	}
	snap, err := c.GetSnapshotIndexCycle(ctx, level, cycle, bundle.Params)
	if err != nil {
		// FIXME: kathmandunet does not know c4 snapshot at block 4097, but should
		log.Errorf("Fetching cycle index for c%d at block %d: %v", cycle, height, err)
		// return err
		snap = &SnapshotIndex{
			Cycle: cycle,
			Base:  bundle.Params.SnapshotBaseCycle(cycle),
			Index: 0, // guess, just return something
		}
	}
	bundle.Snapshot = snap
	info, err := c.GetSnapshotInfoCycle(ctx, level, cycle)
	if err != nil {
		// FIXME: kathmandunet does not know c4 snapshot at block 4097, but should
		log.Error(err)
		// return err
	}
	bundle.SnapInfo = info

	return nil
}
