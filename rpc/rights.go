// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

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
	ActiveStake ActiveStake   `json:"active_stake"`
	Baker       tezos.Address `json:"baker"`
}

// v12+ .. v17: single integer
// v18+: struct
type ActiveStake struct {
	Combined  int64 `json:"-"`
	Frozen    int64 `json:"frozen,string"`
	Delegated int64 `json:"delegated,string"`
}

func (s ActiveStake) Value() int64 {
	return s.Frozen + s.Delegated + s.Combined
}

func (s *ActiveStake) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	switch data[0] {
	case '{':
		type alias *ActiveStake
		return json.Unmarshal(data, alias(s))
	case '"':
		val, err := strconv.ParseInt(string(data[1:len(data)-1]), 10, 64)
		if err != nil {
			return err
		}
		s.Combined = val
	}
	return nil
}

type SnapshotInfo struct {
	LastRoll     []string    `json:"last_roll"`
	Nonces       []string    `json:"nonces"`
	RandomSeed   string      `json:"random_seed"`
	RollSnapshot int         `json:"roll_snapshot"`                         // until v011
	Cycle        int64       `json:"cycle"`                                 // added, not part of RPC response
	BakerStake   []StakeInfo `json:"selected_stake_distribution,omitempty"` // v012+
	TotalStake   ActiveStake `json:"total_active_stake"`                    // v012+, changed type in v18
	// Slashed []??? "slashed_deposits"
}

type SnapshotIndex struct {
	Cycle int64 // the requested cycle that contains rights from the snapshot
	Base  int64 // the cycle where the snapshot happened
	Index int   // the index inside base where snapshot happened
}

type SnapshotOwners struct {
	Cycle int64          `json:"cycle"`
	Index int64          `json:"index"`
	Rolls []SnapshotRoll `json:"rolls"`
}

type SnapshotRoll struct {
	RollId   int64
	OwnerKey tezos.Key
}

func (r *SnapshotRoll) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, []byte(`null`)) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] != '[' || data[len(data)-1] != ']' {
		return fmt.Errorf("SnapshotRoll: invalid json array '%s'", string(data))
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]any, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	return r.decode(unpacked)
}

func (r *SnapshotRoll) decode(unpacked []any) error {
	if l := len(unpacked); l != 2 {
		return fmt.Errorf("SnapshotRoll: invalid json array len %d", l)
	}
	id, err := strconv.ParseInt(unpacked[0].(json.Number).String(), 10, 64)
	if err != nil {
		return fmt.Errorf("SnapshotRoll: invalid roll id: %v", err)
	}
	if err = r.OwnerKey.UnmarshalText([]byte(unpacked[1].(string))); err != nil {
		return err
	}
	r.RollId = id
	return nil
}

// ListBakingRights returns information about baking rights at block id.
// Use max to set a max block priority (before Ithaca) or a max round (after Ithaca).
func (c *Client) ListBakingRights(ctx context.Context, id BlockID, max int, p *Params) ([]BakingRight, error) {
	maxSelector := "max_priority=%d"
	if p.Version >= 12 && p.IsPreIthacaNetworkAtStart() {
		maxSelector = "max_round=%d"
	}
	if p.Version < 6 && p.IsPreIthacaNetworkAtStart() {
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
func (c *Client) ListBakingRightsCycle(ctx context.Context, id BlockID, cycle int64, max int, p *Params) ([]BakingRight, error) {
	maxSelector := "max_round=%d"
	if p.Version < 12 && p.IsPreIthacaNetworkAtStart() {
		maxSelector = "max_priority=%d"
	}
	if p.Version < 6 && p.IsPreIthacaNetworkAtStart() {
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
func (c *Client) ListEndorsingRights(ctx context.Context, id BlockID, p *Params) ([]EndorsingRight, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/endorsing_rights?all=true", id)
	rights := make([]EndorsingRight, 0)
	// Note: future cycles are seen from current protocol (!)
	if p.Version < 12 && p.IsPreIthacaNetworkAtStart() {
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
	} else {
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
	}
	return rights, nil
}

// ListEndorsingRightsCycle returns information about endorsing rights for an entire cycle
// as seen from block id. Note block and cycle must be no further than preserved cycles
// away. On protocol changes future rights must be refetched!
func (c *Client) ListEndorsingRightsCycle(ctx context.Context, id BlockID, cycle int64, p *Params) ([]EndorsingRight, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/endorsing_rights?all=true&cycle=%d", id, cycle)
	rights := make([]EndorsingRight, 0)
	//
	switch {
	case p.Version < 12 && p.IsPreIthacaNetworkAtStart():
		// until Ithaca v012
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
	default:
		// FIXME: it seems this is still not removed
		// case p.Version >= 12 && p.Version <= 15:
		// until Lima v015
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
		// default:
		// Lima+ v016 (cannot fetch full cycle of endorsing rights)
		// TODO: fetch per block in parallel
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
func (c *Client) GetSnapshotIndexCycle(ctx context.Context, id BlockID, cycle int64, p *Params) (*SnapshotIndex, error) {
	idx := &SnapshotIndex{}
	if p.Version < 12 && p.IsPreIthacaNetworkAtStart() {
		// pre-Ithaca we can at most look PRESERVED_CYCLES into the future since
		// the snapshot happened 2 cycles back from the block we're looking from.
		var info SnapshotInfo
		u := fmt.Sprintf("chains/main/blocks/%s/context/raw/json/cycle/%d", id, cycle)
		// log.Infof("GET %s", u)
		if err := c.Get(ctx, u, &info); err != nil {
			return nil, err
		}
		if info.RandomSeed == "" {
			return nil, fmt.Errorf("missing snapshot for cycle %d at block %s", cycle, id)
		}
		idx.Cycle = cycle
		idx.Base = p.SnapshotBaseCycle(cycle)
		idx.Index = info.RollSnapshot
	} else {
		idx.Cycle = cycle
		idx.Base = p.SnapshotBaseCycle(cycle)
		idx.Index = 15
		// if cycle > p.PreservedCycles+1 {
		if idx.Base <= 0 {
			log.Debugf("No snapshot for cycle %d", cycle)
		} else {
			u := fmt.Sprintf("chains/main/blocks/%s/context/selected_snapshot?cycle=%d", id, cycle)
			// log.Infof("GET %s", u)
			if err := c.Get(ctx, u, &idx.Index); err != nil {
				return nil, err
			}
		}
	}
	return idx, nil
}

func (c *Client) FetchRightsByCycle(ctx context.Context, height, cycle int64, bundle *Bundle) error {
	level := BlockLevel(height)
	if bundle.Params == nil {
		p, err := c.GetParams(ctx, level)
		if err != nil {
			return fmt.Errorf("params: %v", err)
		}
		bundle.Params = p
		log.Debugf("Using fresh params for v%03d", p.Version)
	} else {
		log.Debugf("Using passed params for v%03d", bundle.Params.Version)
	}
	p := bundle.Params

	br, err := c.ListBakingRightsCycle(ctx, level, cycle, 0, p)
	if err != nil {
		return fmt.Errorf("baking: %v", err)
	}
	if len(br) == 0 {
		return fmt.Errorf("empty baking rights, make sure your Tezos node runs in archive mode")
	}
	log.Debugf("Fetched %d baking rights for cycle %d at height %d", len(br), cycle, height)
	bundle.Baking = append(bundle.Baking, br)

	er, err := c.ListEndorsingRightsCycle(ctx, level, cycle, p)
	if err != nil {
		return fmt.Errorf("endorsing: %v", err)
	}
	if len(er) == 0 {
		return fmt.Errorf("empty endorsing rights, make sure your Tezos node runs in archive mode")
	}
	log.Debugf("Fetched %d endorsing rights for cycle %d at height %d", len(er), cycle, height)
	bundle.Endorsing = append(bundle.Endorsing, er)

	// unavailable on genesis
	if height > 1 {
		prev, err := c.ListEndorsingRights(ctx, BlockLevel(height-1), p)
		if err != nil {
			return fmt.Errorf("last endorsing: %v", err)
		}
		if len(prev) == 0 {
			return fmt.Errorf("empty endorsing rights from last cycle end, make sure your Tezos node runs in archive mode")
		}
		bundle.PrevEndorsing = prev
	}

	// unavailable for the first preserved + 1 cycles (so 0..6 on mainnet)
	// post-Ithaca testnets have no snapshot for preserved + 1 cycles (0..4)
	snap, err := c.GetSnapshotIndexCycle(ctx, level, cycle, p)
	if err != nil {
		log.Errorf("Fetching snapshot index for c%d at block %d: %v", cycle, height, err)
		// return err
		snap = &SnapshotIndex{
			Cycle: cycle,
			Base:  p.SnapshotBaseCycle(cycle),
			Index: 15, // guess, just return something
		}
	}
	bundle.Snapshot = snap
	info, err := c.GetSnapshotInfoCycle(ctx, level, cycle)
	if err != nil {
		log.Errorf("Fetching snapshot info for c%d at block %d: %v", cycle, height, err)
		// return err
	}
	bundle.SnapInfo = info
	return nil
}

// ListSnapshotRollOwners returns information about a roll snapshot ownership.
// Response is a nested array `[[roll_id, pubkey]]`. Deprecated in Ithaca.
func (c *Client) ListSnapshotRollOwners(ctx context.Context, id BlockID, cycle, index int64) (*SnapshotOwners, error) {
	owners := &SnapshotOwners{Cycle: cycle, Index: index}
	u := fmt.Sprintf("chains/main/blocks/%s/context/raw/json/rolls/owner/snapshot/%d/%d?depth=1", id, cycle, index)
	if err := c.Get(ctx, u, &owners.Rolls); err != nil {
		return nil, err
	}
	return owners, nil
}

// GetEndorsingSlotOwner returns the address if the baker who owns endorsing slot in block.
func (c *Client) GetEndorsingSlotOwner(ctx context.Context, id BlockID, slot int, p *Params) (tezos.Address, error) {
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/endorsing_rights?all=true", id)
	if p.Version < 12 && p.IsPreIthacaNetworkAtStart() {
		type Rights struct {
			Level    int64  `json:"level"`
			Delegate string `json:"delegate"`
			Slots    []int  `json:"slots"`
		}
		rights := make([]Rights, 0)
		if err := c.Get(ctx, u, &rights); err != nil {
			return tezos.ZeroAddress, err
		}
		for _, r := range rights {
			for _, v := range r.Slots {
				if v == slot {
					addr, _ := tezos.ParseAddress(r.Delegate)
					return addr, nil
				}
			}
		}
	} else {
		type Rights struct {
			Level     int64 `json:"level"`
			Delegates []struct {
				Delegate string `json:"delegate"`
				Slot     int    `json:"first_slot"`
			} `json:"delegates"`
		}
		rights := make([]Rights, 0)
		if err := c.Get(ctx, u, &rights); err != nil {
			return tezos.ZeroAddress, err
		}
		for _, v := range rights {
			for _, r := range v.Delegates {
				if r.Slot == slot {
					addr, _ := tezos.ParseAddress(r.Delegate)
					return addr, nil
				}
			}
		}
	}
	return tezos.ZeroAddress, fmt.Errorf("Endorsing slot not found")
}

// GetBakingRightOwner returns the address if the baker who owns endorsing slot in block.
func (c *Client) GetBakingRightOwner(ctx context.Context, id BlockID, round int, p *Params) (tezos.Address, error) {
	maxSelector := "max_priority=%d"
	if p.Version >= 12 && p.IsPreIthacaNetworkAtStart() {
		maxSelector = "max_round=%d"
	}
	if p.Version < 6 && p.IsPreIthacaNetworkAtStart() {
		round++
	}
	rights := make([]BakingRight, 0)
	u := fmt.Sprintf("chains/main/blocks/%s/helpers/baking_rights?all=true&"+maxSelector, id, round)
	if err := c.Get(ctx, u, &rights); err != nil {
		return tezos.ZeroAddress, err
	}
	for _, r := range rights {
		if r.Round != round {
			continue
		}
		addr, _ := tezos.ParseAddress(r.Delegate)
		return addr, nil
	}
	return tezos.ZeroAddress, fmt.Errorf("Baking round not found")
}
