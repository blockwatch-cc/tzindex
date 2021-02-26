// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzindex/micheline"
)

type LazyDiffKind string

const (
	LazyDiffKindBigMap       LazyDiffKind = "big_map"
	LazyDiffKindSaplingState LazyDiffKind = "sapling_state"
)

func (k LazyDiffKind) String() string {
	switch k {
	case LazyDiffKindBigMap:
		return "big_map"
	case LazyDiffKindSaplingState:
		return "sapling_state"
	}
	return ""
}

func ParseLazyDiffKind(data string) (LazyDiffKind, error) {
	switch data {
	case "big_map":
		return LazyDiffKindBigMap, nil
	case "sapling_state":
		return LazyDiffKindSaplingState, nil
	default:
		return "", fmt.Errorf("micheline: invalid diff kind '%s'", data)
	}
}

func (k LazyDiffKind) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

func (k *LazyDiffKind) UnmarshalText(data []byte) error {
	dk, err := ParseLazyDiffKind(string(data))
	if err != nil {
		return err
	}
	*k = dk
	return nil
}

type LazyStorageItem interface {
	Kind() LazyDiffKind
	Id() int64
}

type GenericDiff struct {
	DiffKind LazyDiffKind `json:"kind"`
	DiffId   int64        `json:"id,string"`
}

func (d *GenericDiff) Kind() LazyDiffKind {
	return d.DiffKind
}

func (d *GenericDiff) Id() int64 {
	return d.DiffId
}

type LazyStorageDiff []LazyStorageItem

func (d *LazyStorageDiff) UnmarshalJSON(data []byte) error {
	if data == nil {
		return nil
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*d = make(LazyStorageDiff, len(raw))

loop:
	for i, r := range raw {
		if r == nil {
			continue
		}
		var tmp GenericDiff
		if err := json.Unmarshal(r, &tmp); err != nil {
			return fmt.Errorf("rpc: generic diff: %v", err)
		}

		switch tmp.DiffKind {
		case LazyDiffKindSaplingState:
			(*d)[i] = &LazySaplingDiff{}
		case LazyDiffKindBigMap:
			(*d)[i] = &LazyBigMapDiff{}
		default:
			log.Warnf("unsupported lazy diff kind '%s'", tmp.Kind)
			(*d)[i] = &tmp
			continue loop
		}
		if err := json.Unmarshal(r, (*d)[i]); err != nil {
			return fmt.Errorf("rpc: lazy diff %s: %v", tmp.Kind, err)
		}
	}

	return nil
}

type LazyBigMapDiff struct {
	GenericDiff
	Diff micheline.BigMapDiffElem `json:"diff"`
}

type LazySaplingDiff struct {
	GenericDiff
	Diff micheline.SaplingDiffElem `json:"diff"`
}
