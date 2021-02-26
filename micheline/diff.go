// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"fmt"
)

type DiffAction byte

const (
	DiffActionUpdate DiffAction = iota
	DiffActionRemove
	DiffActionCopy
	DiffActionAlloc
)

func (a DiffAction) String() string {
	switch a {
	case DiffActionUpdate:
		return "update"
	case DiffActionRemove:
		return "remove"
	case DiffActionCopy:
		return "copy"
	case DiffActionAlloc:
		return "alloc"
	}
	return ""
}

func (a DiffAction) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

func (a *DiffAction) UnmarshalText(data []byte) error {
	ac, err := ParseDiffAction(string(data))
	// default to update when empty
	if err != nil && len(data) > 0 {
		return err
	}
	*a = ac
	return nil
}

func ParseDiffAction(data string) (DiffAction, error) {
	switch data {
	case "update":
		return DiffActionUpdate, nil
	case "remove":
		return DiffActionRemove, nil
	case "copy":
		return DiffActionCopy, nil
	case "alloc":
		return DiffActionAlloc, nil
	default:
		return DiffActionUpdate, fmt.Errorf("micheline: invalid big_map_diff action '%s'", string(data))
	}
}
