// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

import (
	"fmt"
)

type RightType int

const (
	RightTypeInvalid RightType = iota
	RightTypeBaking
	RightTypeEndorsing
)

func ParseRightType(s string) RightType {
	switch s {
	case "baking":
		return RightTypeBaking
	case "endorsing":
		return RightTypeEndorsing
	default:
		return RightTypeInvalid
	}
}

func (r RightType) String() string {
	switch r {
	case RightTypeBaking:
		return "baking"
	case RightTypeEndorsing:
		return "endorsing"
	default:
		return ""
	}
}

func (r RightType) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func (r RightType) IsValid() bool {
	return r != RightTypeInvalid
}

func (r *RightType) UnmarshalText(data []byte) error {
	vv := ParseRightType(string(data))
	if !vv.IsValid() {
		return fmt.Errorf("invalid right type '%s'", string(data))
	}
	*r = vv
	return nil
}
