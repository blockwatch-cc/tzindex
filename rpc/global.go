// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/micheline"
)

// Ensure ConstantRegistration implements the TypedOperation interface.
var _ TypedOperation = (*ConstantRegistration)(nil)

// ConstantRegistration represents a global constant registration operation
type ConstantRegistration struct {
	Manager
	Value micheline.Prim `json:"value,omitempty"`
}
