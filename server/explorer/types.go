// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"strconv"

	"blockwatch.cc/packdb/pack"
)

var null = []byte(`null`)

func BoolPtr(b bool) *bool {
	return &b
}

func IntPtr(i int) *int {
	return &i
}

func Float64Ptr(f float64) *float64 {
	return &f
}

// generic list request
type ListRequest struct {
	Limit  uint           `schema:"limit"`
	Offset uint           `schema:"offset"`
	Cursor uint64         `schema:"cursor"`
	Order  pack.OrderType `schema:"order"`
}

type NullMoney int64

func (m NullMoney) MarshalJSON() ([]byte, error) {
	if m < 0 {
		return null, nil
	}
	return []byte(strconv.FormatFloat(float64(m)/1000000, 'f', 6, 64)), nil
}

type Sortable interface {
	Id() uint64
	json.Marshaler
}
