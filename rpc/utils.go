// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// HexBytes represents bytes as a JSON string of hexadecimal digits
type HexBytes []byte

// UnmarshalText umarshalls a hex string to bytes
func (h *HexBytes) UnmarshalText(data []byte) error {
	dst := make([]byte, hex.DecodedLen(len(data)))
	if _, err := hex.Decode(dst, data); err != nil {
		return err
	}
	*h = dst
	return nil
}

func (h HexBytes) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(h)), nil
}

func (h HexBytes) String() string {
	return hex.EncodeToString(h)
}

// Just suppress UnmarshalJSON
// type bigIntStr big.Int

// func (z *bigIntStr) UnmarshalText(data []byte) error {
// 	return (*big.Int)(z).UnmarshalText(data)
// }

/*
unmarshalNamedJSONArray is a helper function used in custom JSON
unmarshallers and intended to decode array-like objects:
	[
		"...", // object ID or hash
		{
			... // ebject with ID ommitted
		}
	]
*/
func unmarshalNamedJSONArray(data []byte, v ...interface{}) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if len(raw) < len(v) {
		return fmt.Errorf("JSON array is too short, expected %d, got %d", len(v), len(raw))
	}

	for i, vv := range v {
		if err := json.Unmarshal(raw[i], vv); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalInSlice unmarshals a JSON array in a way so that each element of the
// interface slice is unmarshaled individually. This is a workaround for the
// case where Go's normal unmarshaling wants to treat the array as a whole.
func unmarshalInSlice(data []byte, s []interface{}) error {
	var aRaw []json.RawMessage
	if err := json.Unmarshal(data, &aRaw); err != nil {
		return err
	}

	if len(aRaw) != len(s) {
		return fmt.Errorf("Array is too short, JSON has %d, we have %d", len(aRaw), len(s))
	}

	for i, raw := range aRaw {
		if err := json.Unmarshal(raw, &s[i]); err != nil {
			return err
		}
	}
	return nil
}

func jsonify(i interface{}) string {
	jsonb, err := json.Marshal(i)
	if err != nil {
		log.Fatal(err)
	}
	return string(jsonb)
}
