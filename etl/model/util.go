// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/hex"
	"strconv"

	"golang.org/x/net/idna"
)

// Correct overflow from negative numbers caused by DB storage/type
func Int16Correct(i int) int {
	return int(uint16(i))
}

func UnpackTnsString(v string) (string, error) {
	b, err := hex.DecodeString(v)
	if err == nil {
		v = string(b)
	}
	v, err = idna.ToUnicode(v)
	if err != nil {
		return "", err
	}
	if vv, err := strconv.Unquote(v); err == nil {
		v = vv
	}
	return v, nil
}

// func UnpackTnsMap(m map[string]string) map[string]string {
// 	for n, v := range m {
// 		if vv, err := unpackTnsString(v); err != nil {
// 			continue
// 		} else {
// 			m[n] = vv
// 		}
// 	}
// 	return m
// }
