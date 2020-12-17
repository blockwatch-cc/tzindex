// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"unicode"
)

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func limit(s string, l int) string {
	return s[:min(len(s), l)]
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
