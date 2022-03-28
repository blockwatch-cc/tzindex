// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

// Correct overflow from negative numbers caused by DB storage/type
func Int16Correct(i int) int {
    return int(uint16(i))
}
