// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"fmt"
	"strings"

	"blockwatch.cc/packdb/pack"
)

func Query(ctx *Context, key string) (mode pack.FilterMode, val string, ok bool) {
	for n, v := range ctx.Request.URL.Query() {
		if !strings.HasPrefix(n, key) {
			continue
		}
		val = v[0]
		k, m, haveMode := strings.Cut(n, ".")
		if haveMode {
			mode = pack.ParseFilterMode(m)
			if !mode.IsValid() {
				panic(EBadRequest(EC_PARAM_INVALID, fmt.Sprintf("invalid filter mode '%s'", k), nil))
			}
		} else {
			mode = pack.FilterModeEqual
			if strings.Contains(val, ",") {
				mode = pack.FilterModeIn
			}
		}
	}
	return
}
