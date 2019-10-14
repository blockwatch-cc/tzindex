// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package index

import logpkg "github.com/echa/log"

var log logpkg.Logger = logpkg.Log

func init() {
	DisableLog()
}

func DisableLog() {
	log = logpkg.Disabled
}

func UseLogger(logger logpkg.Logger) {
	log = logger
}
