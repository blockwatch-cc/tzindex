// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	logpkg "github.com/echa/log"
)

// log is a logger that is initialized with no output filters.  This
// means the package will not perform any logging by default until the caller
// requests it.
var log logpkg.Logger = logpkg.Log

// The default amount of logging is none.
func init() {
	DisableLog()
}

// DisableLog disables all library log output.  Logging output is disabled
// by default until UseLogger is called.
func DisableLog() {
	log = logpkg.Disabled
}

// UseLogger uses a specified Logger to output package logging info.
func UseLogger(logger logpkg.Logger) {
	log = logger
}
