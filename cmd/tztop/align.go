// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/acarl005/stripansi"
)

// AlignLeft align left
func AlignLeft(s string, n int) string {
	if n < 0 {
		return s[:0]
	}
	slen := utf8.RuneCountInString(s)
	if slen > n {
		for index := range s {
			if n == 0 {
				return s[:index]
			}
			n--
		}
	}

	return fmt.Sprintf("%s%s", s, strings.Repeat(" ", n-slen))
}

// AlignRight align right
func AlignRight(t string, n int) string {
	s := stripansi.Strip(t)
	slen := utf8.RuneCountInString(s)
	if n < 0 {
		return s[:0]
	}
	if slen > n {
		return s[:n]
	}

	return fmt.Sprintf("%s%s", strings.Repeat(" ", n-slen), t)
}

// AlignCenter align center
func AlignCenter(t string, n int) string {
	s := stripansi.Strip(t)
	slen := utf8.RuneCountInString(s)
	if n < 0 {
		return s[:0]
	}
	if slen > n {
		return s[:n]
	}

	pad := (n - slen) / 2
	lpad := pad
	rpad := n - slen - lpad

	return fmt.Sprintf("%s%s%s", strings.Repeat(" ", lpad), t, strings.Repeat(" ", rpad))
}

// AlignSlash align on slash so that slash is in center
func AlignSlash(t string, n int) string {
	s := stripansi.Strip(t)
	slen := utf8.RuneCountInString(s)
	if n < 0 {
		return s[:0]
	}
	if slen > n {
		return s[:n]
	}
	idx := strings.IndexRune(s, '/')
	if idx < 0 {
		return AlignCenter(t, n)
	}
	lpad := n/2 - idx
	if lpad < 0 {
		lpad = 0
	}
	rpad := n - slen - lpad
	if rpad < 0 {
		return AlignRight(t, n)
	}
	return fmt.Sprintf("%s%s%s", strings.Repeat(" ", lpad), t, strings.Repeat(" ", rpad))
}
