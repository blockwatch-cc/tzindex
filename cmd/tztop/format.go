// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc

//nolint:unused,deadcode
package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/awesome-gocui/gocui"
)

var (
	K float64 = 1000     // 10^3
	M         = K * 1000 // 10^6
	G         = M * 1000 // 10^9
	T         = G * 1000 // 10^12
	P         = T * 1000 // 10^15
	E         = P * 1000 // 10^18
	Z         = E * 1000 // 10^21
	Y         = Z * 1000 // 10^24
)

var (
	KB float64 = 1024
	MB         = KB * 1024
	GB         = MB * 1024
	TB         = GB * 1024
	PB         = TB * 1024
	EB         = PB * 1024
	ZB         = EB * 1024
	YB         = ZB * 1024
)

func format(f float64, unit string) string {
	if f == math.Trunc(f) {
		return "%.0f " + unit
	}
	return "%.1f " + unit
}

func FormatBytes(b int) string {
	f := float64(b)
	var (
		unit string
		val  float64
	)
	switch {
	case f >= YB:
		val, unit = f/YB, "YB"
	case f >= ZB:
		val, unit = f/ZB, "ZB"
	case f >= EB:
		val, unit = f/EB, "EB"
	case f >= PB:
		val, unit = f/PB, "PB"
	case f >= TB:
		val, unit = f/TB, "TB"
	case f >= GB:
		val, unit = f/GB, "GB"
	case f >= MB:
		val, unit = f/MB, "MB"
	case f >= KB:
		val, unit = f/KB, "kB"
	default:
		val, unit = f, "B"
	}
	return fmt.Sprintf(format(val, unit), val)
}

func FormatShort(b int) string {
	f := float64(b)
	var (
		unit string
		val  float64
	)
	switch {
	case f >= Y:
		val, unit = f/Y, "Y"
	case f >= Z:
		val, unit = f/Z, "Z"
	case f >= E:
		val, unit = f/E, "E"
	case f >= P:
		val, unit = f/P, "P"
	case f >= T:
		val, unit = f/T, "T"
	case f >= G:
		val, unit = f/G, "G"
	case f >= M:
		val, unit = f/M, "M"
	case f >= K:
		val, unit = f/K, "k"
	default:
		val, unit = f, ""
	}
	return fmt.Sprintf(format(val, unit), val)
}

func FormatPretty(val interface{}) string {
	switch v := val.(type) {
	case int:
		return PrettyInt64(int64(v))
	case int32:
		return PrettyInt64(int64(v))
	case int64:
		return PrettyInt64(v)
	case uint:
		return PrettyUint64(uint64(v))
	case uint32:
		return PrettyUint64(uint64(v))
	case uint64:
		return PrettyUint64(v)
	case float32:
		return PrettyFloat64(float64(v))
	case float64:
		return PrettyFloat64(v)
	case string:
		return PrettyString(v)
	case time.Duration:
		return PrettyInt64(int64(v))
	default:
		return fmt.Sprintf("%v", val)
	}
}

// not pretty, but works: 1000000.123 -> 1,000,000.123
func PrettyString(s string) string {
	if l, i := len(s), strings.IndexByte(s, '.'); i == -1 && l > 3 || i > 3 {
		var rem string
		if i > -1 {
			rem = s[i:]
			s = s[:i]
		}
		l = len(s)
		p := s[:l%3]
		if len(p) > 0 {
			p += ","
		}
		for j := 0; j <= l/3; j++ {
			start := l%3 + j*3
			end := start + 3
			if end > len(s) {
				end = len(s)
			}
			p += s[start:end] + ","
		}
		s = p[:len(p)-2] + rem
	}
	return s
}

func PrettyInt(i int) string {
	return PrettyString(strconv.FormatInt(int64(i), 10))
}

func PrettyInt64(i int64) string {
	return PrettyString(strconv.FormatInt(i, 10))
}

func PrettyUint64(i uint64) string {
	return PrettyString(strconv.FormatUint(i, 10))
}

func PrettyFloat64(f float64) string {
	return PrettyString(strconv.FormatFloat(f, 'f', -1, 64))
}

func PrettyFloat64N(f float64, decimals int) string {
	return PrettyString(strconv.FormatFloat(f, 'f', decimals, 64))
}

func SlantedSpacer(v *gocui.View) {
	fmt.Fprint(v, " / ")
}

func VerticalSpacer(v *gocui.View) {
	fmt.Fprint(v, " | ")
}

func Capitalise(s string) string {
	s = RemoveUnderscore(s)
	newString := ""
	for i := 0; i < len(s); i++ {
		if i == 0 {
			newString += strings.ToUpper(string(s[i]))
		} else {
			newString += strings.ToLower(string(s[i]))
		}
	}
	return newString
}

func RemoveUnderscore(s string) string {
	newString := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '_' {
			newString[i] = ' '
			continue
		}
		newString[i] = s[i]
	}
	return string(newString)
}
