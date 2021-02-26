// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
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

func (e BigMapValue) DumpString() string {
	buf := bytes.NewBuffer(nil)
	e.Dump(buf)
	return string(buf.Bytes())
}

func (e BigMapValue) Dump(w io.Writer) {
	dumpTree(w, "", e.Type, e.Value)
}

func dumpTree(w io.Writer, path string, typ *Prim, val *Prim) {
	if s, err := dump(path, typ, val); err != nil {
		io.WriteString(w, err.Error())
	} else {
		io.WriteString(w, s)
	}
	switch val.Type {
	case PrimSequence:
		// keep the type
		for i, v := range val.Args {
			p := path + "." + strconv.Itoa(i)
			dumpTree(w, p, typ, v)
		}
	default:
		// advance type as well
		for i, v := range val.Args {
			t := typ.Args[i]
			p := path + "." + strconv.Itoa(i)
			dumpTree(w, p, t, v)
		}
	}
}

func dump(path string, typ *Prim, val *Prim) (string, error) {
	// value type must must match defined type
	if !val.matchOpCode(typ.OpCode) {
		return "", fmt.Errorf("Type mismatch val_type=%s type_code=%s", val.Type, typ.OpCode)
	}

	var ann string
	if len(typ.Anno) > 0 {
		ann = typ.Anno[0][1:]
	}

	vtyp := "-"
	switch val.Type {
	case PrimSequence, PrimBytes, PrimInt, PrimString:
	default:
		vtyp = val.OpCode.String()
	}

	return fmt.Sprintf("path=%-20s val_prim=%-8s val_type=%-8s val_val=%-10s type_prim=%-8s type_code=%-8s type_name=%-8s\n",
		path, val.Type, vtyp, limit(val.Text(), 512), typ.Type, typ.OpCode, ann,
	), nil
}
