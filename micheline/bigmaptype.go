// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"encoding/json"
	"strconv"
)

type BigMapType Prim

func NewBigMapType() BigMapType {
	return BigMapType(Prim{})
}

func (t *BigMapType) UnmarshalBinary(buf []byte) error {
	return ((*Prim)(t)).UnmarshalBinary(buf)
}

func (t BigMapType) Prim() *Prim {
	return (*Prim)(&t)
}

func (e BigMapType) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 1024)
	if err := walkTypeTree(m, "", e.Prim()); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func walkTypeTree(m map[string]interface{}, path string, typ *Prim) error {
	// use annot as name when exists
	haveName := typ.HasAnno()
	if path == "" {
		if haveName {
			path = typ.GetAnno()
		} else if len(path) == 0 {
			path = strconv.Itoa(len(m))
		}
	}

	switch typ.OpCode {
	case T_LIST, T_SET:
		// list <type>
		// set <comparable type>
		if haveName {
			path = path + "@" + typ.OpCode.String()
		} else {
			path = strconv.Itoa(len(m)) + "@" + typ.OpCode.String()
		}

		// put list elements into own map, never collapse to avoid dropping list name/type
		mm := make(map[string]interface{})
		if err := walkTypeTree(mm, "", typ.Args[0]); err != nil {
			return err
		}
		m[path] = mm

	case T_MAP, T_BIG_MAP:
		// map <comparable type> <type>
		// big_map <comparable type> <type>
		mm := make(map[string]interface{})

		// key and value type
		for i, v := range typ.Args {
			mmm := make(map[string]interface{})
			if err := walkTypeTree(mmm, "", v); err != nil {
				return err
			}
			// lift scalar
			var lifted bool
			if len(mmm) == 1 {
				for n, v := range mmm {
					if n == "0" {
						lifted = true
						mm[strconv.Itoa(i)] = v
					}
				}
			}
			if !lifted {
				mm[strconv.Itoa(i)] = mmm
			}
		}

		if haveName {
			path = path + "@" + typ.OpCode.String()
		} else {
			path = strconv.Itoa(len(m)) + "@" + typ.OpCode.String()
		}

		m[path] = mm

	case T_LAMBDA:
		// LAMBDA <type> <type> { <instruction> ... }
		mm := make(map[string]interface{})
		for _, v := range typ.Args {
			if err := walkTypeTree(mm, "", v); err != nil {
				return err
			}
		}

		if haveName {
			path = path + "@" + typ.OpCode.String()
		} else {
			path = strconv.Itoa(len(m)) + "@" + typ.OpCode.String()
		}

		m[path] = mm

	case T_PAIR:
		// pair <type> <type>
		if !haveName {
			// collapse pairs when annots are empty
			for _, v := range typ.Args {
				if err := walkTypeTree(m, "", v); err != nil {
					return err
				}
			}
		} else {
			// when annots are NOT empty, create a new sub-map unless value is scalar
			mm := make(map[string]interface{})
			// 	log.Debugf("marshal sub pair map %p %s into map %p", mm, path, m)
			for _, v := range typ.Args {
				if err := walkTypeTree(mm, "", v); err != nil {
					return err
				}
			}
			m[path] = mm
		}

	case T_OR, T_OPTION:
		// option <type>
		// or <type> <type>
		mm := make(map[string]interface{})
		for _, v := range typ.Args {
			if err := walkTypeTree(mm, "", v); err != nil {
				return err
			}
		}
		if haveName {
			path = path + "@" + typ.OpCode.String()
		} else {
			path = strconv.Itoa(len(m)) + "@" + typ.OpCode.String()
		}
		// lift scalar
		var lifted bool
		if len(mm) == 1 {
			for n, v := range mm {
				if n == "0" {
					lifted = true
					m[path] = v
				}
			}
		}
		if !lifted {
			m[path] = mm
		}

	default:
		// int
		// nat
		// string
		// bytes
		// mutez
		// bool
		// key_hash
		// timestamp
		// address
		// key
		// unit
		// signature
		// operation
		// contract <type> (??) is this an entrypoint?
		// chain_id
		m[path] = typ.OpCode.String()
	}
	return nil
}
