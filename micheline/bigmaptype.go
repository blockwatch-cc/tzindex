// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"encoding/json"
	"strconv"
	"strings"
)

type BigMapType Prim

type KeyValueType struct {
	KeyType   BigMapType `json:"key_type"`
	ValueType BigMapType `json:"value_type"`
}

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
	if err := walkTypeTree(m, "", e.Prim(), false); err != nil {
		return nil, err
	}
	// lift up embedded scalars unless they are named or container types
	if len(m) == 1 {
		for n, v := range m {
			fields := strings.Split(n, "@")
			oc, err := ParseOpCode(fields[len(fields)-1])
			if err == nil || strings.HasPrefix(n, "0") {
				switch oc {
				case T_LIST, T_MAP, T_SET, T_LAMBDA, T_BIG_MAP, T_OR, T_OPTION, T_PAIR:
				default:
					return json.Marshal(v)
				}
			}
		}
	}
	return json.Marshal(m)
}

type ArgType Prim

func (e ArgType) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 1024)
	if err := walkTypeTree(m, "", e.Prim(), true); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func (t ArgType) Prim() *Prim {
	return (*Prim)(&t)
}

func (t *ArgType) StripAnno(name string) {
	for i := 0; i < len(t.Anno); i++ {
		if t.Anno[i][1:] == name {
			t.Anno = append(t.Anno[:i], t.Anno[i+1:]...)
			i--
		}
	}
}

func walkTypeTree(m map[string]interface{}, path string, typ *Prim, withSequenceNum bool) error {
	// use annot as name when exists; even though this is a type tree we need to produce
	// variable names that match the storage spec so our frontends can match between
	// type desc and values
	haveName := typ.HasAnyAnno()
	if len(path) == 0 {
		if haveName {
			if withSequenceNum {
				path = strconv.Itoa(len(m)) + "@" + typ.GetVarAnnoAny()
			} else {
				path = typ.GetVarAnnoAny()
			}
		} else {
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
		if err := walkTypeTree(mm, "", typ.Args[0], withSequenceNum); err != nil {
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
			if err := walkTypeTree(mmm, "", v, withSequenceNum); err != nil {
				return err
			}
			// lift scalar
			var lifted bool
			if len(mmm) == 1 {
				for n, v := range mmm {
					fields := strings.Split(n, "@")
					liftedName := strconv.Itoa(i)
					lifted = true
					if fields[0] == "0" {
						if len(fields) > 1 {
							liftedName += "@" + strings.Join(fields[1:], "@")
						}
					} else {
						liftedName += "@" + strings.Join(fields, "@")
					}
					mm[liftedName] = v
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
			if err := walkTypeTree(mm, "", v, withSequenceNum); err != nil {
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
				if err := walkTypeTree(m, "", v, withSequenceNum); err != nil {
					return err
				}
			}
		} else {
			// when annots are NOT empty, create a new sub-map unless value is scalar
			mm := make(map[string]interface{})
			// 	log.Debugf("marshal sub pair map %p %s into map %p", mm, path, m)
			for _, v := range typ.Args {
				if err := walkTypeTree(mm, "", v, withSequenceNum); err != nil {
					return err
				}
			}
			m[path] = mm
		}

	case T_OR, T_OPTION:
		// option <type>
		// or <type> <type>
		if len(typ.Args) == 0 {
			p := path
			if haveName {
				p = p + "@" + typ.OpCode.String()
			}
			for _, v := range typ.Args {
				if err := walkTypeTree(m, p, v, withSequenceNum); err != nil {
					return err
				}
			}
		} else {
			mm := m
			p := path
			if haveName {
				mm = make(map[string]interface{})
				p = p + "@" + typ.OpCode.String()
			}
			for _, v := range typ.Args {
				if err := walkTypeTree(mm, "", v, withSequenceNum); err != nil {
					return err
				}
			}
			if haveName {
				m[p] = mm
			}
		}
	case T_TICKET:
		return walkTypeTree(m, path, TicketType(typ.Args[0]), withSequenceNum)

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
