// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type BigMapValue struct {
	Type  *Prim
	Value *Prim
}

func NewBigMapValue() *BigMapValue {
	return &BigMapValue{
		Type:  &Prim{},
		Value: &Prim{},
	}
}

func (e BigMapValue) MarshalJSON() ([]byte, error) {
	if e.Type == nil || e.Value == nil {
		return nil, nil
	}
	// output scalar types as is unless packed
	if e.Type.IsScalar() && !e.Value.IsPacked() {
		return json.Marshal(e.Value.Value(e.Type.OpCode))
	}
	m := make(map[string]interface{}, 1024)
	if err := walkTree(m, "", e.Type, e.Value); err != nil {
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

func walkTree(m map[string]interface{}, path string, typ *Prim, val *Prim) error {
	// make sure value matches type
	if !val.matchOpCode(typ.OpCode) {
		tbuf, _ := typ.MarshalJSON()
		vbuf, _ := val.MarshalJSON()
		return fmt.Errorf("micheline: type mismatch val_type=%s[%s] type_code=%s / type=%s -- value=%s / map=%#v",
			val.Type, val.OpCode, typ.OpCode, limit(string(tbuf), 512), limit(string(vbuf), 512), m)
	}

	// use annot as name when exists
	haveName := typ.HasAnyAnno()
	if haveName {
		path = typ.GetVarAnnoAny()
	} else if len(path) == 0 && typ.OpCode != T_OR {
		path = strconv.Itoa(len(m)) + "@" + typ.OpCode.String()
	}

	// walk the type tree and add values if they exist
	switch typ.OpCode {
	case T_LIST, T_SET:
		// list <type>
		// set <comparable type>
		arr := make([]interface{}, 0, len(val.Args))
		for _, v := range val.Args {
			if v.IsScalar() {
				// array of scalar types
				arr = append(arr, v.Value(typ.Args[0].OpCode))
			} else {
				// array of complex types
				mm := make(map[string]interface{})
				if err := walkTree(mm, "", typ.Args[0], v); err != nil {
					return err
				}
				arr = append(arr, mm)
			}
		}
		m[path] = arr

	case T_LAMBDA:
		// LAMBDA <type> <type> { <instruction> ... }
		// value_type, return_type, code
		m[path] = val

	case T_MAP, T_BIG_MAP:
		// map <comparable type> <type>
		// big_map <comparable type> <type>
		// sequence of Elt (key/value) pairs

		// render bigmap reference
		if typ.OpCode == T_BIG_MAP && len(val.Args) == 0 {
			switch val.Type {
			case PrimInt:
				// Babylon bigmaps contain a reference here
				m[path] = val.Value(T_INT)
			case PrimSequence:
				// pre-babylon there's only an empty sequence
				// FIXME: we could insert the bigmap id, but this is unknown at ths point
				m[path] = nil
			}
			return nil
		}
		mm := make(map[string]interface{})

		switch val.Type {
		case PrimBinary: // single ELT
			// unpack key type
			// build type info if prim was packed
			keyType := typ.Args[0]
			if val.Args[0].WasPacked {
				keyType = val.Args[0].BuildType()
			}
			key, err := NewBigMapKeyAs(keyType, val.Args[0])
			if err != nil {
				return err
			}

			// recurse to unpack value type
			if val.Args[1].IsScalar() {
				// add scalar type to map
				if err := walkTree(mm, key.String(), typ.Args[1], val.Args[1]); err != nil {
					return err
				}
			} else {
				// add complex type to sub-map
				mmm := make(map[string]interface{})
				if err := walkTree(mmm, "", typ.Args[1], val.Args[1]); err != nil {
					return err
				}
				mm[key.String()] = mmm
			}

		case PrimSequence: // sequence of ELTs
			for _, v := range val.Args {
				// unpack Elt
				if v.OpCode != D_ELT {
					return fmt.Errorf("micheline: unexpected type %s [%s] for %s Elt item", v.Type, v.OpCode, typ.OpCode)
				}

				// build type info if prim was packed
				keyType := typ.Args[0]
				if v.Args[0].WasPacked {
					keyType = v.Args[0].BuildType()
				}

				// unpack key type
				key, err := NewBigMapKeyAs(keyType, v.Args[0])
				if err != nil {
					return err
				}

				// build type info if prim was packed
				valType := typ.Args[1]
				if v.Args[1].WasPacked {
					valType = v.Args[1].BuildType()
				}

				// recurse to unpack value type
				if v.Args[1].IsScalar() {
					// add scalar type to map
					if err := walkTree(mm, key.String(), valType, v.Args[1]); err != nil {
						return err
					}
				} else {
					// add complex type to sub-map
					mmm := make(map[string]interface{})
					if err := walkTree(mmm, "", valType, v.Args[1]); err != nil {
						return err
					}
					mm[key.String()] = mmm
				}
			}

		default:
			buf, _ := json.Marshal(val)
			return fmt.Errorf("micheline: unexpected type %s [%s] for %s Elt sequence: %s",
				val.Type, val.OpCode, typ.OpCode, buf)
		}

		m[path] = mm

	case T_PAIR:
		// pair <type> <type>
		if !haveName {
			//log.Debugf("marshal collapse pair %s into map %p", path, m)
			// when annots are empty, collapse values
			for i, v := range val.Args {
				if i < len(typ.Args) {
					t := typ.Args[i]
					if err := walkTree(m, "", t, v); err != nil {
						return err
					}
				}
			}
		} else {
			// when annots are NOT empty, create a new sub-map unless value is scalar
			mm := make(map[string]interface{})
			for i, v := range val.Args {
				if i < len(typ.Args) {
					t := typ.Args[i]
					if err := walkTree(mm, "", t, v); err != nil {
						return err
					}
				}
			}
			m[path] = mm
		}

	case T_OPTION:
		// option <type>
		switch val.OpCode {
		case D_NONE:
			// add empty option values as null
			p := path
			if len(path) == 0 && !haveName {
				if len(typ.Args) > 0 {
					p = strconv.Itoa(len(m)) + "@" + typ.Args[0].OpCode.String()
				} else {
					p = strconv.Itoa(len(m)) + "@" + typ.OpCode.String()
				}
			}
			m[p] = nil // stop recursion
		case D_SOME:
			// continue recursion
			if len(path) == 0 && !haveName {
				// when annots are empty, use sequence + type name as key
				for i, v := range val.Args {
					// ATTN: since people can use lists of options the type tree
					// may not contain as many values as the value tree, to stay
					// resilient we only use the i-th type if it exists
					t := typ.Args[0]
					if len(typ.Args) < i {
						t = typ.Args[i]
					}
					p := strconv.Itoa(len(m)) + "@" + t.OpCode.String()
					if err := walkTree(m, p, t, v); err != nil {
						return err
					}
				}
			} else {
				// with annots (name) use it for scalar or complex render
				if val.IsScalar() {
					for i, v := range val.Args {
						// ATTN: since people can use lists of options the type tree
						// may not contain as many values as the value tree, to stay
						// resilient we only use the i-th type if it exists
						t := typ.Args[0]
						if len(typ.Args) < i {
							t = typ.Args[i]
						}
						if err := walkTree(m, path, t, v); err != nil {
							return err
						}
					}
				} else {
					mm := make(map[string]interface{})
					for i, v := range val.Args {
						// ATTN: since people can use lists of options the type tree
						// may not contain as many values as the value tree, to stay
						// resilient we only use the i-th type if it exists
						t := typ.Args[0]
						if len(typ.Args) < i {
							t = typ.Args[i]
						}
						if err := walkTree(mm, "", t, v); err != nil {
							return err
						}
					}
					m[path] = mm
				}
			}
		case D_PAIR:
			// unpack options from pair
			if val.Args[0].OpCode == D_SOME {
				return walkTree(m, path, typ, val.Args[0])
			}
			if val.Args[1].OpCode == D_SOME {
				return walkTree(m, path, typ, val.Args[1])
			}
			// empty pair, stop recursion
			m[path] = nil
		default:
			return fmt.Errorf("micheline: unexpected T_OPTION code %s [%s]", val.OpCode, val.OpCode)
		}

	case T_OR:
		// or <type> <type>
		mm := m
		if haveName {
			mm = make(map[string]interface{})
		}
		p := path
		switch val.OpCode {
		case D_LEFT:
			if len(p) == 0 && !haveName {
				p = strconv.Itoa(len(mm)) + "@" + typ.Args[0].OpCode.String()
			}
			if err := walkTree(mm, p, typ.Args[0], val.Args[0]); err != nil {
				return err
			}
		case D_RIGHT:
			if len(p) == 0 && !haveName {
				p = strconv.Itoa(len(mm)) + "@" + typ.Args[1].OpCode.String()
			}
			if err := walkTree(mm, p, typ.Args[1], val.Args[0]); err != nil {
				return err
			}
		default:
			if len(p) == 0 && !haveName {
				p = strconv.Itoa(len(mm)) + "@" + typ.Args[0].OpCode.String()
			}
			if err := walkTree(mm, p, typ.Args[0], val); err != nil {
				return err
			}
		}
		if haveName {
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
		// contract <type> (??)
		// chain_id
		// append scalar or other complex value
		if val.IsScalar() {
			m[path] = val.Value(typ.OpCode)
		} else {
			mm := make(map[string]interface{})
			if err := walkTree(mm, "", typ, val); err != nil {
				return err
			}
			m[path] = mm
		}
	}
	return nil
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

func (p Prim) matchOpCode(oc OpCode) bool {
	mismatch := false
	switch p.Type {
	case PrimSequence:
		switch oc {
		case T_LIST, T_MAP, T_BIG_MAP, T_SET, T_LAMBDA, T_OR, T_OPTION:
		default:
			mismatch = true
		}

	case PrimInt:
		switch oc {
		case T_INT, T_NAT, T_MUTEZ, T_TIMESTAMP, T_BIG_MAP, T_OR, T_OPTION:
			// accept references to bigmap
		default:
			mismatch = true
		}

	case PrimString:
		// sometimes timestamps and addresses can be strings
		switch oc {
		case T_STRING, T_ADDRESS, T_CONTRACT, T_KEY_HASH, T_KEY,
			T_SIGNATURE, T_TIMESTAMP, T_OR, T_CHAIN_ID, T_OPTION:
		default:
			mismatch = true
		}

	case PrimBytes:
		switch oc {
		case T_BYTES, T_BOOL, T_ADDRESS, T_KEY_HASH, T_KEY,
			T_CONTRACT, T_SIGNATURE, T_OPERATION, T_LAMBDA, T_OR,
			T_CHAIN_ID, T_OPTION:
		default:
			mismatch = true
		}

	default:
		switch p.OpCode {
		case D_PAIR:
			mismatch = oc != T_PAIR && oc != T_OR && oc != T_LIST && oc != T_OPTION
		case D_SOME, D_NONE:
			mismatch = oc != T_OPTION
		case D_UNIT:
			mismatch = oc != T_UNIT && oc != K_PARAMETER
		case D_LEFT, D_RIGHT:
			mismatch = oc != T_OR
		}
	}
	return !mismatch
}
