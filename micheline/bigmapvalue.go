// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"encoding/json"
	"fmt"
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
		// log.Debugf("marshal scalar direct typ=%s val=%s", e.Type.OpCode, e.Value.Type)
		return json.Marshal(e.Value.Value(e.Type.OpCode))
	}
	m := make(map[string]interface{}, 1024)
	if err := walkTree(m, "", e.Type, e.Value, 0); err != nil {
		log.Error(err)
		type xErrorMessage struct {
			Message string `json:"message"`
			Type    *Prim  `json:"type"`
			Value   *Prim  `json:"value"`
		}
		resp := struct {
			Error xErrorMessage `json:"error"`
		}{
			Error: xErrorMessage{
				Message: err.Error(),
				Type:    e.Type,
				Value:   e.Value,
			},
		}
		return json.Marshal(resp)
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

func walkTree(m map[string]interface{}, path string, typ *Prim, val *Prim, lvl int) error {
	// make sure value matches type
	if !val.matchOpCode(typ.OpCode) {
		return fmt.Errorf("micheline: type mismatch: val_type=%s[%s] type_code=%s type=%s value=%s",
			val.Type, val.OpCode, typ.OpCode, limit(typ.JSONString(), 512), limit(val.JSONString(), 512))
	}

	// use annot as name when exists
	haveName := typ.HasAnyAnno()
	if haveName && len(path) == 0 {
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
		// log.Debugf("%*s> %s key %s len %d into map %p", lvl, "", typ.OpCode, path, len(val.Args), m)
		for _, v := range val.Args {
			if v.IsScalar() && !v.IsSequence() {
				// array of scalar types
				arr = append(arr, v.Value(typ.Args[0].OpCode))
			} else {
				// array of complex types
				mm := make(map[string]interface{})
				if err := walkTree(mm, "", typ.Args[0], v, lvl+1); err != nil {
					return err
				}
				arr = append(arr, mm)
			}
		}
		m[path] = arr

	case T_LAMBDA:
		// LAMBDA <type> <type> { <instruction> ... }
		// value_type, return_type, code
		// log.Debugf("%*s> marshal %s key %s len %d into map %p", lvl, "", typ.OpCode, path, len(val.Args), m)
		m[path] = val

	case T_MAP, T_BIG_MAP:
		// map <comparable type> <type>
		// big_map <comparable type> <type>
		// sequence of Elt (key/value) pairs

		// TEST: render bigmap reference
		if typ.OpCode == T_BIG_MAP && len(val.Args) == 0 {
			// log.Debugf("%*s> marshal %s ref key %s into map %p", lvl, "", typ.OpCode, path, m)
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
		// log.Debugf("%*s> T_MAP marshal %s key %s len %d into map %p adding sub map %p", lvl, "", typ.OpCode, path, len(val.Args), m, mm)

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
				// log.Debugf("%*s> T_MAP/PrimBinary marshal scalar %s key %s into map %p", lvl, "", typ.OpCode, key.String(), mm)
				if err := walkTree(mm, key.String(), typ.Args[1], val.Args[1], lvl+1); err != nil {
					return err
				}
			} else {
				// add complex type to sub-map
				mmm := make(map[string]interface{})
				// log.Debugf("%*s> T_MAP/PrimBinary marshal sub %s key %s %p into map %p", lvl, "", typ.OpCode, key.String(), mmm, mm)
				if err := walkTree(mmm, "", typ.Args[1], val.Args[1], lvl+1); err != nil {
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
					// log.Debugf("%*s> T_MAP/PrimSequence-1 BUILD TYPE for unpacked key %s type [%s,%s] is [%s,%s]",
					// 	lvl, "", path, typ.Args[0].Type, typ.Args[0].OpCode, keyType.Type, keyType.OpCode)
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
					// log.Debugf("%*s> T_MAP/PrimSequence-2 BUILD TYPE for unpacked value %s type [%s,%s] is [%s,%s]",
					// 	lvl, "", path, typ.Args[0].Type, typ.Args[0].OpCode, keyType.Type, keyType.OpCode)
				}

				// recurse to unpack value type directly into map under key
				if v.Args[1].IsScalar() {
					// add scalar type to map
					// log.Debugf("%*s> T_MAP/PrimSequence-3 marshal scalar %s key %s into map %p", lvl, "", valType.OpCode, key.String(), mm)
					if err := walkTree(mm, key.String(), valType, v.Args[1], lvl+1); err != nil {
						return err
					}
				} else {
					// add complex type to sub-map
					mmm := make(map[string]interface{})
					// log.Debugf("%*s> T_MAP/PrimSequence-4 marshal sub %s key %s %p into map %p", lvl, "", valType.OpCode, key.String(), mmm, mm)
					if err := walkTree(mmm, "", valType, v.Args[1], lvl+1); err != nil {
						return err
					}
					mm[key.String()] = mmm
				}
			}

		default:
			buf, _ := json.Marshal(val)
			return fmt.Errorf("%*s> micheline: unexpected type %s [%s] for %s Elt sequence: %s",
				lvl, "", val.Type, val.OpCode, typ.OpCode, buf)
		}

		m[path] = mm

	case T_PAIR:
		// pair <type> <type> or COMB
		// FIXME: does this catch all COMB pairs??
		// - for n=2, [Pair x1 x2],
		// - for n=3, [Pair x1 (Pair x2 x3)],
		// - for n>=4, [{x1; x2; ...; xn}].

		mm := m
		if haveName {
			// when annots are NOT empty, create a new sub-map unless value is scalar
			mm = make(map[string]interface{})
			// log.Debugf("%*s> T_PAIR marshal sub pair map %p %s into map %p", lvl, "", mm, path, m)
			// } else {
			// 	log.Debugf("%*s> T_PAIR marshal direct pair %s into map %p", lvl, "", path, m)
		}

		vals := val.Args
		typs := typ.Args
		if val.IsComb() || typ.IsComb() {
			vals = val.FlattenComb()
			typs = typ.FlattenComb()
			// log.Debugf("%*s> T_PAIR flattening comb %s into map %p // val=%s typ=%s", lvl, "", path, mm, val.JSONString(), typ.JSONString())
			// } else {
			// log.Debugf("%*s> T_PAIR flattening pair %s into map %p // val[%t]=%s typ[%t]=%s", lvl, "", path, mm, val.IsComb(), val.JSONString(), typ.IsComb(), typ.JSONString())
		}

		for i, v := range vals {
			if i >= len(typs) {
				// log.Errorf("T_PAIR %s val/type len mismatch %d <> %d", path, len(vals), len(typs))
				break
			}
			if err := walkTree(mm, "", typs[i], v, lvl+1); err != nil {
				return err
			}
		}

		if haveName {
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
			// log.Debugf("%*s> T_OPTION/D_NONE marshal empty opt %s into map %p", lvl, "", p, m)
			m[p] = nil // stop recursion
		case D_SOME:
			// continue recursion
			// log.Debugf("%*s> T_OPTION/D_SOME-1 marshal some opt at %s name=%t scalar=%t %#v",
			// lvl, "", path, haveName, val.IsScalar(), val)

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
					// log.Debugf("%*s> T_OPTION/D_SOME-2 marshal some opt collapse %s into map %p", lvl, "", p, m)
					if err := walkTree(m, p, t, v, lvl+1); err != nil {
						return err
					}
				}
			} else {
				// with annots (name) use it for scalar or complex render
				if val.IsScalar() {
					// log.Debugf("%*s> T_OPTION/D_SOME-3 marshal some opt collapse %s into map %p", lvl, "", path, m)
					for i, v := range val.Args {
						// ATTN: since people can use lists of options the type tree
						// may not contain as many values as the value tree, to stay
						// resilient we only use the i-th type if it exists
						t := typ.Args[0]
						if len(typ.Args) < i {
							t = typ.Args[i]
						}
						if err := walkTree(m, path, t, v, lvl+1); err != nil {
							return err
						}
					}
				} else {
					mm := make(map[string]interface{})
					// log.Debugf("%*s> T_OPTION/D_SOME-4 marshal some opt map %p sub %s into map %p", lvl, "", mm, path, m)
					for i, v := range val.Args {
						// ATTN: since people can use lists of options the type tree
						// may not contain as many values as the value tree, to stay
						// resilient we only use the i-th type if it exists
						t := typ.Args[0]
						if len(typ.Args) < i {
							t = typ.Args[i]
						}
						if err := walkTree(mm, "", t, v, lvl+1); err != nil {
							return err
						}
					}
					m[path] = mm
				}
			}
		case D_PAIR:
			// unpack options from pair
			if val.Args[0].OpCode == D_SOME {
				// log.Debugf("%*s> T_OPTION/D_SOME-5 marshal some pair LEFT sub %s into map %p", lvl, "", path, m)
				return walkTree(m, path, typ, val.Args[0], lvl+1)
			}
			if val.Args[1].OpCode == D_SOME {
				// log.Debugf("%*s> T_OPTION/D_SOME-6 marshal some pair RIGHT sub %s into map %p", lvl, "", path, m)
				return walkTree(m, path, typ, val.Args[1], lvl+1)
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
			// log.Debugf("%*s> T_OR/D_LEFT marshal OR left type %s into map %p", lvl, "", p, m)
			if err := walkTree(mm, p, typ.Args[0], val.Args[0], lvl+1); err != nil {
				return err
			}
		case D_RIGHT:
			if len(p) == 0 && !haveName {
				p = strconv.Itoa(len(mm)) + "@" + typ.Args[1].OpCode.String()
			}
			// log.Debugf("%*s> T_OR/D_RIGHT marshal OR right type %s into map %p", lvl, "", p, m)
			if err := walkTree(mm, p, typ.Args[1], val.Args[0], lvl+1); err != nil {
				return err
			}
		default:
			if len(p) == 0 && !haveName {
				p = strconv.Itoa(len(mm)) + "@" + typ.Args[0].OpCode.String()
			}
			// log.Debugf("%*s> T_OR/default marshal OR full type %s into map %p", lvl, "", p, m)
			if err := walkTree(mm, p, typ.Args[0], val, lvl+1); err != nil {
				return err
			}
		}
		if haveName {
			m[path] = mm
		}

	case T_TICKET:
		// always Pair( ticketer:address, Pair( original_type, int ))
		ttyp := TicketType(typ.Args[0])
		// log.Debugf("%*s> T_TICKET %s type %s into map %p", lvl, "", path, typ.Args[0].JSONString(), m)
		if err := walkTree(m, path, ttyp, val, lvl+1); err != nil {
			return err
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
			// log.Debugf("%*s> marshal scalar val %s (%s, %s) as %s", lvl, "", path, val.Type, val.OpCode, typ.OpCode)
			m[path] = val.Value(typ.OpCode)
		} else {
			mm := make(map[string]interface{})
			// log.Debugf("%*s> marshal scalar val (%s, %s) type (%s, %s) sub map %p into map %p", lvl, "", val.Type, val.OpCode, typ.Type, typ.OpCode, mm, m)
			if err := walkTree(mm, "", typ, val, lvl+1); err != nil {
				return err
			}
			m[path] = mm
		}
	}
	return nil
}

func (p Prim) matchOpCode(oc OpCode) bool {
	mismatch := false
	switch p.Type {
	case PrimSequence:
		switch oc {
		case T_LIST, T_MAP, T_BIG_MAP, T_SET, T_LAMBDA, T_OR, T_OPTION, T_PAIR, T_SAPLING_STATE:
		default:
			mismatch = true
		}

	case PrimInt:
		switch oc {
		case T_INT, T_NAT, T_MUTEZ, T_TIMESTAMP, T_BIG_MAP, T_OR, T_OPTION, T_SAPLING_STATE:
			// accept references to bigmap and sapling states
		default:
			mismatch = true
		}

	case PrimString:
		// sometimes timestamps and addresses can be strings
		switch oc {
		case T_BYTES, T_STRING, T_ADDRESS, T_CONTRACT, T_KEY_HASH, T_KEY,
			T_SIGNATURE, T_TIMESTAMP, T_OR, T_CHAIN_ID, T_OPTION:
		default:
			mismatch = true
		}

	case PrimBytes:
		switch oc {
		case T_BYTES, T_STRING, T_BOOL, T_ADDRESS, T_KEY_HASH, T_KEY,
			T_CONTRACT, T_SIGNATURE, T_OPERATION, T_LAMBDA, T_OR,
			T_CHAIN_ID, T_OPTION, T_SAPLING_STATE, T_SAPLING_TRANSACTION,
			T_BLS12_381_G1, T_BLS12_381_G2, T_BLS12_381_FR:
		default:
			mismatch = true
		}

	default:
		switch p.OpCode {
		case D_PAIR:
			mismatch = oc != T_PAIR && oc != T_OR && oc != T_LIST && oc != T_OPTION && oc != T_TICKET
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
