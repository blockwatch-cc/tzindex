// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzindex/chain"
)

var (
	EmptyPrim = &Prim{Type: PrimNullary, OpCode: 255}
)

type PrimType byte

const (
	PrimInt          PrimType = iota // 00 {name: 'int'}
	PrimString                       // 01 {name: 'string'}
	PrimSequence                     // 02 []
	PrimNullary                      // 03 {name: 'prim', len: 0, annots: false},
	PrimNullaryAnno                  // 04 {name: 'prim', len: 0, annots: true},
	PrimUnary                        // 05 {name: 'prim', len: 1, annots: false},
	PrimUnaryAnno                    // 06 {name: 'prim', len: 1, annots: true},
	PrimBinary                       // 07 {name: 'prim', len: 2, annots: false},
	PrimBinaryAnno                   // 08 {name: 'prim', len: 2, annots: true},
	PrimVariadicAnno                 // 09 {name: 'prim', len: n, annots: true},
	PrimBytes                        // 0A {name: 'bytes' }
)

func (t PrimType) TypeCode() OpCode {
	switch t {
	case PrimInt:
		return T_INT
	case PrimString:
		return T_STRING
	default:
		return T_BYTES
	}
}

func (t PrimType) IsValid() bool {
	return t <= PrimBytes
}

func ParsePrimType(val string) (PrimType, error) {
	switch val {
	case "int":
		return PrimInt, nil
	case "string":
		return PrimString, nil
	case "bytes":
		return PrimBytes, nil
	default:
		return 0, fmt.Errorf("micheline: invalid prim type '%s'", val)
	}
}

func PrimTypeFromTypeCode(o OpCode) PrimType {
	switch o {
	case T_INT, T_NAT, T_MUTEZ, T_TIMESTAMP:
		return PrimInt
	case T_STRING:
		return PrimString
	default:
		return PrimBytes
	}
}

// non-normative strings, use for debugging only
func (t PrimType) String() string {
	switch t {
	case PrimInt:
		return "int"
	case PrimString:
		return "string"
	case PrimSequence:
		return "sequence"
	case PrimNullary:
		return "prim"
	case PrimNullaryAnno:
		return "prim%"
	case PrimUnary:
		return "unary"
	case PrimUnaryAnno:
		return "unary%"
	case PrimBinary:
		return "binary"
	case PrimBinaryAnno:
		return "binary%"
	case PrimVariadicAnno:
		return "variadic"
	case PrimBytes:
		return "bytes"
	default:
		return "invalid"
	}
}

func (t PrimType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

type Prim struct {
	Type      PrimType // primitive type
	OpCode    OpCode   // primitive opcode (invalid on sequences, strings, bytes, int)
	Args      []*Prim  // optional arguments
	Anno      []string // optional type annotations
	Int       *big.Int // optional data
	String    string   // optional data
	Bytes     []byte   // optional data
	WasPacked bool     // true when content was unpacked (and no type info is available)
}

func (p Prim) Name() string {
	return p.GetVarAnnoAny()
}

func (p Prim) Clone() *Prim {
	clone := &Prim{
		Type:      p.Type,
		OpCode:    p.OpCode,
		String:    p.String,
		WasPacked: p.WasPacked,
	}
	if p.Args != nil {
		clone.Args = make([]*Prim, len(p.Args))
		for i, arg := range p.Args {
			clone.Args[i] = arg.Clone()
		}
	}
	if p.Anno != nil {
		clone.Anno = make([]string, len(p.Anno))
		for i, anno := range p.Anno {
			clone.Anno[i] = anno
		}
	}
	if p.Int != nil {
		clone.Int = big.NewInt(0)
		clone.Int.Set(p.Int)
	}
	if p.Bytes != nil {
		clone.Bytes = make([]byte, len(p.Bytes))
		copy(clone.Bytes, p.Bytes)
	}
	return clone
}

func (p Prim) IsEqual(p2 Prim) bool {
	return IsEqualPrim(p, p2, false)
}

func (p Prim) IsEqualWithAnno(p2 Prim) bool {
	return IsEqualPrim(p, p2, true)
}

func IsEqualPrim(p1, p2 Prim, withAnno bool) bool {
	// opcode
	if p1.OpCode != p2.OpCode {
		return false
	}

	// type
	switch p2.Type {
	case PrimNullaryAnno, PrimUnaryAnno, PrimBinaryAnno:
		if p2.Type != p1.Type && p2.Type-1 != p1.Type {
			return false
		}
	default:
		if p2.Type != p1.Type {
			return false
		}
	}

	// arg len
	if len(p1.Args) != len(p2.Args) {
		return false
	}

	// anno
	if withAnno {
		if len(p1.Anno) != len(p2.Anno) {
			return false
		}
		for i := range p1.Anno {
			if p1.Anno[i] != p2.Anno[i] {
				return false
			}
		}
	}

	// contents
	if p1.String != p2.String {
		return false
	}
	if (p1.Int == nil) != (p2.Int == nil) {
		return false
	}
	if p1.Int != nil {
		if p1.Int.Cmp(p2.Int) != 0 {
			return false
		}
	}
	if (p1.Bytes == nil) != (p2.Bytes == nil) {
		return false
	}
	if p1.Bytes != nil {
		if bytes.Compare(p1.Bytes, p2.Bytes) != 0 {
			return false
		}
	}

	// recurse
	for i := range p1.Args {
		if !IsEqualPrim(*p1.Args[i], *p2.Args[i], withAnno) {
			return false
		}
	}

	// all equal
	return true
}

type PrimWalker func(p *Prim) error

func (p *Prim) Walk(f PrimWalker) error {
	if p == nil {
		return nil
	}
	if err := f(p); err != nil {
		return err
	}
	for _, v := range p.Args {
		if err := v.Walk(f); err != nil {
			return err
		}
	}
	return nil
}

// returns true when the prim can be expressed as a single value
// key/value pairs (ie. prims with annots) do not fit into this category
// used when mapping complex big map values to JSON objects
func (p Prim) IsScalar() bool {
	switch p.Type {
	case PrimInt, PrimString, PrimBytes, PrimNullary, PrimSequence:
		return true // generally ok
	case PrimNullaryAnno, PrimUnaryAnno, PrimBinaryAnno, PrimVariadicAnno:
		return false // all annotated types become JSON properties
	case PrimUnary:
		switch p.OpCode {
		case D_LEFT, D_RIGHT, D_SOME:
			return p.Args[0].IsScalar()
		}
		return false
	case PrimBinary: // mostly not ok, unless type is option/or and sub-types are scalar
		switch p.OpCode {
		case T_OPTION, T_OR:
			return p.Args[0].IsScalar()
		}
		return false
	}
	return false
}

func (p Prim) IsSequence() bool {
	return p.Type == PrimSequence
}

func (p Prim) IsPair() bool {
	switch p.OpCode {
	case T_PAIR, D_PAIR:
		return true
	default:
		return false
	}
}

// For combs, three notations are supported:
//  - a) [Pair x1 (Pair x2 ... (Pair xn-1 xn) ...)],
//  - b) [Pair x1 x2 ... xn-1 xn], and
//  - c) [{x1; x2; ...; xn-1; xn}].
//  In readable mode, we always use b),
//  in optimized mode we use the shortest to serialize:
//  - for n=2, [Pair x1 x2],
//  - for n=3, [Pair x1 (Pair x2 x3)],
//  - for n>=4, [{x1; x2; ...; xn}].
func (p Prim) IsComb() bool {
	if p.Type == PrimSequence && len(p.Args) >= 4 {
		return true
	}
	if p.IsPair() {
		if len(p.Args) > 2 {
			return true
		}
		return !p.Args[0].IsComb() && p.Args[1].IsComb()
	}
	return false
}

func (p Prim) FlattenComb() []*Prim {
	if p.Type == PrimSequence {
		return p.Args
	}
	flat := make([]*Prim, 0)
	for i, v := range p.Args {
		if i == 0 {
			flat = append(flat, v)
			continue
		}
		if v.IsPair() {
			flat = append(flat, v.FlattenComb()...)
		} else {
			flat = append(flat, v)
		}
	}
	return flat
}

func (p Prim) Text() string {
	switch p.Type {
	case PrimInt:
		return p.Int.Text(10)
	case PrimString:
		return p.String
	case PrimBytes:
		return hex.EncodeToString(p.Bytes)
	default:
		v, _ := p.Value(p.OpCode).(string)
		return v
	}
}

func (p Prim) IsPacked() bool {
	return (p.OpCode == T_BYTES || p.Type == PrimBytes) && len(p.Bytes) > 1 && p.Bytes[0] == 0x5
}

func (p Prim) PackedType() PrimType {
	if !p.IsPacked() {
		return PrimNullary
	}
	return PrimType(p.Bytes[1])
}

func (p Prim) Unpack() (pp *Prim, err error) {
	if !p.IsPacked() {
		return nil, fmt.Errorf("prim is not packed")
	}
	defer func() {
		if e := recover(); e != nil {
			pp = nil
			err = fmt.Errorf("prim is not packed")
		}
	}()
	pp = &Prim{WasPacked: true}
	if err := pp.UnmarshalBinary(p.Bytes[1:]); err != nil {
		return nil, err
	}
	// handle recursive packed data
	if pp.IsPackedAny() {
		up, err := pp.UnpackAny()
		if err != nil {
			return nil, err
		}
		pp = up
	}
	return pp, nil
}

func (p Prim) IsPackedAny() bool {
	if p.IsPacked() {
		return true
	}
	for _, v := range p.Args {
		if v.IsPackedAny() {
			return true
		}
	}
	return false
}

func (p *Prim) UnpackAny() (*Prim, error) {
	if p.IsPacked() {
		return p.Unpack()
	}
	pp := *p // copy
	pp.Args = make([]*Prim, len(p.Args))
	for i, v := range p.Args {
		vv := *v // copy
		if vv.IsPackedAny() {
			up, err := v.UnpackAny()
			if err != nil {
				return nil, err
			}
			pp.Args[i] = up
		} else {
			pp.Args[i] = &vv
		}
	}
	return &pp, nil
}

func (p Prim) Value(as OpCode) interface{} {
	var warn bool
	switch p.Type {
	case PrimInt:
		switch as {
		case T_TIMESTAMP:
			tm := time.Unix(p.Int.Int64(), 0).UTC()
			if y := tm.Year(); y < 0 || y >= 10000 {
				return p.Int.Text(10)
			}
			return tm
		default:
			return p.Int.Text(10)
		}

	case PrimString:
		switch as {
		case T_TIMESTAMP:
			if t, err := time.Parse(time.RFC3339, p.String); err == nil {
				return t
			}
			return p.String
		default:
			return p.String
		}

	case PrimBytes:
		switch as {
		case T_BYTES:
			// try address unpack
			a := chain.Address{}
			if err := a.UnmarshalBinary(p.Bytes); err == nil {
				return a
			}

		case T_KEY_HASH, T_ADDRESS, T_CONTRACT:
			a := chain.Address{}
			if err := a.UnmarshalBinary(p.Bytes); err == nil {
				return a
			} else {
				log.Errorf("Rendering prim type %s as %s: %v", p.Type, as, err)
			}
		case T_KEY:
			k := chain.Key{}
			if err := k.UnmarshalBinary(p.Bytes); err == nil {
				return k
			} else {
				log.Errorf("Rendering prim type %s as %s: %v", p.Type, as, err)
			}

		case T_SIGNATURE:
			s := chain.Signature{}
			if err := s.UnmarshalBinary(p.Bytes); err == nil {
				return s
			} else {
				log.Errorf("Rendering prim type %s as %s: %v // %#v", p.Type, as, err, p.Bytes)
				panic(err)
			}

		case T_CHAIN_ID:
			if len(p.Bytes) == chain.HashTypeChainId.Len() {
				return chain.NewChainIdHash(p.Bytes).String()
			}

		case T_BLS12_381_G1, T_BLS12_381_G2, T_BLS12_381_FR, T_SAPLING_STATE:
			// log.Infof("Rendering prim type %s as %s: %#v", p.Type, as, p.Bytes)
			// as hex, fallthrough

		default:
			// case T_LAMBDA:
			// case T_LIST, T_MAP, T_BIG_MAP, T_SET:
			// case T_OPTION, T_OR, T_PAIR, T_UNIT:
			// case T_OPERATION:
			warn = true
		}

		// default is to render bytes as hex string
		return hex.EncodeToString(p.Bytes)

	case PrimUnary, PrimUnaryAnno:
		switch as {
		case T_OR, T_OPTION:
			// expected, just render as prim tree
		default:
			warn = true
		}

	case PrimNullary, PrimNullaryAnno:
		switch p.OpCode {
		case D_FALSE:
			return strconv.FormatBool(false)
		case D_TRUE:
			return strconv.FormatBool(true)
		default:
			return p.OpCode.String()
		}

	case PrimBinary, PrimBinaryAnno:
		switch p.OpCode {
		case D_PAIR, T_PAIR:
			// mangle pair contents into string, used when rendering complex keys
			left := p.Args[0].Value(p.Args[0].OpCode)
			if _, ok := left.(fmt.Stringer); !ok {
				if _, ok := left.(string); !ok {
					left = p.Args[0].OpCode.String()
				}
			}
			right := p.Args[1].Value(p.Args[1].OpCode)
			if _, ok := right.(fmt.Stringer); !ok {
				if _, ok := right.(string); !ok {
					right = p.Args[1].OpCode.String()
				}
			}
			return fmt.Sprintf("%s#%s", left, right)
		}

	case PrimSequence:
		switch p.OpCode {
		case D_PAIR, T_PAIR:
			var b strings.Builder
			for i, v := range p.Args {
				if i > 0 {
					b.WriteByte('#')
				}
				val := v.Value(v.OpCode)
				if stringer, ok := val.(fmt.Stringer); !ok {
					if str, ok := val.(string); !ok {
						b.WriteString(v.OpCode.String())
					} else {
						b.WriteString(str)
					}
				} else {
					b.WriteString(stringer.String())
				}
			}
			return b.String()

		default:
			switch as {
			case T_LAMBDA, T_LIST, T_MAP, T_BIG_MAP, T_SET, T_SAPLING_STATE:
				return &p
			default:
				warn = true
			}
		}

	default:
		switch as {
		case T_BOOL:
			if p.OpCode == D_TRUE {
				return strconv.FormatBool(true)
			} else if p.OpCode == D_FALSE {
				return strconv.FormatBool(false)
			}
		case T_OPERATION:
			return p.OpCode.String()
		case T_BYTES:
			return hex.EncodeToString(p.Bytes)
		default:
			warn = true
		}
	}

	if warn {
		buf, _ := json.Marshal(p)
		log.Warnf("Rendering prim type %s as %s: not implemented (%s)", p.Type, as, string(buf))
	}

	return &p
}

func (p Prim) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	switch p.Type {
	case PrimSequence:
		return json.Marshal(p.Args)
	case PrimInt:
		m["int"] = p.Int.Text(10)
	case PrimString:
		m["string"] = p.String
	case PrimBytes:
		m["bytes"] = hex.EncodeToString(p.Bytes)
	default:
		m["prim"] = p.OpCode.String()
		if len(p.Anno) > 0 {
			m["annots"] = p.Anno
		}
		if len(p.Args) > 0 {
			args := make([]json.RawMessage, 0, len(p.Args))
			for _, v := range p.Args {
				arg, err := json.Marshal(v)
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
			}
			m["args"] = args
		}
	}
	return json.Marshal(m)
}

func (p Prim) JSONString() string {
	buf, _ := p.MarshalJSON()
	return string(buf)
}

func (p Prim) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := p.EncodeBuffer(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p Prim) EncodeBuffer(buf *bytes.Buffer) error {
	// write tag
	buf.WriteByte(byte(p.Type))

	// write contents
	switch p.Type {
	case PrimInt:
		var z Z
		z.Set(p.Int)
		if err := z.EncodeBuffer(buf); err != nil {
			return err
		}

	case PrimString:
		binary.Write(buf, binary.BigEndian, uint32(len(p.String)))
		buf.WriteString(p.String)

	case PrimSequence:
		seq := bytes.NewBuffer(nil)
		binary.Write(seq, binary.BigEndian, uint32(0))
		for _, v := range p.Args {
			if err := v.EncodeBuffer(seq); err != nil {
				return err
			}
		}
		res := seq.Bytes()
		binary.BigEndian.PutUint32(res[:], uint32(len(res)-4))
		buf.Write(res)

	case PrimNullary:
		buf.WriteByte(byte(p.OpCode))

	case PrimNullaryAnno:
		buf.WriteByte(byte(p.OpCode))
		anno := strings.Join(p.Anno, " ")
		binary.Write(buf, binary.BigEndian, uint32(len(anno)))
		buf.WriteString(anno)

	case PrimUnary:
		buf.WriteByte(byte(p.OpCode))
		for _, v := range p.Args {
			if err := v.EncodeBuffer(buf); err != nil {
				return err
			}
		}

	case PrimUnaryAnno:
		buf.WriteByte(byte(p.OpCode))
		for _, v := range p.Args {
			if err := v.EncodeBuffer(buf); err != nil {
				return err
			}
		}
		anno := strings.Join(p.Anno, " ")
		binary.Write(buf, binary.BigEndian, uint32(len(anno)))
		buf.WriteString(anno)

	case PrimBinary:
		buf.WriteByte(byte(p.OpCode))
		for _, v := range p.Args {
			if err := v.EncodeBuffer(buf); err != nil {
				return err
			}
		}

	case PrimBinaryAnno:
		buf.WriteByte(byte(p.OpCode))
		for _, v := range p.Args {
			if err := v.EncodeBuffer(buf); err != nil {
				return err
			}
		}
		anno := strings.Join(p.Anno, " ")
		binary.Write(buf, binary.BigEndian, uint32(len(anno)))
		buf.WriteString(anno)

	case PrimVariadicAnno:
		buf.WriteByte(byte(p.OpCode))

		seq := bytes.NewBuffer(nil)
		binary.Write(seq, binary.BigEndian, uint32(0))
		for _, v := range p.Args {
			if err := v.EncodeBuffer(seq); err != nil {
				return err
			}
		}
		res := seq.Bytes()
		binary.BigEndian.PutUint32(res[:], uint32(len(res)-4))
		buf.Write(res)

		anno := strings.Join(p.Anno, " ")
		binary.Write(buf, binary.BigEndian, uint32(len(anno)))
		buf.WriteString(anno)

	case PrimBytes:
		binary.Write(buf, binary.BigEndian, uint32(len(p.Bytes)))
		buf.Write(p.Bytes)
	}

	return nil
}

func (p *Prim) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '[' {
		var m []interface{}
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		return p.UnpackSequence(m)
	} else {
		var m map[string]interface{}
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		return p.UnpackJSON(m)
	}
}

func (p *Prim) UnpackJSON(val interface{}) error {
	switch t := val.(type) {
	case map[string]interface{}:
		return p.UnpackPrimitive(t)
	case []interface{}:
		return p.UnpackSequence(t)
	default:
		return fmt.Errorf("micheline: unexpected json type %T", val)
	}
}

func (p *Prim) UnpackSequence(val []interface{}) error {
	p.Type = PrimSequence
	p.Args = make([]*Prim, 0)
	for _, v := range val {
		prim := &Prim{}
		if err := prim.UnpackJSON(v); err != nil {
			return err
		}
		p.Args = append(p.Args, prim)
	}
	return nil
}

func (p *Prim) UnpackPrimitive(val map[string]interface{}) error {
	p.Args = make([]*Prim, 0)
	for n, v := range val {
		switch n {
		case "prim":
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("micheline: invalid prim value type %T %v", v, v)
			}
			oc, err := ParseOpCode(str)
			if err != nil {
				return err
			}
			p.OpCode = oc
			p.Type = PrimNullary
		case "int":
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("micheline: invalid int value type %T %v", v, v)
			}
			i := big.NewInt(0)
			if err := i.UnmarshalText([]byte(str)); err != nil {
				return err
			}
			p.Int = i
			p.Type = PrimInt
		case "string":
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("micheline: invalid string value type %T %v", v, v)
			}
			p.String = str
			p.Type = PrimString
		case "bytes":
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("micheline: invalid bytes value type %T %v", v, v)
			}
			b, err := hex.DecodeString(str)
			if err != nil {
				return err
			}
			p.Bytes = b
			p.Type = PrimBytes
		case "annots":
			slist, ok := v.([]interface{})
			if !ok {
				return fmt.Errorf("micheline: invalid annots value type %T %v", v, v)
			}
			for _, s := range slist {
				p.Anno = append(p.Anno, s.(string))
			}
		}
	}

	// update type when annots are present, but no more args are defined
	if len(p.Anno) > 0 && p.Type == PrimNullary {
		p.Type = PrimNullaryAnno
	}

	// process args separately and detect type based on number of args
	if a, ok := val["args"]; ok {
		args, ok := a.([]interface{})
		if !ok {
			return fmt.Errorf("micheline: invalid args value type %T %v", a, a)
		}

		switch len(args) {
		case 0:
			p.Type = PrimNullary
		case 1:
			if len(p.Anno) > 0 {
				p.Type = PrimUnaryAnno
			} else {
				p.Type = PrimUnary
			}
		case 2:
			if len(p.Anno) > 0 {
				p.Type = PrimBinaryAnno
			} else {
				p.Type = PrimBinary
			}
		default:
			p.Type = PrimVariadicAnno
		}

		// every arg is handled as embedded primitive
		for _, v := range args {
			prim := &Prim{}
			if err := prim.UnpackJSON(v); err != nil {
				return err
			}
			p.Args = append(p.Args, prim)
		}
	}
	return nil
}

func (p *Prim) UnmarshalBinary(data []byte) error {
	return p.DecodeBuffer(bytes.NewBuffer(data))
}

func (p *Prim) DecodeBuffer(buf *bytes.Buffer) error {
	b := buf.Next(1)
	if len(b) == 0 {
		return io.ErrShortBuffer
	}
	tag := PrimType(b[0])
	switch tag {
	case PrimInt:
		// data is a zarith number
		var z Z
		if err := z.DecodeBuffer(buf); err != nil {
			return err
		}
		p.Int = z.Big()
		p.OpCode = T_INT

	case PrimString:
		// cross-check content size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		p.String = string(buf.Next(size))
		p.OpCode = T_STRING

	case PrimSequence:
		// cross-check content size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		// extract sub-buffer
		seq := bytes.NewBuffer(buf.Next(size))
		// decode contained primitives
		p.Args = make([]*Prim, 0)
		for seq.Len() > 0 {
			prim := &Prim{}
			if err := prim.DecodeBuffer(seq); err != nil {
				return err
			}
			p.Args = append(p.Args, prim)
		}
		p.OpCode = T_LIST

	case PrimNullary:
		// opcode only
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

	case PrimNullaryAnno:
		// opcode with annotations
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

		// annotation array byte size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		anno := buf.Next(size)
		p.Anno = strings.Split(string(anno), " ")

	case PrimUnary:
		// opcode with single argument
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

		// argument
		prim := &Prim{}
		if err := prim.DecodeBuffer(buf); err != nil {
			return err
		}
		p.Args = append(p.Args, prim)

	case PrimUnaryAnno:
		// opcode with single argument and annotations
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

		// argument
		prim := &Prim{}
		if err := prim.DecodeBuffer(buf); err != nil {
			return err
		}
		p.Args = append(p.Args, prim)

		// annotation array byte size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		anno := buf.Next(size)
		p.Anno = strings.Split(string(anno), " ")

	case PrimBinary:
		// opcode with two arguments
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

		// 2 arguments
		for i := 0; i < 2; i++ {
			prim := &Prim{}
			if err := prim.DecodeBuffer(buf); err != nil {
				return err
			}
			p.Args = append(p.Args, prim)
		}

	case PrimBinaryAnno:
		// opcode with two arguments and annotations
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

		// 2 arguments
		for i := 0; i < 2; i++ {
			prim := &Prim{}
			if err := prim.DecodeBuffer(buf); err != nil {
				return err
			}
			p.Args = append(p.Args, prim)
		}

		// annotation array byte size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		anno := buf.Next(size)
		p.Anno = strings.Split(string(anno), " ")

	case PrimVariadicAnno:
		// opcode with N arguments and optional annotations
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		p.OpCode = OpCode(b[0])

		// argument array byte size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))

		// extract sub-buffer
		seq := bytes.NewBuffer(buf.Next(size))

		// decode contained primitives
		for seq.Len() > 0 {
			prim := &Prim{}
			if err := prim.DecodeBuffer(seq); err != nil {
				return err
			}
			p.Args = append(p.Args, prim)
		}
		// annotation array byte size
		size = int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		anno := buf.Next(size)
		p.Anno = strings.Split(string(anno), " ")

	case PrimBytes:
		// cross-check content size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		p.Bytes = buf.Next(size)
		p.OpCode = T_BYTES

	default:
		return fmt.Errorf("micheline: unknown primitive type 0x%x", tag)
	}
	p.Type = tag
	return nil
}

func (p *Prim) FindType(typ OpCode) (*Prim, bool) {
	if p == nil {
		return nil, false
	}
	if p.OpCode == typ {
		return p, true
	}
	for i := range p.Args {
		if p.Args[i].OpCode == typ {
			return p.Args[i], true
		} else {
			x, ok := p.Args[i].FindType(typ)
			if ok {
				return x, ok
			}
		}
	}
	return nil, false
}

// build matching type tree for value
func (p *Prim) BuildType() *Prim {
	if p == nil {
		return nil
	}
	t := &Prim{}
	if p.OpCode.IsType() {
		t.OpCode = p.OpCode
	}
	switch p.Type {
	case PrimInt:
		t.OpCode = p.Type.TypeCode()
		t.Type = PrimNullary

	case PrimBytes:
		t.Type = PrimNullary
		// detect address encoding first
		var addr chain.Address
		if err := addr.UnmarshalBinary(p.Bytes); err == nil {
			t.OpCode = T_ADDRESS
		} else {
			t.OpCode = p.Type.TypeCode()
		}

	case PrimString:
		t.Type = PrimNullary
		// detect timestamp and address encoding first
		if _, err := time.Parse(time.RFC3339, p.String); err == nil {
			t.OpCode = T_TIMESTAMP
		} else if _, err := chain.ParseAddress(p.String); err == nil {
			t.OpCode = T_ADDRESS
		} else {
			t.OpCode = p.Type.TypeCode()
		}

	case PrimSequence:
		if p.OpCode == D_ELT || len(p.Args) > 0 && p.Args[0].OpCode == D_ELT {
			// ELT can be T_MAP, T_SET, T_BIG_MAP
			// log.Infof("Building an ELT sequence")
			t.OpCode = T_MAP
			t.Type = PrimBinary
			t.Args = []*Prim{
				p.Args[0].Args[0].BuildType(), // key type
				p.Args[0].Args[1].BuildType(), // value type, breaks on polymorph types
			}
		} else if len(p.Args) > 0 && p.Args[0].OpCode.Type() == T_OPERATION {
			// sequences can be T_LIST, T_LAMBDA (if T_OPERATION is included)
			t.Type = PrimNullary // we don't know in/out types
			t.OpCode = T_LAMBDA
		} else {
			// how to represent inner types?
			// log.Infof("Building a LIST")
			t.OpCode = T_LIST
			if len(p.Args) > 0 {
				t.Type = PrimUnary
				t.Args = []*Prim{p.Args[0].BuildType()}
			} else {
				t.Type = PrimNullary
			}
		}
	case PrimNullary, PrimNullaryAnno:
		t.Type = PrimNullary
		t.OpCode = p.OpCode.Type()
	case PrimUnary, PrimUnaryAnno:
		t.OpCode = p.OpCode.Type()
		switch t.OpCode {
		case T_OPERATION:
			t.Type = PrimNullary
		case T_OR:
			// in data we only see one branch, so we have to guess the other type
			t.Type = PrimBinary
			inner := p.Args[0].BuildType()
			t.Args = []*Prim{inner, inner}
		case T_OPTION:
			// we only know the embedded type on D_SOME
			if p.OpCode == D_SOME {
				t.Type = PrimUnary
				t.Args = []*Prim{p.Args[0].BuildType()}
			} else {
				t.Type = PrimNullary
			}
		case T_BOOL, T_UNIT:
			t.Type = PrimNullary
		case T_TICKET:
			t.Type = PrimUnary
			t.Args = []*Prim{p.Args[0].BuildType()}
		}
	case PrimBinary, PrimBinaryAnno:
		if p.OpCode == D_ELT {
			// log.Infof("An ELT pair")
			t.OpCode = T_MAP
			t.Type = PrimBinary
			t.Args = []*Prim{
				p.Args[0].BuildType(),
				p.Args[1].BuildType(), // FIXME: breaks on polymorph types
			}
		} else {
			// probably a regular pair
			t.Type = PrimBinary
			t.OpCode = p.OpCode.Type()
			t.Args = []*Prim{
				p.Args[0].BuildType(),
				p.Args[1].BuildType(), // FIXME: breaks on polymorph types
			}
		}

	case PrimVariadicAnno:
		// ? probably an operation
		t.Type = PrimNullary
		t.OpCode = p.OpCode.Type()
	}
	// tbuf, _ := json.Marshal(t)
	// vbuf, _ := json.Marshal(p)
	// log.Infof("BUILT type %s from data %s", tbuf, vbuf)
	return t
}

func (p *Prim) GetPathString(path string) (*Prim, error) {
	ipath := make([]int, len(path))
	for i, v := range path {
		switch v {
		case 'L', 'l', '0':
			ipath[i] = 0
		case 'R', 'r', '1':
			ipath[i] = 1
		default:
			return nil, fmt.Errorf("micheline: invalid path component '%s' at pos %d", v, i)
		}
	}
	return p.GetPath(ipath)
}

func (p *Prim) GetPath(path []int) (*Prim, error) {
	prim := p
	for i, v := range path {
		if len(prim.Args) < v+1 {
			return nil, fmt.Errorf("micheline: path does not exist at pos %d[%d]", path, i)
		}
		prim = prim.Args[v]
	}
	return prim, nil
}
