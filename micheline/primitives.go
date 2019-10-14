// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package micheline

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
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
		return "prim_1"
	case PrimUnaryAnno:
		return "prim_1%"
	case PrimBinary:
		return "prim_2"
	case PrimBinaryAnno:
		return "prim_2%"
	case PrimVariadicAnno:
		return "prim_n"
	case PrimBytes:
		return "bytes"
	default:
		return "invalid"
	}
}

type Prim struct {
	Type   PrimType // primitive type
	OpCode OpCode   // primitive opcode (invalid on sequences, strings, bytes, int)
	Args   []*Prim  // optional arguments
	Anno   []string // optional type annotations
	Int    int64    // optional data
	String string   // optional data
	Bytes  []byte   // optional data
}

func (p Prim) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	switch p.Type {
	case PrimSequence:
		return json.Marshal(p.Args)
	case PrimInt:
		m["int"] = strconv.FormatInt(int64(p.Int), 10)
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
		z := Z(p.Int)
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
		return p.Unpack(m)
	}
}

func (p *Prim) Unpack(val interface{}) error {
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
		if err := prim.Unpack(v); err != nil {
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
			i, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
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
			if err := prim.Unpack(v); err != nil {
				return err
			}
			p.Args = append(p.Args, prim)
		}
	}
	return nil
}

func (p *Prim) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := p.DecodeBuffer(buf); err != nil {
		return err
	}
	return nil
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
		p.Int = int64(z)

	case PrimString:
		// cross-check content size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
		p.String = string(buf.Next(size))

	case PrimSequence:
		// cross-check content size
		size := int(binary.BigEndian.Uint32(buf.Next(4)))
		if buf.Len() < size {
			return io.ErrShortBuffer
		}
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

	default:
		return fmt.Errorf("micheline: unknown primitive type 0x%x", tag)
	}
	p.Type = tag
	return nil
}
