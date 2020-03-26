// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

type Script struct {
	Code    *Code `json:"code"`    // code section, i.e. parameter & storage types, code
	Storage *Prim `json:"storage"` // data section, i.e. initial contract storage
}

type Code struct {
	Param   *Prim // call types
	Storage *Prim // storage types
	Code    *Prim // program code
	BadCode *Prim // catch-all for ill-formed contracts
}

func NewScript() *Script {
	return &Script{
		Code: &Code{
			Param:   &Prim{Type: PrimSequence, Args: []*Prim{&Prim{Type: PrimUnary, OpCode: K_PARAMETER}}},
			Storage: &Prim{Type: PrimSequence, Args: []*Prim{&Prim{Type: PrimUnary, OpCode: K_STORAGE}}},
			Code:    &Prim{Type: PrimSequence, Args: []*Prim{&Prim{Type: PrimUnary, OpCode: K_CODE}}},
		},
		Storage: &Prim{},
	}
}

func (s *Script) StorageType() BigMapType {
	return BigMapType(*s.Code.Storage.Args[0])
}

func (c Code) MarshalJSON() ([]byte, error) {
	root := &Prim{
		Type: PrimSequence,
		Args: []*Prim{c.Param, c.Storage, c.Code},
	}
	if c.BadCode != nil {
		root = c.BadCode
	}
	return json.Marshal(root)
}

func (p Script) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// 1 write code segment

	// keep space for size
	binary.Write(buf, binary.BigEndian, uint32(0))

	// root element is a sequence
	root := &Prim{
		Type: PrimSequence,
		Args: []*Prim{p.Code.Param, p.Code.Storage, p.Code.Code},
	}

	// store ill-formed contracts
	if p.Code.BadCode != nil {
		root = &Prim{
			Type: PrimSequence,
			Args: []*Prim{EmptyPrim, EmptyPrim, EmptyPrim, p.Code.BadCode},
		}
	}

	if err := root.EncodeBuffer(buf); err != nil {
		return nil, err
	}
	codesize := buf.Len() - 4

	// 2 write data segment

	// use temp buffer because we need to prepend size
	data := bytes.NewBuffer(nil)
	if err := p.Storage.EncodeBuffer(data); err != nil {
		return nil, err
	}

	// write data size
	binary.Write(buf, binary.BigEndian, uint32(data.Len()))

	// append to output buffer
	buf.Write(data.Bytes())

	// patch code size
	res := buf.Bytes()
	binary.BigEndian.PutUint32(res[:], uint32(codesize))

	return res, nil
}

func (c *Code) UnmarshalJSON(data []byte) error {
	// read primitive tree
	var prim Prim
	if err := json.Unmarshal(data, &prim); err != nil {
		return err
	}

	// check for sequence tag
	if prim.Type != PrimSequence {
		log.Warnf("micheline: unexpected program tag 0x%x", prim.Type)
		c.BadCode = &prim
		return nil
	}

	// unpack keyed program parts
	isBadCode := false
stopcode:
	for _, v := range prim.Args {
		switch v.OpCode {
		case K_PARAMETER:
			c.Param = v
		case K_STORAGE:
			c.Storage = v
		case K_CODE:
			c.Code = v
		default:
			isBadCode = true
			log.Warnf("micheline: unexpected program key 0x%x (%d)", byte(v.OpCode), v.OpCode)
			break stopcode
		}
	}
	if isBadCode {
		c.BadCode = &prim
	}
	return nil
}

func (p *Script) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)

	// 1 Code

	// starts with BE uint32 total size
	size := int(binary.BigEndian.Uint32(buf.Next(4)))
	if buf.Len() < size {
		return io.ErrShortBuffer
	}

	// read primitive tree
	var prim Prim
	if err := prim.DecodeBuffer(buf); err != nil {
		return err
	}

	// check for sequence tag
	if prim.Type != PrimSequence {
		return fmt.Errorf("micheline: unexpected program tag 0x%x", prim.Type)
	}

	// unpack keyed program parts
	p.Code = &Code{}
	for _, v := range prim.Args {
		switch v.OpCode {
		case K_PARAMETER:
			p.Code.Param = v
		case K_STORAGE:
			p.Code.Storage = v
		case K_CODE:
			p.Code.Code = v
		case 255:
			p.Code.BadCode = v
		default:
			return fmt.Errorf("micheline: unexpected program key 0x%x", v.OpCode)
		}
	}

	// 2 Storage

	// check storage is present
	if buf.Len() < 4 {
		return io.ErrShortBuffer
	}

	// starts with BE uint32 total size
	size = int(binary.BigEndian.Uint32(buf.Next(4)))
	if buf.Len() < size {
		return io.ErrShortBuffer
	}

	// read primitive tree
	p.Storage = &Prim{}
	if err := p.Storage.DecodeBuffer(buf); err != nil {
		return err
	}

	if buf.Len() > 0 {
		return fmt.Errorf("micheline: %d unexpected extra trailer bytes", buf.Len())
	}

	return nil
}
