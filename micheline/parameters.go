// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package micheline

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
)

type Parameters struct {
	Entrypoint string `json:"entrypoint"`
	Value      *Prim  `json:"value"`
}

func (p Parameters) MarshalJSON() ([]byte, error) {
	if p.Entrypoint == "" || (p.Entrypoint == "default" && p.Value.OpCode == D_UNIT) {
		return json.Marshal(p.Value)
	}
	type alias Parameters
	return json.Marshal(alias(p))
}

// stay compatible with v005 transaction serialization
func (p Parameters) MarshalBinary() ([]byte, error) {
	// single Unit value
	if p.Value != nil && p.Value.OpCode == D_UNIT {
		return []byte{0}, nil
	}
	// entrypoint format, compatible with v005
	buf := bytes.NewBuffer([]byte{1})
	n := 2
	switch p.Entrypoint {
	case "", "default":
		buf.WriteByte(0)
	case "root":
		buf.WriteByte(1)
	case "do":
		buf.WriteByte(2)
	case "set_delegate":
		buf.WriteByte(3)
	case "remove_delegate":
		buf.WriteByte(4)
	default:
		buf.WriteByte(255)
		buf.WriteByte(byte(len(p.Entrypoint)))
		buf.WriteString(p.Entrypoint)
		n += 1 + len(p.Entrypoint)
	}

	// param as size + serialized data
	binary.Write(buf, binary.BigEndian, uint32(0))
	if err := p.Value.EncodeBuffer(buf); err != nil {
		return nil, err
	}

	// patch data size
	res := buf.Bytes()
	binary.BigEndian.PutUint32(res[n:], uint32(len(res)-n-4))

	return res, nil
}

func (p *Parameters) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '[' {
		// non-entrypoint calling convention
		p.Value = &Prim{}
		return json.Unmarshal(data, &p.Value)
	} else {
		// try entrypoint calling convention
		type alias *Parameters
		if err := json.Unmarshal(data, alias(p)); err != nil {
			return err
		}
		if p.Value != nil {
			return nil
		}
		// try legacy calling convention for single prim values
		p.Entrypoint = "default"
		p.Value = &Prim{}
		return json.Unmarshal(data, p.Value)
	}
}

func (p *Parameters) UnmarshalBinary(data []byte) error {
	if len(data) == 1 && data[0] == 0 {
		p.Value = &Prim{Type: PrimNullary, OpCode: D_UNIT}
		p.Entrypoint = "default"
		return nil
	}
	buf := bytes.NewBuffer(data[1:])
	tag := buf.Next(1)
	if len(tag) == 0 {
		return io.ErrShortBuffer
	}
	switch tag[0] {
	case 0:
		p.Entrypoint = "default"
	case 1:
		p.Entrypoint = "root"
	case 2:
		p.Entrypoint = "do"
	case 3:
		p.Entrypoint = "set_delegate"
	case 4:
		p.Entrypoint = "remove_delegate"
	default:
		sz := buf.Next(1)
		if len(sz) == 0 || buf.Len() < int(sz[0]) {
			return io.ErrShortBuffer
		}
		p.Entrypoint = string(buf.Next(int(sz[0])))
	}

	// read serialized data
	size := int(binary.BigEndian.Uint32(buf.Next(4)))
	if buf.Len() < size {
		return io.ErrShortBuffer
	}
	prim := &Prim{}
	if err := prim.DecodeBuffer(buf); err != nil {
		return err
	}
	p.Value = prim
	return nil
}
