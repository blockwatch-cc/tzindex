// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"strings"
)

const (
	TypeAnnoPrefix  = ":"
	VarAnnoPrefix   = "%"
	FieldAnnoPrefix = "@"
)

func (p *Prim) StripAnno(name string) {
	for i := 0; i < len(p.Anno); i++ {
		if p.Anno[i][1:] == name {
			p.Anno = append(p.Anno[:i], p.Anno[i+1:]...)
			i--
		}
	}
}

func (p *Prim) HasAnyAnno() bool {
	if len(p.Anno) == 0 {
		return false
	}
	for _, v := range p.Anno {
		if len(v) > 0 {
			return true
		}
	}
	return false
}

func (p *Prim) HasTypeAnno() bool {
	for _, v := range p.Anno {
		if strings.HasPrefix(v, TypeAnnoPrefix) {
			return true
		}
	}
	return false
}

func (p *Prim) GetTypeAnno() string {
	for _, v := range p.Anno {
		if strings.HasPrefix(v, TypeAnnoPrefix) {
			return v[1:]
		}
	}
	return ""
}

// prefers TypeAnno, first anno otherwise
func (p *Prim) GetTypeAnnoAny() string {
	if len(p.Anno) > 0 {
		if p.HasTypeAnno() {
			return p.GetTypeAnno()
		}
		if len(p.Anno[0]) > 1 {
			return p.Anno[0][1:]
		}
	}
	return ""
}

func (p *Prim) HasVarAnno() bool {
	for _, v := range p.Anno {
		if strings.HasPrefix(v, VarAnnoPrefix) {
			return true
		}
	}
	return false
}

func (p *Prim) GetVarAnno() string {
	for _, v := range p.Anno {
		if strings.HasPrefix(v, VarAnnoPrefix) {
			return v[1:]
		}
	}
	return ""
}

// prefers VarAnno, first anno otherwise
func (p *Prim) GetVarAnnoAny() string {
	if len(p.Anno) > 0 {
		if p.HasVarAnno() {
			return p.GetVarAnno()
		}
		if len(p.Anno[0]) > 1 {
			return p.Anno[0][1:]
		}
	}
	return ""
}

func (p *Prim) HasFieldAnno() bool {
	for _, v := range p.Anno {
		if strings.HasPrefix(v, FieldAnnoPrefix) {
			return true
		}
	}
	return false
}

func (p *Prim) GetFieldAnno() string {
	for _, v := range p.Anno {
		if strings.HasPrefix(v, FieldAnnoPrefix) {
			return v[1:]
		}
	}
	return ""
}

// prefers FieldAnno, first anno otherwise
func (p *Prim) GetFieldAnnoAny() string {
	if len(p.Anno) > 0 {
		if p.HasFieldAnno() {
			return p.GetFieldAnno()
		}
		if len(p.Anno[0]) > 1 {
			return p.Anno[0][1:]
		}
	}
	return ""
}
