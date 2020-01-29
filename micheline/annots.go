// Copyright (c) 2020 Blockwatch Data Inc.
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

func (p *Prim) HasAnno() bool {
	return len(p.Anno) > 0
}

// prefers VarAnno, first anno otherwise
func (p *Prim) GetAnno() string {
	if len(p.Anno) > 0 {
		if p.HasVarAnno() {
			return p.GetVarAnno()
		}
		return p.Anno[0][1:]
	}
	return ""
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
