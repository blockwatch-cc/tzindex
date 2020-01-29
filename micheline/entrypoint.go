// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"fmt"
	"strings"
)

type Entrypoint struct {
	Id     int        `json:"id"`
	Branch string     `json:"branch"`
	Type   BigMapType `json:"type"`
	Prim   *Prim      `json:"prim,omitempty"`
}

type Entrypoints map[string]Entrypoint

func (e Entrypoints) FindBranch(branch string) (Entrypoint, bool) {
	if branch == "" {
		return Entrypoint{}, false
	}
	for _, v := range e {
		if v.Branch == branch {
			return v, true
		}
	}
	return Entrypoint{}, false
}

func (e Entrypoints) FindId(id int) (Entrypoint, bool) {
	for _, v := range e {
		if v.Id == id {
			return v, true
		}
	}
	return Entrypoint{}, false
}

func (s *Script) Entrypoints(withPrim bool) (Entrypoints, error) {
	e := make(Entrypoints)
	if err := listEntrypoints(e, "", s.Code.Param.Args[0]); err != nil {
		return nil, err
	}
	if !withPrim {
		for n, v := range e {
			v.Prim = nil
			e[n] = v
		}
	}
	return e, nil
}

// returns path to named entrypoint
func (s *Script) SearchEntrypointName(name string) string {
	return searchEntrypointName(name, "", s.Code.Param.Args[0])
}

func searchEntrypointName(name, branch string, node *Prim) string {
	if node.GetAnno() == name {
		return branch
	}
	if node.OpCode == T_OR && (len(branch) == 0 || !node.HasAnno()) {
		// LEFT
		b := searchEntrypointName(name, branch+"L", node.Args[0])
		if b != "" {
			return b
		}
		// RIGHT
		b = searchEntrypointName(name, branch+"R", node.Args[1])
		if b != "" {
			return b
		}
	}
	return ""
}

// Explicit list of prefixes for detecting entrypoints.
//
// This is necessary to resolve ambiguities in contract designs that
// use T_OR as call parameter.

// - to handle conflicts between T_OR used for call params vs used for marking entrypoint
//   skip annotated T_OR branches (exclude the root T_OR and any branch called 'default')
var knownEntrypointPrefixes = []string{"_Liq_entry_"}

func isKnownEntrypointPrefix(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, v := range knownEntrypointPrefixes {
		if strings.HasPrefix(s, v) {
			return true
		}
	}
	return false
}

// walks T_OR expressions and stores each non-T_OR branch as entrypoint
func listEntrypoints(e Entrypoints, branch string, node *Prim) error {
	if node.OpCode == T_OR && !isKnownEntrypointPrefix(node.GetAnno()) {
		if l := len(node.Args); l != 2 {
			return fmt.Errorf("micheline: expected 2 arguments for T_OR, git %d", l)
		}

		// LEFT
		if err := listEntrypoints(e, branch+"L", node.Args[0]); err != nil {
			return err
		}

		// RIGHT
		if err := listEntrypoints(e, branch+"R", node.Args[1]); err != nil {
			return err
		}

		return nil
	}

	// process non-T_OR branches
	ep := Entrypoint{
		Id:     len(e),
		Branch: branch,
		Type:   BigMapType(*node),
		Prim:   node,
	}
	var name string
	if node.HasAnno() {
		name = node.GetAnno()
		// lift type tree when under the same name as entrypoint
		ep.Type.Anno = nil
	} else {
		name = fmt.Sprintf("__entry_%02d__", len(e))
	}

	e[name] = ep
	return nil
}
