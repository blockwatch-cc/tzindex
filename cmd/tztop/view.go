// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"errors"

	"github.com/awesome-gocui/gocui"
)

type View struct {
	Name string
	v    *gocui.View
	g    *gocui.Gui
	draw func(v *gocui.View, res Model) error
}

func NewView(n string, x1, y1, x2, y2 int, g *gocui.Gui, fn func(v *gocui.View, res Model) error) (*View, error) {
	v, err := g.SetView(n, x1, y1, x2, y2, 0)
	if !errors.Is(err, gocui.ErrUnknownView) {
		return nil, err
	}
	_ = fn(v, Model{})
	return &View{
		Name: n,
		v:    v,
		draw: fn,
		g:    g,
	}, nil
}

func (v *View) Refresh(res Model) {
	v.g.UpdateAsync(func(g *gocui.Gui) error {
		v.v.Clear()
		return v.draw(v.v, res)
	})
}

func (v *View) View() *gocui.View {
	return v.v
}
