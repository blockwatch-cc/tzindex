// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"math"

	"github.com/awesome-gocui/gocui"
)

// Line height for Header, Table Header and Display Error
const SURROUNDING_SPACE int = 3

func (t *Top) Keybindings() error {
	if err := t.g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, t.quit); err != nil {
		return err
	}
	if err := t.g.SetKeybinding("", gocui.KeyArrowDown, gocui.ModNone, t.moveDown); err != nil {
		return err
	}
	if err := t.g.SetKeybinding("", gocui.KeyArrowUp, gocui.ModNone, t.moveUp); err != nil {
		return err
	}
	key, qModifier := gocui.MustParse("q")
	if err := t.g.SetKeybinding("", key, qModifier, t.quit); err != nil {
		return err
	}
	return nil
}

func (t *Top) quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}

// TODO(abdul): refactor scrolling current implementation isn't pretty but works
func (t *Top) moveUp(g *gocui.Gui, v *gocui.View) error {
	if position > 0 {
		if vv, ok := t.views[TableName]; ok {
			g.UpdateAsync(func(g *gocui.Gui) error {
				position--
				update(vv)
				if (position+1)-writePos <= 0 && writePos > 0 {
					writePos -= 1
				}
				return nil
			})
		}
	}
	return nil
}

func (t *Top) moveDown(g *gocui.Gui, v *gocui.View) error {
	if position < len(next.Table)-1 {
		if vv, ok := t.views[TableName]; ok {
			g.UpdateAsync(func(g *gocui.Gui) error {
				position++
				update(vv)
				_, y := vv.View().Size()
				vh := vv.View().ViewLinesHeight() + writePos
				dh := y - SURROUNDING_SPACE - vh
				if dh < 0 && vh-int(math.Abs(float64(dh))) < position-writePos {
					writePos += 1
				}
				return nil
			})
		}
	}
	return nil
}

func update(vv *View) {
	response := next
	if next.Error != nil {
		response = prev
	}
	vv.Refresh(response)
}
