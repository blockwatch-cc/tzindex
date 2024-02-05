// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"fmt"

	"github.com/awesome-gocui/gocui"
)

const FooterName string = "Footer"

func createFooter(g *gocui.Gui) (*View, error) {
	maxX, maxY := g.Size()
	v, err := NewView(FooterName, 0, maxY-3, maxX-1, maxY-1, g, func(v *gocui.View, res Model) error {
		v.Frame = true
		if res.Error != nil {
			fmt.Fprint(v, res.Error.Error())
		} else {
			fmt.Fprintf(v, " %s ", res.Tip.Name)
			if res.Tip.Network != "" {
				fmt.Fprintf(v, "%s ", res.Tip.Network)
			} else {
				fmt.Fprintf(v, "%s ", res.Tip.ChainId)
			}
			VerticalSpacer(v)
			fmt.Fprintf(v, " %s -- Block %s / %s (%s) ",
				res.Tip.Status.Status,
				PrettyInt64(res.Tip.Status.Indexed),
				PrettyInt64(res.Tip.Status.Blocks),
				res.Tip.Timestamp.Format("2006-01-02 15:04:05"),
			)
			VerticalSpacer(v)
			fmt.Fprintf(v, " Health %d%% ", res.Tip.Health)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}
