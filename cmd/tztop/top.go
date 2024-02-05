// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"errors"
	"time"

	"github.com/awesome-gocui/gocui"
)

var (
	next, prev Model
	position   int = 0
	writePos   int = 0
)

type Top struct {
	g        *gocui.Gui
	api      *Client
	interval time.Duration
	views    map[string]*View
}

func NewTop(c *Client, interval time.Duration) (*Top, error) {
	g, err := gocui.NewGui(gocui.Output256, true)
	if err != nil {
		return nil, err
	}
	t := &Top{
		g:        g,
		api:      c,
		interval: interval,
		views:    make(map[string]*View),
	}
	t.poll()
	return t, nil
}

func (t *Top) poll() {
	// auto get response and update
	ticker := time.NewTicker(t.interval)
	go func() {
		for {
			prev = next
			next.Time = time.Now()
			next.Table, next.Error = t.api.GetTableStats()
			next.Sys, next.Error = t.api.GetSysStats()
			next.Tip, next.Error = t.api.GetTip()
			if next.Error != nil {
				if f, ok := t.views[FooterName]; ok {
					f.Refresh(next)
				}
			} else {
				for _, v := range t.views {
					if v != nil {
						v.Refresh(next)
					}
				}
			}
			<-ticker.C
		}
	}()
}

func (t *Top) Display() error {
	defer t.g.Close()

	t.Layout()
	if err := t.Keybindings(); err != nil {
		log.Errorf("keybindings binding failed: %v", err)
		return err
	}
	if err := t.g.MainLoop(); err != nil && errors.Is(err, gocui.ErrQuit) {
		log.Error(err)
		return err
	}
	return nil
}

func (t *Top) Layout() {
	t.g.SetManagerFunc(func(g *gocui.Gui) error {
		// header container
		hc, err := createHeaderContainer(g)
		if err != nil {
			return err
		}
		if hc != nil {
			t.views[hc.Name] = hc
		}
		// header left
		hl, err := createHeaderLeft(t.g)
		if err != nil {
			return err
		}
		if hl != nil {
			t.views[hl.Name] = hl
		}
		// header center
		hce, err := createHeaderCenter(t.g)
		if err != nil {
			return err
		}
		if hce != nil {
			t.views[hce.Name] = hce
		}
		// header right
		hr, err := createHeaderRight(t.g)
		if err != nil {
			return err
		}
		if hr != nil {
			t.views[hr.Name] = hr
		}
		// table
		tbl, err := createTable(t.g)
		if err != nil {
			return err
		}
		if tbl != nil {
			t.views[tbl.Name] = tbl
		}
		footer, err := createFooter(t.g)
		if err != nil {
			return err
		}
		if footer != nil {
			t.views[footer.Name] = footer
		}
		return nil
	})
}
