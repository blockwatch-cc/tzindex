// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"fmt"

	"github.com/awesome-gocui/gocui"
)

const (
	MIN_CELLWIDTH = 5
)

var headers []*TableHeader = []*TableHeader{
	{
		Label: "#",
		Width: MIN_CELLWIDTH,
	},
	{
		Label: "TABLE",
		Align: AlignLeft,
		Width: 24,
	},
	{
		Label: "TUPLES",
		Align: AlignRight,
		Width: 12,
	},
	{
		Label: "PACKS",
		Align: AlignRight,
		Width: 10,
	},
	{
		Label: "DISKSIZE",
		Align: AlignRight,
		Width: 12,
	},
	{
		Label: "METASIZE",
		Align: AlignRight,
		Width: 10,
	},
	{
		Label: "CACHESIZE",
		Align: AlignSlash,
		Width: 22,
	},
	{
		Label: "HITS",
		Align: AlignSlash,
		Width: 17,
	},
	{
		Label: "JOURNAL",
		Align: AlignSlash,
		Width: 22,
	},
	{
		Label: "JOURNALSIZE",
		Align: AlignRight,
		Width: 15,
	},
	{
		Label: "THROUGHPUT",
		Align: AlignSlash,
		Width: 25,
	},
	{
		Label: "I/O",
		Align: AlignSlash,
	},
	{
		Label: "R/W",
		Align: AlignSlash,
	},
}

func createHeaderContainer(g *gocui.Gui) (*View, error) {
	maxX, _ := g.Size()
	v, err := NewView("HeaderContainer", 0, 0, maxX-1, 2, g, func(g *gocui.View, res Model) error {
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func createHeaderLeft(g *gocui.Gui) (*View, error) {
	maxX, _ := g.Size()
	v, err := NewView("HeaderLeft", 0, 0, maxX/3, 2, g, func(v *gocui.View, res Model) error {
		v.Frame = false
		if res.IsValid() {
			fmt.Fprintf(v, " "+res.Sys.Hostname)
		} else {
			fmt.Fprint(v, " -- ")
		}
		if res.Sys.ContainerName != "" {
			SlantedSpacer(v)
			fmt.Fprintf(v, res.Sys.ContainerName)
		}
		SlantedSpacer(v)
		fmt.Fprint(v, res.Table.NumTables(), " tables")
		SlantedSpacer(v)
		fmt.Fprint(v, res.Table.NumIndexes(), " indexes")
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func createHeaderCenter(g *gocui.Gui) (*View, error) {
	maxX, _ := g.Size()
	v, err := NewView("HeaderCenter", maxX/2-30, 0, maxX/2+30, 2, g, func(v *gocui.View, res Model) error {
		v.Frame = false
		if res.IsValid() {
			fmt.Fprintf(v, "CPU %.2f%%", res.Sys.CpuTotal)
		} else {
			fmt.Fprint(v, "CPU --%")
		}
		VerticalSpacer(v)
		fmt.Fprint(v, "MEM ")
		if res.IsValid() {
			fmt.Fprintf(v, "%s", FormatBytes(int(res.Sys.VmRss)))
		} else {
			fmt.Fprint(v, " -- ")
		}
		SlantedSpacer(v)
		if res.IsValid() {
			fmt.Fprintf(v, "%s", FormatBytes(int(res.Sys.TotalMem)))
		} else {
			fmt.Fprint(v, " -- ")
		}
		VerticalSpacer(v)
		fmt.Fprint(v, "DISK ")
		if res.IsValid() {
			fmt.Fprintf(v, "%s", FormatBytes(int(res.Sys.DiskUsed)))
		} else {
			fmt.Fprint(v, " -- ")
		}
		SlantedSpacer(v)
		if res.IsValid() {
			fmt.Fprintf(v, "%s", FormatBytes(int(res.Sys.DiskSize)))
		} else {
			fmt.Fprint(v, " -- ")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func createHeaderRight(g *gocui.Gui) (*View, error) {
	maxX, _ := g.Size()
	v, err := NewView("HeaderRight", maxX-16, 0, maxX-1, 2, g, func(v *gocui.View, res Model) error {
		v.Frame = false
		if res.IsValid() {
			fmt.Fprintf(v, "%s", res.Sys.Timestamp.Format("15:04 Jan 2"))
		} else {
			fmt.Fprint(v, "--:-- --- --")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}
