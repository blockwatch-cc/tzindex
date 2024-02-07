// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"strconv"

	"github.com/awesome-gocui/gocui"
	"github.com/fatih/color"
)

const TableName string = "Table"

func createTable(g *gocui.Gui) (*View, error) {
	maxX, maxY := g.Size()
	v, err := NewView(TableName, 0, 2, maxX-1, maxY-1, g, func(v *gocui.View, res Model) error {
		maxX, maxY = g.Size()
		tbl := NewTable()
		tbl.SetHeader(headers)
		tbl.SetWidth(maxX - 1)
		for i := writePos; i < len(res.Table); i++ {
			t := res.Table[i]
			p := prev.Table.Find(t.GetName())
			row := make([]*RowCell, 0, 10)
			// set table name
			row = append(row, &RowCell{
				Text: strconv.FormatInt(int64(i+1), 10),
			})
			// set table name
			if t.IsIndex() {
				row = append(row, &RowCell{
					Text: "└─ " + t.IndexName,
				})
			} else {
				row = append(row, &RowCell{
					Text: t.TableName,
				})
			}
			// set tuple
			row = append(row, &RowCell{
				Text: FormatPretty(t.TupleCount),
			})
			// set packs
			row = append(row, &RowCell{
				Text: FormatPretty(t.PacksCount),
			})
			// set size
			row = append(row, &RowCell{
				Text: t.GetDiskSize(),
			})
			// set meta size
			row = append(row, &RowCell{
				Text: FormatBytes(int(t.MetaSize)),
			})
			// set cached
			row = append(row, &RowCell{
				Text: t.GetCached(),
			})
			// set hits
			row = append(row, &RowCell{
				Text: t.GetCacheHitRate(p, prev.Time),
			})
			// set journal
			row = append(row, &RowCell{
				Text: t.GetJournal(),
			})
			// set journal size
			row = append(row, &RowCell{
				Text: FormatBytes(int(t.JournalSize)),
			})
			// set throughput
			row = append(row, &RowCell{
				Text: t.GetThroughput(p, prev.Time),
			})
			// set I/O
			row = append(row, &RowCell{
				Text: t.GetIO(p, prev.Time),
			})
			// set R/W
			row = append(row, &RowCell{
				Text: FormatBytes(int(t.PacksBytesRead)) +
					" / " +
					FormatBytes(int(t.PacksBytesWritten+t.JournalBytesWritten+t.TombstoneBytesWritten)),
			})
			tr := &Row{
				RowCells: row,
			}
			if i == position {
				bgRed := color.New(color.BgCyan, color.FgBlack)
				tr.Color = bgRed.Sprintf
			}
			tbl.AddRow(tr)
		}
		tbl.Fprint(v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}
