// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"fmt"
	"io"
)

type Table struct {
	rows   []*Row
	width  int
	header []*TableHeader
}

func NewTable() *Table {
	return &Table{
		rows: make([]*Row, 0),
	}
}

type RowCell struct {
	Text string
}

type Row struct {
	RowCells []*RowCell
	Color    func(string, ...interface{}) string
}

type TableHeader struct {
	Label string
	Width int
	Align func(string, int) string
}

func (t *Table) SetHeader(header []*TableHeader) {
	t.header = header
	for i := range t.header {
		if t.header[i].Align == nil {
			t.header[i].Align = AlignCenter
		}
	}
}

func (t *Table) SetWidth(w int) {
	t.width = w
}

func (t *Table) AddRow(r *Row) {
	t.rows = append(t.rows, r)
}

func (t *Table) Fprint(w io.Writer) {
	s := ""
	var fixedWidth, fixedCols int
	for _, h := range t.header {
		fixedWidth += h.Width
		if h.Width > 0 {
			fixedCols++
		}
	}
	cellMaxWidth := (t.width - fixedWidth) / (len(t.header) - fixedCols)
	for _, h := range t.header {
		w := h.Width
		if w == 0 {
			w = cellMaxWidth
		}
		s += h.Align(h.Label, w)
	}
	s += "\n"
	for _, row := range t.rows {
		for i, cell := range row.RowCells {
			w := t.header[i].Width
			if w == 0 {
				w = cellMaxWidth
			}
			if row.Color != nil {
				s += row.Color("%s", t.header[i].Align(cell.Text, w))
			} else {
				s += t.header[i].Align(cell.Text, w)
			}
		}
		s += "\n"
	}
	fmt.Fprint(w, s)
}
