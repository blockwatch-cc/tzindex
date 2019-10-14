// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"time"
)

var (
	OneDay = 24 * time.Hour
)

// called when time has crossed midnight UTC
func Yesterday(date time.Time) (sod time.Time, eod time.Time) {
	today := date.Truncate(OneDay).UTC()
	eod = today.Add(-time.Millisecond) // yesterday end of day
	sod = today.Add(-OneDay)           // yesterday start of day
	return
}

func Today(date time.Time) (sod time.Time, eod time.Time) {
	sod = date.Truncate(OneDay)
	eod = sod.Add(OneDay - time.Millisecond) // end of today
	return
}
