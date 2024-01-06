package common

import (
	"testing"
	"time"
)

func newDuration(days int64, hours int64, minutes int64, seconds int64) time.Duration {
	return time.Duration(
		days*24*int64(time.Hour) +
			hours*int64(time.Hour) +
			minutes*int64(time.Minute) +
			seconds*int64(time.Second))
}

func TestFormatSince(t *testing.T) {
	tt := []struct {
		since time.Duration
		want  string
	}{
		{
			newDuration(0, 0, 0, 0),
			"0s ago",
		},
		{
			newDuration(0, 0, 0, 1),
			"1s ago",
		},
		{
			newDuration(0, 0, 1, 0),
			"1m0s ago",
		},
		{
			newDuration(0, 0, 1, 1),
			"1m1s ago",
		},
		{
			newDuration(1, 0, 0, 0),
			"1d0h0m0s ago",
		},
		{
			newDuration(1, 0, 1, 0),
			"1d0h1m0s ago",
		},
		{
			newDuration(9000, 0, 0, 1),
			"9000d0h0m1s ago",
		},
		{
			newDuration(-9000, 0, 0, -1),
			"In 9000d0h0m1s",
		},
	}

	for _, test := range tt {
		out := formatSince(test.since)

		if out != test.want {
			t.Errorf("got: %s, want: %s", out, test.want)
		}
	}
}
