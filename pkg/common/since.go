package common

import (
	"fmt"
	"strings"
	"time"
)

const day = 24 * time.Hour

func formatSince(since time.Duration) string {
	isNegative := since < 0

	since = since.Abs()

	days := since / day
	hours := (since % day) / time.Hour
	minutes := (since % time.Hour) / time.Minute
	seconds := (since % time.Minute) / time.Second

	print0 := false

	builder := new(strings.Builder)

	if days != 0 {
		print0 = true

		fmt.Fprintf(builder, "%dd", days)
	}

	if print0 || hours != 0 {
		print0 = true

		fmt.Fprintf(builder, "%dh", hours)
	}

	if print0 || minutes != 0 {
		fmt.Fprintf(builder, "%dm", minutes)
	}

	fmt.Fprintf(builder, "%ds", seconds)

	if isNegative {
		return "In " + builder.String()
	}

	return builder.String() + " ago"
}

func Since(updatedAt *time.Time) string {
	if updatedAt == nil {
		return "Never"
	}

	return formatSince(time.Since(*updatedAt))
}
