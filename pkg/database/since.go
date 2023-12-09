package database

import (
	"fmt"
	"time"
)

func since(updatedAt *time.Time) string {
	if updatedAt == nil {
		return "Never"
	}

	since := time.Since(*updatedAt)
	hours := int64(since.Hours())
	days := hours / 24
	since -= time.Duration(days) * 24 * time.Hour

	daysStr := ""
	if days != 0 {
		daysStr = fmt.Sprintf("%dd", days)

		if hours%24 == 0 {
			daysStr += "0h"
		}
		if int64(since.Minutes())%60 == 0 {
			daysStr += "0m"
		}
	}

	if since < 0 {
		return "In " + daysStr + (-since).Truncate(time.Second).String()
	}

	return daysStr + since.Truncate(time.Second).String() + " ago"
}