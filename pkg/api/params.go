package api

import (
	"fmt"
	"net/http"
	"strconv"
)

func parseAllYesNoParam(
	w http.ResponseWriter,
	str string,
	param string,
	defaultValue int,
) (int, bool) {
	switch str {
	case "":
		return defaultValue, true
	case "all":
		return -1, true
	case "yes":
		return 1, true
	case "no":
		return 0, true
	}

	value, err := strconv.Atoi(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad %s value: %s\n", param, str)

		return 0, false
	}

	if value != -1 && value != 0 && value != 1 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad %s value: %s. Must be one of all, yes, no\n", param, str)

		return 0, false
	}

	return value, true
}

func parseSyncedParam(w http.ResponseWriter, str string) (int, bool) {
	return parseAllYesNoParam(w, str, "synced", 1)
}

func parseErrorParam(w http.ResponseWriter, str string) (int, bool) {
	return parseAllYesNoParam(w, str, "error", -1)
}

func parseNextForkParam(w http.ResponseWriter, str string) (int, bool) {
	return parseAllYesNoParam(w, str, "next-fork", -1)
}

func parsePageNum(w http.ResponseWriter, pageNumStr string) (int, bool) {
	if pageNumStr == "" {
		return 1, true
	}

	pageNumber, err := strconv.Atoi(pageNumStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad page number value: %s\n", pageNumStr)

		return 0, false
	}

	if pageNumber <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad page number value, must be greater than 0: %d", pageNumber)

		return 0, false
	}

	return pageNumber, true
}

func parseNetworkID(w http.ResponseWriter, networkIDStr string) (int64, bool) {
	if networkIDStr == "" {
		return 1, true
	}

	networkID, err := strconv.ParseInt(networkIDStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad network id value: %s\n", networkIDStr)

		return 0, false
	}

	return networkID, true
}
