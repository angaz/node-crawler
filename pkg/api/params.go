package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
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

func parseTimeParam(w http.ResponseWriter, param string, str string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad %s value: %s. Must be RFC3339 format.\n", param, str)

		return time.Unix(0, 0), false
	}

	return t, true
}

func parseNodeType(
	w http.ResponseWriter,
	r *http.Request,
) (*string, bool) {
	query := r.URL.Query()
	str := query.Get("node-type")

	if str == "all" {
		return nil, true
	}

	if str == "" {
		res := common.NodeTypeExecution.String()
		return &res, true
	}

	nodeType := common.ParseNodeType(str)
	if nodeType == common.NodeTypeUnknown {
		_, _ = fmt.Fprintf(w, "bad node-type value: %s.", str)

		res := common.NodeTypeUnknown.String()
		return &res, false
	}

	res := nodeType.String()
	return &res, true
}

func parseNumberParam(
	w http.ResponseWriter,
	r *http.Request,
	param string,
	required bool,
	defaultValue int,
) (int, bool) {
	query := r.URL.Query()
	str := query.Get(param)

	// Value not set
	if str == "" {
		if required {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "%s query param is required.\n", param)

			return 0, false
		}

		return defaultValue, true
	}

	num, err := strconv.Atoi(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad %s value: %s. Not a valid number.\n", param, str)

		return 0, false
	}

	return num, true
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
