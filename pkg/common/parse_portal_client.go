package common

import (
	"strings"

	"golang.org/x/exp/slog"
)

func ParsePortalClientID(clientName *string) *Client {
	if clientName == nil || *clientName == "" || *clientName == Unknown {
		return nil
	}

	name := strings.ToLower(*clientName)
	parts := strings.Split(name, " ")
	nParts := len(parts)

	switch nParts {
	case 1:
		return &Client{
			Name:     parts[0],
			UserData: Unknown,
			Version:  Unknown,
			Build:    Unknown,
			OS:       OSUnknown,
			Arch:     ArchUnknown,
			Language: Unknown,
		}
	case 2:
		version, err := parseVersion(parts[1])
		if err != nil {
			slog.Error("parse portal client id", "err", err)

			return nil
		}

		if strings.HasPrefix(version.Build, "alpha") ||
			strings.HasPrefix(version.Build, "beta") {
			buildParts := strings.Split(version.Build, "-")
			version.version += "-" + buildParts[0]
		}

		return &Client{
			Name:     parts[0],
			UserData: Unknown,
			Version:  version.Version(),
			Build:    version.Build,
			OS:       OSUnknown,
			Arch:     ArchUnknown,
			Language: Unknown,
		}
	default:
		slog.Error("Unknown portal version", "id", *clientName)

		return nil
	}
}
