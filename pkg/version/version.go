// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package version implements reading of build version information.
package version

import (
	"fmt"
	"runtime"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// These variables are set at build-time by the linker when the build is
// done by build/ci.go.
var gitCommit, version string

var title = cases.Title(language.English)

func VersionWithCommit() string {
	if len(gitCommit) >= 8 {
		return version + "-" + gitCommit[:8]
	}

	return version + "-" + "dev"
}

// ClientName creates a software name/version identifier according to common
// conventions in the Ethereum p2p network.
func ClientName(clientIdentifier string) string {
	return fmt.Sprintf("%s/v%v/%v-%v/%v",
		title.String(clientIdentifier),
		VersionWithCommit(),
		runtime.GOOS, runtime.GOARCH,
		runtime.Version(),
	)
}
