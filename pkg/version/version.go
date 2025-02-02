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
	"runtime/debug"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const ourPath = "github.com/ethereum/go-ethereum" // Path to our module

// These variables are set at build-time by the linker when the build is
// done by build/ci.go.
var gitCommit, gitDate string
var versionMajor, versionMinor, versionPatch string

var title = cases.Title(language.English)

// VCSInfo represents the git repository state.
type VCSInfo struct {
	Commit string // head commit hash
	Date   string // commit time in YYYYMMDD format
	Dirty  bool
}

// VCS returns version control information of the current executable.
func VCS() (VCSInfo, bool) {
	if gitCommit != "" {
		// Use information set by the build script if present.
		return VCSInfo{Commit: gitCommit, Date: gitDate, Dirty: false}, true
	}
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		if buildInfo.Main.Path == ourPath {
			return buildInfoVCS(buildInfo)
		}
	}
	//nolint:exhaustruct
	return VCSInfo{}, false
}

// Semantic holds the textual version string for major.minor.patch.
var Semantic = fmt.Sprintf("%s.%s.%s", versionMajor, versionMinor, versionPatch)

func VersionWithCommit(gitCommit, gitDate string) string {
	vsn := Semantic
	if len(gitCommit) >= 8 {
		vsn += "-" + gitCommit[:8]
	}
	return vsn
}

// ClientName creates a software name/version identifier according to common
// conventions in the Ethereum p2p network.
func ClientName(clientIdentifier string) string {
	git, _ := VCS()
	return fmt.Sprintf("%s/v%v/%v-%v/%v",
		title.String(clientIdentifier),
		VersionWithCommit(git.Commit, git.Date),
		runtime.GOOS, runtime.GOARCH,
		runtime.Version(),
	)
}
