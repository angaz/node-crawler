package common

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/ethereum/go-ethereum/log"
)

var (
	ErrOSArchEmpty   = errors.New("os/arch empty")
	ErrOSArchUnknown = errors.New("os/arch unknown")
	ErrNotGethOrReth = errors.New("not geth or reth")
	ErrUnknownClient = errors.New("unknown client")
	ErrVersionEmpty  = errors.New("version empty")
	Unknown          = "Unknown"
)

type Client struct {
	Name     string
	UserData string
	Version  string
	Build    string
	OS       OS
	Arch     Arch
	Language string
}

func strOrUnknown(s *string) string {
	if s == nil {
		return Unknown
	}

	return *s
}

func osOrUnknown(s *int32) OS {
	if s == nil {
		return OSUnknown
	}

	return OS(*s)
}

func archOrUnknown(s *int32) Arch {
	if s == nil {
		return ArchUnknown
	}

	return Arch(*s)
}

func newClient(
	name *string,
	userData *string,
	version *string,
	build *string,
	os OS,
	arch Arch,
	language *string,
) Client {
	return Client{
		Name:     strOrUnknown(name),
		UserData: strOrUnknown(userData),
		Version:  strOrUnknown(version),
		Build:    strOrUnknown(build),
		OS:       os,
		Arch:     arch,
		Language: strOrUnknown(language),
	}
}

func (c *Client) Deref() Client {
	if c == nil {
		return Client{
			Name:     Unknown,
			UserData: Unknown,
			Version:  Unknown,
			Build:    Unknown,
			OS:       OSUnknown,
			Arch:     ArchUnknown,
			Language: Unknown,
		}
	}

	return *c
}

type OS int32

const (
	OSUnknown OS = iota
	OSAndroid
	OSFreeBSD
	OSLinux
	OSMacOS
	OSWindows
)

var OSStrings = []string{
	Unknown,
	"Android",
	"FreeBSD",
	"Linux",
	"MacOS",
	"Windows",
}

func (os OS) String() string {
	return OSStrings[os]
}

type Arch int32

const (
	ArchUnknown Arch = iota
	ArchAMD64
	ArchARM64
	ArchI386
	ArchS390x
)

var ArchStrings = []string{
	Unknown,
	"amd64",
	"arm64",
	"i386",
	"IBM System/390",
}

func (arch Arch) String() string {
	return ArchStrings[arch]
}

func OSIndex(os string) OS {
	idx := slices.Index(OSStrings, os)
	if idx == -1 {
		panic("unknown os")
		// return OSUnknown
	}

	return OS(idx)
}

func ArchIndex(arch string) Arch {
	idx := slices.Index(ArchStrings, arch)
	if idx == -1 {
		panic("unknown arch")
		// return ArchUnknown
	}

	return Arch(idx)
}

func parseOSArch(osStr string) (OS, Arch, error) {
	if osStr == "" {
		return OSUnknown, ArchUnknown, ErrOSArchEmpty
	}

	parts := strings.FieldsFunc(osStr, func(c rune) bool {
		return c == '-'
	})

	os := OSUnknown
	arch := ArchUnknown

	for _, part := range parts {
		switch part {
		case "musl", "unknown", "gnu":
			// NOOP

		// Operating Systems
		case "android":
			os = OSAndroid
		case "freebsd":
			os = OSFreeBSD
		case "linux":
			os = OSLinux
		case "darwin", "osx", "macos", "apple":
			os = OSMacOS
		case "windows", "win32":
			os = OSWindows

		// Archetectures
		case "amd64", "x64", "x86_64":
			arch = ArchAMD64
		case "arm64", "aarch_64", "aarch64", "arm":
			arch = ArchARM64
		case "386":
			arch = ArchI386
		case "s390x":
			arch = ArchS390x

		default:
			// NOOP
		}
	}

	if os == OSUnknown && arch == ArchUnknown {
		return os, arch, ErrOSArchUnknown
	}

	return os, arch, nil
}

type Version struct {
	version    string
	versionNum []uint64
	Build      string
}

var ErrVersion = Version{
	version:    Unknown,
	versionNum: []uint64{},
	Build:      Unknown,
}

func (a Version) Cmp(b Version) int {
	for i, val := range a.versionNum {
		res := cmp.Compare(val, b.versionNum[i])
		if res == 0 {
			continue
		}

		return res
	}

	return 0
}

func (v Version) String() string {
	if v.Build != "" {
		return v.Version() + "-" + v.Build
	}

	return v.Version()
}

func (v Version) Version() string {
	return "v" + v.version
}

func parseVersion(s string) (Version, error) {
	if s == "" {
		return ErrVersion, ErrVersionEmpty
	}

	if s == "vnull" || s == "vunspecified" || s == "custom" {
		return Version{
			version:    "null",
			versionNum: []uint64{0},
			Build:      "",
		}, nil
	}

	s = strings.TrimLeft(s, "vx")

	var version, build string

	idx := strings.IndexAny(s, "-+")

	if idx == -1 {
		version = s
	} else {
		version = s[:idx]
		build = s[idx+1:]
	}

	versionParts := strings.Split(version, ".")

	if len(versionParts) != 3 {
		return ErrVersion, fmt.Errorf("version not 3 parts: %s", s)
	}

	versionInts := make([]uint64, 0, 3)
	for _, part := range versionParts {
		i, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return ErrVersion, fmt.Errorf("version part not int: %s: %w", s, err)
		}

		versionInts = append(versionInts, i)
	}

	return Version{
		version:    version,
		versionNum: versionInts,
		Build:      build,
	}, nil
}

func isVersion(version string) bool {
	_, err := parseVersion(version)

	return err != nil
}

// Nimbus has a special format for the client name.
// nimbus-eth1 v0.1.0 [linux: amd64, rocksdb, nimvm, 750a07]
func handleNimbus(name string) (*Client, error) {
	newClientName := make([]rune, 0, len(name))
	for _, c := range name {
		switch c {
		case '[', ']', ':', ',':
			// NOOP
		default:
			newClientName = append(newClientName, c)
		}
	}

	parts := strings.Split(string(newClientName), " ")

	if len(parts) == 1 {
		return &Client{
			Name:     parts[0],
			UserData: Unknown,
			Version:  Unknown,
			Build:    Unknown,
			OS:       OSUnknown,
			Arch:     ArchUnknown,
			Language: Unknown,
		}, nil
	}

	if len(parts) != 7 {
		return nil, fmt.Errorf("nimbus-eth1 not valid, name: %s", name)
	}

	version, err := parseVersion(parts[1])
	if err != nil {
		return nil, fmt.Errorf("parse version failed: %w", err)
	}

	os, arch, err := parseOSArch(parts[2])
	if err != nil {
		log.Error("os/arch parse error", "err", err)
	}

	return &Client{
		Name:     parts[0],
		UserData: Unknown,
		Version:  version.Version(),
		Build:    parts[6],
		OS:       os,
		Arch:     arch,
		Language: "nim",
	}, nil
}

func handleLen1(parts []string) (*Client, error) {
	return &Client{
		Name:     parts[0],
		UserData: Unknown,
		Version:  Unknown,
		Build:    Unknown,
		OS:       OSUnknown,
		Arch:     ArchUnknown,
		Language: Unknown,
	}, nil
}

func handleLen2(parts []string) (*Client, error) {
	version, err := parseVersion(parts[1])
	if err != nil {
		return nil, fmt.Errorf("version parse failed: %w", err)
	}

	return &Client{
		Name:     parts[0],
		UserData: Unknown,
		Version:  version.Version(),
		Build:    version.Build,
		OS:       OSUnknown,
		Arch:     ArchUnknown,
		Language: Unknown,
	}, nil
}

func handleLen3(parts []string) (*Client, error) {
	name := parts[0]

	if name == "reth" {
		version, err := parseVersion(parts[1])
		if err != nil {
			return nil, fmt.Errorf("parsing version failed: %w", err)
		}

		os, arch, _ := parseOSArch(parts[2])

		return &Client{
			Name:     name,
			UserData: Unknown,
			Version:  version.Version(),
			Build:    version.Build,
			OS:       os,
			Arch:     arch,
			Language: "rust",
		}, nil
	}

	if name == "geth" || name == "bor" {
		version, err := parseVersion(parts[1])
		if err != nil {
			os, arch, _ := parseOSArch(parts[1])

			return &Client{
				Name:     name,
				UserData: Unknown,
				Version:  Unknown,
				Build:    Unknown,
				OS:       os,
				Arch:     arch,
				Language: parts[2],
			}, nil
		}

		os, arch, _ := parseOSArch(parts[2])

		return &Client{
			Name:     name,
			UserData: Unknown,
			Version:  version.Version(),
			Build:    version.Build,
			OS:       os,
			Arch:     arch,
			Language: "go",
		}, nil
	}

	return nil, ErrNotGethOrReth
}

func handleLen4(parts []string) (*Client, error) {
	os, arch, _ := parseOSArch(parts[2])

	version, err := parseVersion(parts[1])
	if err != nil {
		return nil, fmt.Errorf("parse version failed: %w", err)
	}

	return &Client{
		Name:     parts[0],
		UserData: Unknown,
		Version:  version.Version(),
		Build:    version.Build,
		OS:       os,
		Arch:     arch,
		Language: parts[3],
	}, nil
}

func handleLen5(parts []string) (*Client, error) {
	var versionStr, lang string
	os := OSUnknown
	arch := ArchUnknown
	userData := Unknown

	// handle geth/v1.2.11-e3acd735-20231031/linux-amd64/go1.20.5/{d+}
	if strings.TrimFunc(parts[4], unicode.IsDigit) == "" {
		versionStr = parts[1]
		os, arch, _ = parseOSArch(parts[2])
		lang = parts[3]
	} else {
		userData = parts[1]
		versionStr = parts[2]
		os, arch, _ = parseOSArch(parts[3])
		lang = parts[4]
	}

	version, err := parseVersion(versionStr)
	if err != nil {
		return nil, fmt.Errorf("parse version failed: %w", err)
	}

	return &Client{
		Name:     parts[0],
		UserData: userData,
		Version:  version.Version(),
		Build:    version.Build,
		OS:       os,
		Arch:     arch,
		Language: lang,
	}, nil
}

func handleLen6(parts []string) (*Client, error) {
	if parts[0] != "q-client" {
		return nil, ErrUnknownClient
	}

	os, arch, _ := parseOSArch(parts[4])

	version, err := parseVersion(parts[1])
	if err != nil {
		return nil, fmt.Errorf("parse version failed: %w", err)
	}

	return &Client{
		Name:     parts[0],
		UserData: Unknown,
		Version:  version.Version(),
		Build:    version.Build,
		OS:       os,
		Arch:     arch,
		Language: parts[5],
	}, nil
}

func handleLen7(parts []string) (*Client, error) {
	os, arch, _ := parseOSArch(parts[5])

	version, err := parseVersion(parts[4])
	if err != nil {
		return nil, fmt.Errorf("parse version failed: %w", err)
	}

	userData := strings.Join([]string{
		parts[1],
		parts[2],
		parts[3],
	}, "/")

	return &Client{
		Name:     parts[0],
		UserData: userData,
		Version:  version.Version(),
		Build:    version.Build,
		OS:       os,
		Arch:     arch,
		Language: parts[6],
	}, nil
}

var funcs = []func([]string) (*Client, error){
	func(_ []string) (*Client, error) { panic("not implemented") },
	handleLen1,
	handleLen2,
	handleLen3,
	handleLen4,
	handleLen5,
	handleLen6,
	handleLen7,
}

func ParseClientID(clientName *string) *Client {
	if clientName == nil {
		return nil
	}

	name := strings.ToLower(*clientName)

	if name == "" {
		return nil
	}

	if name == "server" {
		return nil
	}

	if strings.HasPrefix(name, "nimbus-eth1") {
		client, err := handleNimbus(name)
		if err != nil {
			log.Error("parse nimbus failed", "err", err, "name", name)
		}

		return client
	}

	parts := strings.Split(name, "/")

	nParts := len(parts)

	if nParts == 0 {
		log.Error("parts is 0")

		return nil
	}

	if nParts >= len(funcs) {
		log.Error("Too many parts", "name", name)

		return nil
	}

	if parts[0] == "" {
		return nil
	}

	client, err := funcs[nParts](parts)
	if err != nil {
		log.Error("error parsing client", "err", err, "name", name)

		return nil
	}

	return client
}
