package common

import (
	"testing"
)

func clientEq(a, b *Client) bool {
	if a == nil && b == nil {
		return true
	}

	if (a == nil) != (b == nil) {
		return false
	}

	return (a.Name == b.Name &&
		a.UserData == b.UserData &&
		a.Version == b.Version &&
		a.Build == b.Build &&
		a.OS == b.OS &&
		a.Arch == b.Arch &&
		a.Language == b.Language)
}

func TestParseClientID(t *testing.T) {
	tt := []struct {
		name   string
		ci     string
		client *Client
	}{
		{
			name: "numbus slash 5 parts",
			ci:   "nimbus-eth1/v0.1.0-94198bcf/linux-amd64/Nim-2.0.10/nimvm",
			client: &Client{
				Name:     "nimbus-eth1",
				UserData: Unknown,
				Version:  "v0.1.0",
				Build:    "94198bcf",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "nim",
			},
		},
		{
			name: "nimbus 6 parts",
			ci:   "nimbus-eth1 v0.1.0 [linux: amd64, nimvm, c0d52b]",
			client: &Client{
				Name:     "nimbus-eth1",
				UserData: Unknown,
				Version:  "v0.1.0",
				Build:    "c0d52b",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "nim",
			},
		},
		{
			name: "nimbus 7 parts",
			ci:   "nimbus-eth1 v0.1.0 [linux: amd64, rocksdb, nimvm, c0d52b]",
			client: &Client{
				Name:     "nimbus-eth1",
				UserData: Unknown,
				Version:  "v0.1.0",
				Build:    "c0d52b",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "nim",
			},
		},
		{
			name: "ethereumjs-p2p",
			ci:   "ethereumjs-devp2p/darwin-arm64/nodejs",
			client: &Client{
				Name:     "ethereumjs-devp2p",
				UserData: Unknown,
				Version:  Unknown,
				Build:    Unknown,
				OS:       OSMacOS,
				Arch:     ArchARM64,
				Language: "nodejs",
			},
		},
		{
			name: "reth",
			ci:   "reth/v0.1.0-alpha.13-453f699/aarch64-unknown-linux-gnu",
			client: &Client{
				Name:     "reth",
				UserData: Unknown,
				Version:  "v0.1.0-alpha.13",
				Build:    "alpha.13-453f699",
				OS:       OSLinux,
				Arch:     ArchARM64,
				Language: "rust",
			},
		},
		{
			name: "len 3 geth version",
			ci:   "Geth/v1.1.20/linux",
			client: &Client{
				Name:     "geth",
				UserData: Unknown,
				Version:  "v1.1.20",
				Build:    Unknown,
				OS:       OSLinux,
				Arch:     ArchUnknown,
				Language: "go",
			},
		},
		{
			name: "len 3 geth language",
			ci:   "Geth/linux-amd64/go1.20.3",
			client: &Client{
				Name:     "geth",
				UserData: Unknown,
				Version:  Unknown,
				Build:    Unknown,
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "go1.20.3",
			},
		},
		{
			name: "len 3 unknown",
			ci:   "/linux-amd64/go1.21.4",
			client: &Client{
				Name:     Unknown,
				UserData: Unknown,
				Version:  Unknown,
				Build:    Unknown,
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "go1.21.4",
			},
		},
		{
			name: "geth with 2 user data parts",
			ci:   "geth/nodes/node3/v1.13.1-stable-e9d041e7/linux-amd64/go1.21.5",
			client: &Client{
				Name:     "geth",
				UserData: "nodes/node3",
				Version:  "v1.13.1",
				Build:    "stable-e9d041e7",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "go1.21.5",
			},
		},
		{
			name: "besu version with 3 parts",
			ci:   "besu/v24.3-develop-b269020a3e/linux-x86_64/openjdk-java-21",
			client: &Client{
				Name:     "besu",
				UserData: Unknown,
				Version:  "v24.3",
				Build:    "develop-b269020a3e",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "openjdk-java-21",
			},
		},
		{
			name: "reth with user data",
			ci:   "reth/shemnon/v0.2.0-beta.4-c04dbe6e9/x86_64-apple-darwin",
			client: &Client{
				Name:     "reth",
				UserData: "shemnon",
				Version:  "v0.2.0-beta.4",
				Build:    "beta.4-c04dbe6e9",
				OS:       OSMacOS,
				Arch:     ArchAMD64,
				Language: "rust",
			},
		},
		{
			name: "geth vuntagged",
			ci:   "Geth/vuntagged-5a9ac684-20240520/linux-amd64/go1.22.2",
			client: &Client{
				Name:     "geth",
				UserData: Unknown,
				Version:  Unknown,
				Build:    "vuntagged-5a9ac684-20240520",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: "go1.22.2",
			},
		},
		{
			name: "abc/xyz",
			ci:   "abc/xyz/v0.1.0-800ca6d8/x86_64-unknown-linux-gnu",
			client: &Client{
				Name:     "abc",
				UserData: "xyz",
				Version:  "v0.1.0",
				Build:    "abc/xyz/v0.1.0-800ca6d8/x86_64-unknown-linux-gnu",
				OS:       OSLinux,
				Arch:     ArchAMD64,
				Language: Unknown,
			},
		},
		{
			name: "EthereumJS",
			ci:   "EthereumJS/undefined/linux/node20.18.1",
			client: &Client{
				Name:     "ethereumjs",
				UserData: Unknown,
				Version:  Unknown,
				Build:    Unknown,
				OS:       OSLinux,
				Arch:     ArchUnknown,
				Language: "javascript",
			},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			client := ParseClientID(&test.ci)

			if !clientEq(client, test.client) {
				t.Errorf("client doesn't match: got %#v want %#v", client, test.client)
			}
		})
	}
}
