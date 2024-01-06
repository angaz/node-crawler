package common

import (
	"testing"
)

func clientEq(a, b *Client) bool {
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
			name: "nimbus",
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
