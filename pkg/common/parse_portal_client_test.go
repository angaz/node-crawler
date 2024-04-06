package common

import (
	"testing"
)

func TestParsePortalClientID(t *testing.T) {
	tt := []struct {
		name   string
		ci     string
		client *Client
	}{
		{
			name: "git hash as version",
			ci:   "t 8afdf509",
			client: &Client{
				Name:     "t",
				UserData: Unknown,
				Version:  "8afdf509",
				Build:    Unknown,
				OS:       OSUnknown,
				Arch:     ArchUnknown,
				Language: Unknown,
			},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			client := ParsePortalClientID(&test.ci)

			if !clientEq(client, test.client) {
				t.Errorf("client doesn't match: got %#v want %#v", client, test.client)
			}
		})
	}
}
