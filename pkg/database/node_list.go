package database

import (
	"encoding/hex"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
)

func int64PrtToTimePtr(i *int64) *time.Time {
	if i == nil {
		return nil
	}

	u := time.Unix(*i, 0)
	return &u
}

type NodeListRow struct {
	nodeID            []byte
	NodeType          common.NodeType
	nodePubKey        []byte
	UpdatedAt         *time.Time
	ClientName        *string
	ClientVersion     *string
	ClientBuild       *string
	ClientOS          *string
	ClientArch        *string
	Country           *string
	HeadHashTimestamp *time.Time
	DialSuccess       bool
}

func (n NodeListRow) NodeID() string {
	return hex.EncodeToString(n.nodeID)
}

func (n NodeListRow) NodePubKey() string {
	return hex.EncodeToString(n.nodePubKey)
}

func (n NodeListRow) SinceUpdate() string {
	return since(n.UpdatedAt)
}

func (n NodeListRow) IsSynced() string {
	return isSynced(n.UpdatedAt, n.HeadHashTimestamp)
}

type NodeList struct {
	PageNumber     int
	PageSize       int
	HasNextPage    bool
	Synced         int
	Offset         int
	NetworkFilter  int64
	Query          string
	ClientName     string
	ClientUserData string

	List []NodeListRow
}
