package database

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/common"
)

type NodeTableHistory struct {
	CrawledAt time.Time
	Direction common.Direction
	Error     *string
}

func (h NodeTableHistory) CrawledAtLine() string {
	return common.Since(&h.CrawledAt)
}

type NodeTable struct {
	nodeID         []byte
	NodeType       common.NodeType
	nodePubKey     []byte
	firstFound     time.Time
	lastFound      time.Time
	updatedAt      *time.Time
	NodeRecord     *enr.Record
	ClientID       *string
	ClientName     *string
	ClientUserData *string
	ClientVersion  *string
	ClientBuild    *string
	ClientOS       *string
	ClientArch     *string
	ClientLanguage *string
	RlpxVersion    *int64
	Capabilities   *string
	networkID      *int64
	networkName    *string
	ForkID         *common.ForkID
	ForkName       *string
	NextForkID     *uint64
	NextForkName   *string
	HeadHash       *[]byte
	HeadHashTime   *time.Time
	IP             net.IP
	Country        *string
	City           *string
	Latitude       *float64
	Longitude      *float64
	nextCrawl      *time.Time
	DialSuccess    bool

	HistoryAccept []NodeTableHistory
	HistoryDial   []NodeTableHistory
}

func (n NodeTable) RLPXVersion() string {
	if n.RlpxVersion == nil {
		return ""
	}

	return strconv.FormatInt(*n.RlpxVersion, 10)
}

func (n NodeTable) ForkIDStr() string {
	if n.ForkID == nil {
		return ""
	}

	name := common.Unknown

	if n.ForkName != nil {
		name = *n.ForkName
	}

	return fmt.Sprintf("%s (%s)", name, n.ForkID.Hex())
}

func (n NodeTable) NextForkIDStr() string {
	if n.NextForkID == nil || *n.NextForkID == 0 {
		return ""
	}

	name := common.Unknown

	if n.NextForkName != nil {
		name = *n.NextForkName
	}

	return fmt.Sprintf("%s (%d)", name, *n.NextForkID)
}

func StringOrEmpty(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}

func IntOrEmpty[T int | int64](v *T) string {
	if v == nil {
		return ""
	}

	return strconv.FormatInt(int64(*v), 10)
}

func (n NodeTable) NodeID() string {
	return hex.EncodeToString(n.nodeID)
}

func (n NodeTable) NodePubKey() string {
	return hex.EncodeToString(n.nodePubKey)
}

func (n NodeTable) NetworkID() string {
	if n.networkID == nil {
		return ""
	}

	if n.networkName == nil {
		return strconv.FormatInt(*n.networkID, 10)
	}

	return fmt.Sprintf("%s (%d)", *n.networkName, *n.networkID)
}

func (n NodeTable) YOffsetPercent() int {
	if n.Latitude == nil {
		return 50
	}

	return 100 - int((*n.Latitude+90)/180*100)
}

func (n NodeTable) XOffsetPercent() int {
	if n.Longitude == nil {
		return 50
	}

	return int((*n.Longitude + 180) / 360 * 100)
}

var DateFormat = "2006-01-02 15:04:05 MST"

func (n NodeTable) HeadHashLine() string {
	if n.HeadHash == nil {
		return ""
	}

	if n.HeadHashTime == nil {
		return hex.EncodeToString(*n.HeadHash)
	}

	return fmt.Sprintf(
		"%s (%s)",
		hex.EncodeToString(*n.HeadHash),
		n.HeadHashTime.UTC().Format(DateFormat),
	)
}

func isSynced(updatedAt *time.Time, headHash *time.Time) string {
	if updatedAt == nil || headHash == nil {
		return common.Unknown
	}

	// If head hash is within one minute of the crawl time,
	// we can consider the node in sync
	if updatedAt.Sub(*headHash).Abs() < time.Minute {
		return "Yes"
	}

	return "No"
}

func (n NodeTable) IsSynced() string {
	return isSynced(n.updatedAt, n.HeadHashTime)
}

func (n NodeTable) FirstFound() string {
	return fmt.Sprintf(
		"%s (%s)",
		common.Since(&n.firstFound),
		n.lastFound.UTC().Format(DateFormat),
	)
}

func (n NodeTable) LastFound() string {
	return fmt.Sprintf(
		"%s (%s)",
		common.Since(&n.lastFound),
		n.lastFound.UTC().Format(DateFormat),
	)
}

func (n NodeTable) UpdatedAt() string {
	if n.updatedAt == nil {
		return ""
	}

	return fmt.Sprintf(
		"%s (%s)",
		common.Since(n.updatedAt),
		n.updatedAt.UTC().Format(DateFormat),
	)
}

func (n NodeTable) NextCrawl() string {
	if n.nextCrawl == nil {
		return "Never"
	}

	return fmt.Sprintf(
		"%s (%s)",
		common.Since(n.nextCrawl),
		n.nextCrawl.UTC().Format(DateFormat),
	)
}

func NetworkName(networkID *int64) string {
	if networkID == nil {
		return common.Unknown
	}

	switch *networkID {
	case -1:
		return "All"
	case params.MainnetChainConfig.ChainID.Int64():
		return "Mainnet"
	case params.HoleskyChainConfig.ChainID.Int64():
		return "Holešky"
	case params.SepoliaChainConfig.ChainID.Int64():
		return "Sepolia"
	case 56:
		return "BNB Smart Chain Mainnet"
	default:
		return common.Unknown
	}
}

func (n NodeTable) NetworkName() string {
	return NetworkName(n.networkID)
}
