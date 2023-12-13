package database

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/common"
)

type NodeTableHistory struct {
	CrawledAt time.Time
	Direction common.Direction
	Error     string
}

func (h NodeTableHistory) CrawledAtLine() string {
	return since(&h.CrawledAt)
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
	ForkID         *ForkID
	NextForkID     *uint64
	HeadHash       *[]byte
	HeadHashTime   *time.Time
	IP             *string
	ConnectionType *string
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

	name := Unknown

	if n.networkID != nil {
		forkData, ok := Forks[*n.networkID]
		if ok {
			forkName, ok := forkData.Hash[n.ForkID.Uint32()]
			if ok {
				name = forkName
			}
		}
	}

	return fmt.Sprintf("%s (%s)", name, n.ForkID.Hex())
}

func (n NodeTable) NextForkIDStr() string {
	if n.NextForkID == nil {
		return ""
	}

	name := Unknown

	if n.networkID != nil {
		forkData, ok := Forks[*n.networkID]
		if ok {
			forkName, ok := forkData.BlockTime[*n.NextForkID]
			if ok {
				name = forkName
			}
		}
	}

	return fmt.Sprintf("%s (%d)", name, *n.NextForkID)
}

func isReadyForCancun(networkID *int64, forkID *uint32, nextForkID *uint64) int {
	if networkID == nil || nextForkID == nil {
		return -1
	}

	chain, ok := Chains[*networkID]
	if !ok {
		return -1
	}

	if nextForkID == chain.CancunTime {
		return 1
	}

	if forkID == nil {
		return -1
	}

	forks, ok := Forks[*networkID]
	if !ok {
		return -1
	}

	if forks.Hash[*forkID] == ForkNameCancun {
		return 1
	}

	return 0
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

	return fmt.Sprintf("%s (%d)", NetworkName(n.networkID), *n.networkID)
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

var Unknown = "Unknown"

func isSynced(updatedAt *time.Time, headHash *time.Time) string {
	if updatedAt == nil || headHash == nil {
		return Unknown
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
		since(&n.firstFound),
		n.lastFound.UTC().Format(DateFormat),
	)
}

func (n NodeTable) LastFound() string {
	return fmt.Sprintf(
		"%s (%s)",
		since(&n.lastFound),
		n.lastFound.UTC().Format(DateFormat),
	)
}

func (n NodeTable) UpdatedAt() string {
	if n.updatedAt == nil {
		return ""
	}

	return fmt.Sprintf(
		"%s (%s)",
		since(n.updatedAt),
		n.updatedAt.UTC().Format(DateFormat),
	)
}

func (n NodeTable) NextCrawl() string {
	if n.nextCrawl == nil {
		return "Never"
	}

	return fmt.Sprintf("%s (%s)", since(n.nextCrawl), n.nextCrawl.UTC().Format(DateFormat))
}

func NetworkName(networkID *int64) string {
	if networkID == nil {
		return Unknown
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
	case params.GoerliChainConfig.ChainID.Int64():
		return "Goerli"
	case 56:
		return "BNB Smart Chain Mainnet"
	default:
		return Unknown
	}
}

func (n NodeTable) NetworkName() string {
	return NetworkName(n.networkID)
}
