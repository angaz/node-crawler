package p2p

import (
	"context"
	"reflect"

	"log/slog"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/jackc/pgx/v5"
)

func (conn *Conn) GetClientInfo(
	ctx context.Context,
	tx pgx.Tx,
	node *enode.Node,
	direction common.Direction,
	getMissingBlock func(context.Context, pgx.Tx, uint64) (*ethcommon.Hash, error),
) common.NodeJSON {
	err := conn.writeHello()
	if err != nil {
		known, errStr := TranslateError(err)
		if !known {
			slog.Info("write hello failed", "err", err)
		}

		//nolint:exhaustruct  // Missing values because of error.
		return common.NodeJSON{
			N:         node,
			EthNode:   true,
			Direction: direction,
			Error:     errStr,
		}
	}

	var disconnect *Disconnect = nil
	var readError *Error = nil

	//nolint:exhaustruct  // Empty struct will be filled in.
	nodeJSON := common.NodeJSON{
		N:         node,
		EthNode:   true,
		Info:      &common.ClientInfo{},
		Direction: direction,
	}

	gotStatus := false
	gotBlocks := 0

	getBlocks := 1

	for {
		switch msg := conn.Read().(type) {
		case *Ping:
			_ = conn.Write(Pong{})
		case *Pong:
			continue
		case *Hello:
			if msg.Version >= 5 {
				conn.SetSnappy(true)
			}

			nodeJSON.Info.Capabilities = msg.Caps
			nodeJSON.Info.RLPxVersion = msg.Version
			nodeJSON.Info.ClientIdentifier = msg.Name

			conn.NegotiateEthProtocol(nodeJSON.Info.Capabilities)

			if conn.NegotiatedProtoVersion == 0 {
				nodeJSON.Error = "not eth node"
				nodeJSON.EthNode = false
				_ = conn.Write(Disconnect{Reason: p2p.DiscUselessPeer})

				goto loopExit
			}
		case *Status:
			gotStatus = true

			nodeJSON.Info.ForkID = msg.ForkID
			nodeJSON.Info.HeadHash = msg.Head
			nodeJSON.Info.NetworkID = msg.NetworkID

			_ = conn.Write(Status{
				ProtocolVersion: msg.ProtocolVersion,
				NetworkID:       msg.NetworkID,
				TD:              msg.TD,
				Head:            msg.Genesis,
				Genesis:         msg.Genesis,
				ForkID:          msg.ForkID,
			})

			getBlock, err := getMissingBlock(ctx, tx, msg.NetworkID)
			if err != nil {
				slog.Error("could not get missing block", "err", err)
			}

			if getBlock != nil {
				getBlocks = 2

				_ = conn.Write(GetBlockHeaders{
					RequestId: 69419,
					GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
						//nolint:exhaustruct  // Only one field is needed.
						Origin:  eth.HashOrNumber{Hash: *getBlock},
						Amount:  1,
						Skip:    0,
						Reverse: false,
					},
				})
			}

			_ = conn.Write(GetBlockHeaders{
				RequestId: 69420, // Just a random number.
				GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
					//nolint:exhaustruct  // Only one field is needed.
					Origin:  eth.HashOrNumber{Hash: msg.Head},
					Amount:  1,
					Skip:    0,
					Reverse: false,
				},
			})

		case *GetBlockBodies:
			_ = conn.Write(BlockBodies{
				RequestId:           msg.RequestId,
				BlockBodiesResponse: nil,
			})
		case *GetBlockHeaders:
			_ = conn.Write(BlockHeaders{
				RequestId:           msg.RequestId,
				BlockHeadersRequest: nil,
			})
		case *BlockHeaders:
			gotBlocks += 1

			nodeJSON.BlockHeaders = append(
				nodeJSON.BlockHeaders,
				msg.BlockHeadersRequest...,
			)

			// Only exit once we have all the number of blocks we asked for.
			if gotBlocks == getBlocks {
				_ = conn.Write(Disconnect{Reason: p2p.DiscTooManyPeers})

				goto loopExit
			}
		case *Disconnect:
			disconnect = msg

			goto loopExit
		case *Error:
			readError = msg

			goto loopExit

		// NOOP conditions
		case *GetPooledTransactions:
		case *NewBlock:
		case *NewBlockHashes:
		case *NewPooledTransactionHashes:
		case *Transactions:

		default:
			slog.Info("message type not handled", "type", reflect.TypeOf(msg).String())
		}
	}

loopExit:

	_ = conn.Close()

	if !nodeJSON.EthNode || gotStatus {
		return nodeJSON
	}

	if disconnect != nil {
		nodeJSON.Error = disconnect.Reason.String()
	} else if readError != nil {
		known, errStr := TranslateError(readError)
		if !known {
			slog.Info("message read error", "err", readError)
		}

		nodeJSON.Error = errStr
	}

	return nodeJSON
}
