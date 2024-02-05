package types

import "github.com/prysmaticlabs/prysm/v4/consensus-types/primitives"

//go:generate github.com/ferranbt/fastssz/sszgen --path .

type Status struct {
	ForkDigest     []byte `ssz-size:"4"`
	FinalizedRoot  []byte `ssz-size:"32"`
	FinalizedEpoch primitives.Epoch
	HeadRoot       []byte `ssz-size:"32"`
	HeadSlot       primitives.Slot
}
