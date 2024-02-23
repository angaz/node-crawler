package types

//go:generate github.com/ferranbt/fastssz/sszgen --path .

type Status struct {
	ForkDigest     []byte `ssz-size:"4"`
	FinalizedRoot  []byte `ssz-size:"32"`
	FinalizedEpoch uint64
	HeadRoot       []byte `ssz-size:"32"`
	HeadSlot       uint64
}
