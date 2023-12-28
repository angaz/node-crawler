package common

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

type ForkID [4]byte

func (fid ForkID) Uint32() uint32 {
	return binary.BigEndian.Uint32(fid[:])
}

func (fid ForkID) Hex() string {
	return hex.EncodeToString(fid[:])
}

func (fid ForkID) String() string {
	return fid.Hex()
}

func Uint32PtrToForkIDPrt(i *uint32) *ForkID {
	if i == nil {
		return nil
	}

	fid := Uint32ToForkID(*i)
	return &fid
}

func Uint32ToForkID(i uint32) ForkID {
	fid := ForkID{}
	binary.BigEndian.PutUint32(fid[:], i)

	return fid
}

func (dst *ForkID) Scan(src any) error {
	if src == nil {
		return nil
	}

	switch src := src.(type) {
	case int64:
		*dst = Uint32ToForkID(uint32(src))

		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}
