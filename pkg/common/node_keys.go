package common

import (
	"bufio"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"

	"github.com/ethereum/go-ethereum/crypto"
)

type keypair struct {
	privEcdsa *ecdsa.PrivateKey
	priv      []byte
	pub       []byte
}

func genPrefix(numKeys int) (int, byte) {
	nBits := int(math.Log2(float64(numKeys)))

	var out byte

	for i := 0; i <= nBits; i++ {
		out |= 1 << (8 - i)
	}

	return nBits, out
}

// Generate a list of private keys whose public keys are distributed by prefix.
// Only powers of 2 can be generated. If numKeys is not a power of 2, the
// previous power of 2 will be generated.
func WriteNodeKeys(numKeys int, filename string) ([]*ecdsa.PrivateKey, error) {
	nBits, prefix := genPrefix(numKeys)

	found := 0
	keys := make([]*keypair, numKeys)

	for found != numKeys {
		privkey, err := crypto.GenerateKey()
		if err != nil {
			return nil, fmt.Errorf("generate key: %w", err)
		}

		pubkeyBytes := crypto.FromECDSAPub(&privkey.PublicKey)[1:]
		firstByte := pubkeyBytes[0]

		idx := slices.IndexFunc(keys, func(pair *keypair) bool {
			if pair == nil {
				return false
			}

			return (pair.pub[0] & prefix) == (firstByte & prefix)
		})

		// Not found in array, add the value
		if idx == -1 {
			keys[(firstByte&prefix)>>(8-nBits)] = &keypair{
				privEcdsa: privkey,
				priv:      crypto.FromECDSA(privkey),
				pub:       pubkeyBytes,
			}

			found++
		}
	}

	var keysFile io.Writer

	if filename == "-" {
		keysFile = os.Stdout
	} else {
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open file: %w", err)
		}
		defer file.Close()

		keysFile = file
	}

	_, err := fmt.Fprintln(keysFile, "# Private Key                                                     # Public Key")
	if err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	outKeys := make([]*ecdsa.PrivateKey, 0, numKeys)

	for _, pair := range keys {
		_, err := fmt.Fprintf(keysFile, "%x  # %x\n", pair.priv, pair.pub)
		if err != nil {
			return nil, fmt.Errorf("write pair: %w", err)
		}

		outKeys = append(outKeys, pair.privEcdsa)
	}

	return outKeys, nil
}

func ReadNodeKeys(filename string) ([]*ecdsa.PrivateKey, error) {
	keys := make([]*ecdsa.PrivateKey, 0, 16)

	keysFile, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer keysFile.Close()

	reader := bufio.NewReader(keysFile)

	for i := 0; true; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("read line: %w", err)
		}

		// Commented out line
		if line[0] == '#' {
			continue
		}

		if len(line) < 64 {
			return nil, fmt.Errorf("parse line %d: line too short", i)
		}

		privkey, err := crypto.HexToECDSA(line[:64])
		if err != nil {
			return nil, fmt.Errorf("parse line: %d: load key: %w", i, err)
		}

		keys = append(keys, privkey)
	}

	return keys, nil
}
