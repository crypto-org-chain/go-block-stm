package block_stm

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cometbft/cometbft/crypto/secp256k1"
	"github.com/test-go/testify/require"
)

func Tx1(sender string) Tx {
	privKey := secp256k1.GenPrivKey()
	signBytes := make([]byte, 1024)
	cryptorand.Read(signBytes)
	sig, _ := privKey.Sign(signBytes)
	pubKey := privKey.PubKey()
	return func(store KVStore) error {
		// verify a signature
		pubKey.VerifySignature(signBytes, sig)

		nonceKey := []byte("nonce" + sender)
		var nonce uint64
		v, err := store.Get(nonceKey)
		if err != nil {
			return err
		}
		if v != nil {
			nonce = binary.BigEndian.Uint64(v)
		}

		var bz [8]byte
		binary.BigEndian.PutUint64(bz[:], nonce+1)
		if err := store.Set(nonceKey, bz[:]); err != nil {
			return err
		}

		v, err = store.Get(nonceKey)
		if err != nil {
			return err
		}
		if binary.BigEndian.Uint64(v) != nonce+1 {
			return fmt.Errorf("nonce not incremented: %d", binary.BigEndian.Uint64(v))
		}
		return nil
	}
}

func accountName(i int64) string {
	return "account" + strconv.FormatInt(i, 10)
}

func testBlock(size int, accounts int) []Tx {
	blk := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	for i := 0; i < size; i++ {
		account := g.Int63n(int64(accounts))
		blk[i] = Tx1(accountName(account))
	}
	return blk
}

func noConflictBlock(size int) []Tx {
	blk := make([]Tx, size)
	for i := 0; i < size; i++ {
		blk[i] = Tx1(accountName(int64(i)))
	}
	return blk
}

func worstCaseBlock(size int) []Tx {
	blk := make([]Tx, size)
	for i := 0; i < size; i++ {
		blk[i] = Tx1("account0")
	}
	return blk
}

func determisticBlock() []Tx {
	return []Tx{
		Tx1("account0"),
		Tx1("account1"),
		Tx1("account1"),
		Tx1("account1"),
		Tx1("account3"),
		Tx1("account1"),
		Tx1("account4"),
		Tx1("account5"),
		Tx1("account6"),
	}
}

func TestSTM(t *testing.T) {
	testCases := []struct {
		name      string
		blk       []Tx
		executors int
	}{
		{
			name:      "testBlock(100,80),10",
			blk:       testBlock(100, 80),
			executors: 10,
		},
		{
			name:      "testBlock(100,3),10",
			blk:       testBlock(100, 3),
			executors: 10,
		},
		{
			name:      "determisticBlock(),5",
			blk:       determisticBlock(),
			executors: 5,
		},
		{
			name:      "noConflictBlock(100),5",
			blk:       noConflictBlock(100),
			executors: 5,
		},
		{
			name:      "worstCaseBlock(100),5",
			blk:       worstCaseBlock(100),
			executors: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewMemDB()
			ExecuteBlock(storage, tc.blk, tc.executors)

			// check total nonce increased the same amount as the number of transactions
			var total uint64
			storage.Scan(func(k Key, v Value) bool {
				if !bytes.HasPrefix(k, []byte("nonce")) {
					return true
				}
				total += binary.BigEndian.Uint64(v)
				return true
			})
			require.Equal(t, uint64(len(tc.blk)), total)
		})
	}
}
