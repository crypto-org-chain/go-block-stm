package block_stm

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"

	"github.com/test-go/testify/require"
)

func Tx1(sender string) Tx {
	return func(store KVStore) error {
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
		return store.Set(nonceKey, bz[:])
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

func TestSTM(t *testing.T) {
	blockSize := 100
	accounts := 10
	blk := testBlock(blockSize, accounts)
	storage := NewMemDB()
	ExecuteBlock(storage, blk, 1)

	var total uint64
	for i := 0; i < accounts; i++ {
		nonceKey := []byte("nonce" + accountName(int64(i)))
		v, err := storage.Get(nonceKey)
		require.NoError(t, err)
		var nonce uint64
		if v != nil {
			nonce = binary.BigEndian.Uint64(v)
		}
		total += nonce
	}
	require.Equal(t, uint64(blockSize), total)
}
