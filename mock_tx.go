package block_stm

import (
	"encoding/binary"
	"fmt"
	"strings"

	cryptorand "crypto/rand"

	"github.com/cometbft/cometbft/crypto/secp256k1"
)

// Simulated transaction logic for tests and benchmarks

// NoopTx verifies a signature and increases the nonce of the sender
func NoopTx(i int, sender string) Tx {
	verifySig := genRandomSignature()
	return func(store MultiStore) error {
		verifySig()
		return increaseNonce(i, sender, store.GetKVStore("acc"))
	}
}

func BankTransferTx(i int, sender, receiver string, amount uint64) Tx {
	base := NoopTx(i, sender)
	return func(store MultiStore) error {
		if err := base(store); err != nil {
			return err
		}

		return bankTransfer(i, sender, receiver, amount, store.GetKVStore("bank"))
	}
}

func IterateTx(i int, sender, receiver string, amount uint64) Tx {
	base := BankTransferTx(i, sender, receiver, amount)
	return func(store MultiStore) error {
		if err := base(store); err != nil {
			return err
		}

		// find a nearby account, do a bank transfer
		accStore := store.GetKVStore("acc")

		it := accStore.Iterator([]byte(sender), nil)
		defer it.Close()

		var j int
		for ; it.Valid(); it.Next() {
			j++
			if j > 5 {
				recipient := strings.TrimPrefix(string(it.Key()), "nonce")
				return bankTransfer(i, sender, recipient, amount, store.GetKVStore("bank"))
			}
		}

		return nil
	}
}

func genRandomSignature() func() {
	privKey := secp256k1.GenPrivKey()
	signBytes := make([]byte, 1024)
	if _, err := cryptorand.Read(signBytes); err != nil {
		panic(err)
	}
	sig, _ := privKey.Sign(signBytes)
	pubKey := privKey.PubKey()

	return func() {
		pubKey.VerifySignature(signBytes, sig)
	}
}

func increaseNonce(i int, sender string, store KVStore) error {
	nonceKey := []byte("nonce" + sender)
	var nonce uint64
	v := store.Get(nonceKey)
	if v != nil {
		nonce = binary.BigEndian.Uint64(v)
	}

	var bz [8]byte
	binary.BigEndian.PutUint64(bz[:], nonce+1)
	store.Set(nonceKey, bz[:])

	v = store.Get(nonceKey)
	if binary.BigEndian.Uint64(v) != nonce+1 {
		return fmt.Errorf("nonce not incremented: %d", binary.BigEndian.Uint64(v))
	}

	return nil
}

func bankTransfer(i int, sender, receiver string, amount uint64, store KVStore) error {
	senderKey := []byte("balance" + sender)
	receiverKey := []byte("balance" + receiver)

	var senderBalance, receiverBalance uint64
	v := store.Get(senderKey)
	if v != nil {
		senderBalance = binary.BigEndian.Uint64(v)
	}

	v = store.Get(receiverKey)
	if v != nil {
		receiverBalance = binary.BigEndian.Uint64(v)
	}

	if senderBalance >= amount {
		// avoid the failure
		senderBalance -= amount
	}

	receiverBalance += amount

	var bz1, bz2 [8]byte
	binary.BigEndian.PutUint64(bz1[:], senderBalance)
	store.Set(senderKey, bz1[:])

	binary.BigEndian.PutUint64(bz2[:], receiverBalance)
	store.Set(receiverKey, bz2[:])

	return nil
}
