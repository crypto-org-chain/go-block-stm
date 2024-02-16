package block_stm

import (
	"encoding/binary"
	"fmt"

	cryptorand "crypto/rand"

	"github.com/cometbft/cometbft/crypto/secp256k1"
)

// Simulated transaction logic for tests and benchmarks

// NoopTx verifies a signature and increases the nonce of the sender
func NoopTx(sender string) Tx {
	verifySig := genRandomSignature()
	return func(store KVStore) error {
		verifySig()
		return increaseNonce(sender, store)
	}
}

func BankTransferTx(sender, receiver string, amount uint64) Tx {
	verifySig := genRandomSignature()
	return func(store KVStore) error {
		verifySig()
		if err := increaseNonce(sender, store); err != nil {
			return err
		}

		return bankTransfer(sender, receiver, amount, store)
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

func increaseNonce(sender string, store KVStore) error {
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

func bankTransfer(sender, receiver string, amount uint64, store KVStore) error {
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

	var bz [8]byte
	binary.BigEndian.PutUint64(bz[:], senderBalance)
	store.Set(senderKey, bz[:])

	binary.BigEndian.PutUint64(bz[:], receiverBalance)
	store.Set(receiverKey, bz[:])

	return nil
}
