package secrets

import (
	"bytes"
	"testing"
)

func TestBox(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	box, err := NewBox(key)
	if err != nil {
		t.Fatalf("failed to create box: %v", err)
	}

	plaintext := []byte("hello world")
	ciphertext, err := box.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if bytes.Equal(plaintext, ciphertext) {
		t.Errorf("ciphertext is same as plaintext")
	}

	decrypted, err := box.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("decrypted text doesn't match plaintext")
	}
}
