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

func TestNewBox_InvalidKeySize(t *testing.T) {
	tests := []struct {
		name    string
		keySize int
	}{
		{"too short", 16},
		{"too long", 64},
		{"empty", 0},
		{"one short", 31},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := make([]byte, tt.keySize)
			box, err := NewBox(key)
			if box != nil {
				t.Errorf("expected nil box for key size %d", tt.keySize)
			}
			if err == nil {
				t.Errorf("expected error for key size %d", tt.keySize)
			}
		})
	}
}

func TestDecrypt_TooShort(t *testing.T) {
	key := make([]byte, 32)
	box, err := NewBox(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = box.Decrypt([]byte("short"))
	if err == nil {
		t.Error("expected error for short ciphertext")
	}
}

func TestDecrypt_NilInput(t *testing.T) {
	key := make([]byte, 32)
	box, err := NewBox(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = box.Decrypt(nil)
	if err == nil {
		t.Error("expected error for nil ciphertext")
	}
}

func TestDecrypt_CorruptedData(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	box, err := NewBox(key)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("secret data")
	ciphertext, err := box.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	corrupted := make([]byte, len(ciphertext))
	copy(corrupted, ciphertext)
	corrupted[12] ^= 0xFF

	_, err = box.Decrypt(corrupted)
	if err == nil {
		t.Error("expected error for corrupted ciphertext")
	}
}

func TestEncrypt_EmptyPlaintext(t *testing.T) {
	key := make([]byte, 32)
	box, err := NewBox(key)
	if err != nil {
		t.Fatal(err)
	}

	ciphertext, err := box.Encrypt([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := box.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal([]byte{}, decrypted) {
		t.Error("decrypted empty plaintext does not match")
	}
}

func TestDecrypt_DifferentKey(t *testing.T) {
	key1 := make([]byte, 32)
	key1[0] = 1
	key2 := make([]byte, 32)
	key2[0] = 2

	box1, err := NewBox(key1)
	if err != nil {
		t.Fatal(err)
	}
	box2, err := NewBox(key2)
	if err != nil {
		t.Fatal(err)
	}

	ciphertext, err := box1.Encrypt([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = box2.Decrypt(ciphertext)
	if err == nil {
		t.Error("expected error when decrypting with wrong key")
	}
}
