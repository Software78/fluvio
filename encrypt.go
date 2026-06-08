package fluvio

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const aesGCMNonceSize = 12

// KeyProvider encrypts and decrypts job args.
type KeyProvider interface {
	Encrypt(plaintext []byte) (ciphertext []byte, err error)
	Decrypt(ciphertext []byte) (plaintext []byte, err error)
}

type aesGCMKeyProvider struct {
	aead cipher.AEAD
}

// NewAESGCMKeyProvider returns a KeyProvider backed by a 32-byte AES-256-GCM key.
// key must be exactly 32 bytes.
func NewAESGCMKeyProvider(key []byte) (KeyProvider, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("fluvio: AES-256-GCM key must be exactly 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &aesGCMKeyProvider{aead: aead}, nil
}

func (p *aesGCMKeyProvider) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, aesGCMNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return p.aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (p *aesGCMKeyProvider) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < aesGCMNonceSize {
		return nil, errors.New("fluvio: ciphertext too short")
	}
	nonce := ciphertext[:aesGCMNonceSize]
	return p.aead.Open(nil, nonce, ciphertext[aesGCMNonceSize:], nil)
}
