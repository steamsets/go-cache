package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/goccy/go-json"
	"github.com/steamsets/go-cache"
	"github.com/steamsets/go-cache/pkg/types"
)

// this is just another store that wraps another store
// and encrypts the data before storing it and decrypts it when getting it
// This is mostly a wrapper from @unkey/cache
type EncryptedStore struct {
	store             cache.Store
	encryptionKey     string
	encryptionKeyHash string
}

type EncryptedValue struct {
	IV         string `json:"iv"`
	Ciphertext string `json:"ciphertext"`
}

type EncryptedStoreMiddleware struct {
	encryptionKey     string
	encryptionKeyHash string
}

func (m *EncryptedStoreMiddleware) Wrap(store cache.Store) cache.Store {
	return &EncryptedStore{
		store:             store,
		encryptionKey:     m.encryptionKey,
		encryptionKeyHash: m.encryptionKeyHash,
	}
}

func (e EncryptedStore) Name() string {
	return e.store.Name()
}

func (e *EncryptedStore) CreateCacheKey(namespace types.TNamespace, key string) string {
	return strings.Join([]string{key, e.encryptionKeyHash}, "/")
}

func (e *EncryptedStore) Get(ns types.TNamespace, key string, T any) (value types.TValue, found bool, err error) {
	val, found, err := e.store.Get(ns, e.CreateCacheKey(ns, key), &EncryptedValue{})
	if err != nil {
		return types.TValue{}, false, err
	}

	if !found {
		return types.TValue{}, false, nil
	}

	asValue, ok := val.Value.(EncryptedValue)
	if !ok {
		return types.TValue{}, false, nil
	}

	decrypted, err := e.Decrypt(&asValue)
	if err != nil {
		return types.TValue{}, false, err
	}

	localT := reflect.New(reflect.TypeOf(T).Elem()).Interface()
	v, err := types.SetTIntoValue([]byte(decrypted), localT)

	val.Value = v.Value
	return val, true, err
}

func (e *EncryptedStore) GetMany(ns types.TNamespace, keys []string, T any) ([]types.TValue, error) {
	return nil, nil
}

func (e *EncryptedStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	cacheKey := e.CreateCacheKey(ns, key)
	b, err := json.Marshal(value.Value)
	if err != nil {
		return err
	}

	encrypted, err := e.Encrypt(string(b))
	if err != nil {
		return err
	}

	value.Value = encrypted

	return e.store.Set(ns, cacheKey, value)
}

func (e *EncryptedStore) SetMany(ns types.TNamespace, values []types.TValue, opts *types.SetOptions) error {
	return nil
}

func (e *EncryptedStore) Remove(ns types.TNamespace, key []string) error {
	keysToRemove := make([]string, 0)
	for _, k := range key {
		keysToRemove = append(keysToRemove, e.CreateCacheKey(ns, k))
	}

	return e.store.Remove(ns, keysToRemove)
}

func encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func (e *EncryptedStore) Encrypt(plaintext string) (*EncryptedValue, error) {
	key, err := decode(e.encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("invalid encryption key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	ciphertext := aesGCM.Seal(nil, iv, []byte(plaintext), nil)

	return &EncryptedValue{
		IV:         encode(iv),
		Ciphertext: encode(ciphertext),
	}, nil
}

func (e *EncryptedStore) Decrypt(encryptedValue *EncryptedValue) (string, error) {
	key, err := decode(e.encryptionKey)
	if err != nil {
		return "", fmt.Errorf("invalid encryption key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	ivBytes, err := decode(encryptedValue.IV)
	if err != nil {
		return "", err
	}

	ciphertextBytes, err := decode(encryptedValue.Ciphertext)
	if err != nil {
		return "", err
	}

	plaintext, err := aesGCM.Open(nil, ivBytes, ciphertextBytes, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

func FromBase64Key(base64EncodedKey string) cache.StoreMiddleware {
	key, err := base64.StdEncoding.DecodeString(base64EncodedKey)
	if err != nil {
		panic(err)
	}

	// Verify key length for AES-256
	if len(key) != 32 {
		panic(err)
	}

	// Create AES cipher to verify the key
	_, err = aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	// Compute SHA-256 hash of the key
	hash := sha256.Sum256(key)
	hashString := base64.StdEncoding.EncodeToString(hash[:])

	return &EncryptedStoreMiddleware{
		encryptionKey:     encode(key),
		encryptionKeyHash: hashString,
	}
}
