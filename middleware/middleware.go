package middleware

import (
	"github.com/steamsets/go-cache"
	"github.com/steamsets/go-cache/middleware/encryption"
)

// use openssl rand -base64 32 to generate a random key
func WithEncryption(key string) cache.StoreMiddleware {
	return encryption.FromBase64Key(key)
}
