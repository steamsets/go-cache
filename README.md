# go-cache

Golang caching

This is a caching library heavily inspired by [unkey-cache](https://www.npmjs.com/package/@unkey/cache)

It does not support all the features of unkey-cache yet.

- [x] Tiered caching
- [x] Memory Store
- [x] Redis Store

Todo:

- [] Cloudflare Store
- [] Libsql Store
- [] Metric Middleware
- [] Encryption Middleware
- [] Support for
  - [] GetMany
  - [] SetMany
  - [] SwrMany

Extra Features:

- [x] SwrMany
- [x] GetMany
- [x] SetMany

# Notes

> [!NOTE]
> This package is still in development and most likely not ready for production use yet.
> Please report any issues you find.

# Installation

```bash
go get github.com/steamsets/go-cache@latest
```

# Usage

Please see [example](https://github.com/steamsets/go-cache/tree/main/example) folder

### Initialization

```go
package somePackage
import (
	"github.com/steamsets/go-cache"
	memoryStore "github.com/steamsets/go-cache/store/memory"
	redisStore "github.com/steamsets/go-cache/store/redis"
)

type Cache struct {
	User cache.Namespace[User]
	Post cache.Namespace[Post]
}

type User struct {
	Name  string
	Email string
}

type Post struct {
	Title       string
	Description string
}

type Service struct {
	cache *Cache
}

var service *Service

func init() {
	memory := memoryStore.New(memoryStore.Config{
		UnstableEvictOnSet: &memoryStore.UnstableEvictOnSetConfig{
			Frequency: 1,
			MaxItems:  100,
		},
	})

	redis := redisStore.NewRedisStore(redisStore.Config{
		Host:     "localhost",
		Port:     6379,
		Username: "",
		Password: "",
		Database: 0,
	})

	c := Cache{
		User: cache.NewNamespace[User]("user", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				redis,
			},
			Fresh: 45 * time.Minute,
			Stale: 45 * time.Minute,
		}),
		Post: cache.NewNamespace[Post]("post", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				memory,
				redis,
			},
			Fresh: 10 * time.Minute,
			Stale: 10 * time.Minute,
		}),
	}

	service = &Service{
		cache: &c,
	}
}
```

# License

MIT License

# Credits

[unkey-cache](https://www.npmjs.com/package/@unkey/cache)
