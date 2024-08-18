package cache

import (
	"github.com/steamsets/go-cache/pkg/types"
)

type Store interface {
	Name() string

	Get(namespace types.TNamespace, key string, T any) (value types.TValue, found bool, err error)
	Set(namespace types.TNamespace, key string, value types.TValue) error
	Remove(namespace types.TNamespace, key []string) error
}
