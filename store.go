package cache

import (
	"github.com/steamsets/go-cache/pkg/types"
)

type Store interface {
	Name() string

	Get(namespace types.TNamespace, key string, T any) (value types.TValue, found bool, err error)
	GetMany(namespace types.TNamespace, keys []string, T any) ([]types.TValue, error)
	Set(namespace types.TNamespace, key string, value types.TValue) error
	SetMany(namespace types.TNamespace, values []types.TValue, opts *types.SetOptions) error
	Remove(namespace types.TNamespace, key []string) error // This is actuall a removeMany
}
