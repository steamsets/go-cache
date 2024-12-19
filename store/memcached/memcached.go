package memcached

import (
	"encoding/json"
	"reflect"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/steamsets/go-cache/pkg/types"
)

type MemcachedStore struct {
	name   string
	config Config
}

type Config struct {
	Client *memcache.Client
}

func New(cfg Config) *MemcachedStore {
	return &MemcachedStore{
		config: cfg,
		name:   "memcache",
	}
}

func (m *MemcachedStore) Name() string {
	return m.name
}

func (m *MemcachedStore) CreateCacheKey(namespace types.TNamespace, key string) string {
	return string(namespace) + "::" + key
}

func (m *MemcachedStore) Get(ns types.TNamespace, key string, T any) (value types.TValue, found bool, err error) {
	item, err := m.config.Client.Get(m.CreateCacheKey(ns, key))
	if err == memcache.ErrCacheMiss {
		return value, false, nil
	}

	if err != nil {
		return value, false, err
	}

	v, err := types.SetTIntoTValue(item.Value, T)
	if err != nil {
		return value, true, err
	}

	value = *v
	return value, true, nil
}

func (m *MemcachedStore) GetMany(ns types.TNamespace, keys []string, T any) ([]types.TValue, error) {
	keysToGet := make([]string, 0)
	for _, k := range keys {
		keysToGet = append(keysToGet, m.CreateCacheKey(ns, k))
	}

	items, err := m.config.Client.GetMulti(keysToGet)
	if err != nil {
		return nil, err
	}

	values := make([]types.TValue, 0)

	for key, value := range items {
		// I just assume this means not found
		if value == nil {
			values = append(values, types.TValue{
				Found: false,
				Value: nil,
				Key:   key,
			})
			continue
		}

		localT := reflect.New(reflect.TypeOf(T).Elem()).Interface()

		v, err := types.SetTIntoTValue(value.Value, localT)
		if err != nil {
			return nil, err
		}

		v.Found = true
		values = append(values, *v)
	}

	return values, nil
}

func (m *MemcachedStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return m.config.Client.Set(&memcache.Item{
		Expiration: int32(value.StaleUntil.Unix()),
		Key:        m.CreateCacheKey(ns, key),
		Value:      b,
	})
}

func (m *MemcachedStore) SetMany(ns types.TNamespace, values []types.TValue, opts *types.SetOptions) error {
	for _, v := range values {
		b, err := json.Marshal(v)

		if err != nil {
			return err
		}

		if err := m.config.Client.Set(&memcache.Item{
			Expiration: int32(v.StaleUntil.Unix()),
			Key:        m.CreateCacheKey(ns, v.Key),
			Value:      b,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (m *MemcachedStore) Remove(ns types.TNamespace, keys []string) error {
	keysToRemove := make([]string, 0)
	for _, k := range keys {
		keysToRemove = append(keysToRemove, m.CreateCacheKey(ns, k))
	}

	for _, key := range keysToRemove {
		if err := m.config.Client.Delete(key); err != nil {
			return err
		}
	}

	return nil
}
