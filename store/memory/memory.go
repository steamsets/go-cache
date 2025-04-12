package memory

import (
	"math/rand/v2"
	"time"

	"github.com/maypok86/otter"
	"github.com/steamsets/go-cache/pkg/types"
)

type UnstableEvictOnSetConfig struct {
	Frequency float64
	MaxItems  int
}

type Config struct {
	MaxSize            int
	UnstableEvictOnSet *UnstableEvictOnSetConfig
}

type MemoryStore struct {
	name   string
	config Config
	otter  *otter.Cache[string, types.TValue]
}

func New(cfg Config) *MemoryStore {
	if cfg.MaxSize == 0 {
		cfg.MaxSize = 10_000
	}

	otter, err := otter.MustBuilder[string, types.TValue](cfg.MaxSize).
		CollectStats().
		Cost(func(key string, value types.TValue) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	return &MemoryStore{
		name:   "memory",
		otter:  &otter,
		config: cfg,
	}
}

func (m *MemoryStore) Name() string {
	return m.name
}

func (m *MemoryStore) CreateCacheKey(namespace types.TNamespace, key string) string {
	return string(namespace) + "::" + key
}

func (m *MemoryStore) Get(ns types.TNamespace, key string, T any) (value types.TValue, found bool, err error) {
	k := m.CreateCacheKey(ns, key)

	value, found = m.otter.Get(k)

	if !found {
		return value, false, nil
	}

	if time.Now().After(value.StaleUntil) {
		m.Remove(ns, []string{key})
	}

	return value, true, nil
}

func (m *MemoryStore) GetMany(ns types.TNamespace, keys []string, T any) ([]types.TValue, error) {
	values := make([]types.TValue, 0)

	for _, k := range keys {
		value, found := m.otter.Get(m.CreateCacheKey(ns, k))

		if !found {
			value = types.TValue{
				Found: false,
				Value: nil,
				Key:   k,
			}
			continue
		}

		value.Found = true
		values = append(values, value)
	}

	return values, nil
}

func (m *MemoryStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	k := m.CreateCacheKey(ns, key)
	m.otter.Set(k, value)

	if m.config.UnstableEvictOnSet != nil && rand.Float64() > m.config.UnstableEvictOnSet.Frequency {
		now := time.Now()
		m.otter.Range(func(key string, value types.TValue) bool {
			if now.After(value.StaleUntil) {
				m.otter.Delete(key)
			}
			return true
		})

		if m.otter.Size() > m.config.UnstableEvictOnSet.MaxItems {
			m.otter.Range(func(key string, value types.TValue) bool {
				if m.otter.Size() <= m.config.UnstableEvictOnSet.MaxItems {
					return false
				}

				m.otter.Delete(key)
				return true
			})
		}
	}

	return nil
}

// This just wraps around the set function
func (m *MemoryStore) SetMany(ns types.TNamespace, values []types.TValue, opts *types.SetOptions) error {
	for _, v := range values {
		if err := m.Set(ns, v.Key, v); err != nil {
			return err
		}
	}

	return nil
}

func (m *MemoryStore) Remove(ns types.TNamespace, key []string) error {
	for _, k := range key {
		m.otter.Delete(m.CreateCacheKey(ns, k))
	}

	return nil
}
