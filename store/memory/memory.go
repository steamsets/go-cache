package memory

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Flo4604/go-cache/pkg/types"
)

type UnstableEvictOnSetConfig struct {
	Frequency float64
	MaxItems  int
}

type Config struct {
	UnstableEvictOnSet *UnstableEvictOnSetConfig
}

type MemoryStore struct {
	name   string
	config Config
	state  map[string]types.TValue
	mutex  sync.Mutex
}

func New(config Config) *MemoryStore {
	return &MemoryStore{
		name:   "memory",
		config: config,
		mutex:  sync.Mutex{},
		state:  make(map[string]types.TValue),
	}
}

func (m *MemoryStore) Name() string {
	return m.name
}

func (m *MemoryStore) CreateCacheKey(namespace types.TNamespace, key string) string {
	return string(namespace) + "::" + key
}

func (m *MemoryStore) Get(ns types.TNamespace, key string, T any) (value types.TValue, found bool, err error) {
	m.mutex.Lock()

	k := m.CreateCacheKey(ns, key)

	value, found = m.state[k]

	m.mutex.Unlock()

	if !found {
		return value, false, nil
	}

	if time.Now().After(value.StaleUntil) {
		m.Remove(ns, []string{key})
	}

	return value, true, nil
}

func (m *MemoryStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	m.mutex.Lock()
	k := m.CreateCacheKey(ns, key)
	m.state[k] = value
	m.mutex.Unlock()

	if m.config.UnstableEvictOnSet != nil && rand.Float64() > m.config.UnstableEvictOnSet.Frequency {
		now := time.Now()
		for k, v := range m.state {
			if now.After(v.StaleUntil) {
				delete(m.state, k)
			}
		}

		if len(m.state) > m.config.UnstableEvictOnSet.MaxItems {
			for k := range m.state {
				if len(m.state) <= m.config.UnstableEvictOnSet.MaxItems {
					break
				}

				delete(m.state, k)
			}
		}
	}

	return nil
}

func (m *MemoryStore) Remove(ns types.TNamespace, key []string) error {
	m.mutex.Lock()

	for _, k := range key {
		delete(m.state, m.CreateCacheKey(ns, k))
	}
	m.mutex.Unlock()

	return nil
}
