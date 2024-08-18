package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/steamsets/go-cache/pkg/types"
)

type Namespace[T any] struct {
	fresh        time.Duration
	stale        time.Duration
	ns           types.TNamespace
	store        tieredCache[T]
	revalidating *sync.Map
}

type NamespaceConfig struct {
	Stores []Store
	Fresh  time.Duration
	Stale  time.Duration
}

func NewNamespace[T any](ns types.TNamespace, ctx context.Context, cfg NamespaceConfig) Namespace[T] {
	tieredCache := newTieredCache[T](types.TNamespace(ns), cfg.Stores, cfg.Fresh, cfg.Stale)

	return Namespace[T]{
		ns:           ns,
		fresh:        cfg.Fresh,
		stale:        cfg.Stale,
		store:        tieredCache,
		revalidating: &sync.Map{},
	}
}

func (n Namespace[T]) Get(key string) (value *T, found bool, err error) {
	val, found, err := n.store.Get(n.ns, key)

	if err != nil {
		return nil, false, err
	}

	if val == nil || val.Value == nil {
		return nil, false, nil
	}

	v := val.Value.(T)

	if time.Now().After(val.StaleUntil) {
		n.store.Remove(n.ns, []string{key})
		return nil, false, nil
	}

	return &v, found, nil
}

func (n Namespace[T]) Set(key string, value T, opts *types.SetOptions) error {
	return n.store.Set(n.ns, key, value, opts)
}

type SetMany[T any] struct {
	Value T
	Key   string
	Opts  *types.SetOptions
}

type GetMany[T any] struct {
	Key   string
	Value *T
	Found bool
}

func (n Namespace[T]) GetMany([]string) []GetMany[T] {

	return nil
}

func (n Namespace[T]) SetMany(values map[string]T, opts *types.SetOptions) error {

	return nil
}

func (n Namespace[T]) Remove(key []string) error {
	return n.store.Remove(n.ns, key)
}

func (n Namespace[T]) Swr(key string, refreshFromOrigin func(string) (*T, error)) (*T, bool, error) {
	value, found, err := n.store.Get(n.ns, key)
	if err != nil {
		return nil, false, err
	}

	now := time.Now()

	if found {
		if now.After(value.FreshUntil) {
			newValue, error := n.deduplicateLoadFromOrigin(n.ns, key, refreshFromOrigin)
			if error != nil {
				return nil, found, error
			}

			if err := n.store.Set(n.ns, key, *newValue, nil); err != nil {
				return nil, found, err
			}
		}

		v := value.Value.(T)
		return &v, found, nil
	}

	newValue, error := n.deduplicateLoadFromOrigin(n.ns, key, refreshFromOrigin)
	if error != nil {
		return nil, found, error
	}

	if err := n.store.Set(n.ns, key, *newValue, nil); err != nil {
		return nil, found, err
	}

	return newValue, found, nil
}

type deduplicateEntry[T any] struct {
	value *T
	err   error
}

func (n Namespace[T]) deduplicateLoadFromOrigin(ns types.TNamespace, key string, refreshFromOrigin func(string) (*T, error)) (*T, error) {
	revalidateKey := fmt.Sprintf("%s::%s", ns, key)

	// if we are currently revalidating this key, wait for the result (hopefully)
	if val, ok := n.revalidating.Load(revalidateKey); ok {
		future := val.(chan deduplicateEntry[T])
		result := <-future
		return result.value, result.err
	}

	future := make(chan deduplicateEntry[T], 1)

	n.revalidating.Store(revalidateKey, future)

	defer n.revalidating.Delete(revalidateKey)

	value, err := refreshFromOrigin(key)

	// Send the result through the channel
	future <- deduplicateEntry[T]{value, err}

	return value, err
}
