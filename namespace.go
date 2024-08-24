package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	return Namespace[T]{
		ns:           ns,
		fresh:        cfg.Fresh,
		stale:        cfg.Stale,
		store:        newTieredCache[T](ns, cfg.Stores, cfg.Fresh, cfg.Stale),
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
	if key == "" {
		return errors.New("key is empty")
	}

	return n.store.Set(n.ns, key, value, opts)
}

type GetMany[T any] struct {
	Key   string
	Value *T
	Found bool
}

func (n Namespace[T]) GetMany(keys []string) ([]GetMany[T], error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys provided")
	}

	values, err := n.store.GetMany(n.ns, keys)
	if err != nil {
		return nil, err
	}

	ret := make([]GetMany[T], 0)
	toRemove := make([]string, 0)

	for _, val := range values {
		if val.Value == nil {
			ret = append(ret, GetMany[T]{
				Key:   val.Key,
				Value: nil,
				Found: false,
			})

			continue
		}

		if time.Now().After(val.StaleUntil) {
			toRemove = append(toRemove, val.Key)
		}

		v := val.Value.(T)
		ret = append(ret, GetMany[T]{
			Key:   val.Key,
			Value: &v,
			Found: true,
		})
	}

	if len(toRemove) > 0 {
		if err := n.store.Remove(n.ns, toRemove); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

type SetMany[T any] struct {
	Value T
	Key   string
	Opts  *types.SetOptions
}

func (n Namespace[T]) SetMany(values []SetMany[T], opts *types.SetOptions) error {
	if len(values) == 0 {
		return errors.New("no values provided")
	}

	return n.store.SetMany(n.ns, values, opts)
}

func (n Namespace[T]) Remove(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	return n.store.Remove(n.ns, keys)
}

func (n Namespace[T]) Swr(key string, refreshFromOrigin func(string) (*T, error)) (*T, error) {
	if key == "" {
		return nil, errors.New("key is empty")
	}

	value, found, err := n.store.Get(n.ns, key)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	if found {
		if now.After(value.FreshUntil) {
			newValue, error := n.deduplicateLoadFromOrigin(n.ns, key, refreshFromOrigin)
			if error != nil {
				return nil, error
			}

			if err := n.store.Set(n.ns, key, *newValue, nil); err != nil {
				return nil, err
			}
		}

		v := value.Value.(T)
		return &v, nil
	}

	newValue, error := n.deduplicateLoadFromOrigin(n.ns, key, refreshFromOrigin)
	if error != nil {
		return nil, error
	}

	if err := n.store.Set(n.ns, key, *newValue, nil); err != nil {
		return nil, err
	}

	return newValue, nil
}

func (n Namespace[T]) SwrMany(keys []string, refreshFromOrigin func([]string) ([]GetMany[T], error)) ([]GetMany[T], error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys provided")
	}

	values, err := n.store.GetMany(n.ns, keys)

	if err != nil {
		return nil, err
	}

	returnMap := make(map[string]GetMany[T])
	keysToFetchFromOrigin := make([]string, 0)

	for _, val := range values {
		if !val.Found {
			keysToFetchFromOrigin = append(keysToFetchFromOrigin, val.Key)
			continue
		}

		if time.Now().After(val.StaleUntil) {
			keysToFetchFromOrigin = append(keysToFetchFromOrigin, val.Key)
			// We want to get the new value from the origin but will remove
			// the result from the origin and just keep this value in the response
		}

		v := val.Value.(T)
		returnMap[val.Key] = GetMany[T]{
			Key:   val.Key,
			Value: &v,
			Found: true,
		}
	}

	// if we have keys to get, we need to get them
	if len(keysToFetchFromOrigin) > 0 {
		values, err := n.deduplicateLoadFromOriginMany(n.ns, keysToFetchFromOrigin, refreshFromOrigin)
		if err != nil {
			return nil, err
		}

		for _, v := range values {
			if _, ok := returnMap[v.Key]; !ok {
				returnMap[v.Key] = v
			}
		}

		valuesToSet := make([]SetMany[T], 0)
		for _, v := range returnMap {
			valuesToSet = append(valuesToSet, SetMany[T]{
				Value: *v.Value,
				Key:   v.Key,
				Opts:  nil,
			})
		}

		if err := n.store.SetMany(n.ns, valuesToSet, nil); err != nil {
			return nil, err
		}
	}

	returnValues := make([]GetMany[T], 0)
	for _, v := range returnMap {
		returnValues = append(returnValues, v)
	}

	return returnValues, nil
}

type deduplicateEntry[T any] struct {
	value *T
	err   error
}

type deduplicateManyEntry[T any] struct {
	value []GetMany[T]
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

func (n Namespace[T]) deduplicateLoadFromOriginMany(ns types.TNamespace, keys []string, refreshFromOrigin func([]string) ([]GetMany[T], error)) ([]GetMany[T], error) {
	revalidateKey := fmt.Sprintf("%s::%s", ns, strings.Join(keys, ","))

	// if we are currently revalidating this key, wait for the result (hopefully)
	if val, ok := n.revalidating.Load(revalidateKey); ok {
		future := val.(chan deduplicateManyEntry[T])
		result := <-future
		return result.value, result.err
	}

	future := make(chan deduplicateManyEntry[T], 1)

	n.revalidating.Store(revalidateKey, future)

	defer n.revalidating.Delete(revalidateKey)

	values, err := refreshFromOrigin(keys)

	// Send the result through the channel
	future <- deduplicateManyEntry[T]{values, err}

	return values, err
}
