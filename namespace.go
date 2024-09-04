package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/steamsets/go-cache/pkg/telemetry"
	"github.com/steamsets/go-cache/pkg/types"
)

type Namespace[T any] struct {
	fresh        time.Duration
	stale        time.Duration
	telemetry    bool
	ns           types.TNamespace
	store        tieredCache[T]
	revalidating *sync.Map
}

type NamespaceConfig struct {
	Stores    []Store
	Telemetry bool
	Fresh     time.Duration
	Stale     time.Duration
}

func NewNamespace[T any](ns types.TNamespace, ctx context.Context, cfg NamespaceConfig) Namespace[T] {
	return Namespace[T]{
		ns:           ns,
		fresh:        cfg.Fresh,
		stale:        cfg.Stale,
		store:        newTieredCache[T](ns, cfg.Stores, cfg.Fresh, cfg.Stale, cfg.Telemetry),
		revalidating: &sync.Map{},
	}
}

func (n Namespace[T]) Get(ctx context.Context, key string) (value *T, found bool, err error) {
	ctx, span := telemetry.NewSpan(ctx, "namespace.get")
	defer span.End()
	telemetry.WithAttributes(span,
		telemetry.AttributeKV{Key: "key", Value: key},
		telemetry.AttributeKV{Key: "namespace", Value: string(n.ns)},
	)

	val, found, err := n.store.Get(ctx, n.ns, key)

	if err != nil {
		return nil, false, err
	}

	if val == nil || val.Value == nil {
		return nil, false, nil
	}

	v := getT[T](val.Value)

	if time.Now().After(val.StaleUntil) {
		n.store.Remove(ctx, n.ns, []string{key})
		return nil, false, nil
	}

	return v, found, nil
}

func (n Namespace[T]) Set(ctx context.Context, key string, value T, opts *types.SetOptions) error {
	ctx, span := telemetry.NewSpan(ctx, "namespace.set")
	defer span.End()
	telemetry.WithAttributes(span,
		telemetry.AttributeKV{Key: "key", Value: key},
		telemetry.AttributeKV{Key: "namespace", Value: string(n.ns)},
	)

	if key == "" {
		return errors.New("key is empty")
	}

	return n.store.Set(ctx, n.ns, key, &value, opts)
}

type GetMany[T any] struct {
	Key   string
	Value *T
	Found bool
}

func (n Namespace[T]) GetMany(ctx context.Context, keys []string) ([]GetMany[T], error) {
	ctx, span := telemetry.NewSpan(ctx, "namespace.get-many")
	defer span.End()
	telemetry.WithAttributes(span,
		telemetry.AttributeKV{Key: "keys", Value: keys},
		telemetry.AttributeKV{Key: "namespace", Value: string(n.ns)},
	)

	if len(keys) == 0 {
		return nil, errors.New("no keys provided")
	}

	values, err := n.store.GetMany(ctx, n.ns, keys)
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
				Found: val.Found,
			})

			continue
		}

		if time.Now().After(val.StaleUntil) {
			toRemove = append(toRemove, val.Key)
		}

		v := getT[T](val.Value)

		ret = append(ret, GetMany[T]{
			Key:   val.Key,
			Value: v,
			Found: val.Found,
		})
	}

	if len(toRemove) > 0 {
		if err := n.store.Remove(ctx, n.ns, toRemove); err != nil {
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

func (n Namespace[T]) SetMany(ctx context.Context, values []SetMany[*T], opts *types.SetOptions) error {
	ctx, span := telemetry.NewSpan(ctx, "namespace.set-many")
	defer span.End()

	if len(values) == 0 {
		return errors.New("no values provided")
	}

	return n.store.SetMany(ctx, n.ns, values, opts)
}

func (n Namespace[T]) Remove(ctx context.Context, keys []string) error {
	ctx, span := telemetry.NewSpan(ctx, "namespace.remove")
	defer span.End()

	if len(keys) == 0 {
		return nil
	}

	return n.store.Remove(ctx, n.ns, keys)
}

func (n Namespace[T]) Swr(ctx context.Context, key string, refreshFromOrigin func(string) (*T, error)) (*T, error) {
	ctx, span := telemetry.NewSpan(ctx, "namespace.swr")
	defer span.End()

	if key == "" {
		return nil, errors.New("key is empty")
	}

	value, found, err := n.store.Get(ctx, n.ns, key)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	if found {
		if now.After(value.FreshUntil) {
			newValue, error := n.deduplicateLoadFromOrigin(ctx, n.ns, key, refreshFromOrigin)
			if error != nil {
				return nil, error
			}

			if err := n.store.Set(ctx, n.ns, key, newValue, nil); err != nil {
				return nil, err
			}
		}

		v := getT[T](value.Value)

		return v, nil
	}

	newValue, error := n.deduplicateLoadFromOrigin(ctx, n.ns, key, refreshFromOrigin)
	if error != nil {
		return nil, error
	}

	if err := n.store.Set(ctx, n.ns, key, newValue, nil); err != nil {
		return nil, err
	}

	return newValue, nil
}

func getT[T any](val interface{}) *T {
	if v1, ok := val.(T); ok {
		return &v1
	}

	if v2, ok := val.(*T); ok {
		return v2
	}

	return nil
}

func (n Namespace[T]) SwrMany(ctx context.Context, keys []string, refreshFromOrigin func([]string) ([]GetMany[T], error)) ([]GetMany[T], error) {
	ctx, span := telemetry.NewSpan(ctx, "namespace.swr-many")
	defer span.End()

	if len(keys) == 0 {
		return nil, errors.New("no keys provided")
	}

	values, err := n.store.GetMany(ctx, n.ns, keys)

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

		v := getT[T](val.Value)

		returnMap[val.Key] = GetMany[T]{
			Key:   val.Key,
			Value: v,
			Found: val.Found,
		}
	}

	// if we have keys to get, we need to get them
	if len(keysToFetchFromOrigin) > 0 {
		values, err := n.deduplicateLoadFromOriginMany(ctx, n.ns, keysToFetchFromOrigin, refreshFromOrigin)
		if err != nil {
			return nil, err
		}

		for _, v := range values {
			if _, ok := returnMap[v.Key]; !ok {
				returnMap[v.Key] = v
			}
		}

		valuesToSet := make([]SetMany[*T], 0)
		for _, v := range returnMap {
			valuesToSet = append(valuesToSet, SetMany[*T]{
				Value: v.Value,
				Key:   v.Key,
				Opts:  nil,
			})
		}

		if err := n.store.SetMany(ctx, n.ns, valuesToSet, nil); err != nil {
			return nil, err
		}
	}

	for _, key := range keys {
		if _, ok := returnMap[key]; !ok {
			returnMap[key] = GetMany[T]{
				Key:   key,
				Value: nil,
				Found: false,
			}
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

func (n Namespace[T]) deduplicateLoadFromOrigin(ctx context.Context, ns types.TNamespace, key string, refreshFromOrigin func(string) (*T, error)) (*T, error) {
	ctx, span := telemetry.NewSpan(ctx, "namespace.deduplicate-load-from-origin")
	defer span.End()

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

	_, span2 := telemetry.NewSpan(ctx, "namespace.refreshFromOrigin")
	value, err := refreshFromOrigin(key)
	span2.End()

	// Send the result through the channel
	future <- deduplicateEntry[T]{value, err}

	return value, err
}

func (n Namespace[T]) deduplicateLoadFromOriginMany(ctx context.Context, ns types.TNamespace, keys []string, refreshFromOrigin func([]string) ([]GetMany[T], error)) ([]GetMany[T], error) {
	ctx, span := telemetry.NewSpan(ctx, "namespace.deduplicate-load-from-origin-many")
	defer span.End()

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

	_, span2 := telemetry.NewSpan(ctx, "namespace.refreshFromOrigin")
	telemetry.WithAttributes(span2,
		telemetry.AttributeKV{Key: "keys", Value: keys},
		telemetry.AttributeKV{Key: "namespace", Value: string(n.ns)},
	)
	values, err := refreshFromOrigin(keys)
	span2.End()

	// Send the result through the channel
	future <- deduplicateManyEntry[T]{values, err}

	return values, err
}
