package cache

import (
	"errors"
	"strings"
	"time"

	"github.com/Southclaws/fault"
	"github.com/Southclaws/fault/fmsg"
	"github.com/steamsets/go-cache/pkg/types"
)

type tieredCache[T any] struct {
	stores []Store
	ns     types.TNamespace
	fresh  time.Duration
	stale  time.Duration
}

func newTieredCache[T any](ns types.TNamespace, stores []Store, fresh time.Duration, stale time.Duration) tieredCache[T] {
	return tieredCache[T]{
		stores: stores,
		ns:     ns,
		fresh:  fresh,
		stale:  stale,
	}
}

func (t tieredCache[T]) Name() string {
	return "tiered"
}

func (t tieredCache[T]) Get(ns types.TNamespace, key string) (*types.TValue, bool, error) {
	if len(t.stores) == 0 {
		return nil, false, errors.New("no stores found")
	}

	var result T
	for idx, store := range t.stores {
		value, found, err := store.Get(t.ns, key, &result)
		if err != nil {
			return nil, false, fault.Wrap(err, fmsg.With(store.Name()+" failed to get key: "+key))
		}

		if value.Value != nil {
			for lIdx, lowerStore := range t.stores {
				// No need to reset the value in our current store, just in all other ones.
				if lIdx == idx {
					continue
				}

				if err := lowerStore.Set(t.ns, key, value); err != nil {
					return nil, false, fault.Wrap(err, fmsg.With(lowerStore.Name()+" failed to set key: "+key))
				}
			}

			return &value, found, nil
		}
	}

	return nil, false, nil
}

func (t tieredCache[T]) Set(ns types.TNamespace, key string, value T, opts *types.SetOptions) error {
	now := time.Now()
	fresh := now.Add(t.fresh)
	stale := now.Add(t.stale)

	if opts != nil {
		if opts.Fresh != 0 {
			fresh = now.Add(opts.Fresh)
		}
		if opts.Stale != 0 {
			stale = now.Add(opts.Stale)
		}
	}

	for _, store := range t.stores {
		if err := store.Set(t.ns, key, types.TValue{
			Value:      value,
			FreshUntil: fresh,
			StaleUntil: stale,
		}); err != nil {
			return fault.Wrap(err, fmsg.With(store.Name()+" failed to set key: "+key))
		}
	}

	return nil
}

func (t *tieredCache[T]) Remove(ns types.TNamespace, key []string) error {
	for _, store := range t.stores {
		if err := store.Remove(t.ns, key); err != nil {
			return fault.Wrap(err, fmsg.With(store.Name()+" failed to remove key(s): "+strings.Join(key, ",")))
		}
	}

	return nil
}
