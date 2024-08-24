package cache

import (
	"errors"
	"slices"
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

func getStaleFreshTime(now time.Time, freshDuration time.Duration, staleDuration time.Duration, opts *types.SetOptions) (time.Time, time.Time) {
	fresh := now.Add(freshDuration)
	stale := now.Add(staleDuration)

	if opts != nil {
		if opts.Fresh != 0 {
			fresh = now.Add(opts.Fresh)
		}
		if opts.Stale != 0 {
			stale = now.Add(opts.Stale)
		}
	}

	return fresh, stale
}

func (t tieredCache[T]) Get(ns types.TNamespace, key string) (*types.TValue, bool, error) {
	if len(t.stores) == 0 {
		return nil, false, errors.New("no stores found")
	}

	for idx, store := range t.stores {
		var result T
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

func (t tieredCache[T]) GetMany(ns types.TNamespace, keys []string) ([]types.TValue, error) {
	if len(t.stores) == 0 {
		return nil, errors.New("no stores found")
	}

	valuesToSet := make([]types.TValue, 0)
	foundValues := make(map[string]types.TValue)

	// We need to check for all keys in all stores
	// If we can't find one key in a store we need to check the lower stores for all values except
	// the ones that we already found

	// Hack, not sure why but I need to copy the whole array in order to not modify the original
	keysToFind := make([]string, len(keys))
	copy(keysToFind, keys)

	for idx, store := range t.stores {

		// we already found all keys
		if len(keysToFind) == 0 {
			break
		}

		var result T
		values, err := store.GetMany(t.ns, keysToFind, &result)
		if err != nil {
			return nil, fault.Wrap(err, fmsg.With(store.Name()+" failed to get keys: "+strings.Join(keys, ",")))
		}

		for _, v := range values {
			if v.Value != nil {
				// Since we found it set it to the lower stores
				valuesToSet = append(valuesToSet, v)

				// But we should not look for it again
				for i, k := range keysToFind {
					if k == v.Key {
						keysToFind = slices.Delete(keysToFind, i, i+1)
						break
					}
				}

				foundValues[v.Key] = v
			}
		}

		if len(valuesToSet) > 0 {
			for lIdx, lowerStore := range t.stores {
				if lIdx == idx {
					continue
				}

				if err := lowerStore.SetMany(t.ns, valuesToSet, nil); err != nil {
					return nil, fault.Wrap(err, fmsg.With(lowerStore.Name()+" failed to set keys: "+strings.Join(keys, ",")))
				}
			}

			// Clear out again
			valuesToSet = make([]types.TValue, 0)
		}
	}

	valuesToReturn := make([]types.TValue, 0)
	// Now we need to map all the values which we did find or didn't '
	for _, key := range keys {
		if v, ok := foundValues[key]; !ok {
			valuesToReturn = append(valuesToReturn, types.TValue{
				Found: false,
				Value: nil,
				Key:   key,
			})
		} else {
			v.Key = key
			v.Found = true
			valuesToReturn = append(valuesToReturn, v)
		}
	}

	return valuesToReturn, nil
}

func (t tieredCache[T]) Set(ns types.TNamespace, key string, value T, opts *types.SetOptions) error {
	if len(t.stores) == 0 {
		return errors.New("no stores found")
	}

	fresh, stale := getStaleFreshTime(time.Now(), t.fresh, t.stale, opts)
	for _, store := range t.stores {
		if err := store.Set(t.ns, key, types.TValue{
			Value:      value,
			FreshUntil: fresh,
			StaleUntil: stale,
			Key:        key,
		}); err != nil {
			return fault.Wrap(err, fmsg.With(store.Name()+" failed to set key: "+key))
		}
	}

	return nil
}

func (t *tieredCache[T]) SetMany(ns types.TNamespace, values []SetMany[T], opts *types.SetOptions) error {
	if len(t.stores) == 0 {
		return errors.New("no stores found")
	}

	now := time.Now()

	valuesToSet := make([]types.TValue, 0)

	// adjust keys to have the correct stale times
	for _, value := range values {
		fresh, stale := getStaleFreshTime(now, t.fresh, t.stale, opts)

		valuesToSet = append(valuesToSet, types.TValue{
			Value:      value.Value,
			FreshUntil: fresh,
			StaleUntil: stale,
			Key:        value.Key,
		})
	}

	for _, store := range t.stores {
		if err := store.SetMany(t.ns, valuesToSet, opts); err != nil {
			return fault.Wrap(err, fmsg.With(store.Name()+" failed to set keys: "))
		}
	}

	return nil
}

func (t *tieredCache[T]) Remove(ns types.TNamespace, key []string) error {
	if len(t.stores) == 0 {
		return errors.New("no stores found")
	}

	for _, store := range t.stores {
		if err := store.Remove(t.ns, key); err != nil {
			return fault.Wrap(err, fmsg.With(store.Name()+" failed to remove key(s): "+strings.Join(key, ",")))
		}
	}

	return nil
}
