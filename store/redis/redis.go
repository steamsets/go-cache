package redis

import (
	"context"
	"reflect"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/rueidis"
	"github.com/steamsets/go-cache/pkg/types"
)

// This will work for any redis compatible source e.g dragonfly, redis, etc
type RedisStore struct {
	name   string
	config Config
}

type Config struct {
	Client rueidis.Client
}

func New(cfg Config) *RedisStore {
	return &RedisStore{
		config: cfg,
		name:   "redis",
	}
}

func (r *RedisStore) Name() string {
	return r.name
}

func (r *RedisStore) CreateCacheKey(namespace types.TNamespace, key string) string {
	return string(namespace) + "::" + key
}

func (r *RedisStore) Get(ns types.TNamespace, key string, T any) (value types.TValue, found bool, err error) {
	var resp rueidis.RedisResult

	resp = r.config.Client.DoCache(context.Background(), r.config.Client.B().Get().Key(r.CreateCacheKey(ns, key)).Cache(), time.Minute)

	msg, err := resp.ToMessage()
	if err == rueidis.Nil {
		return value, false, nil
	}

	if err != nil {
		return value, false, err
	}

	b, err := msg.AsBytes()
	if err != nil {
		return value, true, err
	}

	v, err := types.SetTIntoTValue(b, T)
	if err != nil {
		return value, true, err
	}

	value = *v

	return value, true, nil
}

func (r *RedisStore) GetMany(ns types.TNamespace, keys []string, T any) ([]types.TValue, error) {
	ctx := context.Background()

	keysToGet := make([]string, 0)
	for _, k := range keys {
		keysToGet = append(keysToGet, r.CreateCacheKey(ns, k))
	}

	ret, err := rueidis.MGetCache(r.config.Client, ctx, time.Minute, keysToGet)

	if err != nil {
		return nil, err
	}

	values := make([]types.TValue, 0)
	for str, v := range ret {
		keyError := v.Error()
		if keyError == rueidis.Nil {
			values = append(values, types.TValue{
				Found: false,
				Value: nil,
				Key:   str,
			})
			continue
		}

		if keyError != nil {
			return values, keyError
		}

		raw, err := v.AsBytes()
		if err != nil {
			return values, err
		}

		localT := reflect.New(reflect.TypeOf(T).Elem()).Interface()

		v, err := types.SetTIntoTValue(raw, localT)
		if err != nil {
			return nil, err
		}

		v.Found = true
		values = append(values, *v)
	}

	return values, nil
}

func (r *RedisStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err := r.config.Client.Do(
		context.Background(),
		r.config.Client.B().Set().Key(r.CreateCacheKey(ns, key)).Value(string(b)).Pxat(value.StaleUntil).Build(),
	).Error(); err != nil {
		return err
	}

	return nil
}

func (r *RedisStore) SetMany(ns types.TNamespace, values []types.TValue, opts *types.SetOptions) error {
	cmd := r.config.Client.B().Mset()
	for _, v := range values {
		b, err := json.Marshal(v)

		if err != nil {
			return err
		}

		cmd.KeyValue().KeyValue(r.CreateCacheKey(ns, v.Key), string(b))
	}

	if err := r.config.Client.Do(context.Background(), cmd.KeyValue().Build()).Error(); err != nil {
		return err
	}

	return nil
}

func (r *RedisStore) Remove(ns types.TNamespace, key []string) error {
	keys := make([]string, 0)
	for _, k := range key {
		keys = append(keys, r.CreateCacheKey(ns, k))
	}

	res := r.config.Client.Do(context.Background(), r.config.Client.B().Del().Key(keys...).Build())

	msg, err := res.ToMessage()
	if err == rueidis.Nil {
		return nil
	}

	if err != nil {
		return err
	}

	if msg.Error() != nil {
		return msg.Error()
	}

	return nil
}
