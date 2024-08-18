package redis

import (
	"context"
	"strconv"

	"github.com/Flo4604/go-cache/pkg/types"
	"github.com/goccy/go-json"
	"github.com/redis/rueidis"
)

// This will work for any redis compatible source e.g dragonfly, redis, etc
type RedisStore struct {
	name   string
	client rueidis.Client
}

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Dataase  int
}

func NewRedisStore(cfg Config) *RedisStore {
	client, err := rueidis.NewClient(
		rueidis.ClientOption{
			SelectDB:    cfg.Dataase,
			InitAddress: []string{cfg.Host + ":" + strconv.Itoa(cfg.Port)},
			Username:    cfg.Username,
			Password:    cfg.Password,
			ClientName:  "go-cache",
		},
	)
	if err != nil {
		panic(err)
	}

	return &RedisStore{
		client: client,
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
	resp := r.client.Do(context.Background(), r.client.B().Get().Key(r.CreateCacheKey(ns, key)).Build())

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

func (r *RedisStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err := r.client.Do(
		context.Background(),
		r.client.B().Set().Key(r.CreateCacheKey(ns, key)).Value(string(b)).Pxat(value.StaleUntil).Build(),
	).Error(); err != nil {
		return err
	}

	return nil
}

func (r *RedisStore) Remove(ns types.TNamespace, key []string) error {
	keys := make([]string, len(key))
	for i, k := range key {
		keys[i] = r.CreateCacheKey(ns, k)
	}

	res := r.client.Do(context.Background(), r.client.B().Del().Key(keys...).Build())

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
