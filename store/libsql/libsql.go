package libsql

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/Southclaws/fault"
	"github.com/Southclaws/fault/fmsg"
	"github.com/steamsets/go-cache/pkg/types"
)

type LibsqlStore struct {
	name   string
	config Config
}

type Config struct {
	TableName string
	DB        *sql.DB
}

var DefaultTableName string = "cache"

func New(cfg Config) *LibsqlStore {
	if cfg.TableName == "" {
		cfg.TableName = DefaultTableName
	}

	if cfg.DB == nil {
		panic("DB is nil")
	}

	return &LibsqlStore{
		config: cfg,
		name:   "libsql",
	}
}

func (l *LibsqlStore) Name() string {
	return l.name
}

func (l *LibsqlStore) CreateCacheKey(namespace types.TNamespace, key string) string {
	return string(namespace) + "::" + key
}

func (l *LibsqlStore) UndoCacheKey(namespace types.TNamespace, key string) string {
	return strings.TrimPrefix(key, string(namespace)+"::")
}

func (l *LibsqlStore) Get(ns types.TNamespace, key string, T any) (value types.TValue, found bool, err error) {
	cacheKey := l.CreateCacheKey(ns, key)
	val := types.TValue{Found: false, Key: cacheKey}
	raw := make([]byte, 0)

	staleUntil := ""
	freshUntil := ""
	err = l.config.DB.
		QueryRow("SELECT key, fresh_until, stale_until, value FROM "+l.config.TableName+" WHERE key = ?", cacheKey).
		Scan(&val.Key, &freshUntil, &staleUntil, &raw)

	if err == sql.ErrNoRows {
		return value, false, nil
	}

	freshAsTime, err := time.Parse(time.RFC3339, freshUntil)
	if err != nil {
		return value, false, err
	}
	staleAsTime, err := time.Parse(time.RFC3339, staleUntil)
	if err != nil {
		return value, false, err
	}

	if err != nil {
		return value, false, err
	}

	localT := reflect.New(reflect.TypeOf(T).Elem()).Interface()
	v, err := types.SetTIntoValue(raw, localT)
	if err != nil {
		return value, false, err
	}

	val.Key = l.UndoCacheKey(ns, val.Key)
	val.Found = true
	val.Value = v.Value
	val.FreshUntil = freshAsTime
	val.StaleUntil = staleAsTime

	return val, true, nil
}

func (l *LibsqlStore) GetMany(ns types.TNamespace, keys []string, T any) ([]types.TValue, error) {
	placeHolders := make([]string, 0)
	for range keys {
		placeHolders = append(placeHolders, "?")
	}

	keysToGet := make([]interface{}, 0)
	for _, key := range keys {
		keysToGet = append(keysToGet, l.CreateCacheKey(ns, key))
	}

	rows, err := l.config.DB.Query("SELECT key, fresh_until, stale_until, value FROM "+l.config.TableName+" WHERE key IN ("+strings.Join(placeHolders, ",")+")", keysToGet...)
	if err != nil {
		return nil, fault.Wrap(err, fmsg.With("failed to exec query"))
	}

	defer rows.Close()

	values := make([]types.TValue, 0)

	for rows.Next() {
		val := types.TValue{}
		raw := make([]byte, 0)

		staleUntil := ""
		freshUntil := ""
		if err := rows.Scan(&val.Key, &freshUntil, &staleUntil, &raw); err != nil {
			return nil, fault.Wrap(err, fmsg.With("failed to scan row"))
		}

		freshAsTime, err := time.Parse(time.RFC3339, freshUntil)
		if err != nil {
			return nil, err
		}
		staleAsTime, err := time.Parse(time.RFC3339, staleUntil)
		if err != nil {
			return nil, err
		}

		localT := reflect.New(reflect.TypeOf(T).Elem()).Interface()
		v, err := types.SetTIntoValue(raw, localT)
		if err != nil {
			return nil, err
		}

		val.Key = l.UndoCacheKey(ns, val.Key)
		val.Found = true
		val.FreshUntil = freshAsTime
		val.StaleUntil = staleAsTime
		val.Value = v.Value
		values = append(values, val)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return values, nil
}

func (l *LibsqlStore) Set(ns types.TNamespace, key string, value types.TValue) error {
	b, err := json.Marshal(value.Value)
	if err != nil {
		return err
	}

	_, err = l.config.DB.Exec(
		"INSERT OR REPLACE INTO "+l.config.TableName+" (key, fresh_until, stale_until, value) VALUES(?, ?, ?, ?)",
		l.CreateCacheKey(ns, key),
		value.FreshUntil,
		value.StaleUntil,
		string(b),
	)

	return err
}

func (l *LibsqlStore) SetMany(ns types.TNamespace, values []types.TValue, opts *types.SetOptions) error {
	// IMPORTANT: This is not a transaction and will be a max of 2000 rows at a time
	chunks := make([][]types.TValue, 0)
	chunkSize := 2000
	for i, v := range values {
		if i%chunkSize == 0 {
			chunks = append(chunks, make([]types.TValue, 0))
		}
		chunks[len(chunks)-1] = append(chunks[len(chunks)-1], v)
	}

	for _, chunk := range chunks {
		sql := "INSERT OR REPLACE INTO " + l.config.TableName + " (key, fresh_until, stale_until, value) VALUES "
		params := make([]interface{}, 0)
		for _, v := range chunk {
			b, err := json.Marshal(v.Value)
			if err != nil {
				return err
			}

			sql = sql + "(?, ?, ?, ?),"
			params = append(params, l.CreateCacheKey(ns, v.Key), v.FreshUntil, v.StaleUntil, string(b))
		}

		sql = sql[:len(sql)-1]

		_, err := l.config.DB.Exec(sql, params...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *LibsqlStore) Remove(ns types.TNamespace, key []string) error {
	placeHolders := make([]string, 0)
	for range key {
		placeHolders = append(placeHolders, "?")
	}

	keysToDelete := make([]any, 0)

	for _, key := range key {
		keysToDelete = append(keysToDelete, l.CreateCacheKey(ns, key))
	}

	_, err := l.config.DB.Exec("DELETE FROM "+l.config.TableName+" WHERE key IN ("+strings.Join(placeHolders, ",")+")", keysToDelete...)
	return err
}
