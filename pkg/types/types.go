package types

import (
	"reflect"
	"time"

	"github.com/goccy/go-json"
)

type TValue struct {
	Value      interface{}
	FreshUntil time.Time
	StaleUntil time.Time
}

type TNamespace string

type SetOptions struct {
	Fresh time.Duration
	Stale time.Duration
}

// This will move our raw json string into a TValue
// and then unmarshal it into the T type that is not know to the store but just passed in when
// getting the key
func SetTIntoTValue(bytes []byte, T interface{}) (*TValue, error) {
	tValue := TValue{
		Value: T,
	}

	if err := json.Unmarshal(bytes, &tValue); err != nil {
		return nil, err
	}

	tValue.Value = reflect.ValueOf(tValue.Value).Elem().Interface()
	return &tValue, nil
}
