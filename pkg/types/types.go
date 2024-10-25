package types

import (
	"reflect"
	"time"

	"github.com/goccy/go-json"
)

type TValue struct {
	Found      bool   `json:"-"` // Whether the value was found or not -> used by GetMany
	Key        string // Optionally the key that was used to get/set the value, incase of we retrieve multiple values
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

	if tValue.Value != nil {
		tValue.Value = reflect.ValueOf(tValue.Value).Elem().Interface()
	}

	return &tValue, nil
}

// This will move our raw json string into a TValue
// and then unmarshal it into the T type that is not know to the store but just passed in when
// getting the key
func SetTIntoValue(bytes []byte, T interface{}) (*TValue, error) {
	tValue := TValue{
		Value: T,
	}

	if err := json.Unmarshal(bytes, &tValue.Value); err != nil {
		return nil, err
	}

	if tValue.Value != nil {
		tValue.Value = reflect.ValueOf(tValue.Value).Elem().Interface()
	}

	return &tValue, nil
}
