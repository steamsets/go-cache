package cache

// StoreMiddleware represents a middleware for wrapping a Store
type StoreMiddleware interface {
	// Wrap takes a Store and returns a new Store with added functionality
	Wrap(Store) Store
}
