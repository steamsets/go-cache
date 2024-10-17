package main

import (
	"context"
	"log"
	"time"

	"github.com/steamsets/go-cache"
	memoryStore "github.com/steamsets/go-cache/store/memory"
	redisStore "github.com/steamsets/go-cache/store/redis"
)

type Cache struct {
	User cache.Namespace[User]
	Post cache.Namespace[Post]
}

type Address struct {
	Street string
	City   string
	Zip    string
}

type User struct {
	Name    string
	Email   string
	Address *Address
}

type Post struct {
	Title       string
	Description string
}

type Service struct {
	cache *Cache
}

var service *Service

func init() {
	memory := memoryStore.New(memoryStore.Config{
		UnstableEvictOnSet: &memoryStore.UnstableEvictOnSetConfig{
			Frequency: 1,
			MaxItems:  100,
		},
	})

	dsn := "redis://localhost:6379/0"
	redis := redisStore.New(redisStore.Config{
		DSN:      dsn,
		Database: 0,
	})

	c := Cache{
		User: cache.NewNamespace[User]("user", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				memory,
				redis,
			},
			Fresh: 45 * time.Minute,
			Stale: 45 * time.Minute,
		}),
		Post: cache.NewNamespace[Post]("post", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				memory,
				redis,
			},
			Fresh: 10 * time.Minute,
			Stale: 10 * time.Minute,
		}),
	}

	service = &Service{
		cache: &c,
	}
}

func main() {
	u := User{
		Name:  "Flo",
		Email: "test@example.com",
	}

	p := Post{
		Title:       "Hello World!",
		Description: "This is a test post",
	}

	ctx := context.Background()

	err := service.cache.User.Set(ctx, "user1", u, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = service.cache.Post.Set(ctx, "post1", p, nil)
	if err != nil {
		log.Fatal(err)
	}

	users := []User{
		{
			Name:  "Flo",
			Email: "test1@example.com",
			Address: &Address{
				Street: "Street 1",
				City:   "City 1",
				Zip:    "12345",
			},
		},
		{
			Name:  "Flo",
			Email: "test2@example.com",
			Address: &Address{
				Street: "Street 2",
				City:   "City 3",
				Zip:    "54321",
			},
		},
		{
			Name:  "Flo",
			Email: "test3@example.com",
		},
	}

	usersToSet := make([]cache.SetMany[*User], 0)
	usersToGet := make([]string, 0)
	for _, user := range users {
		usersToSet = append(usersToSet, cache.SetMany[*User]{
			Value: &user,
			Key:   user.Email,
			Opts:  nil,
		})

		usersToGet = append(usersToGet, user.Email)
	}

	setManyError := service.cache.User.SetMany(ctx, usersToSet, nil)
	if setManyError != nil {
		log.Fatal(setManyError)
	}

	many, err := service.cache.User.GetMany(ctx, usersToGet)

	if err != nil {
		log.Fatal(err)
	}

	for _, m := range many {
		log.Printf("m.Key: %s", m.Key)
		log.Printf("m.Value: %+v", m.Value)
		log.Printf("m.found: %+v", m.Found)
	}

	getUser, found, err := service.cache.User.Get(ctx, "user1")

	log.Printf("getUser has value: %+v", getUser)
	log.Printf("getUser has found: %+v", found)
	log.Printf("getUser has error: %+v", err)

	swrUser, err := service.cache.User.Swr(ctx, "user2", func(string) (*User, error) {
		time.Sleep(3 * time.Second)
		return &User{
			Name:  "Flo (User2)",
			Email: "test2@example.com",
		}, nil
	})

	log.Printf("swrUser has value: %+v", swrUser)
	log.Printf("swrUser has found: %+v", found)
	log.Printf("swrUser has error: %+v", err)

	// In this case user2 is already in the cache, so we should get it via cache, user3 is not in the cache so we should get it from the origin
	swrUsers, err := service.cache.User.SwrMany(ctx, []string{"user2", "user3", "user4"}, func(s []string) ([]cache.GetMany[User], error) {
		return []cache.GetMany[User]{
			{
				Key: "user3",
				Value: &User{
					Name:  "User 3",
					Email: "test3@example.com",
					Address: &Address{
						Street: "Street 3",
						City:   "City 3",
						Zip:    "54321",
					},
				},
				Found: true,
			},
			{
				Key: "user4",
				Value: &User{
					Name:  "User 4",
					Email: "test4@example.com",
					Address: &Address{
						Street: "Street 4",
						City:   "City 4",
						Zip:    "54321",
					},
				},
				Found: true,
			},
		}, nil
	})

	for _, user := range swrUsers {
		log.Printf("swrUsers [%s] has value: %+v", user.Key, user.Value)
	}

	service.cache.User.Remove(ctx, []string{"user1"})

	getUser, found, err = service.cache.User.Get(ctx, "user1")

	log.Printf("getUser is now value: %+v", getUser)
	log.Printf("getUser is now found: %+v", found)
	log.Printf("getUser is now error: %+v", err)

	getPost, found, err := service.cache.Post.Get(ctx, "post1")

	log.Printf("getPost has value: %+v", getPost)
	log.Printf("getPost has found: %+v", found)
	log.Printf("getPost has error: %+v", err)

}
