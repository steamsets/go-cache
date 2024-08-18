package main

import (
	"log"
	"time"

	"github.com/Flo4604/go-cache"
	memoryStore "github.com/Flo4604/go-cache/store/memory"
	redisStore "github.com/Flo4604/go-cache/store/redis"
)

type User struct {
	Name  string
	Email string
}

type Post struct {
	Title       string
	Description string
}

type Cache struct {
	User cache.Namespace[User]
	Post cache.Namespace[Post]
}

func main() {
	memory := memoryStore.New(memoryStore.Config{
		UnstableEvictOnSet: &memoryStore.UnstableEvictOnSetConfig{
			Frequency: 1,
			MaxItems:  100,
		},
	})

	redis := redisStore.NewRedisStore(redisStore.Config{
		Host:     "localhost",
		Port:     6379,
		Username: "",
		Password: "",
		Database: 0,
	})

	c := Cache{
		User: cache.NewNamespace[User]("user", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				redis,
			},
			Fresh: 1 * time.Second,
			Stale: 2 * time.Second,
		}),
		Post: cache.NewNamespace[Post]("post", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				memory,
				redis,
			},
			Fresh: 10 * time.Second,
			Stale: 10 * time.Second,
		}),
	}

	u := User{
		Name:  "Flo",
		Email: "test@example.com",
	}

	p := Post{
		Title:       "Hello World!",
		Description: "This is a test post",
	}

	err := c.User.Set("user1", u, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = c.Post.Set("post1", p, nil)
	if err != nil {
		log.Fatal(err)
	}

	getUser, found, err := c.User.Get("user1")

	log.Printf("getUser has value: %+v", getUser)
	log.Printf("getUser has found: %+v", found)
	log.Printf("getUser has error: %+v", err)

	swrUser, found, err := c.User.Swr("user2", func(string) (*User, error) {
		time.Sleep(3 * time.Second)
		return &User{
			Name:  "User 2",
			Email: "test2@example.com",
		}, nil
	})

	log.Printf("swrUser has value: %+v", swrUser)
	log.Printf("swrUser has found: %+v", found)
	log.Printf("swrUser has error: %+v", err)

	c.User.Remove([]string{"user1"})

	getUser, found, err = c.User.Get("user1")

	log.Printf("getUser is now value: %+v", getUser)
	log.Printf("getUser is now found: %+v", found)
	log.Printf("getUser is now error: %+v", err)

	getPost, found, err := c.Post.Get("post1")

	log.Printf("getPost has value: %+v", getPost)
	log.Printf("getPost has found: %+v", found)
	log.Printf("getPost has error: %+v", err)

}
