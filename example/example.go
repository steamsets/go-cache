package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/redis/rueidis"
	"github.com/steamsets/go-cache"
	"github.com/steamsets/go-cache/middleware"
	libsqlStore "github.com/steamsets/go-cache/store/libsql"
	memoryStore "github.com/steamsets/go-cache/store/memory"
	redisStore "github.com/steamsets/go-cache/store/redis"

	"github.com/tursodatabase/go-libsql"
)

type Cache struct {
	User            cache.Namespace[User]
	Post            cache.Namespace[Post]
	String          cache.Namespace[string]
	EncryptedStruct cache.Namespace[Post]
	Encrypted       cache.Namespace[string]
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
	CreatedAt   time.Time
	Updated     *time.Time
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

	clientCfg := rueidis.MustParseURL("redis://localhost:6379/0")
	clientCfg.SelectDB = 0
	clientCfg.ClientName = "go-cache"
	clientCfg.DisableCache = true
	client, err := rueidis.NewClient(clientCfg)
	redis := redisStore.New(redisStore.Config{
		Client: client,
	})

	dbName := "local.db"
	primaryUrl := os.Getenv("TURSO_URL")
	authToken := os.Getenv("TURSO_TOKEN")

	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		log.Printf("Error creating temporary directory:", err)
		os.Exit(1)
	}

	dbPath := filepath.Join(dir, dbName)

	connector, err := libsql.NewEmbeddedReplicaConnector(
		dbPath,
		primaryUrl,
		libsql.WithAuthToken(authToken),
	)

	if err != nil {
		log.Printf("Error creating embedded replica connector:", err)
		os.Exit(1)
	}

	db := sql.OpenDB(connector)

	libsql := libsqlStore.New(libsqlStore.Config{
		DB:        db,
		TableName: "cache",
	})

	encryption := middleware.WithEncryption(os.Getenv("ENCRYPTION_KEY"))
	libSqlEncrypted := encryption.Wrap(libsql)

	c := Cache{
		User: cache.NewNamespace[User]("user", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				memory,
				libsql,
				redis,
			},
			Fresh: 45 * time.Minute,
			Stale: 45 * time.Minute,
		}),
		Post: cache.NewNamespace[Post]("post", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				libsql,
			},
			Fresh: 10 * time.Minute,
			Stale: 10 * time.Minute,
		}),
		String: cache.NewNamespace[string]("string", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				libsql,
			},
			Fresh: 10 * time.Minute,
			Stale: 10 * time.Minute,
		}),
		EncryptedStruct: cache.NewNamespace[Post]("encrypted_struct", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				libSqlEncrypted,
			},
			Fresh: 10 * time.Minute,
			Stale: 10 * time.Minute,
		}),
		Encrypted: cache.NewNamespace[string]("encrypted_string", nil, cache.NamespaceConfig{
			Stores: []cache.Store{
				libSqlEncrypted,
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
	ctx := context.Background()

	in5Minutes := time.Now().Add(time.Minute * 5)
	u := User{
		Name:  "Flo",
		Email: "test@example.com",
	}
	p := Post{
		Title:       "Hello World!",
		Description: "This is a test post",
		CreatedAt:   time.Now().Add(time.Hour * 24 * 180),
		Updated:     &in5Minutes,
	}

	stringValue, err := service.cache.String.Swr(ctx, "hallo", func(string) (*string, error) {
		str := "welt"
		return &str, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("stringValue has value: %+v err %+v", *stringValue, err)
	if err := service.cache.EncryptedStruct.Set(ctx, "p1", p, nil); err != nil {
		log.Printf("error: %+v", err)
	}
	post, found, err := service.cache.EncryptedStruct.Get(ctx, "p1")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("encryptedPost: %+v", post)
	log.Printf("found: %+v", found)

	if err := service.cache.Encrypted.Set(ctx, "hello", "world", nil); err != nil {
		log.Printf("error: %+v", err)
	}

	err = service.cache.User.Set(ctx, "user1", u, nil)
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
		if user.Found {
			log.Printf("swrUsers [%s] has value: %+v %+v", user.Key, user.Value, user.Value.Address)
		} else {
			log.Printf("swrUsers [%s] not found", user.Key)
		}
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

	twentyThousand := make([]cache.SetMany[*string], 0)
	for i := range 20_000 {
		str := fmt.Sprintf("%d", i)
		twentyThousand = append(twentyThousand, cache.SetMany[*string]{
			Value: &str,
			Key:   str,
			Opts:  nil,
		})
	}

	if err := service.cache.String.SetMany(ctx, twentyThousand, nil); err != nil {
		log.Fatal(err)
	}
}
