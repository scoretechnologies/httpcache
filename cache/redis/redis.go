package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bxcodec/httpcache/cache"
	"github.com/datasapiens/cachier/compression"
	"github.com/redis/go-redis/v9"
)

// CacheOptions for storing data for Redis connections
type CacheOptions struct {
	Addr     string
	Password string
	DB       int // 0 for default DB
}

type redisCache struct {
	ctx               context.Context
	cache             *redis.Client
	expiryTime        time.Duration
	compressionEngine *compression.Engine
}

// NewCache will return the redis cache handler
func NewCache(ctx context.Context, c *redis.Client, exptime time.Duration) cache.ICacheInteractor {
	engine, err := compression.NewEngine(compression.ProviderIDZstd, compression.CompressionParams{})
	if err != nil {
		panic(err)
	}
	return &redisCache{
		ctx:               ctx,
		cache:             c,
		compressionEngine: engine,
		expiryTime:        exptime,
	}
}

func (i *redisCache) Set(key string, value cache.CachedResponse) (err error) { //nolint
	valueJSON, _ := json.Marshal(value)

	input, err := i.compressionEngine.Compress(valueJSON)
	if err != nil {

		return cache.ErrStorageInternal
	}

	set := i.cache.Set(i.ctx, key, input, i.expiryTime*time.Second)
	if err := set.Err(); err != nil {
		fmt.Println(err)
		return cache.ErrStorageInternal
	}
	return nil
}

func (i *redisCache) Get(key string) (res cache.CachedResponse, err error) {
	get := i.cache.Get(i.ctx, key)
	if err = get.Err(); err != nil {
		if err == redis.Nil {
			return cache.CachedResponse{}, cache.ErrCacheMissed
		}
		return cache.CachedResponse{}, cache.ErrStorageInternal
	}
	input, err := i.compressionEngine.Decompress([]byte(get.Val()))
	if err != nil {
		// backward compatibility for not compressed entries
		i.cache.Del(i.ctx, key)
		return cache.CachedResponse{}, cache.ErrInvalidCachedResponse
	}

	err = json.Unmarshal(input, &res)
	if err != nil {
		return cache.CachedResponse{}, cache.ErrStorageInternal
	}
	return
}

func (i *redisCache) Delete(key string) (err error) {
	// deleting in redis equal to setting expiration time for key to 0
	set := i.cache.Set(i.ctx, key, nil, 0)
	if err := set.Err(); err != nil {
		return cache.ErrStorageInternal
	}
	return nil
}

func (i *redisCache) Origin() string {
	return cache.CacheRedis
}

func (i *redisCache) Flush() error {
	flush := i.cache.FlushAll(i.ctx)
	if err := flush.Err(); err != nil {
		return cache.ErrStorageInternal
	}
	return nil
}
