package apicache

import (
	"context"
	"errors"
	"github.com/dgraph-io/ristretto"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"net/url"
	"path/filepath"
	"reflect"
	"time"
)

type Cache interface {
	SetWithTTL(key string, data []byte, ttl time.Duration)
	Set(key string, data []byte)
	SetValue(key string, value any)
	Get(ctx context.Context, key string) (CacheItem, bool)
	GetValue(ctx context.Context, key string, value any) bool
	Delete(ctx context.Context, key string)
}

type CacheItem struct {
	Data       []byte
	InsertUnix int64
}

func (c *CacheItem) Age() int64 {
	return time.Now().Unix() - c.InsertUnix
}

type CacheStruct struct {
	Cache
}

type InMemoryCache struct {
	c *ristretto.Cache
}

func NewInMemory(maxSize int64) (*CacheStruct, error) {
	inMemoryCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxSize / 10,
		MaxCost:     maxSize,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	memoryCache := &InMemoryCache{
		c: inMemoryCache,
	}
	return &CacheStruct{memoryCache}, nil
}

func (i *InMemoryCache) SetWithTTL(key string, data []byte, ttl time.Duration) {
	i.c.SetWithTTL(key, CacheItem{
		Data:       data,
		InsertUnix: time.Now().Unix(),
	}, int64(len(data)), ttl)
}

func (i *InMemoryCache) Set(key string, data []byte) {
	i.c.Set(key, CacheItem{
		Data:       data,
		InsertUnix: time.Now().Unix(),
	}, int64(len(data)))
}

func (i *InMemoryCache) SetValue(key string, value any) {
	i.c.Set(key, value, 1)
}

func (i *InMemoryCache) Get(ctx context.Context, key string) (CacheItem, bool) {
	value, hit := i.c.Get(key)
	if hit {
		return value.(CacheItem), true
	}
	return CacheItem{}, false
}

func (i *InMemoryCache) GetValue(_ context.Context, key string, value any) bool {
	hitValue, hit := i.c.Get(key)
	if hit {
		// 获取value的反射值对象
		valPtrValue := reflect.ValueOf(value)
		if valPtrValue.Kind() != reflect.Ptr || valPtrValue.IsNil() {
			return false
		}
		// 获取value指针指向的实际值对象
		valueElem := valPtrValue.Elem()
		if !reflect.TypeOf(hitValue).AssignableTo(valueElem.Type()) {
			return false
		}
		// 将hitValue的值设置到value里
		valueElem.Set(reflect.ValueOf(hitValue))
	}
	return hit
}

func (i *InMemoryCache) Delete(ctx context.Context, key string) {
	i.c.Del(key)
}

func (i *InMemoryCache) Close() error {
	if i.c != nil {
		i.c.Close()
		i.c = nil
	}
	return nil
}

type RedisCache struct {
	c   *cache.Cache
	log *zap.Logger
}

func NewRedis(connectionString string, log *zap.Logger) (*CacheStruct, error) {

	u, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}

	userName := u.User.Username()
	password, _ := u.User.Password()
	addr := filepath.ToSlash(filepath.Join(u.Host, u.Path))

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: userName,
		Password: password,
	})
	if err = client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	redisCache := &RedisCache{
		c: cache.New(&cache.Options{
			Redis:        client,
			StatsEnabled: false,
		}),
		log: log,
	}

	return &CacheStruct{redisCache}, nil
}

func (r *RedisCache) SetWithTTL(key string, data []byte, ttl time.Duration) {
	err := r.c.Set(&cache.Item{
		Value: &CacheItem{
			InsertUnix: time.Now().Unix(),
			Data:       data,
		},
		Key:            key,
		TTL:            ttl,
		SkipLocalCache: true,
	})
	if err != nil {
		r.log.Error("RedisCache.SetWithTTL", zap.Error(err))
	}
}

func (r *RedisCache) Set(key string, data []byte) {
	err := r.c.Set(&cache.Item{
		Value: &CacheItem{
			InsertUnix: time.Now().Unix(),
			Data:       data,
		},
		Key:            key,
		SkipLocalCache: true,
	})
	if err != nil {
		r.log.Error("RedisCache.Set", zap.Error(err))
	}
}

func (r *RedisCache) SetValue(key string, value any) {
	err := r.c.Set(&cache.Item{
		Value:          value,
		Key:            key,
		SkipLocalCache: true,
	})
	if err != nil {
		r.log.Error("RedisCache.Set", zap.Error(err))
	}
}

func (r *RedisCache) Get(ctx context.Context, key string) (CacheItem, bool) {
	var item CacheItem
	err := r.c.GetSkippingLocalCache(ctx, key, &item)
	if err != nil {
		if errors.Is(err, cache.ErrCacheMiss) {
			return CacheItem{}, false
		}
		r.log.Error("RedisCache.Get",
			zap.Error(err),
		)
		return CacheItem{}, false
	}
	return item, true
}

func (r *RedisCache) GetValue(ctx context.Context, key string, value any) bool {
	err := r.c.GetSkippingLocalCache(ctx, key, &value)
	return err == nil
}

func (r *RedisCache) Delete(ctx context.Context, key string) {
	err := r.c.Delete(ctx, key)
	if err != nil {
		r.log.Error("RedisCache.Delete", zap.Error(err))
	}
}

type NoOpCache struct{}

func (n *NoOpCache) SetWithTTL(key string, data []byte, ttl time.Duration) {}

func (n *NoOpCache) Set(key string, data []byte) {}

func (n *NoOpCache) SetValue(key string, value any) {}

func (n *NoOpCache) Get(ctx context.Context, key string) (CacheItem, bool) {
	return CacheItem{}, false
}

func (n *NoOpCache) GetValue(context.Context, string, any) bool {
	return false
}

func (n *NoOpCache) Delete(ctx context.Context, key string) {}
