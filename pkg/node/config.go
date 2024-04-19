package node

import (
	"fmt"
	"github.com/spf13/cast"
	"os"
	"strings"
	"time"

	"github.com/docker/go-units"

	"github.com/wundergraph/wundergraph/pkg/apihandler"
	"github.com/wundergraph/wundergraph/pkg/loadvariable"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
)

const (
	defaultInMemoryCacheSize    int64 = 128
	wgInMemoryCacheConfigEnvKey       = "WG_IN_MEMORY_CACHE"
	wgInRedisCacheConfigEnvKey        = "WG_IN_REDIS_CACHE"
)

type Server struct {
	GracefulShutdownTimeout int64
	KeepAlive               int64
	ReadTimeout             int64
	WriteTimeout            int64
	IdleTimeout             int64
}

type WunderNodeConfig struct {
	Server *Server
	Api    *apihandler.Api
}

func CreateConfig(graphConfig *wgpb.WunderGraphConfiguration) (WunderNodeConfig, error) {
	const (
		defaultTimeout = 10 * time.Second
	)

	logLevelStr := loadvariable.String(graphConfig.Api.NodeOptions.Logger.Level)

	logLevel, err := logging.FindLogLevel(logLevelStr)
	if err != nil {
		return WunderNodeConfig{}, err
	}

	listener := &apihandler.Listener{
		Host: loadvariable.String(graphConfig.Api.NodeOptions.Listen.Host),
		Port: uint16(loadvariable.Int(graphConfig.Api.NodeOptions.Listen.Port)),
	}

	defaultRequestTimeout := defaultTimeout
	if graphConfig.Api.NodeOptions.DefaultRequestTimeoutSeconds > 0 {
		defaultRequestTimeout = time.Duration(graphConfig.Api.NodeOptions.DefaultRequestTimeoutSeconds) * time.Second
	}

	var cacheConfig *wgpb.ApiCacheConfig
	inRedisCacheConfig := os.Getenv(wgInRedisCacheConfigEnvKey)
	if inRedisCacheConfig != "" {
		cacheConfig = &wgpb.ApiCacheConfig{
			Kind:        wgpb.ApiCacheKind_REDIS_CACHE,
			RedisConfig: &wgpb.RedisCacheConfig{RedisUrlEnvVar: wgInRedisCacheConfigEnvKey},
		}
	} else {
		cacheSize := defaultInMemoryCacheSize
		if inMemoryCacheConfig := cast.ToInt64(os.Getenv(wgInMemoryCacheConfigEnvKey)); inMemoryCacheConfig > 0 {
			cacheSize = inMemoryCacheConfig
		}
		cacheConfig = &wgpb.ApiCacheConfig{
			Kind:           wgpb.ApiCacheKind_IN_MEMORY_CACHE,
			InMemoryConfig: &wgpb.InMemoryCacheConfig{MaxSize: cacheSize * units.MB},
		}
	}
	config := WunderNodeConfig{
		Api: &apihandler.Api{
			PrimaryHost:           fmt.Sprintf("%s:%d", listener.Host, listener.Port),
			Hosts:                 loadvariable.Strings(graphConfig.Api.AllowedHostNames),
			EnableSingleFlight:    true,
			CacheConfig:           cacheConfig,
			EnableGraphqlEndpoint: graphConfig.Api.EnableGraphqlEndpoint,
			EngineConfiguration:   graphConfig.Api.EngineConfiguration,
			Operations:            graphConfig.Api.Operations,
			InvalidOperationNames: graphConfig.Api.InvalidOperationNames,
			CorsConfiguration:     graphConfig.Api.CorsConfiguration,
			S3UploadConfiguration: graphConfig.Api.S3UploadConfiguration,
			AuthenticationConfig:  graphConfig.Api.AuthenticationConfig,
			Webhooks:              graphConfig.Api.Webhooks,
			Options: &apihandler.Options{
				ServerUrl:     strings.TrimSuffix(loadvariable.String(graphConfig.Api.ServerOptions.ServerUrl), "/"),
				PublicNodeUrl: loadvariable.String(graphConfig.Api.NodeOptions.PublicNodeUrl),
				Listener:      listener,
				Logging: apihandler.Logging{
					Level: logLevel,
				},
				DefaultTimeout: defaultRequestTimeout,
			},
		},
		Server: &Server{
			GracefulShutdownTimeout: 0,
			KeepAlive:               5,
			ReadTimeout:             5,
			WriteTimeout:            5,
			IdleTimeout:             10,
		},
	}

	return config, nil
}
