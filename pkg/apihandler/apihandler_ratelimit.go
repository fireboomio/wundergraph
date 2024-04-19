package apihandler

import (
	"fmt"
	"github.com/spf13/cast"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	"math"
	"net/http"
	"sync"
	"time"
)

const (
	HeaderParamRateLimitUniqueKey = "x-rateLimit-uniqueKey"
	HeaderParamRateLimitRequests  = "x-rateLimit-requests"
	HeaderParamRateLimitPerSecond = "x-rateLimit-perSecond"
	headerParamRateLimitFormat    = "%s(%s)"
)

func ensureRequiresRateLimiter(operation *wgpb.Operation, handler http.Handler) http.Handler {
	rateLimit := operation.RateLimit
	if rateLimit == nil || !rateLimit.Enabled {
		return handler
	}

	privateLimiters := sync.Map{}
	publicLimiter := rate.NewLimiter(rate.Every(time.Duration(rateLimit.PerSecond)*time.Second), int(rateLimit.Requests))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rateLimitInvoked bool
		paramLimitUniqueKey := r.Header[HeaderParamRateLimitUniqueKey]
		if len(paramLimitUniqueKey) == 0 {
			rateLimitInvoked = !publicLimiter.Allow()
		} else {
			var uniqueLimiters []*rate.Limiter
			for _, key := range paramLimitUniqueKey {
				paramLimitRequests := r.Header[fmt.Sprintf(headerParamRateLimitFormat, HeaderParamRateLimitRequests, key)]
				paramLimitPerSecond := r.Header[fmt.Sprintf(headerParamRateLimitFormat, HeaderParamRateLimitPerSecond, key)]
				for i := 0; i < int(math.Min(float64(len(paramLimitRequests)), float64(len(paramLimitPerSecond)))); i++ {
					uniqueKey := fmt.Sprintf("%s#%s#%d", r.URL.Path, key, i)
					newLimit := rate.Every(cast.ToDuration(paramLimitPerSecond[i]) * time.Second)
					newRequests := cast.ToInt(paramLimitRequests[i])
					var uniqueLimiter *rate.Limiter
					value, ok := privateLimiters.Load(uniqueKey)
					if ok {
						uniqueLimiter = value.(*rate.Limiter)
						if newLimit != uniqueLimiter.Limit() || newRequests != uniqueLimiter.Burst() {
							uniqueLimiter.SetLimit(newLimit)
							uniqueLimiter.SetBurst(newRequests)
						}
					} else {
						uniqueLimiter = rate.NewLimiter(newLimit, newRequests)
						privateLimiters.Store(uniqueKey, uniqueLimiter)
					}
					uniqueLimiters = append(uniqueLimiters, uniqueLimiter)
				}
			}
			rateLimitInvoked = slices.ContainsFunc(uniqueLimiters, func(limiter *rate.Limiter) bool { return !limiter.Allow() })
		}

		if rateLimitInvoked {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Too many Requests, Try again later"))
			return
		}

		handler.ServeHTTP(w, r)
	})
}
