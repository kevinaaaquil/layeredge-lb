package loadbalancer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// Redis key for the list of available IPs
	availableIPsKey = "available_ips"
	// Redis key for the set of in-use IPs
	inUseIPsKey = "in_use_ips"
	// Redis key prefix for cached responses
	cacheKeyPrefix = "response_cache:"
	// Default cache expiration time
	defaultCacheExpiration = 30 * time.Minute
)

// LoadBalancer represents a Redis-backed round-robin load balancer
type LoadBalancer struct {
	redisClient     *redis.Client
	mutex           sync.Mutex
	timeout         time.Duration
	cacheEnabled    bool
	cacheExpiration time.Duration
}

// NewLoadBalancer creates a new load balancer instance
func NewLoadBalancer(redisAddr, redisPassword string, redisDB int, timeout time.Duration, cacheEnabled bool, cacheExpiration time.Duration) (*LoadBalancer, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	// Test connection
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	if cacheExpiration == 0 {
		cacheExpiration = defaultCacheExpiration
	}

	return &LoadBalancer{
		redisClient:     client,
		timeout:         timeout,
		cacheEnabled:    cacheEnabled,
		cacheExpiration: cacheExpiration,
	}, nil
}

// AddIP adds a new IP to the available pool
func (lb *LoadBalancer) AddIP(ctx context.Context, ip string) error {
	return lb.redisClient.SAdd(ctx, availableIPsKey, ip).Err()
}

// RemoveIP removes an IP from both available and in-use pools
func (lb *LoadBalancer) RemoveIP(ctx context.Context, ip string) error {
	pipe := lb.redisClient.Pipeline()
	pipe.SRem(ctx, availableIPsKey, ip)
	pipe.SRem(ctx, inUseIPsKey, ip)
	_, err := pipe.Exec(ctx)
	return err
}

// getNextAvailableIP gets the next available IP using round-robin and marks it as in-use
func (lb *LoadBalancer) getNextAvailableIP(ctx context.Context) (string, error) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	// Get the next available IP
	ip, err := lb.redisClient.SPop(ctx, availableIPsKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", errors.New("no available IPs")
		}
		return "", err
	}

	// Mark the IP as in-use
	err = lb.redisClient.SAdd(ctx, inUseIPsKey, ip).Err()
	if err != nil {
		// Try to put the IP back in the available pool
		lb.redisClient.SAdd(ctx, availableIPsKey, ip)
		return "", err
	}

	return ip, nil
}

// releaseIP returns an IP to the available pool
func (lb *LoadBalancer) releaseIP(ctx context.Context, ip string) error {
	pipe := lb.redisClient.Pipeline()
	pipe.SRem(ctx, inUseIPsKey, ip)
	pipe.SAdd(ctx, availableIPsKey, ip)
	_, err := pipe.Exec(ctx)
	return err
}

// ServeHTTP implements the http.Handler interface to handle load balancing
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check if caching is enabled and this is a POST request
	if lb.cacheEnabled && r.Method == http.MethodPost {
		// Read the request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			log.Printf("Error reading request body: %v", err)
			return
		}

		// Restore the body for later use
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		// Try to extract the data field to use as cache key
		var requestData map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &requestData); err == nil {
			if data, ok := requestData["data"]; ok {
				// Create a cache key from the data field
				dataBytes, err := json.Marshal(data)
				if err == nil {
					cacheKey := cacheKeyPrefix + string(dataBytes)

					// Check if we have a cached response
					cachedResponse, err := lb.redisClient.Get(ctx, cacheKey).Bytes()
					if err == nil && len(cachedResponse) > 0 {
						// We have a cached response, return it
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set("X-Cache", "HIT")
						w.Write(cachedResponse)
						return
					}

					// No cached response, proceed with proxying but capture the response
					responseCapture := &responseCapturer{
						ResponseWriter: w,
						cacheKey:       cacheKey,
						lb:             lb,
						ctx:            ctx,
					}

					// Continue with normal proxy handling but use our capturing writer
					lb.handleProxy(responseCapture, r)
					return
				}
			}
		}

		// Restore the body again if we couldn't use caching
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	// Normal proxy handling without caching
	lb.handleProxy(w, r)
}

// handleProxy handles the actual proxying logic
func (lb *LoadBalancer) handleProxy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get the next available IP
	ip, err := lb.getNextAvailableIP(ctx)
	if err != nil {
		http.Error(w, "No available servers", http.StatusServiceUnavailable)
		log.Printf("Error getting available IP: %v", err)
		return
	}

	// Create a URL from the IP
	targetURL, err := url.Parse("http://" + ip)
	if err != nil {
		lb.releaseIP(ctx, ip)
		http.Error(w, "Invalid server address", http.StatusInternalServerError)
		log.Printf("Error parsing URL for IP %s: %v", ip, err)
		return
	}

	// Create a reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Set up a custom director to modify the request
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = targetURL.Host
	}

	// Set up error handler
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		lb.releaseIP(ctx, ip)
		http.Error(w, "Server error", http.StatusBadGateway)
		log.Printf("Proxy error for IP %s: %v", ip, err)
	}

	// Set up a custom response writer that will release the IP when done
	responseWriter := &responseWriterWithCallback{
		ResponseWriter: w,
		callback: func() {
			// Release the IP when the response is complete
			if err := lb.releaseIP(ctx, ip); err != nil {
				log.Printf("Error releasing IP %s: %v", ip, err)
			}
		},
	}

	// Set up a timeout for the request
	if lb.timeout > 0 {
		requestCtx, cancel := context.WithTimeout(ctx, lb.timeout)
		defer cancel()
		r = r.WithContext(requestCtx)
	}

	// Forward the request to the target server
	proxy.ServeHTTP(responseWriter, r)
}

// responseCapturer captures the response to cache it
type responseCapturer struct {
	http.ResponseWriter
	cacheKey string
	lb       *LoadBalancer
	ctx      context.Context
	buffer   bytes.Buffer
	status   int
	headers  http.Header
}

func (rc *responseCapturer) WriteHeader(statusCode int) {
	rc.status = statusCode
	rc.ResponseWriter.WriteHeader(statusCode)
}

func (rc *responseCapturer) Write(b []byte) (int, error) {
	// Only cache successful responses
	if rc.status == 0 || rc.status == http.StatusOK {
		rc.buffer.Write(b)
	}
	return rc.ResponseWriter.Write(b)
}

func (rc *responseCapturer) Flush() {
	if flusher, ok := rc.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rc *responseCapturer) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := rc.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("hijacking not supported")
}

func (rc *responseCapturer) Finalize() {
	// Cache the response if it was successful
	if (rc.status == 0 || rc.status == http.StatusOK) && rc.buffer.Len() > 0 {
		// Store the response in Redis
		err := rc.lb.redisClient.Set(
			rc.ctx,
			rc.cacheKey,
			rc.buffer.Bytes(),
			rc.lb.cacheExpiration,
		).Err()

		if err != nil {
			log.Printf("Error caching response: %v", err)
		}
	}

	// Call the original finalizer if it exists
	if finalizer, ok := rc.ResponseWriter.(interface{ Finalize() }); ok {
		finalizer.Finalize()
	}
}

// responseWriterWithCallback wraps http.ResponseWriter to call a callback when the response is complete
type responseWriterWithCallback struct {
	http.ResponseWriter
	callback func()
	once     sync.Once
}

func (w *responseWriterWithCallback) Write(b []byte) (int, error) {
	return w.ResponseWriter.Write(b)
}

func (w *responseWriterWithCallback) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterWithCallback) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *responseWriterWithCallback) CloseNotify() <-chan bool {
	if cn, ok := w.ResponseWriter.(http.CloseNotifier); ok {
		return cn.CloseNotify()
	}
	return nil
}

func (w *responseWriterWithCallback) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		conn, buf, err := hj.Hijack()
		if err == nil {
			w.once.Do(w.callback)
		}
		return conn, buf, err
	}
	return nil, nil, errors.New("hijacking not supported")
}

// Finalize calls the callback
func (w *responseWriterWithCallback) Finalize() {
	w.once.Do(w.callback)
}
