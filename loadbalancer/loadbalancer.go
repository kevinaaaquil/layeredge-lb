package loadbalancer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
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
	// Redis key prefix for pending responses
	pendingCacheKeyPrefix = "pending_response:"
	// Default cache expiration time
	defaultCacheExpiration = 60 * time.Minute
	// Placeholder response message
	placeholderResponse = `{"status":"pending","message":"Your request is being processed"}`
	// How often to check for a real response when waiting
	cacheCheckInterval = 100 * time.Millisecond
	// Maximum time to wait for a real response
	maxWaitTime = 30 * time.Minute
	// Lock key for IP allocation
	ipAllocationLockKey = "ip_allocation_lock"
	// Lock expiration time to prevent deadlocks
	lockExpiration = 5 * time.Second
	// How long to wait between lock acquisition attempts
	lockRetryDelay = 1 * time.Second
	// Maximum retries for lock acquisition
	maxLockRetries = 120
)

// WebhookPayload represents the payload to send to the webhook
type WebhookPayload struct {
	Text string `json:"text"`
}

// LoadBalancer represents a Redis-backed round-robin load balancer

type LoadBalancer struct {
	redisClient     *redis.Client
	timeout         time.Duration
	cacheEnabled    bool
	cacheExpiration time.Duration
	pendingRequests sync.Map // Track requests that are being processed
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

	// flush all keys
	client.FlushAll(ctx).Result()

	if cacheExpiration == 0 {
		cacheExpiration = defaultCacheExpiration
	}

	log.Printf("Redis client created: %v", client)
	log.Printf("Timeout: %v", timeout)
	log.Printf("Cache enabled: %v", cacheEnabled)
	log.Printf("Cache expiration: %v", cacheExpiration)

	return &LoadBalancer{
		redisClient:     client,
		timeout:         timeout,
		cacheEnabled:    cacheEnabled,
		cacheExpiration: cacheExpiration,
	}, nil
}

// NewLoadBalancerWithURL creates a new load balancer instance using a Redis URL
func NewLoadBalancerWithURL(redisURL string, timeout time.Duration, cacheEnabled bool, cacheExpiration time.Duration) (*LoadBalancer, error) {
	// Parse the Redis URL
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx := context.Background()
	_, err = client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	// flush all keys
	client.FlushAll(ctx).Result()

	if cacheExpiration == 0 {
		cacheExpiration = defaultCacheExpiration
	}

	log.Printf("Redis client created: %v", client)
	log.Printf("Timeout: %v", timeout)
	log.Printf("Cache enabled: %v", cacheEnabled)
	log.Printf("Cache expiration: %v", cacheExpiration)

	return &LoadBalancer{
		redisClient:     client,
		timeout:         timeout,
		cacheEnabled:    cacheEnabled,
		cacheExpiration: cacheExpiration,
	}, nil
}

// AddIP adds a new IP to the available pool
func (lb *LoadBalancer) AddIP(ctx context.Context, ip string, isProve bool) error {
	var ipStr string

	if isProve {
		ipStr = fmt.Sprintf("%s:%s", ip, "prove")
	} else {
		ipStr = fmt.Sprintf("%s:%s", ip, "verify")
	}

	return lb.redisClient.SAdd(ctx, availableIPsKey, ipStr).Err()
}

// RemoveIP removes an IP from both available and in-use pools
func (lb *LoadBalancer) RemoveIP(ctx context.Context, ip string) error {
	pipe := lb.redisClient.Pipeline()
	pipe.SRem(ctx, availableIPsKey, ip)
	pipe.SRem(ctx, inUseIPsKey, ip)
	_, err := pipe.Exec(ctx)
	return err
}

// generateLockValue creates a random value for the lock to ensure ownership
func generateLockValue() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// acquireLock tries to acquire a Redis-based distributed lock
func (lb *LoadBalancer) acquireLock(ctx context.Context, lockKey string, expiration time.Duration) (string, bool) {
	// Generate a unique lock identifier
	lockValue := generateLockValue()

	// Try to set the key with NX option (only if it doesn't exist)
	success, err := lb.redisClient.SetNX(ctx, lockKey, lockValue, expiration).Result()
	if err != nil {
		log.Printf("Error acquiring lock: %v", err)
		return "", false
	}

	return lockValue, success
}

// releaseLock releases a previously acquired lock
func (lb *LoadBalancer) releaseLock(ctx context.Context, lockKey, lockValue string) bool {
	// Use a Lua script to ensure we only delete the lock if it's still ours
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	result, err := lb.redisClient.Eval(ctx, script, []string{lockKey}, lockValue).Result()
	if err != nil {
		log.Printf("Error releasing lock: %v", err)
		return false
	}

	// If result is 1, the lock was successfully deleted
	return result.(int64) == 1
}

// getNextAvailableIP gets the next available IP using round-robin and marks it as in-use
func (lb *LoadBalancer) getNextAvailableIP(ctx context.Context, isProve bool) (string, error) {
	// Try to acquire the distributed lock
	var lockValue string
	var lockAcquired bool

	// Try multiple times to acquire the lock
	for i := 0; i < maxLockRetries; i++ {
		lockValue, lockAcquired = lb.acquireLock(ctx, ipAllocationLockKey, lockExpiration)
		if lockAcquired {
			break
		}

		// Wait a bit before retrying
		select {
		case <-time.After(lockRetryDelay):
			// Continue to next attempt
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	// If we couldn't acquire the lock after multiple attempts, fail
	if !lockAcquired {
		return "", fmt.Errorf("could not acquire lock for IP allocation after %d attempts", maxLockRetries)
	}

	// Make sure to release the lock when we're done
	defer lb.releaseLock(ctx, ipAllocationLockKey, lockValue)

	// Get all available IPs
	ips, err := lb.redisClient.SMembers(ctx, availableIPsKey).Result()
	if err != nil {
		return "", err
	}

	// Filter IPs based on the pattern
	pattern := ":verify"
	if isProve {
		pattern = ":prove"
	}

	// Find an IP with the matching pattern
	var selectedIP string
	for _, ip := range ips {
		if strings.HasSuffix(ip, pattern) {
			selectedIP = ip
			break
		}
	}

	if selectedIP == "" {
		return "", errors.New("no available IPs matching required type")
	}

	// Remove the selected IP from available pool
	err = lb.redisClient.SRem(ctx, availableIPsKey, selectedIP).Err()
	if err != nil {
		return "", err
	}

	// Mark the IP as in-use
	err = lb.redisClient.SAdd(ctx, inUseIPsKey, selectedIP).Err()
	if err != nil {
		// Try to put the IP back in the available pool
		lb.redisClient.SAdd(ctx, availableIPsKey, selectedIP)
		return "", err
	}

	return selectedIP, nil
}

// waitForAvailableIP waits for an available IP with a backoff strategy
func (lb *LoadBalancer) waitForAvailableIP(ctx context.Context, isProve bool) (string, error) {
	// Initial wait time
	waitTime := 100 * time.Millisecond
	maxWaitTime := 5 * time.Second

	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			// Try to get an IP
			ip, err := lb.getNextAvailableIP(ctx, isProve)
			if err == nil {
				return ip, nil
			}

			// If error is not "no available IPs", return the error
			if !errors.Is(err, redis.Nil) && err.Error() != "no available IPs" {
				return "", err
			}

			// Log that we're waiting
			log.Printf("No IPs available, waiting %v before retry", waitTime)

			// Wait with exponential backoff
			select {
			case <-time.After(waitTime):
				// Increase wait time for next iteration (with a cap)
				waitTime = time.Duration(float64(waitTime) * 1.5)
				if waitTime > maxWaitTime {
					waitTime = maxWaitTime
				}
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}
}

// releaseIP returns an IP to the available pool
func (lb *LoadBalancer) releaseIP(ctx context.Context, ip string) error {
	// Try to acquire the distributed lock
	var lockValue string
	var lockAcquired bool

	// Try multiple times to acquire the lock
	for i := 0; i < maxLockRetries; i++ {
		lockValue, lockAcquired = lb.acquireLock(ctx, ipAllocationLockKey, lockExpiration)
		if lockAcquired {
			break
		}

		// Wait a bit before retrying
		select {
		case <-time.After(lockRetryDelay):
			// Continue to next attempt
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// If we couldn't acquire the lock after multiple attempts, fail
	if !lockAcquired {
		return fmt.Errorf("could not acquire lock for IP release after %d attempts", maxLockRetries)
	}

	// Make sure to release the lock when we're done
	defer lb.releaseLock(ctx, ipAllocationLockKey, lockValue)

	// Use a pipeline to perform both operations atomically
	pipe := lb.redisClient.Pipeline()
	pipe.SRem(ctx, inUseIPsKey, ip)
	pipe.SAdd(ctx, availableIPsKey, ip)
	_, err := pipe.Exec(ctx)
	return err
}

// CacheKeyData represents the data used to create a cache key
type CacheKeyData struct {
	Data      interface{} `json:"data,omitempty"`
	Operation interface{} `json:"operation,omitempty"`
}

// getCacheKeyFromRequest generates a cache key from request data and operation
func getCacheKeyFromRequest(requestData map[string]interface{}) (string, string, error) {
	// Extract data and operation fields
	data, hasData := requestData["data"]
	operation, hasOperation := requestData["operation"]

	if !hasData {
		return "", "", errors.New("request has no data field")
	}

	// Create a combined key structure
	keyData := CacheKeyData{
		Data: data,
	}

	// Add operation if it exists
	if hasOperation {
		keyData.Operation = operation
	}

	// Marshal to JSON to create a consistent key
	keyBytes, err := json.Marshal(keyData)
	if err != nil {
		return "", "", err
	}

	cacheKey := cacheKeyPrefix + string(keyBytes)
	pendingCacheKey := pendingCacheKeyPrefix + string(keyBytes)

	return cacheKey, pendingCacheKey, nil
}

// sendWebhook sends a notification to the webhook URL
func (lb *LoadBalancer) sendWebhook(ip string, errMsg string) {
	webhookURL := "https://hooks.slack.com/services/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" // Webhook URL

	// Create the payload
	payload := WebhookPayload{
		Text: fmt.Sprintf("Connection refused to IP %s <!channel>", ip),
	}

	// Convert payload to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error creating webhook payload: %v", err)
		return
	}

	// Create the request
	req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error creating webhook request: %v", err)
		return
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{
		Timeout: 5 * time.Second, // Set a timeout for the webhook call
	}

	// Send the request in a goroutine to not block the main flow
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error sending webhook: %v", err)
			return
		}
		defer resp.Body.Close()

		// Log the response status
		log.Printf("Webhook sent, status: %s", resp.Status)

		// Read and log response body for debugging
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading webhook response: %v", err)
			return
		}

		if len(body) > 0 {
			log.Printf("Webhook response: %s", string(body))
		}
	}()
}

// ServeHTTP implements the http.Handler interface to handle load balancing
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log.Printf("LoadBalancer.ServeHTTP called for %s %s", r.Method, r.URL.Path)

	// Add a diagnostic endpoint
	if r.URL.Path == "/ping" {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","message":"loadbalancer is running"}`))
		log.Printf("Responded to ping request")
		return
	}

	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Only POST requests are supported.", http.StatusMethodNotAllowed)
		log.Printf("Rejected %s request to %s", r.Method, r.URL.Path)
		return
	}

	// Read the request body
	log.Printf("Reading request body")
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		log.Printf("Error reading request body: %v", err)
		return
	}

	// Restore the body for later use
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// Try to parse as ZKProverPayload
	var payload ZKProverPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		http.Error(w, "Invalid payload: could not parse JSON", http.StatusBadRequest)
		log.Printf("Error parsing JSON payload: %v", err)
		return
	}

	// Validate it's a proper ZKProverPayload
	if payload.Operation == "" {
		log.Printf("No operation field found in ZKProverPayload")
		http.Error(w, "Invalid payload: no operation field", http.StatusBadRequest)
		return
	}

	// Validate the payload
	if err := ValidateZKProverPayload(&payload); err != nil {
		http.Error(w, fmt.Sprintf("Invalid payload: %v", err), http.StatusBadRequest)
		log.Printf("Invalid ZKProverPayload: %v", err)
		return
	}

	log.Printf("Valid ZKProverPayload received with operation: %s", payload.Operation)

	// Restore the body again for processing
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// Continue with caching and proxying logic
	if lb.cacheEnabled {
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

			// Try to extract the data and operation fields to use as cache key
			var requestData map[string]interface{}
			if err := json.Unmarshal(bodyBytes, &requestData); err == nil {
				// Check if we have at least the data field
				if _, hasData := requestData["data"]; hasData {
					// Create cache keys using both data and operation fields
					cacheKey, pendingCacheKey, err := getCacheKeyFromRequest(requestData)
					if err != nil {
						log.Printf("Error creating cache key: %v", err)
						lb.handleProxy(w, r) // Fall back to normal proxy
						return
					}

					// Log the cache key components for debugging
					operation := "none"
					if op, hasOp := requestData["operation"]; hasOp {
						operation = fmt.Sprintf("%v", op)
					}
					log.Printf("Cache key created with operation: %s", operation)

					// Check if we have a cached response
					cachedResponse, err := lb.redisClient.Get(ctx, cacheKey).Bytes()
					if err == nil && len(cachedResponse) > 0 {
						// We have a cached response, return it
						log.Printf("Cache hit for key: %s", cacheKey)
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set("X-Cache", "HIT")
						w.Write(cachedResponse)
						return
					}

					// Check if this request is already being processed
					pendingExists, err := lb.redisClient.Exists(ctx, pendingCacheKey).Result()
					if err == nil && pendingExists > 0 {
						// This request is already being processed - we have a duplicate request
						log.Printf("Request for same data+operation is already pending, extending TTL")

						// Since this is a duplicate request for the same data, extend the TTL of the pending key
						err := lb.redisClient.Expire(ctx, pendingCacheKey, lb.cacheExpiration).Err()
						if err != nil {
							log.Printf("Error refreshing pending cache TTL: %v", err)
						} else {
							log.Printf("Refreshed TTL for pending key: %s", pendingCacheKey)
						}

						// Set up a timeout context for the wait
						waitCtx, cancel := context.WithTimeout(ctx, maxWaitTime)
						defer cancel()

						// Set up a ticker to periodically check for the real response
						ticker := time.NewTicker(cacheCheckInterval)
						defer ticker.Stop()

						// Wait for either the real response or timeout
						for {
							select {
							case <-ticker.C:
								// Check if the real response is now available
								cachedResponse, err := lb.redisClient.Get(waitCtx, cacheKey).Bytes()
								if err == nil && len(cachedResponse) > 0 {
									// We have a real response now, return it
									log.Printf("Found cached response while waiting")
									w.Header().Set("Content-Type", "application/json")
									w.Header().Set("X-Cache", "WAIT-HIT")
									w.Write(cachedResponse)
									return
								}

								// Check if the pending key still exists
								pendingExists, err := lb.redisClient.Exists(waitCtx, pendingCacheKey).Result()
								if err != nil || pendingExists == 0 {
									// The pending key is gone but no real response is available
									// This is an error condition - the original request failed
									log.Printf("Pending key disappeared without a real response")
									http.Error(w, "Request processing failed", http.StatusInternalServerError)
									return
								}

							case <-waitCtx.Done():
								// We've waited too long, give up
								log.Printf("Timeout waiting for real response after %v", maxWaitTime)
								http.Error(w, "Timeout waiting for response", http.StatusGatewayTimeout)
								return

							case <-r.Context().Done():
								// The client disconnected
								log.Printf("Client disconnected while waiting for response")
								return
							}
						}
					}

					// This is a new request, store a placeholder in Redis
					log.Printf("Setting pending cache key: %s", pendingCacheKey)
					err = lb.redisClient.Set(ctx, pendingCacheKey, []byte(placeholderResponse), lb.cacheExpiration).Err()
					if err != nil {
						log.Printf("Error setting pending cache: %v", err)
						// Continue with proxy even if caching fails
					}

					// Create a response capturer to update the cache with the real response
					responseCapture := &responseCapturer{
						ResponseWriter:  w,
						cacheKey:        cacheKey,
						pendingCacheKey: pendingCacheKey,
						lb:              lb,
						ctx:             ctx,
					}

					// Continue with normal proxy handling but use our capturing writer
					log.Printf("Proceeding with proxy using response capturer")
					lb.handleProxy(responseCapture, r)
					return
				}
			}

			// Restore the body again if we couldn't use caching
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}
	}

	// Handle the proxy request
	lb.handleProxy(w, r)
}

func (lb *LoadBalancer) handleProxy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		log.Printf("Error reading request body: %v", err)
		return
	}

	// Restore the body for later use
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// Try to parse as ZKProverPayload
	var payload ZKProverPayload
	var isProve bool

	if err := json.Unmarshal(bodyBytes, &payload); err == nil {
		isProve = payload.Operation == "prove"
	}

	// Keep track of the last error
	var lastErr error

	var currentIP string

	// Make sure we always release any IP we've acquired
	defer func() {
		if currentIP != "" {
			log.Printf("Request context done, releasing IP %s", currentIP)
			if err := lb.releaseIP(context.Background(), currentIP); err != nil {
				log.Printf("Error releasing IP %s during cleanup: %v", currentIP, err)
			}
		}
	}()

	// Try to find a working IP - infinite loop until success or context cancellation
	for {
		// Check if the context is already done (client disconnected or timeout)
		select {
		case <-ctx.Done():
			if lastErr != nil {
				http.Error(w, "Request cancelled: "+lastErr.Error(), http.StatusGatewayTimeout)
			} else {
				http.Error(w, "Request cancelled", http.StatusGatewayTimeout)
			}
			log.Printf("Request context cancelled: %v", ctx.Err())
			return
		default:
			// Continue with the request
		}

		// Get the next available IP, waiting if necessary
		ip, err := lb.waitForAvailableIP(ctx, isProve)
		if err != nil {
			http.Error(w, "Failed to get available server: "+err.Error(), http.StatusServiceUnavailable)
			log.Printf("Error waiting for available IP: %v", err)
			return
		}

		// Track the current IP so we can release it in the defer function if needed
		currentIP = ip
		log.Printf("Attempting to proxy request to IP: %s", ip)

		// Create a URL from the IP
		// Split IP into base IP and type (prove/verify)
		parts := strings.Split(ip, ":")
		baseIP := parts[0]
		targetURL, err := url.Parse("http://" + baseIP + ":3001")
		log.Printf("targetURL: %s", targetURL)
		if err != nil {
			lb.releaseIP(ctx, ip)
			log.Printf("Error parsing URL for IP %s: %v", ip, err)
			lastErr = err
			continue // Try next IP
		}

		// Create a reverse proxy
		proxy := httputil.NewSingleHostReverseProxy(targetURL)

		// Set up a custom director to modify the request
		originalDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			originalDirector(req)
			req.Host = targetURL.Host

			// Add debugging headers
			req.Header.Set("X-Forwarded-For", req.RemoteAddr)
			req.Header.Set("X-Forwarded-Host", req.Host)
			req.Header.Set("X-Forwarded-Proto", "http")

			// Log the outgoing request for debugging
			log.Printf("Proxying request to %s: %s %s", targetURL.String(), req.Method, req.URL.Path)
		}

		// Create a channel to capture proxy errors
		proxyErrChan := make(chan error, 1)

		// Set up error handler
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("Proxy error handler triggered: %v", err)
			proxyErrChan <- err
		}

		// Set up a custom response writer that will release the IP when done
		responseWriter := &responseWriterWithCallback{
			ResponseWriter: w,
			callback: func() {
				// Release the IP when the response is complete
				if err := lb.releaseIP(ctx, ip); err != nil {
					log.Printf("Error releasing IP %s: %v", ip, err)
				}
				log.Printf("Response completed and IP %s released", ip)
			},
		}

		// Create a context with cancel for this specific proxy attempt
		proxyCtx, cancelProxy := context.WithCancel(ctx)

		// Create a channel to signal when the proxy is done
		proxyDone := make(chan struct{})

		// Forward the request to the target server in a goroutine
		go func() {
			// We need to clone the request to use the new context
			proxyReq := r.Clone(proxyCtx)

			// If this is a POST request with a body, we need to recreate the body
			if proxyReq.Body != nil && proxyReq.Method == http.MethodPost {
				// Read the body
				bodyBytes, err := io.ReadAll(r.Body)
				if err != nil {
					proxyErrChan <- fmt.Errorf("error reading request body: %w", err)
					close(proxyDone)
					return
				}

				// Log the request body for debugging
				log.Printf("Request body: %s", string(bodyBytes))

				// Restore the original body for future attempts
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

				// Set the body for this attempt
				proxyReq.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				proxyReq.ContentLength = int64(len(bodyBytes))
			}

			// Log that we're about to call ServeHTTP
			log.Printf("Calling proxy.ServeHTTP for IP %s", ip)

			// Serve the request
			proxy.ServeHTTP(responseWriter, proxyReq)

			// Log that ServeHTTP has completed
			log.Printf("proxy.ServeHTTP completed for IP %s", ip)

			// Make sure to finalize the response writer to release the IP
			responseWriter.Finalize()

			close(proxyDone)
		}()

		// Wait for either the proxy to complete or an error
		select {
		case <-proxyDone:
			// Proxy completed successfully
			log.Printf("Proxy completed successfully for IP %s", ip)
			cancelProxy() // Clean up the context
			return

		case err := <-proxyErrChan:
			log.Printf("Received error from proxy for IP %s: %v", ip, err)

			// Check if it's a connection refused error
			if isConnectionRefused(err) {
				log.Printf("Connection refused for IP %s, trying next IP", ip)

				// Send webhook notification about the connection refused
				lb.sendWebhook(ip, err.Error())

				lb.releaseIP(ctx, ip)
				lastErr = err
				cancelProxy() // Cancel the current proxy attempt

				// Add a small delay before trying the next IP to avoid hammering servers
				select {
				case <-time.After(100 * time.Millisecond):
					// Continue after delay
				case <-ctx.Done():
					// Client disconnected during delay
					return
				}

				continue // Try next IP
			}

			// For other errors, fail immediately
			lb.releaseIP(ctx, ip)
			cancelProxy() // Clean up the context
			http.Error(w, "Server error: "+err.Error(), http.StatusBadGateway)
			log.Printf("Proxy error for IP %s: %v", ip, err)
			return
		}
	}
}

// isConnectionRefused checks if an error is a connection refused error
func isConnectionRefused(err error) bool {
	if err == nil {
		return false
	}

	// Check for "connection refused" in the error message
	if strings.Contains(err.Error(), "connection refused") {
		return true
	}

	// Check for specific network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}

	// Check for specific syscall errors
	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		return true
	}

	return false
}

// responseWriterWithCallback wraps http.ResponseWriter to call a callback when the response is complete
type responseWriterWithCallback struct {
	http.ResponseWriter
	callback func()
	once     sync.Once
	written  bool
	status   int
}

func (w *responseWriterWithCallback) Write(b []byte) (int, error) {
	w.written = true
	n, err := w.ResponseWriter.Write(b)
	log.Printf("responseWriterWithCallback.Write: wrote %d bytes, err: %v", n, err)
	return n, err
}

func (w *responseWriterWithCallback) WriteHeader(statusCode int) {
	w.status = statusCode
	log.Printf("responseWriterWithCallback.WriteHeader: status %d", statusCode)
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterWithCallback) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		log.Printf("responseWriterWithCallback.Flush called")
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
	log.Printf("responseWriterWithCallback.Hijack called")
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
	log.Printf("responseWriterWithCallback.Finalize called, written: %v, status: %d", w.written, w.status)
	w.once.Do(w.callback)
}

// Make sure responseWriterWithCallback implements all necessary interfaces
var (
	_ http.ResponseWriter = &responseWriterWithCallback{}
	_ http.Flusher        = &responseWriterWithCallback{}
	_ http.Hijacker       = &responseWriterWithCallback{}
)

// responseCapturer captures the response to cache it
type responseCapturer struct {
	http.ResponseWriter
	cacheKey        string
	pendingCacheKey string
	lb              *LoadBalancer
	ctx             context.Context
	buffer          bytes.Buffer
	status          int
	headers         http.Header
	written         bool
	once            sync.Once
}

func (rc *responseCapturer) WriteHeader(statusCode int) {
	rc.status = statusCode
	rc.ResponseWriter.WriteHeader(statusCode)
	log.Printf("responseCapturer.WriteHeader: status %d", statusCode)
}

func (rc *responseCapturer) Write(b []byte) (int, error) {
	// Mark that we've written data
	rc.written = true

	// Only cache successful responses
	if rc.status == 0 || rc.status == http.StatusOK {
		rc.buffer.Write(b)
	}

	n, err := rc.ResponseWriter.Write(b)
	log.Printf("responseCapturer.Write: wrote %d bytes, err: %v", n, err)

	// Update cache as soon as we get data (don't wait for Finalize)
	// This ensures the cache is updated even if the client disconnects
	if rc.written && (rc.status == 0 || rc.status == http.StatusOK) {
		rc.updateCache()
	}

	return n, err
}

func (rc *responseCapturer) updateCache() {
	// Only update the cache once
	rc.once.Do(func() {
		if rc.buffer.Len() > 0 {
			log.Printf("Updating cache for key: %s", rc.cacheKey)

			// Use a pipeline to update both cache keys atomically
			pipe := rc.lb.redisClient.Pipeline()

			// Store the real response
			pipe.Set(rc.ctx, rc.cacheKey, rc.buffer.Bytes(), rc.lb.cacheExpiration)

			// Remove the pending marker
			pipe.Del(rc.ctx, rc.pendingCacheKey)

			// Execute the pipeline
			if _, err := pipe.Exec(rc.ctx); err != nil {
				log.Printf("Error updating cache: %v", err)
			} else {
				log.Printf("Successfully updated cache and removed pending key")
			}
		} else {
			log.Printf("Not updating cache - buffer is empty")
		}
	})
}

func (rc *responseCapturer) Flush() {
	if flusher, ok := rc.ResponseWriter.(http.Flusher); ok {
		log.Printf("responseCapturer.Flush called")
		flusher.Flush()
	}
}

func (rc *responseCapturer) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	log.Printf("responseCapturer.Hijack called")
	if hj, ok := rc.ResponseWriter.(http.Hijacker); ok {
		conn, buf, err := hj.Hijack()
		if err == nil {
			// Update cache before hijacking
			rc.updateCache()
		}
		return conn, buf, err
	}
	return nil, nil, errors.New("hijacking not supported")
}

// Finalize calls the callback
func (rc *responseCapturer) Finalize() {
	log.Printf("responseCapturer.Finalize called, written: %v, status: %d", rc.written, rc.status)

	// Update cache one last time if needed
	rc.updateCache()

	// Call the original finalizer if it exists
	if finalizer, ok := rc.ResponseWriter.(interface{ Finalize() }); ok {
		finalizer.Finalize()
	}
}

// Make sure responseCapturer implements all necessary interfaces
var (
	_ http.ResponseWriter = &responseCapturer{}
	_ http.Flusher        = &responseCapturer{}
	_ http.Hijacker       = &responseCapturer{}
)

// ZKProverPayload represents the payload for ZK proving operations
type ZKProverPayload struct {
	Operation    string      `json:"operation"`
	Data         []string    `json:"data"`
	ProofRequest *string     `json:"proof_request,omitempty"`
	Proof        interface{} `json:"proof,omitempty"`
}

// ValidateZKProverPayload validates the ZKProverPayload struct
// Returns an error if validation fails, nil otherwise
func ValidateZKProverPayload(payload *ZKProverPayload) error {
	// Validate operation type
	log.Printf("Validating operation: %s", payload.Operation)
	if payload.Operation != "verify" && payload.Operation != "prove" {
		log.Printf("Invalid operation: %s. Must be 'verify' or 'prove'", payload.Operation)
		return fmt.Errorf("invalid operation: %s. Must be 'verify' or 'prove'", payload.Operation)
	}
	log.Printf("Validated operation: %s", payload.Operation)
	// Validate required fields based on operation
	if payload.Operation == "prove" {
		// For "prove" operation, ProofRequest should be set and Proof should be nil
		if payload.ProofRequest == nil {
			return fmt.Errorf("prove operation requires proof_request field")
		}
		if payload.Proof != nil {
			return fmt.Errorf("prove operation should not include proof field")
		}
	} else if payload.Operation == "verify" {
		// For "verify" operation, Proof should be set and ProofRequest should be nil
		if payload.Proof == nil {
			return fmt.Errorf("verify operation requires proof field")
		}
		if payload.ProofRequest != nil {
			return fmt.Errorf("verify operation should not include proof_request field")
		}
	}

	// Validate data field
	if len(payload.Data) == 0 {
		return fmt.Errorf("data field is required and cannot be empty")
	}

	return nil
}
