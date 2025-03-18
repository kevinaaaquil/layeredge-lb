package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/kevinaaaquil/layeredge-lb/loadbalancer"
	"golang.org/x/time/rate"
)

const (
	defaultCacheExpiration = 30 * time.Minute
	defaultRateLimit       = 2 // requests per minute
	defaultBurstSize       = 4 // burst capacity
	cleanupInterval        = 5 * time.Minute
)

// IPRateLimiter implements per-IP rate limiting
type IPRateLimiter struct {
	ips       map[string]*rate.Limiter
	mu        sync.Mutex
	rateLimit float64 // requests per minute
	burst     int
}

// NewIPRateLimiter creates a new rate limiter for IP addresses
func NewIPRateLimiter(ratePerMin float64, burst int) *IPRateLimiter {
	// Convert rate per minute to rate per second for the underlying limiter
	rl := &IPRateLimiter{
		ips:       make(map[string]*rate.Limiter),
		rateLimit: ratePerMin,
		burst:     burst,
	}

	// Start a goroutine to periodically clean up old limiters
	go rl.cleanupLimiters()

	return rl
}

// AddIP adds an IP address to the limiter
func (rl *IPRateLimiter) AddIP(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Convert rate per minute to rate per second
	ratePerSec := rl.rateLimit / 60.0
	limiter := rate.NewLimiter(rate.Limit(ratePerSec), rl.burst)
	rl.ips[ip] = limiter

	return limiter
}

// GetLimiter returns the rate limiter for the provided IP address
func (rl *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, exists := rl.ips[ip]
	if !exists {
		limiter = rl.AddIP(ip)
	}

	return limiter
}

// cleanupLimiters periodically removes unused IP limiters to prevent memory leaks
func (rl *IPRateLimiter) cleanupLimiters() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		// In a real implementation, you might track last access time
		// and remove limiters that haven't been used recently
		log.Printf("Cleaning up rate limiters, current count: %d", len(rl.ips))
		rl.mu.Unlock()
	}
}

// RateLimiterMiddleware implements a per-IP rate limiter middleware
type RateLimiterMiddleware struct {
	limiter *IPRateLimiter
	next    http.Handler
}

func NewRateLimiterMiddleware(rpm float64, burst int, next http.Handler) *RateLimiterMiddleware {
	return &RateLimiterMiddleware{
		limiter: NewIPRateLimiter(rpm, burst),
		next:    next,
	}
}

func (m *RateLimiterMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Get the client's IP address
	ip := getClientIP(r)

	// Get the rate limiter for this IP
	limiter := m.limiter.GetLimiter(ip)

	if !limiter.Allow() {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		log.Printf("Rate limit exceeded for IP: %s", ip)
		return
	}

	// Set headers to inform clients about rate limits
	w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%.0f", m.limiter.rateLimit))
	w.Header().Set("X-RateLimit-Remaining", "N/A") // Would need additional tracking to compute accurately

	m.next.ServeHTTP(w, r)
}

// getClientIP extracts the client IP address from the request
func getClientIP(r *http.Request) string {
	// Check for X-Forwarded-For header first (when behind a proxy)
	ip := r.Header.Get("X-Forwarded-For")
	if ip != "" {
		// X-Forwarded-For can contain multiple IPs; take the first one
		ips := strings.Split(ip, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check for X-Real-IP header next
	ip = r.Header.Get("X-Real-IP")
	if ip != "" {
		return ip
	}

	// Fall back to RemoteAddr
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// If we can't split it, just return the RemoteAddr as is
		return r.RemoteAddr
	}

	return ip
}

func main() {
	// Read Redis URL from environment
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0" // default value
	}
	log.Printf("REDIS_URL: %s", redisURL)

	// Read other configuration from environment variables
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080" // default value
	}
	log.Printf("LISTEN_ADDR: %s", listenAddr)

	timeoutSec := 60 * 10 // 10 minutes
	if timeoutStr := os.Getenv("TIMEOUT_SEC"); timeoutStr != "" {
		if timeout, err := strconv.Atoi(timeoutStr); err == nil {
			timeoutSec = timeout
		}
	}
	log.Printf("TIMEOUT_SEC: %d", timeoutSec)

	cacheEnabled := true
	if cacheStr := os.Getenv("CACHE_ENABLED"); cacheStr == "false" || cacheStr == "0" {
		cacheEnabled = false
	}

	cacheExpiration := defaultCacheExpiration
	if expirationStr := os.Getenv("CACHE_EXPIRATION_SECONDS"); expirationStr != "" {
		if expiration, err := strconv.Atoi(expirationStr); err == nil && expiration > 0 {
			cacheExpiration = time.Duration(expiration) * time.Second
		}
	}

	// Read IPs from environment
	ipsEnv := os.Getenv("IPS")
	var ips []string

	if ipsEnv != "" {
		// Split IPs by comma
		ips = strings.Split(ipsEnv, ",")
		// Trim whitespace from each IP
		for i, ip := range ips {
			ips[i] = strings.TrimSpace(ip)
		}
		log.Printf("Loaded %d IPs from environment", len(ips))
	} else {
		log.Println("No IPs specified in environment")
	}

	// Initialize load balancer with Redis URL
	lb, err := loadbalancer.NewLoadBalancerWithURL(
		redisURL,
		time.Duration(timeoutSec)*time.Second,
		cacheEnabled,
		cacheExpiration,
	)
	if err != nil {
		log.Fatalf("Failed to create load balancer: %v", err)
	}

	// Add IPs to the load balancer
	ctx := context.Background()
	for _, ip := range ips {
		if ip != "" {
			if err := lb.AddIP(ctx, ip); err != nil {
				log.Printf("Failed to add IP %s: %v", ip, err)
			} else {
				log.Printf("Added IP %s to load balancer", ip)
			}
		}
	}

	// Check if we need to enable rate limiting
	var handler http.Handler = lb
	appEnv := os.Getenv("APP_ENV")

	if appEnv == "public" {
		// Read rate limit configuration - now in requests per minute
		rateLimit := defaultRateLimit
		if rateLimitStr := os.Getenv("RATE_LIMIT_PER_MINUTE"); rateLimitStr != "" {
			if rl, err := strconv.Atoi(rateLimitStr); err == nil && rl > 0 {
				rateLimit = rl
			}
		}

		burstSize := defaultBurstSize
		if burstSizeStr := os.Getenv("BURST_SIZE"); burstSizeStr != "" {
			if bs, err := strconv.Atoi(burstSizeStr); err == nil && bs > 0 {
				burstSize = bs
			}
		}

		log.Printf("PUBLIC MODE ENABLED: Rate limiting to %d requests/minute per IP with burst size %d", rateLimit, burstSize)
		handler = NewRateLimiterMiddleware(float64(rateLimit), burstSize, lb)
	} else {
		log.Printf("Running in non-public mode, rate limiting disabled")
	}

	// Start the HTTP server
	log.Printf("Starting load balancer on %s", listenAddr)
	server := &http.Server{
		Addr:    listenAddr,
		Handler: handler,
	}
	log.Fatal(server.ListenAndServe())
}
