package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/kevinaaaquil/layeredge-lb/loadbalancer"
)

const (
	defaultCacheExpiration = 30 * time.Minute
)

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

	timeoutSec := 60 * 15 // 15 minutes
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
			log.Printf("Loaded IP %s from environment", ips[i])
		}
		log.Printf("Loaded %d IPs from environment", len(ips))
	} else {
		log.Println("No IPs specified in environment")
	}

	// Initialize load balancer
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
	for i, ip := range ips {
		if ip != "" {
			if err := lb.AddIP(ctx, ip, i%2 == 0); err != nil {
				log.Printf("Failed to add IP %s: %v", ip, err)
			}
		}
	}

	// Start the HTTP server
	log.Printf("Starting load balancer on %s", listenAddr)
	server := &http.Server{
		Addr:    listenAddr,
		Handler: lb,
	}
	log.Fatal(server.ListenAndServe())
}
