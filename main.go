package main

import (
	"bufio"
	"context"
	"fmt"
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
	// // load env from .env file
	// err := godotenv.Load()
	// if err != nil {
	// 	log.Fatal("Error loading .env file")
	// }

	// Read configuration from environment variables
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		fmt.Println("REDIS_ADDR is not set")
		redisAddr = "localhost:6379" // default value
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisDB := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		if db, err := strconv.Atoi(dbStr); err == nil {
			redisDB = db
		}
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080" // default value
	}

	timeoutSec := 60 * 10 // 10 minutes
	if timeoutStr := os.Getenv("TIMEOUT_SEC"); timeoutStr != "" {
		if timeout, err := strconv.Atoi(timeoutStr); err == nil {
			timeoutSec = timeout
		}
	}

	// // Read IPs from file
	// ipsFilePath := os.Getenv("IPS_FILE")
	// if ipsFilePath == "" {
	// 	ipsFilePath = "ips.txt" // default value
	// }

	// ips, err := readIPsFromFile(ipsFilePath)
	// if err != nil {
	// 	log.Fatalf("Failed to read IPs from file: %v \n Error: %v", ipsFilePath, err)
	// }

	// if len(ips) == 0 {
	// 	log.Fatalf("No IPs provided in the file: %v", ipsFilePath)
	// }

	cacheEnabled := true
	if cacheStr := os.Getenv("CACHE_ENABLED"); cacheStr == "true" || cacheStr == "1" {
		cacheEnabled = true
	}

	cacheExpiration := defaultCacheExpiration
	if expirationStr := os.Getenv("CACHE_EXPIRATION_SECONDS"); expirationStr != "" {
		if expiration, err := strconv.Atoi(expirationStr); err == nil && expiration > 0 {
			cacheExpiration = time.Duration(expiration) * time.Second
		}
	}

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

	// Initialize load balancer with caching options
	lb, err := loadbalancer.NewLoadBalancer(
		redisAddr,
		redisPassword,
		redisDB,
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
		if err := lb.AddIP(ctx, ip); err != nil {
			log.Printf("Failed to add IP %s: %v", ip, err)
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

// readIPsFromFile reads IPs from a text file, one IP per line
func readIPsFromFile(filepath string) ([]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var ips []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ip := strings.TrimSpace(scanner.Text())
		if ip != "" && !strings.HasPrefix(ip, "#") { // Skip empty lines and comments
			ips = append(ips, ip)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ips, nil
}
