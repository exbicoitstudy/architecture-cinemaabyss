package main

import (
    "encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	port := getEnv("PORT", "8000")

	monolithURL := getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")

	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")
	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		migrationPercent = 50
	}

	monolithProxy := createReverseProxy(monolithURL)
	moviesProxy := createReverseProxy(moviesServiceURL)

	rand.Seed(time.Now().UnixNano())

    http.HandleFunc("/api/proxy/health", handleHealth)

	http.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		n := rand.Intn(100)
		if n < migrationPercent {
			moviesProxy.ServeHTTP(w, r)
		} else {
			monolithProxy.ServeHTTP(w, r)
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		monolithProxy.ServeHTTP(w, r)
	})

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func createReverseProxy(target string) *httputil.ReverseProxy {
	url, err := url.Parse(target)
	if err != nil {
		log.Fatalf("Invalid service URL: %s", target)
	}
	proxy := httputil.NewSingleHostReverseProxy(url)

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = url.Host
	}
	return proxy
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}
