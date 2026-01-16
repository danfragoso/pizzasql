package httpserver

import (
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// gzipPool is a pool of gzip writers to reduce allocations.
var gzipPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(io.Discard)
	},
}

// gzipResponseWriter wraps http.ResponseWriter to provide gzip compression.
type gzipResponseWriter struct {
	http.ResponseWriter
	writer *gzip.Writer
}

func (g *gzipResponseWriter) Write(data []byte) (int, error) {
	return g.writer.Write(data)
}

// compressionMiddleware adds gzip compression for responses.
func (s *Server) compressionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if client accepts gzip
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		// Get gzip writer from pool
		gz := gzipPool.Get().(*gzip.Writer)
		gz.Reset(w)
		defer func() {
			gz.Close()
			gzipPool.Put(gz)
		}()

		// Set headers
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length") // Length changes with compression

		// Wrap response writer
		gzw := &gzipResponseWriter{ResponseWriter: w, writer: gz}
		next.ServeHTTP(gzw, r)
	})
}

// loggingMiddleware logs HTTP requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		lw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(lw, r)

		duration := time.Since(start)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, lw.statusCode, duration)
	})
}

// loggingResponseWriter wraps http.ResponseWriter to capture status code.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lw *loggingResponseWriter) WriteHeader(code int) {
	lw.statusCode = code
	lw.ResponseWriter.WriteHeader(code)
}

// corsMiddleware adds CORS headers.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// authMiddleware validates API keys.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health check
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		// Check Authorization header
		auth := r.Header.Get("Authorization")
		if auth == "" {
			writeError(w, http.StatusUnauthorized, "MISSING_AUTH", "Authorization header is required", nil)
			return
		}

		// Simple bearer token validation
		var token string
		if len(auth) > 7 && auth[:7] == "Bearer " {
			token = auth[7:]
		} else {
			writeError(w, http.StatusUnauthorized, "INVALID_AUTH", "Invalid authorization format", nil)
			return
		}

		// Validate token against API keys
		valid := false
		for _, key := range s.config.APIKeys {
			if token == key {
				valid = true
				break
			}
		}

		if !valid {
			writeError(w, http.StatusForbidden, "INVALID_API_KEY", "Invalid API key", nil)
			return
		}

		next.ServeHTTP(w, r)
	})
}
