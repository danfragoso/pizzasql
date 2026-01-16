package httpserver

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/executor"
	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// Config holds HTTP server configuration.
type Config struct {
	Host              string
	Port              int
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	MaxConnections    int
	EnableCORS        bool
	EnableAuth        bool
	EnableCompression bool
	APIKeys           []string
	TLSCertFile       string
	TLSKeyFile        string
}

// DefaultConfig returns default server configuration.
func DefaultConfig() *Config {
	return &Config{
		Host:              "localhost",
		Port:              8080,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		MaxConnections:    1000,
		EnableCORS:        true,
		EnableAuth:        false,
		EnableCompression: true,
		APIKeys:           []string{},
	}
}

// Server represents the HTTP API server.
type Server struct {
	config   *Config
	executor *executor.Executor
	schema   *storage.SchemaManager
	server   *http.Server
	stats    *Stats
}

// Stats tracks server statistics.
type Stats struct {
	QueriesExecuted int64
	QueriesSuccess  int64
	QueriesError    int64
	StartTime       time.Time
}

// New creates a new HTTP server.
func New(config *Config, exec *executor.Executor, schema *storage.SchemaManager) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	s := &Server{
		config:   config,
		executor: exec,
		schema:   schema,
		stats: &Stats{
			StartTime: time.Now(),
		},
	}

	mux := http.NewServeMux()

	// Apply middleware (order matters: logging -> auth -> cors -> compression -> handler)
	var handler http.Handler = mux

	if config.EnableCompression {
		handler = s.compressionMiddleware(handler)
	}

	if config.EnableCORS {
		handler = s.corsMiddleware(handler)
	}

	if config.EnableAuth {
		handler = s.authMiddleware(handler)
	}

	handler = s.loggingMiddleware(handler)

	// Register routes
	mux.HandleFunc("/query", s.handleQuery)
	mux.HandleFunc("/execute", s.handleExecute)
	mux.HandleFunc("/schema/tables", s.handleSchemaTables)
	mux.HandleFunc("/schema/tables/", s.handleSchemaTable)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/transaction/begin", s.handleTransactionBegin)
	mux.HandleFunc("/transaction/commit", s.handleTransactionCommit)
	mux.HandleFunc("/transaction/rollback", s.handleTransactionRollback)

	s.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", config.Host, config.Port),
		Handler:      handler,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	return s
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	addr := s.server.Addr
	log.Printf("Starting HTTP server on http://%s", addr)

	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		return s.server.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile)
	}

	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down HTTP server...")
	return s.server.Shutdown(ctx)
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.server.Addr
}
