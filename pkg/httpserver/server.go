package httpserver

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
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
	config    *Config
	executor  *executor.Executor  // Default executor (for backward compatibility)
	schema    *storage.SchemaManager
	dbManager *storage.DatabaseManager // Multi-database support
	server    *http.Server
	stats     *Stats

	// Per-server executor cache for multi-database support
	executorCache   map[string]*executor.Executor
	executorCacheMu sync.RWMutex
}

// Stats tracks server statistics.
type Stats struct {
	QueriesExecuted int64
	QueriesSuccess  int64
	QueriesError    int64
	StartTime       time.Time
}

// New creates a new HTTP server.
// Deprecated: Use NewWithDatabaseManager for multi-database support.
func New(config *Config, exec *executor.Executor, schema *storage.SchemaManager) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	s := &Server{
		config:        config,
		executor:      exec,
		schema:        schema,
		executorCache: make(map[string]*executor.Executor),
		stats: &Stats{
			StartTime: time.Now(),
		},
	}

	return s.init()
}

// NewWithDatabaseManager creates a new HTTP server with multi-database support.
func NewWithDatabaseManager(config *Config, dbManager *storage.DatabaseManager) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	// Initialize executor cache
	execCache := make(map[string]*executor.Executor)

	// Get the default database for backward compatibility
	defaultDB, _ := dbManager.GetDatabase("")
	var defaultExec *executor.Executor
	var defaultSchema *storage.SchemaManager
	if defaultDB != nil {
		defaultExec = executor.New(defaultDB.Schema, defaultDB.Table)
		defaultExec.SyncCatalog()
		defaultSchema = defaultDB.Schema
		// Pre-populate cache with default executor
		execCache[defaultDB.Name] = defaultExec
	}

	s := &Server{
		config:        config,
		executor:      defaultExec,
		schema:        defaultSchema,
		dbManager:     dbManager,
		executorCache: execCache,
		stats: &Stats{
			StartTime: time.Now(),
		},
	}

	return s.init()
}

// init initializes the server routes and middleware.
func (s *Server) init() *Server {
	mux := http.NewServeMux()

	// Apply middleware (order matters: logging -> auth -> cors -> compression -> handler)
	var handler http.Handler = mux

	if s.config.EnableCompression {
		handler = s.compressionMiddleware(handler)
	}

	if s.config.EnableCORS {
		handler = s.corsMiddleware(handler)
	}

	if s.config.EnableAuth {
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
	mux.HandleFunc("/export", s.handleExport)
	mux.HandleFunc("/import", s.handleImport)

	s.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", s.config.Host, s.config.Port),
		Handler:      handler,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
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

// getExecutorForDatabase returns an executor for the specified database.
// If dbName is empty, returns the default executor.
// If multi-database support is not enabled, always returns the default executor.
func (s *Server) getExecutorForDatabase(dbName string) (*executor.Executor, *storage.SchemaManager, error) {
	// If no database manager, use the default executor
	if s.dbManager == nil {
		return s.executor, s.schema, nil
	}

	// Get the database instance - this ensures we get the correct SchemaManager
	dbInstance, err := s.dbManager.GetDatabase(dbName)
	if err != nil {
		return nil, nil, err
	}

	// IMPORTANT: Always use dbInstance.Schema for isolation
	// The SchemaManager contains the database name and ensures queries
	// are scoped to the correct database namespace

	// Check per-server executor cache
	s.executorCacheMu.RLock()
	exec, exists := s.executorCache[dbInstance.Name]
	s.executorCacheMu.RUnlock()

	if exists {
		// Return cached executor with the correct schema from dbInstance
		return exec, dbInstance.Schema, nil
	}

	// Create new executor and cache it
	s.executorCacheMu.Lock()
	defer s.executorCacheMu.Unlock()

	// Double-check after acquiring write lock
	if exec, exists := s.executorCache[dbInstance.Name]; exists {
		return exec, dbInstance.Schema, nil
	}

	// Create executor with the database-specific schema and table managers
	exec = executor.New(dbInstance.Schema, dbInstance.Table)
	exec.SyncCatalog()
	s.executorCache[dbInstance.Name] = exec

	log.Printf("Created executor for database: %s", dbInstance.Name)

	return exec, dbInstance.Schema, nil
}
