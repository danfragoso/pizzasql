package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// Config holds server configuration
type Config struct {
	Host            string
	Port            int
	MaxConnections  int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	DefaultDatabase string
	Quiet           bool // Disable query logging
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Host:            "localhost",
		Port:            5432,
		MaxConnections:  100,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		DefaultDatabase: "pizzasql",
	}
}

// Server represents the PostgreSQL wire protocol server
type Server struct {
	config    *Config
	dbManager *storage.DatabaseManager
	listener  net.Listener
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// New creates a new PostgreSQL wire protocol server
func New(config *Config, dbManager *storage.DatabaseManager) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		config:    config,
		dbManager: dbManager,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start starts the server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start PostgreSQL server: %w", err)
	}

	s.listener = listener
	log.Printf("PostgreSQL wire protocol server listening on %s", addr)

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				// Server is shutting down
				return nil
			default:
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle connection in a goroutine
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

// handleConnection handles a client connection
func (s *Server) handleConnection(conn net.Conn) {
	// Don't set static deadlines - let the connection be persistent
	// Timeouts will be handled by context cancellation if needed

	c := NewConnection(conn, s.dbManager, s.config.Quiet)

	if err := c.Handle(); err != nil {
		log.Printf("Connection error: %v", err)
	}
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down PostgreSQL server...")

	// Cancel context to stop accepting new connections
	s.cancel()

	// Close listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}

	// Wait for connections to finish with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("PostgreSQL server stopped gracefully")
		return nil
	case <-ctx.Done():
		log.Println("PostgreSQL server shutdown timeout")
		return ctx.Err()
	}
}

// Addr returns the server address
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
}
