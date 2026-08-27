package storage

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// KVClient represents a connection to PizzaKV.
type KVClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex
}

// NewKVClient creates a new KV client connected to the given address.
// addr may be "host:port" for TCP or "unix:<path>" for a Unix socket.
func NewKVClient(addr string) (*KVClient, error) {
	network, target := parseAddr(addr)
	conn, err := net.Dial(network, target)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PizzaKV: %w", err)
	}

	return &KVClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

// parseAddr splits an addr string into (network, address).
// "unix:<path>" → ("unix", "<path>"), anything else → ("tcp", addr).
func parseAddr(addr string) (string, string) {
	if strings.HasPrefix(addr, "unix:") {
		return "unix", strings.TrimPrefix(addr, "unix:")
	}
	return "tcp", addr
}

// Close closes the connection.
func (c *KVClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SetDeadline sets the read/write deadline.
func (c *KVClient) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// Write stores a key-value pair.
func (c *KVClient) Write(key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("write %s|%s\r", key, value)
	if _, err := c.writer.WriteString(cmd); err != nil {
		return fmt.Errorf("write command failed: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	resp, err := c.reader.ReadString('\r')
	if err != nil {
		return fmt.Errorf("read response failed: %w", err)
	}

	resp = strings.TrimSuffix(resp, "\r")
	if resp != "success" {
		return fmt.Errorf("write failed: %s", resp)
	}

	return nil
}

// Read retrieves a value by key.
func (c *KVClient) Read(key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("read %s\r", key)
	if _, err := c.writer.WriteString(cmd); err != nil {
		return "", fmt.Errorf("read command failed: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("flush failed: %w", err)
	}

	resp, err := c.reader.ReadString('\r')
	if err != nil {
		return "", fmt.Errorf("read response failed: %w", err)
	}

	resp = strings.TrimSuffix(resp, "\r")
	if resp == "error" {
		return "", ErrKeyNotFound
	}

	return resp, nil
}

// Delete removes a key.
func (c *KVClient) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("delete %s\r", key)
	if _, err := c.writer.WriteString(cmd); err != nil {
		return fmt.Errorf("delete command failed: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	resp, err := c.reader.ReadString('\r')
	if err != nil {
		return fmt.Errorf("read response failed: %w", err)
	}

	resp = strings.TrimSuffix(resp, "\r")
	if resp != "success" && resp != "error" {
		return fmt.Errorf("delete failed: %s", resp)
	}

	return nil
}

// Reads retrieves all values with a key prefix.
func (c *KVClient) Reads(prefix string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("reads %s\r", prefix)
	if _, err := c.writer.WriteString(cmd); err != nil {
		return nil, fmt.Errorf("reads command failed: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("flush failed: %w", err)
	}

	resp, err := c.reader.ReadString('\r')
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	resp = strings.TrimSuffix(resp, "\r")
	if resp == "" {
		return nil, nil
	}

	values := strings.Split(resp, "\n")
	result := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			result = append(result, v)
		}
	}

	return result, nil
}

// IsAlive checks if the connection is still alive.
func (c *KVClient) IsAlive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return false
	}

	// Try to set a short deadline and do a no-op check
	c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer c.conn.SetReadDeadline(time.Time{})

	one := make([]byte, 1)
	c.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	_, err := c.conn.Read(one)

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return true // Timeout is expected
		}
		return false
	}
	return true
}

// ErrKeyNotFound is returned when a key doesn't exist.
var ErrKeyNotFound = fmt.Errorf("key not found")

// KVPool manages a pool of KV client connections.
type KVPool struct {
	addr    string
	pool    chan *KVClient
	size    int
	timeout time.Duration
	mu      sync.Mutex
	closed  bool
}

// NewKVPool creates a new connection pool.
func NewKVPool(addr string, size int, timeout time.Duration) (*KVPool, error) {
	p := &KVPool{
		addr:    addr,
		pool:    make(chan *KVClient, size),
		size:    size,
		timeout: timeout,
	}

	// Pre-create connections
	for i := 0; i < size; i++ {
		client, err := NewKVClient(addr)
		if err != nil {
			// Close any created connections
			p.Close()
			return nil, fmt.Errorf("failed to create connection pool: %w", err)
		}
		p.pool <- client
	}

	return p, nil
}

// Get retrieves a connection from the pool.
func (p *KVPool) Get() (*KVClient, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}
	p.mu.Unlock()

	select {
	case client := <-p.pool:
		// Validate connection
		if client != nil && client.conn != nil {
			if p.timeout > 0 {
				client.SetDeadline(time.Now().Add(p.timeout))
			}
			return client, nil
		}
		// Stale connection — replace with a fresh one
		return NewKVClient(p.addr)
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("kv pool timeout: no connection available after 30s")
	}
}

// Put returns a connection to the pool.
func (p *KVPool) Put(client *KVClient) {
	if client == nil {
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		client.Close()
		return
	}
	p.mu.Unlock()

	// Clear deadline
	client.SetDeadline(time.Time{})

	select {
	case p.pool <- client:
		// Returned to pool
	default:
		// Pool full, close connection
		client.Close()
	}
}

// Close closes all connections in the pool.
func (p *KVPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	close(p.pool)
	for client := range p.pool {
		if client != nil {
			client.Close()
		}
	}
	return nil
}

// WithClient executes a function with a pooled connection.
func (p *KVPool) WithClient(fn func(*KVClient) error) error {
	client, err := p.Get()
	if err != nil {
		return err
	}
	if err := fn(client); err != nil {
		if !isConnectionError(err) {
			p.Put(client)
			return err
		}
		// A timeout or short response can leave an acknowledgement buffered on
		// this connection. Never let the next request consume that response.
		client.Close()
		p.mu.Lock()
		if !p.closed {
			select {
			case p.pool <- nil:
			default:
			}
		}
		p.mu.Unlock()
		return err
	}
	p.Put(client)
	return nil
}

func isConnectionError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	message := err.Error()
	return strings.Contains(message, "write command failed:") ||
		strings.Contains(message, "flush failed:") ||
		strings.Contains(message, "read response failed:")
}
