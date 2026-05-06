package storage

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type testKVServer struct {
	mu      sync.Mutex
	data    map[string]string
	writes  map[string]int
	closers []net.Conn
}

func newTestKVServer(t *testing.T) *testKVServer {
	t.Helper()

	return &testKVServer{
		data:   make(map[string]string),
		writes: make(map[string]int),
	}
}

func newTestKVPool(kv *testKVServer, size int, timeout time.Duration) *KVPool {
	pool := &KVPool{
		pool:    make(chan *KVClient, size),
		size:    size,
		timeout: timeout,
	}
	for i := 0; i < size; i++ {
		pool.pool <- kv.client()
	}
	return pool
}

func (s *testKVServer) close() {
	s.mu.Lock()
	closers := append([]net.Conn(nil), s.closers...)
	s.mu.Unlock()

	for _, conn := range closers {
		_ = conn.Close()
	}
}

func (s *testKVServer) client() *KVClient {
	clientConn, serverConn := net.Pipe()

	s.mu.Lock()
	s.closers = append(s.closers, clientConn, serverConn)
	s.mu.Unlock()

	go s.handle(serverConn)
	return &KVClient{
		conn:   clientConn,
		reader: bufio.NewReader(clientConn),
		writer: bufio.NewWriter(clientConn),
	}
}

func (s *testKVServer) writeCount(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count int
	for key, writes := range s.writes {
		if strings.Contains(key, prefix) {
			count += writes
		}
	}
	return count
}

func (s *testKVServer) handle(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	for {
		cmd, err := r.ReadString('\r')
		if err != nil {
			return
		}
		cmd = strings.TrimSuffix(cmd, "\r")

		resp := s.execute(cmd)
		if _, err := fmt.Fprintf(conn, "%s\r", resp); err != nil {
			return
		}
	}
}

func (s *testKVServer) execute(cmd string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case strings.HasPrefix(cmd, "write "):
		parts := strings.SplitN(strings.TrimPrefix(cmd, "write "), "|", 2)
		if len(parts) != 2 {
			return "error"
		}
		s.data[parts[0]] = parts[1]
		s.writes[parts[0]]++
		return "success"
	case strings.HasPrefix(cmd, "read "):
		key := strings.TrimPrefix(cmd, "read ")
		value, ok := s.data[key]
		if !ok {
			return "error"
		}
		return value
	case strings.HasPrefix(cmd, "delete "):
		key := strings.TrimPrefix(cmd, "delete ")
		delete(s.data, key)
		return "success"
	case strings.HasPrefix(cmd, "reads "):
		prefix := strings.TrimPrefix(cmd, "reads ")
		values := make([]string, 0)
		for key, value := range s.data {
			if strings.HasPrefix(key, prefix) {
				values = append(values, value)
			}
		}
		return strings.Join(values, "\n")
	default:
		return "error"
	}
}

func TestInsertDoesNotRewriteSchemaForRowIDUpdates(t *testing.T) {
	kv := newTestKVServer(t)
	defer kv.close()

	pool := newTestKVPool(kv, 2, 5*time.Second)
	defer pool.Close()

	schemas := NewSchemaManager(pool, "testdb")
	tables := NewTableManager(pool, schemas, "testdb")

	err := schemas.CreateTable(&Schema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false, PrimaryKey: true},
			{Name: "name", Type: "TEXT", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	initialSchemaWrites := kv.writeCount(":_schema:")
	if initialSchemaWrites != 1 {
		t.Fatalf("expected create table to write schema once, got %d", initialSchemaWrites)
	}

	for i := int64(1); i <= 3; i++ {
		err := tables.Insert("users", Row{"id": i, "name": fmt.Sprintf("user-%d", i)})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if got := kv.writeCount(":_schema:"); got != initialSchemaWrites {
		t.Fatalf("expected inserts not to rewrite schema, got %d schema writes", got)
	}
	if got := kv.writeCount(":_sys:rowid:"); got != 3 {
		t.Fatalf("expected rowid counter writes for inserts, got %d", got)
	}
}
