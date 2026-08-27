package pgserver

import (
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/danfragoso/pizzasql-next/pkg/executor"
	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
)

func TestBindQuery(t *testing.T) {
	query, err := bindQuery(
		"SELECT $1, $2, $3, $4, '$5'",
		[]boundParameter{
			{value: []byte("O'Reilly")},
			{value: []byte("42")},
			{null: true},
			{value: []byte("true")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT 'O''Reilly', 42, NULL, TRUE, '$5'"
	if query != want {
		t.Fatalf("bound query = %q, want %q", query, want)
	}
}

func TestBindQueryRequiresEveryParameter(t *testing.T) {
	if _, err := bindQuery("SELECT $2", []boundParameter{{value: []byte("one")}}); err == nil {
		t.Fatal("expected missing parameter error")
	}
}

func parseStmt(t *testing.T, sql string) parser.Statement {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return stmt
}

func TestGetCommandTagSavepointKeepsTransaction(t *testing.T) {
	c := &Connection{txStatus: TxStatusIdle}
	tag := c.getCommandTag(parseStmt(t, "SAVEPOINT sp1"), executor.NewResult("SAVEPOINT"))
	if tag != "SAVEPOINT" {
		t.Fatalf("tag = %q, want %q", tag, "SAVEPOINT")
	}
	if c.txStatus != TxStatusInBlock {
		t.Fatalf("txStatus = %c, want %c", c.txStatus, TxStatusInBlock)
	}
}

func TestGetCommandTagReleaseKeepsTransaction(t *testing.T) {
	c := &Connection{txStatus: TxStatusInBlock}
	tag := c.getCommandTag(parseStmt(t, "RELEASE SAVEPOINT sp1"), executor.NewResult("RELEASE"))
	if tag != "RELEASE" {
		t.Fatalf("tag = %q, want %q", tag, "RELEASE")
	}
	if c.txStatus != TxStatusInBlock {
		t.Fatalf("txStatus = %c, want %c", c.txStatus, TxStatusInBlock)
	}
}

func TestGetCommandTagRollbackToSavepointKeepsTransaction(t *testing.T) {
	c := &Connection{txStatus: TxStatusFailed}
	tag := c.getCommandTag(parseStmt(t, "ROLLBACK TO SAVEPOINT sp1"), executor.NewResult("ROLLBACK"))
	if tag != "ROLLBACK" {
		t.Fatalf("tag = %q, want %q", tag, "ROLLBACK")
	}
	if c.txStatus != TxStatusInBlock {
		t.Fatalf("txStatus = %c, want %c", c.txStatus, TxStatusInBlock)
	}
}

func TestGetCommandTagFullRollbackEndsTransaction(t *testing.T) {
	c := &Connection{txStatus: TxStatusFailed}
	tag := c.getCommandTag(parseStmt(t, "ROLLBACK"), executor.NewResult("ROLLBACK"))
	if tag != "ROLLBACK" {
		t.Fatalf("tag = %q, want %q", tag, "ROLLBACK")
	}
	if c.txStatus != TxStatusIdle {
		t.Fatalf("txStatus = %c, want %c", c.txStatus, TxStatusIdle)
	}
}

func newTestConnection(t *testing.T) (*Connection, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() {
		server.Close()
		client.Close()
	})
	c := &Connection{
		conn:       server,
		reader:     bufio.NewReader(server),
		writer:     bufio.NewWriter(server),
		params:     map[string]string{"user": "tester"},
		statements: make(map[string]*preparedStatement),
		portals:    make(map[string]*portal),
		txStatus:   TxStatusIdle,
		quiet:      true,
	}
	return c, client
}

func runQuery(t *testing.T, c *Connection, client net.Conn, msg *Message) []*Message {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.handleQuery(msg) }()

	reader := bufio.NewReader(client)
	var msgs []*Message
	for {
		m, err := ReadMessage(reader)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		msgs = append(msgs, m)
		if m.Type == MsgReadyForQuery {
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	return msgs
}

func errorCode(msg *Message) string {
	if msg == nil || msg.Type != MsgErrorResponse {
		return ""
	}
	data := msg.Data
	for i := 0; i < len(data); {
		if data[i] == 0 {
			break
		}
		field := data[i]
		i++
		end := bytes.IndexByte(data[i:], 0)
		if end < 0 {
			break
		}
		value := string(data[i : i+end])
		if field == ErrorFieldCode {
			return value
		}
		i += end + 1
	}
	return ""
}

func TestHandleQueryEmptyPayloadReturnsProtocolError(t *testing.T) {
	c, client := newTestConnection(t)
	msgs := runQuery(t, c, client, &Message{Type: MsgQuery, Data: []byte{}})
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	if code := errorCode(msgs[0]); code != ErrCodeProtocolViolation {
		t.Fatalf("error code = %q, want %q", code, ErrCodeProtocolViolation)
	}
}

func TestHandleQueryMissingNullTerminatorReturnsProtocolError(t *testing.T) {
	c, client := newTestConnection(t)
	msgs := runQuery(t, c, client, &Message{Type: MsgQuery, Data: []byte("SELECT 1")})
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	if code := errorCode(msgs[0]); code != ErrCodeProtocolViolation {
		t.Fatalf("error code = %q, want %q", code, ErrCodeProtocolViolation)
	}
}

func TestHandleQueryEmptyQueryReturnsEmptyQueryResponse(t *testing.T) {
	for _, data := range [][]byte{{0}, []byte(";\x00"), []byte("   \x00")} {
		c, client := newTestConnection(t)
		msgs := runQuery(t, c, client, &Message{Type: MsgQuery, Data: data})
		if len(msgs) != 2 {
			t.Fatalf("payload %q: got %d messages, want 2", data, len(msgs))
		}
		if msgs[0].Type != MsgEmptyQueryResponse {
			t.Fatalf("payload %q: first message type = %c, want %c", data, msgs[0].Type, MsgEmptyQueryResponse)
		}
	}
}

func TestHandleQueryFailedTransactionInterceptsSpecialQueries(t *testing.T) {
	queries := []string{
		"SELECT version()",
		"SELECT current_user",
		"SHOW server_version",
		"SELECT * FROM information_schema.tables",
	}
	for _, q := range queries {
		c, client := newTestConnection(t)
		c.txStatus = TxStatusFailed
		msgs := runQuery(t, c, client, &Message{Type: MsgQuery, Data: []byte(q + "\x00")})
		if len(msgs) != 2 {
			t.Fatalf("query %q: got %d messages, want 2", q, len(msgs))
		}
		if code := errorCode(msgs[0]); code != ErrCodeTransactionAborted {
			t.Fatalf("query %q: error code = %q, want %q", q, code, ErrCodeTransactionAborted)
		}
	}
}

func TestIsRollbackStatement(t *testing.T) {
	for _, sql := range []string{"ROLLBACK", "ROLLBACK TO SAVEPOINT sp1", "rollback;", "  rollback  ;"} {
		if !isRollbackStatement(sql) {
			t.Errorf("isRollbackStatement(%q) = false, want true", sql)
		}
	}
	for _, sql := range []string{"SELECT version()", "SHOW server_version", "SAVEPOINT sp1"} {
		if isRollbackStatement(sql) {
			t.Errorf("isRollbackStatement(%q) = true, want false", sql)
		}
	}
	for _, sql := range []string{"ROLLBACKBOGUS", "ROLLBACK; SELECT 1"} {
		if isRollbackStatement(sql) {
			t.Errorf("isRollbackStatement(%q) = true, want false", sql)
		}
	}
}
