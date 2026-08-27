package pgserver

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadMessageRejectsOversizedMessage(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte('Q')
	if err := binary.Write(&buf, binary.BigEndian, uint32(MaxMessageSize+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadMessage(&buf); err == nil {
		t.Fatal("expected error for message larger than MaxMessageSize")
	}
}

func TestReadMessageAcceptsNormalMessage(t *testing.T) {
	payload := []byte("SELECT 1\x00")
	var buf bytes.Buffer
	buf.WriteByte(MsgQuery)
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(payload)+4)); err != nil {
		t.Fatal(err)
	}
	buf.Write(payload)

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msg.Type != MsgQuery {
		t.Fatalf("type = %c, want %c", msg.Type, MsgQuery)
	}
	if !bytes.Equal(msg.Data, payload) {
		t.Fatalf("data = %q, want %q", msg.Data, payload)
	}
}

func TestReadStartupMessageRejectsOversizedMessage(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint32(MaxStartupMessageSize+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadStartupMessage(&buf); err == nil {
		t.Fatal("expected error for startup message larger than MaxStartupMessageSize")
	}
}

func TestReadStartupMessageAcceptsNormalMessage(t *testing.T) {
	// Build a minimal startup payload: protocol version + user param.
	var payload bytes.Buffer
	if err := binary.Write(&payload, binary.BigEndian, uint32(196608)); err != nil { // 3.0
		t.Fatal(err)
	}
	payload.WriteString("user\x00tester\x00\x00")

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint32(payload.Len()+4)); err != nil {
		t.Fatal(err)
	}
	buf.Write(payload.Bytes())

	params, err := ReadStartupMessage(&buf)
	if err != nil {
		t.Fatalf("ReadStartupMessage: %v", err)
	}
	if params["user"] != "tester" {
		t.Fatalf("user = %q, want %q", params["user"], "tester")
	}
	if params["protocol_version"] != "196608" {
		t.Fatalf("protocol_version = %q, want %q", params["protocol_version"], "196608")
	}
}
