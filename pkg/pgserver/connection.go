package pgserver

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/danfragoso/pizzasql-next/pkg/executor"
	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// Connection represents a client connection
type Connection struct {
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	executor  *executor.Executor
	schema    *storage.SchemaManager
	dbManager *storage.DatabaseManager
	database  string
	params    map[string]string
	txStatus  byte
	quiet     bool // Disable query logging
}

// NewConnection creates a new connection handler
func NewConnection(conn net.Conn, dbManager *storage.DatabaseManager, quiet bool) *Connection {
	return &Connection{
		conn:      conn,
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		dbManager: dbManager,
		params:    make(map[string]string),
		txStatus:  TxStatusIdle,
		quiet:     quiet,
	}
}

// Handle processes the connection
func (c *Connection) Handle() error {
	defer c.conn.Close()

	// First, check for SSL request (sent before startup message)
	// SSL request is 8 bytes: length(4) + code(4) where code = 80877103
	firstBytes := make([]byte, 8)
	n, err := io.ReadFull(c.reader, firstBytes)
	if err != nil {
		return fmt.Errorf("failed to read initial bytes: %w", err)
	}

	// Check if it's an SSL request (code 80877103 = 0x04D2162F)
	if n == 8 {
		length := binary.BigEndian.Uint32(firstBytes[0:4])
		code := binary.BigEndian.Uint32(firstBytes[4:8])

		if length == 8 && code == 80877103 {
			// SSL request - we don't support SSL, send 'N'
			if !c.quiet {
				log.Printf("Client requested SSL, sending rejection")
			}
			if _, err := c.conn.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("failed to send SSL rejection: %w", err)
			}
			// Now read the actual startup message
		} else {
			// Not SSL request, this is part of startup message
			// We need to prepend these bytes back for ReadStartupMessage
			// Create a multi-reader that first reads our buffered bytes, then continues with the reader
			c.reader = bufio.NewReader(io.MultiReader(bytes.NewReader(firstBytes), c.reader))
		}
	}

	// Read startup message
	if !c.quiet {
		log.Printf("Reading startup message...")
	}
	params, err := ReadStartupMessage(c.reader)
	if err != nil {
		return fmt.Errorf("failed to read startup message: %w", err)
	}

	c.params = params
	if !c.quiet {
		log.Printf("Startup params: %+v", params)
	}

	// Get database name from params (default to "pizzasql")
	dbName := params["database"]
	if dbName == "" {
		dbName = "pizzasql"
	}
	c.database = dbName

	if !c.quiet {
		log.Printf("New connection: user=%s database=%s", params["user"], dbName)
	}

	// Initialize database
	if err := c.initDatabase(dbName); err != nil {
		c.sendError("FATAL", ErrCodeConnectionFailure, fmt.Sprintf("Failed to initialize database: %v", err))
		return err
	}

	// Send authentication OK (no auth for now)
	if err := c.sendAuthenticationOk(); err != nil {
		return err
	}

	// Send parameter status messages
	if err := c.sendParameterStatus("server_version", "14.0 (PizzaSQL)"); err != nil {
		return err
	}
	if err := c.sendParameterStatus("server_encoding", "UTF8"); err != nil {
		return err
	}
	if err := c.sendParameterStatus("client_encoding", "UTF8"); err != nil {
		return err
	}
	if err := c.sendParameterStatus("DateStyle", "ISO, MDY"); err != nil {
		return err
	}
	if err := c.sendParameterStatus("TimeZone", "UTC"); err != nil {
		return err
	}

	// Send backend key data (for cancellation - we don't implement this yet)
	if err := c.sendBackendKeyData(12345, 67890); err != nil {
		return err
	}

	// Send ready for query
	if err := c.sendReadyForQuery(); err != nil {
		return err
	}

	// Message loop
	for {
		msg, err := ReadMessage(c.reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("Connection closed by client")
				return nil
			}
			return fmt.Errorf("failed to read message: %w", err)
		}

		if err := c.handleMessage(msg); err != nil {
			if err == io.EOF {
				// Normal termination
				log.Printf("Connection closed normally")
				return nil
			}
			log.Printf("Error handling message: %v", err)
			return err
		}
	}
}

// initDatabase initializes the database connection
func (c *Connection) initDatabase(dbName string) error {
	db, err := c.dbManager.GetDatabase(dbName)
	if err != nil {
		return err
	}

	c.schema = db.Schema
	c.executor = executor.New(db.Schema, db.Table)
	c.executor.SyncCatalog()

	return nil
}

// handleMessage processes a client message
func (c *Connection) handleMessage(msg *Message) error {
	switch msg.Type {
	case MsgQuery:
		return c.handleQuery(msg)

	case MsgTerminate:
		log.Printf("Client requested termination")
		return io.EOF

	case MsgParse, MsgBind, MsgDescribe, MsgExecute, MsgSync, MsgClose:
		// Extended query protocol - not implemented yet
		c.sendError("ERROR", ErrCodeFeatureNotSupported, "Extended query protocol not yet supported")
		return c.sendReadyForQuery()

	default:
		log.Printf("Unknown message type: %c (%d)", msg.Type, msg.Type)
		c.sendError("ERROR", ErrCodeProtocolViolation, fmt.Sprintf("Unknown message type: %c", msg.Type))
		return c.sendReadyForQuery()
	}
}

// handleQuery processes a simple query
func (c *Connection) handleQuery(msg *Message) error {
	// Parse query string (null-terminated)
	sql := string(msg.Data[:len(msg.Data)-1])

	if !c.quiet {
		log.Printf("Query: %s", sql)
	}

	// Handle empty query
	sqlTrimmed := strings.TrimSpace(sql)
	if sqlTrimmed == "" || sqlTrimmed == ";" {
		if err := c.sendEmptyQueryResponse(); err != nil {
			return err
		}
		return c.sendReadyForQuery()
	}

	// Handle special PostgreSQL system queries that drivers send
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	// lib/pq and other drivers query these for connection validation
	if strings.Contains(sqlUpper, "SELECT VERSION()") {
		// Return a fake PostgreSQL version
		return c.handleVersionQuery()
	}

	if strings.Contains(sqlUpper, "SELECT CURRENT_USER") {
		// Return the current user
		return c.handleCurrentUserQuery()
	}

	if strings.Contains(sqlUpper, "SHOW") && (strings.Contains(sqlUpper, "SERVER_VERSION") ||
		strings.Contains(sqlUpper, "SERVER_ENCODING") ||
		strings.Contains(sqlUpper, "CLIENT_ENCODING")) {
		// Handle SHOW commands
		return c.handleShowCommand(sqlUpper)
	}

	// Execute query
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		c.sendError("ERROR", ErrCodeSyntaxError, fmt.Sprintf("Syntax error: %v", err))
		return c.sendReadyForQuery()
	}

	result, err := c.executor.Execute(stmt)
	if err != nil {
		c.sendError("ERROR", ErrCodeInternalError, fmt.Sprintf("Execution error: %v", err))
		return c.sendReadyForQuery()
	}

	// Send result based on statement type
	if err := c.sendResult(result, stmt); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// sendResult sends query results
func (c *Connection) sendResult(result *executor.Result, stmt parser.Statement) error {
	// For SELECT statements, send row description and data rows
	if _, isSelect := stmt.(*parser.SelectStmt); isSelect && len(result.Columns) > 0 {
		// Send row description
		if err := c.sendRowDescription(result.Columns, result.ColumnTypes); err != nil {
			return err
		}

		// Send data rows
		for _, row := range result.Rows {
			if err := c.sendDataRow(row, result.Columns); err != nil {
				return err
			}
		}

		// Send command complete
		tag := fmt.Sprintf("SELECT %d", len(result.Rows))
		return c.sendCommandComplete(tag)
	}

	// For other statements, just send command complete
	tag := c.getCommandTag(stmt, result)
	return c.sendCommandComplete(tag)
}

// getCommandTag returns the command completion tag
func (c *Connection) getCommandTag(stmt parser.Statement, result *executor.Result) string {
	switch stmt.(type) {
	case *parser.CreateTableStmt:
		return "CREATE TABLE"
	case *parser.DropTableStmt:
		return "DROP TABLE"
	case *parser.CreateIndexStmt:
		return "CREATE INDEX"
	case *parser.DropIndexStmt:
		return "DROP INDEX"
	case *parser.InsertStmt:
		return fmt.Sprintf("INSERT 0 %d", result.RowsAffected)
	case *parser.UpdateStmt:
		return fmt.Sprintf("UPDATE %d", result.RowsAffected)
	case *parser.DeleteStmt:
		return fmt.Sprintf("DELETE %d", result.RowsAffected)
	case *parser.BeginStmt:
		c.txStatus = TxStatusInBlock
		return "BEGIN"
	case *parser.CommitStmt:
		c.txStatus = TxStatusIdle
		return "COMMIT"
	case *parser.RollbackStmt:
		c.txStatus = TxStatusIdle
		return "ROLLBACK"
	default:
		return "OK"
	}
}

// sendAuthenticationOk sends authentication OK message
func (c *Connection) sendAuthenticationOk() error {
	mb := NewMessageBuilder()
	mb.WriteInt32(0) // Auth OK
	return c.writeMessage(MsgAuthenticationOk, mb.Bytes())
}

// sendParameterStatus sends a parameter status message
func (c *Connection) sendParameterStatus(name, value string) error {
	mb := NewMessageBuilder()
	mb.WriteString(name)
	mb.WriteString(value)
	return c.writeMessage(MsgParameterStatus, mb.Bytes())
}

// sendBackendKeyData sends backend key data
func (c *Connection) sendBackendKeyData(processID, secretKey int32) error {
	mb := NewMessageBuilder()
	mb.WriteInt32(processID)
	mb.WriteInt32(secretKey)
	return c.writeMessage(MsgBackendKeyData, mb.Bytes())
}

// sendReadyForQuery sends ready for query message
func (c *Connection) sendReadyForQuery() error {
	mb := NewMessageBuilder()
	mb.WriteByte(c.txStatus)
	return c.writeMessage(MsgReadyForQuery, mb.Bytes())
}

// sendEmptyQueryResponse sends empty query response
func (c *Connection) sendEmptyQueryResponse() error {
	return c.writeMessage(MsgEmptyQueryResponse, []byte{})
}

// sendRowDescription sends row description (column metadata)
func (c *Connection) sendRowDescription(columns []string, columnTypes []string) error {
	mb := NewMessageBuilder()
	mb.WriteInt16(int16(len(columns)))

	for i, col := range columns {
		colType := ""
		if i < len(columnTypes) {
			colType = columnTypes[i]
		}
		mb.WriteString(col)
		mb.WriteInt32(0)                             // table OID
		mb.WriteInt16(0)                             // column attribute number
		mb.WriteInt32(c.getOIDForType(colType))      // type OID
		mb.WriteInt16(c.getTypeSizeForType(colType)) // type size
		mb.WriteInt32(-1)                            // type modifier
		mb.WriteInt16(0)                             // format code (text)
	}

	return c.writeMessage(MsgRowDescription, mb.Bytes())
}

// sendDataRow sends a data row
func (c *Connection) sendDataRow(row []interface{}, columns []string) error {
	mb := NewMessageBuilder()
	mb.WriteInt16(int16(len(row)))

	for _, value := range row {
		if value == nil {
			mb.WriteInt32(-1) // NULL indicator
			continue
		}

		// Convert value to string
		strValue := c.valueToString(value)
		mb.WriteInt32(int32(len(strValue)))
		mb.WriteBytes([]byte(strValue))
	}

	return c.writeMessage(MsgDataRow, mb.Bytes())
}

// sendCommandComplete sends command complete message
func (c *Connection) sendCommandComplete(tag string) error {
	mb := NewMessageBuilder()
	mb.WriteString(tag)
	return c.writeMessage(MsgCommandComplete, mb.Bytes())
}

// sendError sends an error response
func (c *Connection) sendError(severity, code, message string) error {
	mb := NewMessageBuilder()
	mb.WriteByte(ErrorFieldSeverity)
	mb.WriteString(severity)
	mb.WriteByte(ErrorFieldCode)
	mb.WriteString(code)
	mb.WriteByte(ErrorFieldMessage)
	mb.WriteString(message)
	mb.WriteByte(0) // Terminator

	return c.writeMessage(MsgErrorResponse, mb.Bytes())
}

// writeMessage writes a message to the connection
func (c *Connection) writeMessage(msgType byte, data []byte) error {
	if !c.quiet {
		log.Printf("Sending message type=%c length=%d", msgType, len(data)+4)
	}
	if err := WriteMessage(c.writer, msgType, data); err != nil {
		return err
	}
	return c.writer.Flush()
}

// getOIDForType returns PostgreSQL OID for type
func (c *Connection) getOIDForType(typeName string) int32 {
	switch strings.ToUpper(typeName) {
	case "INTEGER", "INT":
		return 23 // INT4OID
	case "TEXT", "VARCHAR", "CHAR":
		return 25 // TEXTOID
	case "REAL", "FLOAT":
		return 700 // FLOAT4OID
	case "DOUBLE":
		return 701 // FLOAT8OID
	case "BOOLEAN", "BOOL":
		return 16 // BOOLOID
	case "BLOB":
		return 17 // BYTEAOID
	default:
		return 25 // Default to TEXT
	}
}

// getTypeSizeForType returns type size
func (c *Connection) getTypeSizeForType(typeName string) int16 {
	switch strings.ToUpper(typeName) {
	case "INTEGER", "INT":
		return 4
	case "REAL", "FLOAT":
		return 4
	case "DOUBLE":
		return 8
	case "BOOLEAN", "BOOL":
		return 1
	default:
		return -1 // Variable length
	}
}

// valueToString converts a value to string
func (c *Connection) valueToString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

// handleVersionQuery handles SELECT version()
func (c *Connection) handleVersionQuery() error {
	columns := []string{"version"}
	columnTypes := []string{"TEXT"}

	if err := c.sendRowDescription(columns, columnTypes); err != nil {
		return err
	}

	row := []interface{}{"PostgreSQL 14.0 (PizzaSQL)"}
	if err := c.sendDataRow(row, columns); err != nil {
		return err
	}

	if err := c.sendCommandComplete("SELECT 1"); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// handleCurrentUserQuery handles SELECT current_user
func (c *Connection) handleCurrentUserQuery() error {
	columns := []string{"current_user"}
	columnTypes := []string{"TEXT"}

	if err := c.sendRowDescription(columns, columnTypes); err != nil {
		return err
	}

	user := c.params["user"]
	if user == "" {
		user = "pizzasql"
	}

	row := []interface{}{user}
	if err := c.sendDataRow(row, columns); err != nil {
		return err
	}

	if err := c.sendCommandComplete("SELECT 1"); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// handleShowCommand handles SHOW commands
func (c *Connection) handleShowCommand(sqlUpper string) error {
	var value string
	var name string

	if strings.Contains(sqlUpper, "SERVER_VERSION") {
		name = "server_version"
		value = "14.0"
	} else if strings.Contains(sqlUpper, "SERVER_ENCODING") {
		name = "server_encoding"
		value = "UTF8"
	} else if strings.Contains(sqlUpper, "CLIENT_ENCODING") {
		name = "client_encoding"
		value = "UTF8"
	} else {
		// Unknown SHOW command
		c.sendError("ERROR", ErrCodeFeatureNotSupported, "SHOW command not supported")
		return c.sendReadyForQuery()
	}

	columns := []string{name}
	columnTypes := []string{"TEXT"}

	if err := c.sendRowDescription(columns, columnTypes); err != nil {
		return err
	}

	row := []interface{}{value}
	if err := c.sendDataRow(row, columns); err != nil {
		return err
	}

	if err := c.sendCommandComplete("SHOW"); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}
