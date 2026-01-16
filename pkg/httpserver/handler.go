package httpserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
)

// QueryRequest represents a single query request.
type QueryRequest struct {
	SQL    string        `json:"sql"`
	Params []interface{} `json:"params"`
}

// ExecuteRequest represents a batch execution request.
type ExecuteRequest struct {
	Statements  []QueryRequest `json:"statements"`
	Transaction bool           `json:"transaction"`
}

// TransactionRequest represents a transaction management request.
type TransactionRequest struct {
	TransactionID string `json:"transactionId"`
}

// handleQuery handles POST /query
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed", nil)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON in request body", nil)
		return
	}

	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "MISSING_SQL", "SQL query is required", nil)
		return
	}

	// Check for pretty print
	pretty := r.URL.Query().Get("pretty") == "true"
	explain := r.URL.Query().Get("explain") == "true"
	readonly := r.URL.Query().Get("readonly") == "true"

	// Parse timeout
	timeout := 5 * time.Minute
	if t := r.URL.Query().Get("timeout"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}

	// Execute with timeout
	resultChan := make(chan *QueryResponse, 1)
	errorChan := make(chan error, 1)

	go func() {
		start := time.Now()

		// Check readonly mode
		if readonly {
			upper := strings.ToUpper(strings.TrimSpace(req.SQL))
			if strings.HasPrefix(upper, "INSERT") ||
				strings.HasPrefix(upper, "UPDATE") ||
				strings.HasPrefix(upper, "DELETE") ||
				strings.HasPrefix(upper, "CREATE") ||
				strings.HasPrefix(upper, "DROP") ||
				strings.HasPrefix(upper, "ALTER") {
				errorChan <- &HTTPError{
					Code:    "READ_ONLY_MODE",
					Message: "Write operations not allowed in read-only mode",
					Status:  http.StatusForbidden,
				}
				return
			}
		}

		// Substitute parameters
		sql := substituteParams(req.SQL, req.Params)

		// Parse SQL
		l := lexer.New(sql)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			errorChan <- &HTTPError{
				Code:    "SYNTAX_ERROR",
				Message: err.Error(),
				Status:  http.StatusBadRequest,
			}
			return
		}

		// Execute
		result, err := s.executor.Execute(stmt)
		if err != nil {
			errorChan <- &HTTPError{
				Code:    "EXECUTION_ERROR",
				Message: err.Error(),
				Status:  http.StatusInternalServerError,
			}
			return
		}

		duration := time.Since(start)

		// Build response
		resp := &QueryResponse{
			Columns:       make([]ColumnInfo, len(result.Columns)),
			Rows:          result.Rows,
			RowsAffected:  result.RowsAffected,
			LastInsertID:  result.LastInsertID,
			ExecutionTime: duration.String(),
		}

		for i, col := range result.Columns {
			colType := result.GetColumnType(i)
			// If no type info, infer from first row values
			if colType == "ANY" && len(result.Rows) > 0 && i < len(result.Rows[0]) {
				colType = inferType(result.Rows[0][i])
			}
			resp.Columns[i] = ColumnInfo{
				Name: col,
				Type: colType,
			}
		}

		if explain {
			resp.QueryPlan = []string{"Full table scan"} // TODO: Real query plan
		}

		resultChan <- resp
	}()

	select {
	case resp := <-resultChan:
		atomic.AddInt64(&s.stats.QueriesExecuted, 1)
		atomic.AddInt64(&s.stats.QueriesSuccess, 1)
		writeJSON(w, http.StatusOK, resp, pretty)
	case err := <-errorChan:
		atomic.AddInt64(&s.stats.QueriesExecuted, 1)
		atomic.AddInt64(&s.stats.QueriesError, 1)
		if httpErr, ok := err.(*HTTPError); ok {
			writeError(w, httpErr.Status, httpErr.Code, httpErr.Message, httpErr.Details)
		} else {
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), nil)
		}
	case <-time.After(timeout):
		atomic.AddInt64(&s.stats.QueriesExecuted, 1)
		atomic.AddInt64(&s.stats.QueriesError, 1)
		writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Query execution timeout", nil)
	}
}

// handleExecute handles POST /execute for batch operations
func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed", nil)
		return
	}

	var req ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON in request body", nil)
		return
	}

	if len(req.Statements) == 0 {
		writeError(w, http.StatusBadRequest, "MISSING_STATEMENTS", "At least one statement is required", nil)
		return
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	start := time.Now()

	results := make([]ExecuteResult, 0, len(req.Statements))

	// Start transaction if requested
	if req.Transaction {
		l := lexer.New("BEGIN")
		p := parser.New(l)
		stmt, _ := p.Parse()
		s.executor.Execute(stmt)
	}

	var executeErr error
	for _, stmt := range req.Statements {
		// Substitute parameters
		sql := substituteParams(stmt.SQL, stmt.Params)

		l := lexer.New(sql)
		p := parser.New(l)
		parsed, err := p.Parse()
		if err != nil {
			executeErr = err
			break
		}

		result, err := s.executor.Execute(parsed)
		if err != nil {
			executeErr = err
			break
		}

		results = append(results, ExecuteResult{
			RowsAffected: result.RowsAffected,
			LastInsertID: result.LastInsertID,
		})
	}

	// Handle transaction
	if req.Transaction {
		if executeErr != nil {
			// Rollback on error
			l := lexer.New("ROLLBACK")
			p := parser.New(l)
			stmt, _ := p.Parse()
			s.executor.Execute(stmt)

			writeError(w, http.StatusBadRequest, "TRANSACTION_ERROR", executeErr.Error(), nil)
			return
		} else {
			// Commit on success
			l := lexer.New("COMMIT")
			p := parser.New(l)
			stmt, _ := p.Parse()
			s.executor.Execute(stmt)
		}
	} else if executeErr != nil {
		writeError(w, http.StatusBadRequest, "EXECUTION_ERROR", executeErr.Error(), nil)
		return
	}

	resp := &ExecuteResponse{
		Results:       results,
		ExecutionTime: time.Since(start).String(),
	}

	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleSchemaTables handles GET /schema/tables
func (s *Server) handleSchemaTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET method is allowed", nil)
		return
	}

	tables, err := s.schema.ListTables()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "SCHEMA_ERROR", err.Error(), nil)
		return
	}

	resp := map[string]interface{}{
		"tables": tables,
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleSchemaTable handles GET /schema/tables/{table}
func (s *Server) handleSchemaTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET method is allowed", nil)
		return
	}

	// Extract table name from path
	path := strings.TrimPrefix(r.URL.Path, "/schema/tables/")
	tableName := strings.TrimSpace(path)

	if tableName == "" {
		writeError(w, http.StatusBadRequest, "MISSING_TABLE_NAME", "Table name is required", nil)
		return
	}

	schema, err := s.schema.GetSchema(tableName)
	if err != nil {
		writeError(w, http.StatusNotFound, "TABLE_NOT_FOUND", fmt.Sprintf("Table '%s' not found", tableName), nil)
		return
	}

	columns := make([]map[string]interface{}, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = map[string]interface{}{
			"name":       col.Name,
			"type":       col.Type,
			"nullable":   col.Nullable,
			"primaryKey": col.PrimaryKey,
			"default":    col.Default,
		}
	}

	resp := map[string]interface{}{
		"name":    schema.Name,
		"columns": columns,
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"status":  "ok",
		"version": "0.1.0",
		"uptime":  time.Since(s.stats.StartTime).String(),
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleStats handles GET /stats
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	tables, _ := s.schema.ListTables()

	var avgQueryTime string
	if s.stats.QueriesExecuted > 0 {
		avgQueryTime = "N/A" // TODO: Track actual query times
	} else {
		avgQueryTime = "0ms"
	}

	resp := map[string]interface{}{
		"queriesExecuted": atomic.LoadInt64(&s.stats.QueriesExecuted),
		"queriesSuccess":  atomic.LoadInt64(&s.stats.QueriesSuccess),
		"queriesError":    atomic.LoadInt64(&s.stats.QueriesError),
		"tablesCount":     len(tables),
		"avgQueryTime":    avgQueryTime,
		"uptime":          time.Since(s.stats.StartTime).String(),
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleTransactionBegin handles POST /transaction/begin
func (s *Server) handleTransactionBegin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed", nil)
		return
	}

	l := lexer.New("BEGIN")
	p := parser.New(l)
	stmt, _ := p.Parse()
	_, err := s.executor.Execute(stmt)

	if err != nil {
		writeError(w, http.StatusInternalServerError, "TRANSACTION_ERROR", err.Error(), nil)
		return
	}

	// Generate transaction ID (simple implementation)
	txID := fmt.Sprintf("tx-%d", time.Now().UnixNano())

	resp := map[string]interface{}{
		"transactionId": txID,
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleTransactionCommit handles POST /transaction/commit
func (s *Server) handleTransactionCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed", nil)
		return
	}

	var req TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Allow commit without transaction ID for simplicity
	}

	l := lexer.New("COMMIT")
	p := parser.New(l)
	stmt, _ := p.Parse()
	_, err := s.executor.Execute(stmt)

	if err != nil {
		writeError(w, http.StatusInternalServerError, "TRANSACTION_ERROR", err.Error(), nil)
		return
	}

	resp := map[string]interface{}{
		"status": "committed",
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// substituteParams replaces ? placeholders with actual parameter values.
// This is a simple implementation that handles basic SQL escaping.
func substituteParams(sql string, params []interface{}) string {
	if len(params) == 0 {
		return sql
	}

	result := sql
	for _, param := range params {
		idx := strings.Index(result, "?")
		if idx == -1 {
			break
		}

		var replacement string
		switch v := param.(type) {
		case nil:
			replacement = "NULL"
		case string:
			// Escape single quotes in strings
			escaped := strings.ReplaceAll(v, "'", "''")
			replacement = "'" + escaped + "'"
		case int, int64, int32, int16, int8:
			replacement = fmt.Sprintf("%d", v)
		case float64, float32:
			replacement = fmt.Sprintf("%g", v)
		case bool:
			if v {
				replacement = "1"
			} else {
				replacement = "0"
			}
		default:
			// For other types, convert to string
			escaped := strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''")
			replacement = "'" + escaped + "'"
		}

		result = result[:idx] + replacement + result[idx+1:]
	}

	return result
}

// inferType infers SQL type from a Go value.
func inferType(v interface{}) string {
	switch v.(type) {
	case nil:
		return "NULL"
	case int, int64, int32, int16, int8:
		return "INTEGER"
	case float64, float32:
		return "REAL"
	case string:
		return "TEXT"
	case []byte:
		return "BLOB"
	case bool:
		return "INTEGER"
	default:
		return "ANY"
	}
}

// handleTransactionRollback handles POST /transaction/rollback
func (s *Server) handleTransactionRollback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed", nil)
		return
	}

	var req TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Allow rollback without transaction ID for simplicity
	}

	l := lexer.New("ROLLBACK")
	p := parser.New(l)
	stmt, _ := p.Parse()
	_, err := s.executor.Execute(stmt)

	if err != nil {
		writeError(w, http.StatusInternalServerError, "TRANSACTION_ERROR", err.Error(), nil)
		return
	}

	resp := map[string]interface{}{
		"status": "rolled back",
	}

	pretty := r.URL.Query().Get("pretty") == "true"
	writeJSON(w, http.StatusOK, resp, pretty)
}

// handleMetrics handles GET /metrics in Prometheus format
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET method is allowed", nil)
		return
	}

	tables, _ := s.schema.ListTables()
	uptime := time.Since(s.stats.StartTime).Seconds()

	queriesTotal := atomic.LoadInt64(&s.stats.QueriesExecuted)
	queriesSuccess := atomic.LoadInt64(&s.stats.QueriesSuccess)
	queriesError := atomic.LoadInt64(&s.stats.QueriesError)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	// Write Prometheus format metrics
	fmt.Fprintf(w, "# HELP pizzasql_queries_total Total number of queries executed\n")
	fmt.Fprintf(w, "# TYPE pizzasql_queries_total counter\n")
	fmt.Fprintf(w, "pizzasql_queries_total{status=\"success\"} %d\n", queriesSuccess)
	fmt.Fprintf(w, "pizzasql_queries_total{status=\"error\"} %d\n", queriesError)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "# HELP pizzasql_queries_executed_total Total queries executed (all statuses)\n")
	fmt.Fprintf(w, "# TYPE pizzasql_queries_executed_total counter\n")
	fmt.Fprintf(w, "pizzasql_queries_executed_total %d\n", queriesTotal)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "# HELP pizzasql_tables_count Number of tables in the database\n")
	fmt.Fprintf(w, "# TYPE pizzasql_tables_count gauge\n")
	fmt.Fprintf(w, "pizzasql_tables_count %d\n", len(tables))
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "# HELP pizzasql_uptime_seconds Server uptime in seconds\n")
	fmt.Fprintf(w, "# TYPE pizzasql_uptime_seconds gauge\n")
	fmt.Fprintf(w, "pizzasql_uptime_seconds %.2f\n", uptime)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "# HELP pizzasql_info PizzaSQL server information\n")
	fmt.Fprintf(w, "# TYPE pizzasql_info gauge\n")
	fmt.Fprintf(w, "pizzasql_info{version=\"0.1.0\"} 1\n")
}
