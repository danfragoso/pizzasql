package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/executor"
	"github.com/danfragoso/pizzasql-next/pkg/httpserver"
	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

var (
	kvAddr     = flag.String("kv", "localhost:8085", "PizzaKV server address")
	database   = flag.String("db", "pizzasql", "Database name")
	poolSize   = flag.Int("pool", 5, "Connection pool size")
	timeout    = flag.Duration("timeout", 30*time.Second, "Query timeout")
	httpEnable = flag.Bool("http", false, "Enable HTTP server")
	httpHost   = flag.String("http-host", "localhost", "HTTP server host")
	httpPort   = flag.Int("http-port", 8080, "HTTP server port")
	httpCORS   = flag.Bool("http-cors", true, "Enable CORS")
	httpAuth   = flag.Bool("http-auth", false, "Enable authentication")
	apiKeys    = flag.String("api-keys", "", "Comma-separated API keys")
)

func main() {
	flag.Parse()

	// Check if HTTP server mode is enabled
	if *httpEnable {
		runHTTPServer()
		return
	}

	// Check for command-line SQL
	args := flag.Args()
	if len(args) > 0 {
		// Execute single SQL statement
		sql := strings.Join(args, " ")
		executeSingle(sql)
		return
	}

	// Check for piped input
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		// Input is from pipe
		executePipe()
		return
	}

	// Interactive REPL mode
	runREPL()
}

func executeSingle(sql string) {
	// Try to connect to PizzaKV
	pool, err := storage.NewKVPool(*kvAddr, *poolSize, *timeout)
	if err != nil {
		// Fall back to expression-only mode
		executeExpressionOnly(sql)
		return
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, *database)
	table := storage.NewTableManager(pool, schema, *database)
	exec := executor.New(schema, table)
	exec.SyncCatalog()

	result, err := executeSQL(exec, sql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(result.String())
}

func executePipe() {
	// Try to connect to PizzaKV
	pool, err := storage.NewKVPool(*kvAddr, *poolSize, *timeout)
	if err != nil {
		// Fall back to expression-only mode
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			sql := strings.TrimSpace(scanner.Text())
			if sql == "" || strings.HasPrefix(sql, "--") {
				continue
			}
			executeExpressionOnly(sql)
		}
		return
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, *database)
	table := storage.NewTableManager(pool, schema, *database)
	exec := executor.New(schema, table)
	exec.SyncCatalog()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		sql := strings.TrimSpace(scanner.Text())
		if sql == "" || strings.HasPrefix(sql, "--") {
			continue
		}
		result, err := executeSQL(exec, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}
		fmt.Print(result.String())
	}
}

func runREPL() {
	fmt.Println("PizzaSQL - SQL-92 compatible database")
	fmt.Println("Type 'help' for usage, 'quit' to exit")
	fmt.Println()

	// Try to connect to PizzaKV
	var pool *storage.KVPool
	var schema *storage.SchemaManager
	var table *storage.TableManager
	var exec *executor.Executor

	pool, err := storage.NewKVPool(*kvAddr, *poolSize, *timeout)
	if err != nil {
		fmt.Printf("Warning: Cannot connect to PizzaKV at %s\n", *kvAddr)
		fmt.Println("Running in expression-only mode (no table storage)")
		fmt.Println()
	} else {
		schema = storage.NewSchemaManager(pool, *database)
		table = storage.NewTableManager(pool, schema, *database)
		exec = executor.New(schema, table)
		exec.SyncCatalog()
		fmt.Printf("Connected to PizzaKV at %s (database: %s)\n\n", *kvAddr, *database)
	}

	reader := bufio.NewReader(os.Stdin)
	var sqlBuffer strings.Builder

	for {
		if sqlBuffer.Len() == 0 {
			fmt.Print("pizzasql> ")
		} else {
			fmt.Print("       -> ")
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println()
			break
		}

		line = strings.TrimSpace(line)

		// Handle special commands
		switch strings.ToLower(line) {
		case "quit", "exit", "\\q":
			fmt.Println("Goodbye!")
			if pool != nil {
				pool.Close()
			}
			return
		case "help", "\\h":
			printHelp()
			continue
		case "tables", "\\dt":
			if schema != nil {
				listTables(schema)
			} else {
				fmt.Println("Not connected to database")
			}
			continue
		case "clear", "\\c":
			sqlBuffer.Reset()
			fmt.Println("Buffer cleared")
			continue
		}

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		// Accumulate SQL
		if sqlBuffer.Len() > 0 {
			sqlBuffer.WriteString(" ")
		}
		sqlBuffer.WriteString(line)

		// Check if statement is complete (ends with semicolon)
		sql := sqlBuffer.String()
		if !strings.HasSuffix(sql, ";") {
			continue
		}

		// Remove semicolon and execute
		sql = strings.TrimSuffix(sql, ";")
		sqlBuffer.Reset()

		if exec != nil {
			result, err := executeSQL(exec, sql)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Print(result.String())
		} else {
			executeExpressionOnly(sql)
		}
	}
}

func executeSQL(exec *executor.Executor, sql string) (*executor.Result, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return exec.Execute(stmt)
}

func executeExpressionOnly(sql string) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse error: %v\n", err)
		return
	}

	// For SELECT statements without FROM, we can evaluate expressions
	if sel, ok := stmt.(*parser.SelectStmt); ok && len(sel.From) == 0 {
		exec := &executor.Executor{}
		result, err := executeSelectExpr(exec, sel)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		fmt.Print(result.String())
		return
	}

	// For other statements, just print what was parsed
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		fmt.Printf("SELECT statement with %d columns\n", len(s.Columns))
		if len(s.From) > 0 {
			fmt.Printf("  FROM: %s\n", s.From[0].Name)
		}
		if s.Where != nil {
			fmt.Println("  WHERE: <condition>")
		}
		fmt.Println("(Not connected to database - cannot execute)")
	case *parser.InsertStmt:
		fmt.Printf("INSERT into %s (%d rows)\n", s.Table.Name, len(s.Values))
		fmt.Println("(Not connected to database - cannot execute)")
	case *parser.UpdateStmt:
		fmt.Printf("UPDATE %s (%d assignments)\n", s.Table.Name, len(s.Set))
		fmt.Println("(Not connected to database - cannot execute)")
	case *parser.DeleteStmt:
		fmt.Printf("DELETE from %s\n", s.Table.Name)
		fmt.Println("(Not connected to database - cannot execute)")
	case *parser.CreateTableStmt:
		fmt.Printf("CREATE TABLE %s (%d columns)\n", s.Table.Name, len(s.Columns))
		fmt.Println("(Not connected to database - cannot execute)")
	case *parser.DropTableStmt:
		fmt.Printf("DROP TABLE %s\n", s.Tables[0].Name)
		fmt.Println("(Not connected to database - cannot execute)")
	default:
		fmt.Printf("Parsed: %T\n", stmt)
	}
}

// executeSelectExpr handles SELECT without FROM (expression evaluation)
func executeSelectExpr(exec *executor.Executor, stmt *parser.SelectStmt) (*executor.Result, error) {
	result := executor.NewResult("SELECT")

	// Determine columns
	for i, col := range stmt.Columns {
		if col.Alias != "" {
			result.AddColumn(col.Alias)
		} else {
			result.AddColumn(fmt.Sprintf("column%d", i+1))
		}
	}

	// Evaluate expressions using reflection to access private method
	// For simplicity, we'll use a minimal evaluator here
	values := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		val, err := evalExprSimple(col.Expr)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	result.AddRow(values...)

	return result, nil
}

// evalExprSimple is a simplified expression evaluator for standalone expressions
func evalExprSimple(expr parser.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		switch e.Type {
		case lexer.TokenNumber:
			if strings.Contains(e.Value, ".") {
				var f float64
				fmt.Sscanf(e.Value, "%f", &f)
				return f, nil
			}
			var i int64
			fmt.Sscanf(e.Value, "%d", &i)
			return i, nil
		case lexer.TokenString:
			return e.Value, nil
		case lexer.TokenNULL:
			return nil, nil
		case lexer.TokenTRUE:
			return true, nil
		case lexer.TokenFALSE:
			return false, nil
		}
	case *parser.BinaryExpr:
		left, err := evalExprSimple(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := evalExprSimple(e.Right)
		if err != nil {
			return nil, err
		}
		return evalBinarySimple(e.Op, left, right)
	case *parser.UnaryExpr:
		val, err := evalExprSimple(e.Operand)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case lexer.TokenMinus:
			return -toFloatSimple(val), nil
		case lexer.TokenNOT:
			return !toBoolSimple(val), nil
		}
		return val, nil
	case *parser.ParenExpr:
		return evalExprSimple(e.Expr)
	}
	return nil, fmt.Errorf("unsupported expression type: %T", expr)
}

func evalBinarySimple(op lexer.TokenType, left, right interface{}) (interface{}, error) {
	switch op {
	case lexer.TokenPlus:
		return toFloatSimple(left) + toFloatSimple(right), nil
	case lexer.TokenMinus:
		return toFloatSimple(left) - toFloatSimple(right), nil
	case lexer.TokenStar:
		return toFloatSimple(left) * toFloatSimple(right), nil
	case lexer.TokenSlash:
		r := toFloatSimple(right)
		if r == 0 {
			return nil, nil
		}
		return toFloatSimple(left) / r, nil
	case lexer.TokenEq:
		return compareSimple(left, right) == 0, nil
	case lexer.TokenNeq:
		return compareSimple(left, right) != 0, nil
	case lexer.TokenLt:
		return compareSimple(left, right) < 0, nil
	case lexer.TokenGt:
		return compareSimple(left, right) > 0, nil
	case lexer.TokenLte:
		return compareSimple(left, right) <= 0, nil
	case lexer.TokenGte:
		return compareSimple(left, right) >= 0, nil
	case lexer.TokenAND:
		return toBoolSimple(left) && toBoolSimple(right), nil
	case lexer.TokenOR:
		return toBoolSimple(left) || toBoolSimple(right), nil
	}
	return nil, fmt.Errorf("unsupported operator: %v", op)
}

func toFloatSimple(v interface{}) float64 {
	switch val := v.(type) {
	case int64:
		return float64(val)
	case float64:
		return val
	case bool:
		if val {
			return 1
		}
		return 0
	}
	return 0
}

func toBoolSimple(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	}
	return false
}

func compareSimple(a, b interface{}) int {
	fa := toFloatSimple(a)
	fb := toFloatSimple(b)
	if fa < fb {
		return -1
	}
	if fa > fb {
		return 1
	}
	return 0
}

func printHelp() {
	fmt.Println("PizzaSQL Commands:")
	fmt.Println("  help, \\h     Show this help")
	fmt.Println("  quit, \\q     Exit the program")
	fmt.Println("  tables, \\dt  List all tables")
	fmt.Println("  clear, \\c    Clear the input buffer")
	fmt.Println()
	fmt.Println("SQL Statements (end with semicolon):")
	fmt.Println("  SELECT ... FROM ... WHERE ...")
	fmt.Println("  INSERT INTO table (cols) VALUES (...)")
	fmt.Println("  UPDATE table SET col = val WHERE ...")
	fmt.Println("  DELETE FROM table WHERE ...")
	fmt.Println("  CREATE TABLE table (col TYPE, ...)")
	fmt.Println("  DROP TABLE table")
	fmt.Println()
	fmt.Println("Expression Mode (SELECT without FROM):")
	fmt.Println("  SELECT 1 + 2 * 3;")
	fmt.Println("  SELECT UPPER('hello');")
}

func listTables(schema *storage.SchemaManager) {
	tables, err := schema.ListTables()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	if len(tables) == 0 {
		fmt.Println("No tables found")
		return
	}

	fmt.Println("Tables:")
	for _, t := range tables {
		fmt.Printf("  %s\n", t)
	}
}
func runHTTPServer() {
	// Connect to PizzaKV
	pool, err := storage.NewKVPool(*kvAddr, *poolSize, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to PizzaKV at %s: %v\n", *kvAddr, err)
		fmt.Fprintf(os.Stderr, "Make sure PizzaKV is running: pizzakv\n")
		os.Exit(1)
	}
	defer pool.Close()

	// Create schema and executor
	schema := storage.NewSchemaManager(pool, *database)
	table := storage.NewTableManager(pool, schema, *database)
	exec := executor.New(schema, table)

	// Configure HTTP server
	config := httpserver.DefaultConfig()
	config.Host = *httpHost
	config.Port = *httpPort
	config.EnableCORS = *httpCORS
	config.EnableAuth = *httpAuth

	if *apiKeys != "" {
		config.APIKeys = strings.Split(*apiKeys, ",")
	}

	// Create and start server
	server := httpserver.New(config, exec, schema)

	// Handle graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Printf("PizzaSQL HTTP server started on http://%s:%d\n", *httpHost, *httpPort)
	fmt.Printf("Database: %s\n", *database)
	fmt.Printf("PizzaKV: %s\n", *kvAddr)
	fmt.Println()
	fmt.Println("Endpoints:")
	fmt.Println("  POST   /query                - Execute SQL query")
	fmt.Println("  POST   /execute              - Batch execution")
	fmt.Println("  GET    /schema/tables        - List tables")
	fmt.Println("  GET    /schema/tables/{name} - Table schema")
	fmt.Println("  GET    /health               - Health check")
	fmt.Println("  GET    /stats                - Statistics")
	fmt.Println("  GET    /metrics              - Prometheus metrics")
	fmt.Println("  POST   /transaction/begin    - Begin transaction")
	fmt.Println("  POST   /transaction/commit   - Commit transaction")
	fmt.Println("  POST   /transaction/rollback - Rollback transaction")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Printf("  curl -X POST http://%s:%d/query -H 'Content-Type: application/json' -d '{\"sql\":\"SELECT 1+1\"}'\n", *httpHost, *httpPort)
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop")

	<-stop
	fmt.Println("\nShutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error during shutdown: %v\n", err)
	}

	fmt.Println("Server stopped")
}
