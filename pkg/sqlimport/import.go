package sqlimport

import (
	"fmt"
	"strings"

	"github.com/danfragoso/pizzasql-next/pkg/executor"
	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
)

// ImportOptions configures import behavior.
type ImportOptions struct {
	IgnoreErrors bool // Continue on individual statement errors
}

// DefaultImportOptions returns sensible defaults.
func DefaultImportOptions() ImportOptions {
	return ImportOptions{
		IgnoreErrors: false,
	}
}

// ImportResult contains the results of an import operation.
type ImportResult struct {
	StatementsExecuted int      `json:"statementsExecuted"`
	TablesCreated      []string `json:"tablesCreated"`
	TablesDropped      []string `json:"tablesDropped"`
	RowsInserted       int64    `json:"rowsInserted"`
	Errors             []string `json:"errors,omitempty"`
}

// ImportSQL executes SQL statements from text.
func ImportSQL(exec *executor.Executor, sql string, opts ImportOptions) (*ImportResult, error) {
	result := &ImportResult{
		TablesCreated: []string{},
		TablesDropped: []string{},
		Errors:        []string{},
	}

	// Split SQL into statements
	statements := splitStatements(sql)

	for _, stmtSQL := range statements {
		stmtSQL = strings.TrimSpace(stmtSQL)
		if stmtSQL == "" || isComment(stmtSQL) {
			continue
		}

		// Parse and execute the statement
		err := executeStatement(exec, stmtSQL, result)
		if err != nil {
			errMsg := fmt.Sprintf("Error executing statement: %s - %v", truncateSQL(stmtSQL), err)
			result.Errors = append(result.Errors, errMsg)

			if !opts.IgnoreErrors {
				return result, fmt.Errorf("import failed: %w", err)
			}
		} else {
			result.StatementsExecuted++
		}
	}

	return result, nil
}

// executeStatement parses and executes a single SQL statement.
func executeStatement(exec *executor.Executor, sql string, result *ImportResult) error {
	// Parse the statement
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Execute the statement
	execResult, err := exec.Execute(stmt)
	if err != nil {
		return fmt.Errorf("execution error: %w", err)
	}

	// Track what happened
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	if strings.HasPrefix(upperSQL, "CREATE TABLE") {
		tableName := extractTableName(sql, "CREATE TABLE")
		if tableName != "" {
			result.TablesCreated = append(result.TablesCreated, tableName)
		}
	} else if strings.HasPrefix(upperSQL, "DROP TABLE") {
		tableName := extractTableName(sql, "DROP TABLE")
		if tableName != "" {
			result.TablesDropped = append(result.TablesDropped, tableName)
		}
	} else if strings.HasPrefix(upperSQL, "INSERT") {
		result.RowsInserted += execResult.RowsAffected
	}

	return nil
}

// splitStatements splits SQL text into individual statements.
func splitStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		// Handle string literals
		if (c == '\'' || c == '"') && !inString {
			inString = true
			stringChar = c
			current.WriteByte(c)
			continue
		}

		if inString {
			current.WriteByte(c)
			// Check for escape (doubled quote)
			if c == stringChar {
				if i+1 < len(sql) && sql[i+1] == stringChar {
					// Escaped quote - write next char and skip
					i++
					current.WriteByte(sql[i])
				} else {
					// End of string
					inString = false
					stringChar = 0
				}
			}
			continue
		}

		// Handle statement terminator
		if c == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		// Handle single-line comments
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			// Skip to end of line
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}

		current.WriteByte(c)
	}

	// Don't forget the last statement if no trailing semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// isComment checks if a line is a SQL comment.
func isComment(line string) bool {
	trimmed := strings.TrimSpace(line)
	return strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*")
}

// extractTableName extracts the table name from a CREATE TABLE or DROP TABLE statement.
func extractTableName(sql string, prefix string) string {
	// Remove the prefix
	upper := strings.ToUpper(sql)
	prefixUpper := strings.ToUpper(prefix)

	idx := strings.Index(upper, prefixUpper)
	if idx == -1 {
		return ""
	}

	rest := strings.TrimSpace(sql[idx+len(prefix):])

	// Handle IF EXISTS / IF NOT EXISTS
	restUpper := strings.ToUpper(rest)
	if strings.HasPrefix(restUpper, "IF EXISTS") {
		rest = strings.TrimSpace(rest[9:])
	} else if strings.HasPrefix(restUpper, "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[13:])
	}

	// Extract table name (until space, paren, or end)
	var tableName strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(rest); i++ {
		c := rest[i]

		if (c == '"' || c == '`' || c == '[') && !inQuote {
			inQuote = true
			quoteChar = c
			if c == '[' {
				quoteChar = ']'
			}
			continue
		}

		if inQuote && c == quoteChar {
			inQuote = false
			continue
		}

		if !inQuote && (c == ' ' || c == '\t' || c == '\n' || c == '(' || c == ';') {
			break
		}

		tableName.WriteByte(c)
	}

	return tableName.String()
}

// truncateSQL truncates SQL for error messages.
func truncateSQL(sql string) string {
	sql = strings.ReplaceAll(sql, "\n", " ")
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) > 50 {
		return sql[:50] + "..."
	}
	return sql
}
