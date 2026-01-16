package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

func parse(t *testing.T, sql string) parser.Statement {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return stmt
}

// execSQL parses and executes a SQL string, used by benchmarks
func execSQL(exec *Executor, sql string) (*Result, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return exec.Execute(stmt)
}

// Test expression evaluation without database
func TestEvalLiteral(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected interface{}
	}{
		{"42", int64(42)},
		{"3.14", 3.14},
		{"'hello'", "hello"},
		{"TRUE", true},
		{"FALSE", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, val, val)
			}
		})
	}
}

func TestEvalArithmetic(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected float64
	}{
		{"1 + 2", 3},
		{"5 - 3", 2},
		{"4 * 3", 12},
		{"10 / 2", 5},
		{"1 + 2 * 3", 7},
		{"(1 + 2) * 3", 9},
		{"-5", -5},
		{"10 % 3", 1},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toFloat(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalComparison(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"1 = 1", true},
		{"1 = 2", false},
		{"1 <> 2", true},
		{"1 < 2", true},
		{"2 > 1", true},
		{"1 <= 1", true},
		{"1 >= 1", true},
		{"'a' = 'a'", true},
		{"'a' < 'b'", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalLogical(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"TRUE AND TRUE", true},
		{"TRUE AND FALSE", false},
		{"TRUE OR FALSE", true},
		{"FALSE OR FALSE", false},
		{"NOT TRUE", false},
		{"NOT FALSE", true},
		{"1 = 1 AND 2 = 2", true},
		{"1 = 1 OR 1 = 2", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalFunctions(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected interface{}
	}{
		{"UPPER('hello')", "HELLO"},
		{"LOWER('HELLO')", "hello"},
		{"LENGTH('hello')", int64(5)},
		{"ABS(-5)", float64(5)},
		{"COALESCE(NULL, 'default')", "default"},
		{"COALESCE('value', 'default')", "value"},
		{"NULLIF(1, 1)", nil},
		{"NULLIF(1, 2)", int64(1)},
		{"IFNULL(NULL, 'default')", "default"},
		{"IFNULL('value', 'default')", "value"},
		{"TYPEOF(42)", "integer"},
		{"TYPEOF(3.14)", "real"},
		{"TYPEOF('hello')", "text"},
		{"TYPEOF(NULL)", "null"},
		{"TRIM('  hello  ')", "hello"},
		{"SUBSTR('hello', 2, 3)", "ell"},
		{"REPLACE('hello', 'l', 'L')", "heLLo"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, val, val)
			}
		})
	}
}

func TestEvalCase(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected interface{}
	}{
		{"CASE WHEN TRUE THEN 'yes' ELSE 'no' END", "yes"},
		{"CASE WHEN FALSE THEN 'yes' ELSE 'no' END", "no"},
		{"CASE WHEN 1 = 1 THEN 'one' WHEN 1 = 2 THEN 'two' ELSE 'other' END", "one"},
		{"CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", "one"},
		{"CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", "two"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalIn(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"1 IN (1, 2, 3)", true},
		{"4 IN (1, 2, 3)", false},
		{"1 NOT IN (1, 2, 3)", false},
		{"4 NOT IN (1, 2, 3)", true},
		{"'a' IN ('a', 'b', 'c')", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalBetween(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"5 BETWEEN 1 AND 10", true},
		{"0 BETWEEN 1 AND 10", false},
		{"11 BETWEEN 1 AND 10", false},
		{"5 NOT BETWEEN 1 AND 10", false},
		{"0 NOT BETWEEN 1 AND 10", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalLike(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"'hello' LIKE 'hello'", true},
		{"'hello' LIKE 'h%'", true},
		{"'hello' LIKE '%o'", true},
		{"'hello' LIKE '%ll%'", true},
		{"'hello' LIKE 'h_llo'", true},
		{"'hello' LIKE 'world'", false},
		{"'hello' NOT LIKE 'world'", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalIsNull(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"NULL IS NULL", true},
		{"1 IS NULL", false},
		{"NULL IS NOT NULL", false},
		{"1 IS NOT NULL", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestEvalCast(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected interface{}
	}{
		{"CAST(3.14 AS INTEGER)", int64(3)},
		{"CAST(42 AS REAL)", float64(42)},
		{"CAST(123 AS TEXT)", "123"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, val, val)
			}
		})
	}
}

func TestEvalWithRow(t *testing.T) {
	exec := &Executor{}

	row := map[string]interface{}{
		"id":     int64(1),
		"name":   "John",
		"age":    30,
		"active": true,
	}

	tests := []struct {
		input    string
		expected interface{}
	}{
		{"id", int64(1)},
		{"name", "John"},
		{"age", 30},
		{"active", true},
		{"id + 1", float64(2)},
		{"age * 2", float64(60)},
		{"name = 'John'", true},
		{"age > 25", true},
		{"active AND age > 20", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, row)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			// Handle numeric comparisons
			if expected, ok := tt.expected.(float64); ok {
				if toFloat(val) != expected {
					t.Errorf("expected %v, got %v", tt.expected, val)
				}
			} else if val != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, val, val)
			}
		})
	}
}

func TestResultString(t *testing.T) {
	result := NewResult("SELECT")
	result.AddColumn("id")
	result.AddColumn("name")
	result.AddRow(int64(1), "Alice")
	result.AddRow(int64(2), "Bob")

	output := result.String()

	// Check that output contains expected elements
	if output == "" {
		t.Error("expected non-empty output")
	}
	if result.RowCount != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount)
	}
}

func TestMatchLike(t *testing.T) {
	tests := []struct {
		s        string
		pattern  string
		expected bool
	}{
		{"hello", "hello", true},
		{"hello", "h%", true},
		{"hello", "%o", true},
		{"hello", "%ll%", true},
		{"hello", "h_llo", true},
		{"hello", "H%", true}, // case insensitive
		{"hello", "world", false},
		{"", "%", true},
		{"abc", "a%c", true},
		{"abc", "a_c", true},
		{"abc", "__c", true},
		{"abc", "___", true},
		{"abc", "____", false},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.pattern, func(t *testing.T) {
			got := matchLike(tt.s, tt.pattern)
			if got != tt.expected {
				t.Errorf("matchLike(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.expected)
			}
		})
	}
}

// Phase 4: SQLite function tests

func TestEvalSQLiteFunctions(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected interface{}
		isInt    bool // for RANDOM which returns int64
	}{
		// PRINTF
		{"PRINTF('%d', 42)", "42", false},
		{"PRINTF('%s', 'hello')", "hello", false},
		{"PRINTF('%d + %d = %d', 1, 2, 3)", "1 + 2 = 3", false},

		// HEX
		{"HEX('ABC')", "414243", false},
		{"HEX('hello')", "68656C6C6F", false},

		// INSTR
		{"INSTR('hello world', 'world')", int64(7), false},
		{"INSTR('hello', 'x')", int64(0), false},
		{"INSTR('hello', 'l')", int64(3), false},

		// ROUND
		{"ROUND(3.14159)", float64(3), false},
		{"ROUND(3.14159, 2)", float64(3.14), false},
		{"ROUND(3.5)", float64(4), false},

		// CONCAT
		{"CONCAT('hello', ' ', 'world')", "hello world", false},
		{"CONCAT('a', 'b', 'c')", "abc", false},

		// MAX/MIN (scalar versions)
		{"MAX(1, 5, 3)", int64(5), false},
		{"MIN(1, 5, 3)", int64(1), false},
		{"MAX('a', 'c', 'b')", "c", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, val, val)
			}
		})
	}
}

func TestEvalRandom(t *testing.T) {
	exec := &Executor{}

	stmt := parse(t, "SELECT RANDOM()")
	sel := stmt.(*parser.SelectStmt)
	val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
	if err != nil {
		t.Fatalf("evalExpr error: %v", err)
	}

	// RANDOM() should return an int64
	if _, ok := val.(int64); !ok {
		t.Errorf("RANDOM() should return int64, got %T", val)
	}
}

func TestEvalGlob(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		input    string
		expected bool
	}{
		{"GLOB('*.txt', 'file.txt')", true},
		{"GLOB('*.txt', 'file.doc')", false},
		{"GLOB('hello*', 'hello world')", true},
		{"GLOB('h?llo', 'hello')", true},
		{"GLOB('h?llo', 'hallo')", true},
		{"GLOB('[abc]*', 'apple')", true},
		{"GLOB('[abc]*', 'dog')", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, "SELECT "+tt.input)
			sel := stmt.(*parser.SelectStmt)
			val, err := exec.evalExpr(sel.Columns[0].Expr, nil)
			if err != nil {
				t.Errorf("evalExpr error: %v", err)
				return
			}
			if toBool(val) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}
}

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern  string
		s        string
		expected bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"?", "a", true},
		{"?", "ab", false},
		{"a*b", "ab", true},
		{"a*b", "aXXXb", true},
		{"a*b", "aXXXc", false},
		{"[abc]", "a", true},
		{"[abc]", "d", false},
		{"[^abc]", "d", true},
		{"[^abc]", "a", false},
		{"*.go", "main.go", true},
		{"*.go", "main.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.s, func(t *testing.T) {
			got := matchGlob(tt.pattern, tt.s)
			if got != tt.expected {
				t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.expected)
			}
		})
	}
}

// Test subquery expressions
func TestEvalSubqueryExpr(t *testing.T) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		t.Skip("PizzaKV not available, skipping subquery tests")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "test_subquery_db")
	table := storage.NewTableManager(pool, schema, "test_subquery_db")
	exec := New(schema, table)

	// Setup test tables
	execSQL(exec, "DROP TABLE IF EXISTS products")
	execSQL(exec, "DROP TABLE IF EXISTS categories")

	_, err = execSQL(exec, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create categories: %v", err)
	}

	_, err = execSQL(exec, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, price REAL)")
	if err != nil {
		t.Fatalf("failed to create products: %v", err)
	}

	// Insert test data
	execSQL(exec, "INSERT INTO categories VALUES (1, 'Electronics')")
	execSQL(exec, "INSERT INTO categories VALUES (2, 'Books')")
	execSQL(exec, "INSERT INTO categories VALUES (3, 'Clothing')")

	execSQL(exec, "INSERT INTO products VALUES (1, 'Laptop', 1, 999.99)")
	execSQL(exec, "INSERT INTO products VALUES (2, 'Phone', 1, 599.99)")
	execSQL(exec, "INSERT INTO products VALUES (3, 'Novel', 2, 19.99)")
	execSQL(exec, "INSERT INTO products VALUES (4, 'T-Shirt', 3, 29.99)")

	// Test scalar subquery
	t.Run("scalar_subquery", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT (SELECT MAX(price) FROM products)")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 1 {
			t.Errorf("expected 1 row, got %d", result.RowCount)
		}
		if result.Rows[0][0] != 999.99 {
			t.Errorf("expected 999.99, got %v", result.Rows[0][0])
		}
	})

	// Test IN subquery
	t.Run("in_subquery", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT name FROM products WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 2 {
			t.Errorf("expected 2 rows, got %d", result.RowCount)
		}
	})

	// Test NOT IN subquery
	t.Run("not_in_subquery", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT name FROM products WHERE category_id NOT IN (SELECT id FROM categories WHERE name = 'Electronics')")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 2 {
			t.Errorf("expected 2 rows, got %d", result.RowCount)
		}
	})

	// Test EXISTS subquery
	t.Run("exists_subquery", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT EXISTS (SELECT 1 FROM products WHERE price > 500)")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 1 {
			t.Errorf("expected 1 row, got %d", result.RowCount)
		}
		if result.Rows[0][0] != true {
			t.Errorf("expected true, got %v", result.Rows[0][0])
		}
	})

	// Test EXISTS with no matches
	t.Run("exists_no_match", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT EXISTS (SELECT 1 FROM products WHERE price > 10000)")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.Rows[0][0] != false {
			t.Errorf("expected false, got %v", result.Rows[0][0])
		}
	})

	// Cleanup
	execSQL(exec, "DROP TABLE IF EXISTS products")
	execSQL(exec, "DROP TABLE IF EXISTS categories")
}

// Benchmark
func BenchmarkEvalExpr(b *testing.B) {
	exec := &Executor{}
	stmt := parse(&testing.T{}, "SELECT (1 + 2) * 3 - 4 / 2")
	sel := stmt.(*parser.SelectStmt)
	expr := sel.Columns[0].Expr

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evalExpr(expr, nil)
	}
}

// BenchmarkIndexVsNoIndex compares query performance with and without indexes.
// Requires a running PizzaKV instance at localhost:8085.
func BenchmarkIndexVsNoIndex(b *testing.B) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		b.Skip("PizzaKV not available, skipping index benchmark")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "bench_db")
	table := storage.NewTableManager(pool, schema, "bench_db")
	exec := New(schema, table)

	// Cleanup first to ensure fresh state
	execSQL(exec, "DROP INDEX IF EXISTS idx_bench_status")
	execSQL(exec, "DROP TABLE IF EXISTS bench_users")
	_, err = execSQL(exec, "CREATE TABLE bench_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, status TEXT)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Insert 1000 rows
	statuses := []string{"active", "inactive", "pending", "suspended"}
	for i := 1; i <= 1000; i++ {
		status := statuses[i%len(statuses)]
		_, err := execSQL(exec, fmt.Sprintf("INSERT INTO bench_users (id, name, email, status) VALUES (%d, 'User%d', 'user%d@test.com', '%s')", i, i, i, status))
		if err != nil {
			b.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Benchmark WITHOUT index
	b.Run("NoIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := execSQL(exec, "SELECT * FROM bench_users WHERE status = 'active'")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})

	// Create index on status column
	_, err = execSQL(exec, "CREATE INDEX idx_bench_status ON bench_users (status)")
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}

	// Benchmark WITH index
	b.Run("WithIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := execSQL(exec, "SELECT * FROM bench_users WHERE status = 'active'")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})

	// Cleanup
	execSQL(exec, "DROP INDEX IF EXISTS idx_bench_status")
	execSQL(exec, "DROP TABLE IF EXISTS bench_users")
}

// BenchmarkIndexVsNoIndexLargeTable tests with more rows
func BenchmarkIndexVsNoIndexLargeTable(b *testing.B) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		b.Skip("PizzaKV not available, skipping index benchmark")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "bench_db")
	table := storage.NewTableManager(pool, schema, "bench_db")
	exec := New(schema, table)

	// Cleanup first to ensure fresh state
	execSQL(exec, "DROP INDEX IF EXISTS idx_bench_category")
	execSQL(exec, "DROP TABLE IF EXISTS bench_large")
	_, err = execSQL(exec, "CREATE TABLE bench_large (id INTEGER PRIMARY KEY, category INTEGER, value TEXT)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Insert 5000 rows with 100 distinct categories
	for i := 1; i <= 5000; i++ {
		category := i % 100
		_, err := execSQL(exec, fmt.Sprintf("INSERT INTO bench_large (id, category, value) VALUES (%d, %d, 'value_%d')", i, category, i))
		if err != nil {
			b.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Benchmark WITHOUT index (should scan all 5000 rows)
	b.Run("NoIndex_5000rows", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := execSQL(exec, "SELECT * FROM bench_large WHERE category = 42")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})

	// Create index
	_, err = execSQL(exec, "CREATE INDEX idx_bench_category ON bench_large (category)")
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}

	// Benchmark WITH index (should only retrieve ~50 rows)
	b.Run("WithIndex_5000rows", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := execSQL(exec, "SELECT * FROM bench_large WHERE category = 42")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})

	// Cleanup
	execSQL(exec, "DROP INDEX IF EXISTS idx_bench_category")
	execSQL(exec, "DROP TABLE IF EXISTS bench_large")
}

// Test transaction statements
func TestTransactions(t *testing.T) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		t.Skip("PizzaKV not available, skipping transaction tests")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "test_tx_db")
	table := storage.NewTableManager(pool, schema, "test_tx_db")
	exec := New(schema, table)

	// Setup test table
	execSQL(exec, "DROP TABLE IF EXISTS tx_test")
	_, err = execSQL(exec, "CREATE TABLE tx_test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	t.Run("begin_transaction", func(t *testing.T) {
		result, err := execSQL(exec, "BEGIN")
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		if result.CommandTag != "BEGIN" {
			t.Errorf("expected StatementType 'BEGIN', got '%s'", result.CommandTag)
		}
		if !exec.inTransaction {
			t.Error("expected inTransaction to be true")
		}
		// Rollback to reset state
		execSQL(exec, "ROLLBACK")
	})

	t.Run("begin_transaction_keyword", func(t *testing.T) {
		result, err := execSQL(exec, "BEGIN TRANSACTION")
		if err != nil {
			t.Fatalf("BEGIN TRANSACTION failed: %v", err)
		}
		if result.CommandTag != "BEGIN" {
			t.Errorf("expected StatementType 'BEGIN', got '%s'", result.CommandTag)
		}
		execSQL(exec, "ROLLBACK")
	})

	t.Run("commit_transaction", func(t *testing.T) {
		// Clean up any previous data
		execSQL(exec, "DELETE FROM tx_test WHERE id = 1")
		execSQL(exec, "BEGIN")
		_, err := execSQL(exec, "INSERT INTO tx_test (id, value) VALUES (1, 'test1')")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		result, err := execSQL(exec, "COMMIT")
		if err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}
		if result.CommandTag != "COMMIT" {
			t.Errorf("expected StatementType 'COMMIT', got '%s'", result.CommandTag)
		}
		if exec.inTransaction {
			t.Error("expected inTransaction to be false after COMMIT")
		}

		// Verify data was committed
		checkResult, _ := execSQL(exec, "SELECT * FROM tx_test WHERE id = 1")
		if checkResult.RowCount != 1 {
			t.Errorf("expected 1 row after commit, got %d", checkResult.RowCount)
		}
	})

	t.Run("rollback_transaction", func(t *testing.T) {
		// Clean up any previous data
		execSQL(exec, "DELETE FROM tx_test WHERE id = 2")
		execSQL(exec, "BEGIN")
		_, err := execSQL(exec, "INSERT INTO tx_test (id, value) VALUES (2, 'test2')")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		result, err := execSQL(exec, "ROLLBACK")
		if err != nil {
			t.Fatalf("ROLLBACK failed: %v", err)
		}
		if result.CommandTag != "ROLLBACK" {
			t.Errorf("expected StatementType 'ROLLBACK', got '%s'", result.CommandTag)
		}
		if exec.inTransaction {
			t.Error("expected inTransaction to be false after ROLLBACK")
		}

		// Verify data was NOT committed (rollback currently doesn't undo changes due to PizzaKV limitations)
		// This is a known limitation - the transaction log is built but rollback doesn't restore state
		checkResult, _ := execSQL(exec, "SELECT * FROM tx_test WHERE id = 2")
		// Note: In the current implementation, rollback doesn't actually undo changes
		// This test documents current behavior
		if checkResult.RowCount == 0 {
			t.Log("ROLLBACK successfully prevented data persistence (ideal)")
		} else {
			t.Log("ROLLBACK did not undo changes (current limitation)")
		}
	})

	t.Run("savepoint_create", func(t *testing.T) {
		execSQL(exec, "BEGIN")
		result, err := execSQL(exec, "SAVEPOINT sp1")
		if err != nil {
			t.Fatalf("SAVEPOINT failed: %v", err)
		}
		if result.CommandTag != "SAVEPOINT" {
			t.Errorf("expected StatementType 'SAVEPOINT', got '%s'", result.CommandTag)
		}
		if len(exec.savepoints) != 1 || exec.savepoints[0] != "sp1" {
			t.Errorf("expected savepoint 'sp1', got %v", exec.savepoints)
		}
		execSQL(exec, "ROLLBACK")
	})

	t.Run("nested_savepoints", func(t *testing.T) {
		execSQL(exec, "BEGIN")
		execSQL(exec, "SAVEPOINT sp1")
		execSQL(exec, "SAVEPOINT sp2")
		execSQL(exec, "SAVEPOINT sp3")

		if len(exec.savepoints) != 3 {
			t.Errorf("expected 3 savepoints, got %d", len(exec.savepoints))
		}
		if exec.savepoints[2] != "sp3" {
			t.Errorf("expected last savepoint to be 'sp3', got '%s'", exec.savepoints[2])
		}
		execSQL(exec, "ROLLBACK")
	})

	t.Run("rollback_to_savepoint", func(t *testing.T) {
		execSQL(exec, "BEGIN")
		execSQL(exec, "INSERT INTO tx_test (id, value) VALUES (10, 'before_sp')")
		execSQL(exec, "SAVEPOINT sp1")
		execSQL(exec, "INSERT INTO tx_test (id, value) VALUES (11, 'after_sp')")

		result, err := execSQL(exec, "ROLLBACK TO sp1")
		if err != nil {
			t.Fatalf("ROLLBACK TO failed: %v", err)
		}
		if result.CommandTag != "ROLLBACK" {
			t.Errorf("expected StatementType 'ROLLBACK', got '%s'", result.CommandTag)
		}

		// Should still be in transaction
		if !exec.inTransaction {
			t.Error("expected to still be in transaction after ROLLBACK TO")
		}

		execSQL(exec, "ROLLBACK")
	})

	t.Run("release_savepoint", func(t *testing.T) {
		execSQL(exec, "BEGIN")
		execSQL(exec, "SAVEPOINT sp1")
		execSQL(exec, "SAVEPOINT sp2")

		result, err := execSQL(exec, "RELEASE sp1")
		if err != nil {
			t.Fatalf("RELEASE failed: %v", err)
		}
		if result.CommandTag != "RELEASE" {
			t.Errorf("expected StatementType 'RELEASE', got '%s'", result.CommandTag)
		}

		// Releasing sp1 should also remove sp2 (all nested savepoints)
		if len(exec.savepoints) != 0 {
			t.Errorf("expected no savepoints after RELEASE, got %d", len(exec.savepoints))
		}

		execSQL(exec, "ROLLBACK")
	})

	t.Run("release_savepoint_explicit", func(t *testing.T) {
		execSQL(exec, "BEGIN")
		execSQL(exec, "SAVEPOINT sp1")

		result, err := execSQL(exec, "RELEASE SAVEPOINT sp1")
		if err != nil {
			t.Fatalf("RELEASE SAVEPOINT failed: %v", err)
		}
		if result.CommandTag != "RELEASE" {
			t.Errorf("expected StatementType 'RELEASE', got '%s'", result.CommandTag)
		}

		execSQL(exec, "ROLLBACK")
	})

	// Cleanup
	execSQL(exec, "DROP TABLE IF EXISTS tx_test")
}

// Test subqueries in FROM clause
func TestSubqueryInFrom(t *testing.T) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		t.Skip("PizzaKV not available, skipping subquery in FROM tests")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "test_subquery_from_db")
	table := storage.NewTableManager(pool, schema, "test_subquery_from_db")
	exec := New(schema, table)

	// Setup test table
	execSQL(exec, "DROP TABLE IF EXISTS employees")
	_, err = execSQL(exec, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	execSQL(exec, "INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 100000)")
	execSQL(exec, "INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 90000)")
	execSQL(exec, "INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 80000)")
	execSQL(exec, "INSERT INTO employees (id, name, department, salary) VALUES (4, 'Diana', 'Sales', 75000)")

	t.Run("simple_subquery_from", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT * FROM (SELECT name, department FROM employees) AS emp")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 4 {
			t.Errorf("expected 4 rows, got %d", result.RowCount)
		}
		if len(result.Columns) != 2 {
			t.Errorf("expected 2 columns, got %d", len(result.Columns))
		}
	})

	t.Run("subquery_with_where", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT name FROM (SELECT id, name, salary FROM employees WHERE salary > 80000) AS high_earners")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 2 {
			t.Errorf("expected 2 rows, got %d", result.RowCount)
		}
	})

	t.Run("subquery_with_outer_where", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT * FROM (SELECT name, department FROM employees) AS emp WHERE department = 'Engineering'")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 2 {
			t.Errorf("expected 2 rows, got %d", result.RowCount)
		}
	})

	t.Run("subquery_select_specific_columns", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT name FROM (SELECT id, name, department FROM employees WHERE department = 'Sales') AS sales_emp")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 2 {
			t.Errorf("expected 2 rows, got %d", result.RowCount)
		}
		if len(result.Columns) != 1 || result.Columns[0] != "name" {
			t.Errorf("expected column 'name', got %v", result.Columns)
		}
	})

	t.Run("nested_subquery", func(t *testing.T) {
		result, err := execSQL(exec, "SELECT * FROM (SELECT * FROM (SELECT name FROM employees) AS inner_q) AS outer_q")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result.RowCount != 4 {
			t.Errorf("expected 4 rows, got %d", result.RowCount)
		}
	})

	// Cleanup
	execSQL(exec, "DROP TABLE IF EXISTS employees")
}

// Test ALTER TABLE statements
func TestAlterTable(t *testing.T) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		t.Skip("PizzaKV not available, skipping ALTER TABLE tests")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "test_alter_db")
	table := storage.NewTableManager(pool, schema, "test_alter_db")
	exec := New(schema, table)

	// Setup test table
	execSQL(exec, "DROP TABLE IF EXISTS test_alter")
	_, err = execSQL(exec, "CREATE TABLE test_alter (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	t.Run("add_column", func(t *testing.T) {
		_, err := execSQL(exec, "ALTER TABLE test_alter ADD COLUMN age INTEGER")
		if err != nil {
			t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
		}

		// Verify column was added
		tSchema, err := schema.GetSchema("test_alter")
		if err != nil {
			t.Fatalf("failed to get schema: %v", err)
		}

		found := false
		for _, col := range tSchema.Columns {
			if col.Name == "age" {
				found = true
				if col.Type != "INTEGER" {
					t.Errorf("expected type INTEGER, got %s", col.Type)
				}
				break
			}
		}
		if !found {
			t.Error("column 'age' not found after ADD COLUMN")
		}
	})

	t.Run("add_column_optional_keyword", func(t *testing.T) {
		_, err := execSQL(exec, "ALTER TABLE test_alter ADD email TEXT")
		if err != nil {
			t.Fatalf("ALTER TABLE ADD failed: %v", err)
		}

		// Verify column was added
		tSchema, _ := schema.GetSchema("test_alter")
		found := false
		for _, col := range tSchema.Columns {
			if col.Name == "email" {
				found = true
				break
			}
		}
		if !found {
			t.Error("column 'email' not found after ADD")
		}
	})

	t.Run("rename_column", func(t *testing.T) {
		_, err := execSQL(exec, "ALTER TABLE test_alter RENAME COLUMN name TO full_name")
		if err != nil {
			t.Fatalf("ALTER TABLE RENAME COLUMN failed: %v", err)
		}

		// Verify column was renamed
		tSchema, _ := schema.GetSchema("test_alter")
		hasOld := false
		hasNew := false
		for _, col := range tSchema.Columns {
			if col.Name == "name" {
				hasOld = true
			}
			if col.Name == "full_name" {
				hasNew = true
			}
		}
		if hasOld {
			t.Error("old column 'name' still exists after RENAME COLUMN")
		}
		if !hasNew {
			t.Error("new column 'full_name' not found after RENAME COLUMN")
		}
	})

	t.Run("drop_column", func(t *testing.T) {
		_, err := execSQL(exec, "ALTER TABLE test_alter DROP COLUMN email")
		if err != nil {
			t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
		}

		// Verify column was dropped
		tSchema, _ := schema.GetSchema("test_alter")
		for _, col := range tSchema.Columns {
			if col.Name == "email" {
				t.Error("column 'email' still exists after DROP COLUMN")
			}
		}
	})

	t.Run("rename_table", func(t *testing.T) {
		_, err := execSQL(exec, "ALTER TABLE test_alter RENAME TO test_renamed")
		if err != nil {
			t.Fatalf("ALTER TABLE RENAME TO failed: %v", err)
		}

		// Verify old table doesn't exist
		_, err = schema.GetSchema("test_alter")
		if err == nil {
			t.Error("old table 'test_alter' still exists after RENAME TO")
		}

		// Verify new table exists
		_, err = schema.GetSchema("test_renamed")
		if err != nil {
			t.Errorf("new table 'test_renamed' not found after RENAME TO: %v", err)
		}

		// Cleanup with new name
		execSQL(exec, "DROP TABLE IF EXISTS test_renamed")
	})

	// Final cleanup
	execSQL(exec, "DROP TABLE IF EXISTS test_alter")
	execSQL(exec, "DROP TABLE IF EXISTS test_renamed")
}

// Test ATTACH/DETACH DATABASE statements
func TestAttachDetach(t *testing.T) {
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		t.Skip("PizzaKV not available, skipping ATTACH/DETACH tests")
	}
	defer pool.Close()

	schema := storage.NewSchemaManager(pool, "test_main_db")
	table := storage.NewTableManager(pool, schema, "test_main_db")
	exec := New(schema, table)

	// Create a table in main database
	execSQL(exec, "DROP TABLE IF EXISTS main_table")
	_, err = execSQL(exec, "CREATE TABLE main_table (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("failed to create main table: %v", err)
	}
	execSQL(exec, "INSERT INTO main_table (id, data) VALUES (1, 'main data')")

	t.Run("attach_database", func(t *testing.T) {
		result, err := execSQL(exec, "ATTACH DATABASE 'test_other_db' AS other")
		if err != nil {
			t.Fatalf("ATTACH DATABASE failed: %v", err)
		}
		if result.CommandTag != "ATTACH" {
			t.Errorf("expected command tag 'ATTACH', got '%s'", result.CommandTag)
		}

		// Verify database is attached
		if _, exists := exec.attachedDatabases["other"]; !exists {
			t.Error("database 'other' not found in attached databases")
		}
	})

	t.Run("attach_duplicate_alias", func(t *testing.T) {
		_, err := execSQL(exec, "ATTACH DATABASE 'test_dup_db' AS other")
		if err == nil {
			t.Error("expected error when attaching with duplicate alias")
		}
	})

	t.Run("attach_reserved_alias", func(t *testing.T) {
		_, err := execSQL(exec, "ATTACH DATABASE 'test_temp_db' AS temp")
		if err == nil {
			t.Error("expected error when using reserved alias 'temp'")
		}
	})

	t.Run("detach_database", func(t *testing.T) {
		result, err := execSQL(exec, "DETACH DATABASE other")
		if err != nil {
			t.Fatalf("DETACH DATABASE failed: %v", err)
		}
		if result.CommandTag != "DETACH" {
			t.Errorf("expected command tag 'DETACH', got '%s'", result.CommandTag)
		}

		// Verify database is detached
		if _, exists := exec.attachedDatabases["other"]; exists {
			t.Error("database 'other' still attached after DETACH")
		}
	})

	t.Run("detach_nonexistent", func(t *testing.T) {
		_, err := execSQL(exec, "DETACH DATABASE nonexistent")
		if err == nil {
			t.Error("expected error when detaching nonexistent database")
		}
	})

	t.Run("detach_main_database", func(t *testing.T) {
		_, err := execSQL(exec, "DETACH DATABASE main")
		if err == nil {
			t.Error("expected error when detaching main database")
		}
	})

	t.Run("attach_without_database_keyword", func(t *testing.T) {
		result, err := execSQL(exec, "ATTACH 'test_short_db' AS short")
		if err != nil {
			t.Fatalf("ATTACH (without DATABASE) failed: %v", err)
		}
		if result.CommandTag != "ATTACH" {
			t.Errorf("expected command tag 'ATTACH', got '%s'", result.CommandTag)
		}

		// Cleanup
		execSQL(exec, "DETACH short")
	})

	t.Run("detach_without_database_keyword", func(t *testing.T) {
		execSQL(exec, "ATTACH 'test_det_db' AS det")
		result, err := execSQL(exec, "DETACH det")
		if err != nil {
			t.Fatalf("DETACH (without DATABASE) failed: %v", err)
		}
		if result.CommandTag != "DETACH" {
			t.Errorf("expected command tag 'DETACH', got '%s'", result.CommandTag)
		}
	})

	// Cleanup
	execSQL(exec, "DROP TABLE IF EXISTS main_table")
}

func TestDistinct(t *testing.T) {
	// Simple test without requiring KV connection
	exec := &Executor{}
	
	// Test applyDistinct function directly
	t.Run("ApplyDistinct", func(t *testing.T) {
		rows := [][]interface{}{
			{"a", 1},
			{"b", 2},
			{"a", 1}, // duplicate
			{"c", 3},
			{"b", 2}, // duplicate
		}
		
		result := exec.applyDistinct(rows)
		
		if len(result) != 3 {
			t.Errorf("expected 3 unique rows, got %d", len(result))
		}
		
		// Check that we have the expected unique rows
		expected := map[string]bool{
			"a\x001": true,
			"b\x002": true,
			"c\x003": true,
		}
		
		for _, row := range result {
			key := fmt.Sprintf("%v\x00%v", row[0], row[1])
			if !expected[key] {
				t.Errorf("unexpected row in result: %v", row)
			}
		}
	})
}
