# PizzaSQL Testing Guide

This document describes all the tests available in PizzaSQL and how to run them.

## Table of Contents

- [Unit Tests](#unit-tests)
- [Stress Test Suite](#stress-test-suite)
- [Running Tests](#running-tests)
- [Test Coverage](#test-coverage)

---

## Unit Tests

PizzaSQL includes comprehensive unit tests for each major component written in Go.

### Lexer Tests (`pkg/lexer/lexer_test.go`)

Tests the SQL tokenizer/lexer that breaks SQL strings into tokens.

**What it tests:**
- Single token parsing (operators, keywords, punctuation)
- Multi-character operators (`<=`, `>=`, `<>`, `!=`, `||`)
- Keywords (case-insensitive): `SELECT`, `FROM`, `WHERE`, `JOIN`, etc.
- Identifiers and quoted identifiers
- String literals (single and double quotes)
- Numeric literals (integers and floats)
- Comments (single-line `--` and multi-line `/* */`)
- Whitespace handling

**Run lexer tests:**
```bash
make test-lexer
# or
go test -v ./pkg/lexer/...
```

### Parser Tests (`pkg/parser/parser_test.go`)

Tests the SQL parser that converts tokens into Abstract Syntax Trees (AST).

**What it tests:**
- **SELECT statements**: `*`, column lists, aliases, DISTINCT
- **FROM clause**: Single tables, multiple tables, table aliases
- **JOIN operations**: INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **WHERE clause**: Conditions, operators, complex expressions
- **GROUP BY**: Single/multiple columns, expressions
- **HAVING**: Aggregate filtering
- **ORDER BY**: ASC/DESC, multiple columns, NULL handling
- **LIMIT/OFFSET**: Result pagination
- **INSERT statements**: Single row, multiple rows, column lists
- **UPDATE statements**: SET clauses, WHERE conditions
- **DELETE statements**: WHERE conditions
- **CREATE TABLE**: Columns, constraints, PRIMARY KEY, FOREIGN KEY
- **ALTER TABLE**: ADD/DROP COLUMN, RENAME
- **DROP TABLE**: IF EXISTS
- **CREATE INDEX**: Single/multiple columns
- **Expressions**: Binary operators, functions, CASE, subqueries
- **Subqueries**: Scalar, EXISTS, IN
- **Aggregate functions**: COUNT, SUM, AVG, MIN, MAX
- **String functions**: UPPER, LOWER, LENGTH, SUBSTRING
- **Date/time functions**: NOW, DATE, TIME

**Run parser tests:**
```bash
make test-parser
# or
go test -v ./pkg/parser/...
```

### Analyzer Tests (`pkg/analyzer/analyzer_test.go`)

Tests semantic analysis and type checking of SQL statements.

**What it tests:**
- **Table existence**: Verifying referenced tables exist
- **Column validation**: Checking columns exist in referenced tables
- **Type checking**: Data type compatibility
- **Scope resolution**: Table and column name resolution
- **Aggregate validation**: Proper use of aggregate functions
- **JOIN validation**: Column references across tables
- **Subquery validation**: Correlation and scope
- **Function validation**: Argument counts and types
- **Constraint checking**: PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL

**Run analyzer tests:**
```bash
go test -v ./pkg/analyzer/...
```

### Executor Tests (`pkg/executor/executor_test.go`)

Tests SQL execution and query evaluation.

**What it tests:**
- **Literal evaluation**: Integers, floats, strings, booleans, NULL
- **Binary expressions**: Arithmetic, comparison, logical operators
- **Unary expressions**: Negation, NOT
- **Function calls**: Built-in SQL functions
- **DISTINCT deduplication**: Hash-based row uniqueness (applyDistinct function)
- **Type conversion**: toBool, toInt, toFloat, toString
- **NULL handling**: NULL propagation in expressions
- **Column references**: Qualified and unqualified
- **Table scans**: Full table iteration
- **Filtering**: WHERE clause evaluation
- **Sorting**: ORDER BY implementation
- **Grouping**: GROUP BY with aggregates
- **Joining**: INNER JOIN, LEFT JOIN, etc.
- **Subqueries**: Scalar and EXISTS subqueries
- **DML operations**: INSERT, UPDATE, DELETE
- **DDL operations**: CREATE, ALTER, DROP
- **Transaction handling**: Isolation and consistency

**Run executor tests:**
```bash
go test -v ./pkg/executor/...
```

### HTTP Server Tests (`pkg/httpserver/server_test.go`)

Tests the HTTP API endpoints and request handling.

**What it tests:**
- **Query endpoint**: POST /query
- **Batch execute**: POST /execute
- **Schema endpoints**: GET /schema/tables, GET /schema/tables/{name}
- **Health check**: GET /health
- **Statistics**: GET /stats
- **Metrics**: GET /metrics (Prometheus format)
- **Transaction endpoints**: POST /transaction/begin, /commit, /rollback
- **Request validation**: Invalid JSON, missing fields
- **Error handling**: SQL errors, timeouts, invalid queries
- **Response formats**: JSON structure, column info, row data
- **Compression**: GZIP encoding
- **Authentication**: API key validation (when enabled)
- **CORS**: Cross-origin headers
- **Query parameters**: `pretty`, `explain`, `readonly`, `timeout`

**Run HTTP server tests:**
```bash
go test -v ./pkg/httpserver/...
```

---

## Stress Test Suite

The stress test suite (`stress_test.js`) is a comprehensive end-to-end test that validates the entire database system with realistic workloads.

### Overview

**Test Data Scale:**
- 1,000 users
- 500 products across 5 categories
- 2,000 orders
- 5,000 order items
- **Total: 8,505 rows**

**Test Duration:** ~30 seconds  
**Total Queries:** ~8,600  
**Success Rate:** 100% (32/32 tests)

### Test Categories

#### 📋 Schema Tests
- **Health check endpoint**: Verifies server is running and responsive
- **Create tables**: Tests DDL operations (CREATE TABLE with constraints)
- **Schema introspection**: Tests metadata queries (columns, types, keys)

#### 📥 Insert Tests
- **Insert 1,000 users**: Batch INSERT with parameterized queries
- **Insert categories**: Multi-row inserts
- **Insert 500 products**: Batch operations with foreign keys
- **Insert 2,000 orders**: High-volume inserts
- **Insert 5,000 order items**: Stress test batch performance

**Verbose Output Example:**
```
  Testing Insert 1000 users... 
    → Preparing 1000 user records... done
    → Executing batch insert... done
    → Verified 1000 users in database
  ✓ PASSED (121ms)
```

#### 🔎 SELECT Tests
- **Basic SELECT queries**: Simple queries, WHERE clauses, column selection
- **SELECT with ORDER BY**: Sorting, ASC/DESC, multiple columns
- **SELECT with GROUP BY**: Aggregation grouping
- **SELECT with HAVING**: Post-aggregation filtering
- **SELECT with JOIN**: Two-table INNER JOINs
- **SELECT with multiple JOINs**: 4-table joins (users → orders → order_items → products)
- **Aggregation functions**: COUNT, SUM, AVG, MIN, MAX
- **Subqueries**: Scalar subqueries, EXISTS, IN clauses

#### 🧮 Expression Tests
- **LIKE operator**: Pattern matching with wildcards
- **BETWEEN operator**: Range queries
- **CASE expression**: Conditional logic
- **NULL handling**: IS NULL, IS NOT NULL, COALESCE
- **String functions**: UPPER, LOWER, LENGTH
- **Numeric functions**: ABS, ROUND, CEIL, FLOOR
- **Parameter types**: String, integer, float, boolean parameters

#### ✏️ UPDATE/DELETE Tests
- **UPDATE records**: Single and bulk updates with WHERE
- **DELETE records**: Conditional deletion

#### 🚀 Advanced Features Tests
- **Create indexes**: Single and composite indexes
- **Transaction handling**: BEGIN, COMMIT, ROLLBACK
- **Batch execute**: Multi-statement execution
- **ALTER TABLE**: ADD COLUMN, DROP COLUMN

#### ⚡ Performance Tests
- **Concurrent queries**: 10 simultaneous queries
- **Large result set**: Queries returning 100+ rows
- **Complex query**: Multi-table JOINs with GROUP BY, HAVING, ORDER BY

### Complex Query Example

The most complex test validates a real-world analytics query:

```sql
SELECT
  u.username,
  COUNT(DISTINCT o.id) as order_count,
  SUM(oi.quantity * oi.price) as total_spent,
  AVG(oi.price) as avg_item_price
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
LEFT JOIN order_items oi ON o.id = oi.order_id
WHERE u.id <= 100
GROUP BY u.id, u.username
HAVING COUNT(o.id) > 0
ORDER BY total_spent DESC
LIMIT 10
```

This tests:
- Multiple LEFT JOINs
- Aggregate functions (COUNT, SUM, AVG)
- COUNT(DISTINCT ...) in aggregates
- GROUP BY multiple columns
- HAVING with aggregates
- ORDER BY computed columns
- LIMIT

### DISTINCT Test

Dedicated test for DISTINCT functionality:

```sql
-- Insert test data with duplicates
INSERT INTO test_distinct VALUES (1, 'pending'), (2, 'completed'),
  (3, 'pending'), (4, 'shipped'), (5, 'pending');

-- Without DISTINCT: Returns all 5 rows
SELECT status FROM test_distinct;
-- Result: pending, completed, pending, shipped, pending

-- With DISTINCT: Returns only 3 unique values
SELECT DISTINCT status FROM test_distinct ORDER BY status;
-- Result: completed, pending, shipped
```

**What it validates:**
- Duplicate removal works correctly
- Compatible with ORDER BY
- Handles multiple data types
- Returns correct row count (3 unique from 5 total)
- Hash-based deduplication is efficient

**Test output:**
```
✅ DISTINCT is working correctly!
   Expected 3 unique values, got 3
   Values: completed, pending, shipped
```

---

## Running Tests

### Prerequisites

1. **PizzaKV must be running** (for storage-backed tests):
   ```bash
   # In a separate terminal
   pizzakv -port 8085
   ```

2. **Node.js** (for stress test):
   ```bash
   node --version  # Should be v14+
   ```

### Run All Unit Tests

```bash
# Run all Go tests
make test

# Run with verbose output
make test-v

# Run with coverage report
make test-cover
# Open coverage.html in browser
```

### Run Specific Component Tests

```bash
# Lexer only
make test-lexer

# Parser only
make test-parser

# All tests with race detection
make test-race

# Run benchmarks
make bench
```

### Run Stress Test

**Step 1: Build and start PizzaSQL server**
```bash
make build
./pizzasql -http
```

**Step 2: Run stress test** (in another terminal)
```bash
./stress_test.js
```

**Clean run with fresh database:**
```bash
# Kill server, delete database, restart, and run test
pkill -9 pizzasql; rm -f .db && ./pizzasql -http > /dev/null 2>&1 & sleep 2 && ./stress_test.js
```

### Stress Test Configuration

Edit `stress_test.js` to change test parameters:

```javascript
const CONFIG = {
  numUsers: 1000,        // Number of test users
  numProducts: 500,      // Number of products
  numOrders: 2000,       // Number of orders
  numOrderItems: 5000,   // Number of order items
  concurrentRequests: 10 // Concurrent query limit
};
```

**Environment variables:**
```bash
# Custom server URL
PIZZASQL_URL=http://localhost:9000 ./stress_test.js

# With API key
PIZZASQL_API_KEY=your-secret-key ./stress_test.js
```

---

## Test Coverage

### Current Coverage

Run `make test-cover` to generate coverage report. Expected coverage:

- **Lexer**: ~95% (token parsing, error handling)
- **Parser**: ~90% (SQL grammar, AST construction)
- **Analyzer**: ~85% (semantic validation, type checking)
- **Executor**: ~80% (query execution, complex operations)
- **HTTP Server**: ~75% (endpoint handlers, middleware)

### Coverage Report

After running `make test-cover`, open `coverage.html`:

```bash
make test-cover
open coverage.html  # macOS
# or
xdg-open coverage.html  # Linux
```

---

## Interpreting Test Results

### Unit Test Output

```bash
$ make test-v
=== RUN   TestLexerSingleTokens
--- PASS: TestLexerSingleTokens (0.00s)
=== RUN   TestParseSelectStar
--- PASS: TestParseSelectStar (0.00s)
...
PASS
ok      github.com/danfragoso/pizzasql-next/pkg/lexer    0.012s
ok      github.com/danfragoso/pizzasql-next/pkg/parser   0.089s
```

### Stress Test Output

```
╔════════════════════════════════════════════════════════════╗
║                      TEST SUMMARY                          ║
╚════════════════════════════════════════════════════════════╝

  Total tests:     32
  Passed:          32 ✓
  Failed:          0 ✗
  Success rate:    100.0%

  Total queries:   8,591
  Total time:      29,805ms
  Avg query time:  3.47ms
  Queries/sec:     288
```

**Metrics explained:**
- **Total tests**: Number of test scenarios
- **Success rate**: Percentage of passing tests
- **Total queries**: All SQL queries executed (including setup)
- **Avg query time**: Mean execution time per query
- **Queries/sec**: Throughput (queries per second)

---

## Troubleshooting

### Common Issues

**1. "PizzaKV not available"**
```bash
# Start PizzaKV first
pizzakv -port 8085
```

**2. Stress test timeout errors**
```bash
# Increase server timeout (default: 5 minutes)
# Edit pkg/httpserver/handler.go, line 56:
timeout := 10 * time.Minute
```

**3. "Connection refused" during stress test**
```bash
# Make sure server is running
./pizzasql -http

# Check port 8080 is available
lsof -i :8080
```

**4. Tests failing after code changes**
```bash
# Rebuild and restart
make build
pkill -9 pizzasql
rm -f .db
./pizzasql -http &
sleep 2
./stress_test.js
```

---

## Continuous Integration

To run all tests in CI:

```bash
#!/bin/bash
set -e

# Start PizzaKV
pizzakv -port 8085 &
PIZZAKV_PID=$!

# Run unit tests
make test-v

# Build server
make build

# Start server
./pizzasql -http > /dev/null 2>&1 &
PIZZASQL_PID=$!
sleep 3

# Run stress test
./stress_test.js

# Cleanup
kill $PIZZASQL_PID $PIZZAKV_PID
```

---

## Writing New Tests

### Adding Unit Tests

Create test file in same package:

```go
// pkg/mypackage/myfile_test.go
package mypackage

import "testing"

func TestMyFunction(t *testing.T) {
    result := MyFunction("input")
    if result != "expected" {
        t.Errorf("got %v, want %v", result, "expected")
    }
}
```

### Adding Stress Test Scenarios

Edit `stress_test.js`:

```javascript
async function testMyFeature() {
  const result = await query('SELECT ...');
  assertEqual(result.rows.length, 10, 'Should return 10 rows');
}

// Add to test suite
await runTest('My feature', testMyFeature);
```

---

## Performance Benchmarks

Run benchmarks to measure performance:

```bash
make bench
```

Example benchmark output:
```
BenchmarkExecuteSelect-8        1000    1123456 ns/op    24576 B/op    245 allocs/op
BenchmarkExecuteJoin-8           100   10234567 ns/op   245760 B/op   2456 allocs/op
```

**Metrics:**
- **Operations/sec**: Iterations in 1 second
- **ns/op**: Nanoseconds per operation
- **B/op**: Bytes allocated per operation
- **allocs/op**: Number of allocations per operation

---

## Summary

- **Unit tests**: Fast, isolated component testing (~1 second total)
- **Stress test**: End-to-end validation with realistic data (~30 seconds)
- **Coverage**: Comprehensive testing of all major features
- **Automation**: Easy to run in CI/CD pipelines

Run `make test && ./stress_test.js` for complete validation! 🍕
