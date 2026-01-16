# PizzaSQL HTTP API Documentation

PizzaSQL provides a RESTful HTTP API for executing SQL queries and managing your database. This document describes all available endpoints and how to use them.

## Starting the HTTP Server

```bash
# Start with default settings (localhost:8080)
./pizzasql -http

# Custom host and port
./pizzasql -http -http-host 0.0.0.0 -http-port 3000

# With authentication enabled
./pizzasql -http -http-auth -api-keys "key1,key2,key3"

# Full example with all options
./pizzasql -http \
  -http-host 0.0.0.0 \
  -http-port 8080 \
  -http-cors \
  -http-auth \
  -api-keys "your-secret-api-key" \
  -kv localhost:8085 \
  -db mydb
```

## Authentication

When authentication is enabled (`-http-auth`), all requests must include an `Authorization` header with a valid API key:

```bash
curl -H "Authorization: Bearer your-secret-api-key" ...
```

---

## Endpoints

### POST /query

Execute a single SQL query.

**Request:**
```json
{
  "sql": "SELECT * FROM users WHERE id = ?",
  "params": [1]
}
```

**Response:**
```json
{
  "columns": [
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "TEXT"},
    {"name": "email", "type": "TEXT"}
  ],
  "rows": [
    [1, "Alice", "alice@example.com"]
  ],
  "rowsAffected": 0,
  "lastInsertId": 0,
  "executionTime": "1.234ms"
}
```

**Query Parameters:**
- `?pretty=true` - Format JSON output with indentation
- `?readonly=true` - Reject write operations (INSERT, UPDATE, DELETE)
- `?timeout=5000` - Query timeout in milliseconds
- `?explain=true` - Include query plan in response

**Examples:**

```bash
# Simple SELECT
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 + 1 AS result"}'

# SELECT with pretty output
curl -X POST "http://localhost:8080/query?pretty=true" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'

# SELECT DISTINCT to remove duplicates
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT DISTINCT status FROM orders"}'

# INSERT with parameters
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "INSERT INTO users (name, email) VALUES (?, ?)",
    "params": ["Alice", "alice@example.com"]
  }'

# SELECT with DISTINCT
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT DISTINCT status FROM orders"}'

# SELECT with parameters
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM users WHERE name LIKE ?",
    "params": ["%alice%"]
  }'

# CREATE TABLE
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"}'
```

---

### POST /execute

Execute multiple SQL statements, optionally within a transaction.

**Request:**
```json
{
  "statements": [
    {"sql": "INSERT INTO users (name) VALUES (?)", "params": ["Alice"]},
    {"sql": "INSERT INTO users (name) VALUES (?)", "params": ["Bob"]},
    {"sql": "UPDATE users SET active = 1"}
  ],
  "transaction": true
}
```

**Response:**
```json
{
  "results": [
    {"rowsAffected": 1, "lastInsertId": 1},
    {"rowsAffected": 1, "lastInsertId": 2},
    {"rowsAffected": 2, "lastInsertId": 0}
  ],
  "totalRowsAffected": 4,
  "executionTime": "5.678ms"
}
```

**Examples:**

```bash
# Batch insert with transaction
curl -X POST http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [
      {"sql": "INSERT INTO products (name, price) VALUES (?, ?)", "params": ["Widget", 9.99]},
      {"sql": "INSERT INTO products (name, price) VALUES (?, ?)", "params": ["Gadget", 19.99]},
      {"sql": "INSERT INTO products (name, price) VALUES (?, ?)", "params": ["Gizmo", 29.99]}
    ],
    "transaction": true
  }'

# Multiple operations without transaction
curl -X POST http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [
      {"sql": "DELETE FROM logs WHERE created_at < date(\"now\", \"-30 days\")"},
      {"sql": "VACUUM"}
    ],
    "transaction": false
  }'
```

---

### GET /schema/tables

List all tables in the database.

**Response:**
```json
{
  "tables": ["users", "products", "orders"],
  "count": 3
}
```

**Example:**
```bash
curl http://localhost:8080/schema/tables
```

---

### GET /schema/tables/{name}

Get detailed schema information for a specific table.

**Response:**
```json
{
  "name": "users",
  "columns": [
    {
      "name": "id",
      "type": "INTEGER",
      "nullable": false,
      "primaryKey": true,
      "default": null
    },
    {
      "name": "name",
      "type": "TEXT",
      "nullable": true,
      "primaryKey": false,
      "default": null
    },
    {
      "name": "email",
      "type": "TEXT",
      "nullable": true,
      "primaryKey": false,
      "default": null
    }
  ],
  "primaryKey": "id",
  "autoIncrement": true
}
```

**Example:**
```bash
curl http://localhost:8080/schema/tables/users
```

---

### GET /health

Health check endpoint for monitoring and load balancers.

**Response:**
```json
{
  "status": "ok",
  "database": "mydb",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Example:**
```bash
curl http://localhost:8080/health
```

---

### GET /stats

Server statistics and metrics.

**Response:**
```json
{
  "queriesExecuted": 1234,
  "queriesSuccess": 1200,
  "queriesError": 34,
  "uptime": "2h30m15s",
  "startTime": "2024-01-15T08:00:00Z",
  "tables": 5
}
```

**Example:**
```bash
curl http://localhost:8080/stats
```

---

### GET /metrics

Prometheus-format metrics for monitoring systems.

**Response:**
```
# HELP pizzasql_queries_total Total number of queries executed
# TYPE pizzasql_queries_total counter
pizzasql_queries_total{status="success"} 1200
pizzasql_queries_total{status="error"} 34

# HELP pizzasql_queries_executed_total Total queries executed (all statuses)
# TYPE pizzasql_queries_executed_total counter
pizzasql_queries_executed_total 1234

# HELP pizzasql_tables_count Number of tables in the database
# TYPE pizzasql_tables_count gauge
pizzasql_tables_count 5

# HELP pizzasql_uptime_seconds Server uptime in seconds
# TYPE pizzasql_uptime_seconds gauge
pizzasql_uptime_seconds 9015.00

# HELP pizzasql_info PizzaSQL server information
# TYPE pizzasql_info gauge
pizzasql_info{version="0.1.0"} 1
```

**Example:**
```bash
curl http://localhost:8080/metrics
```

---

### POST /transaction/begin

Start a new transaction.

**Response:**
```json
{
  "status": "started"
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/transaction/begin
```

---

### POST /transaction/commit

Commit the current transaction.

**Response:**
```json
{
  "status": "committed"
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/transaction/commit
```

---

### POST /transaction/rollback

Rollback the current transaction.

**Response:**
```json
{
  "status": "rolled back"
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/transaction/rollback
```

---

## Error Responses

All error responses follow this format:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": {}
  }
}
```

**Error Codes:**

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `MISSING_SQL` | 400 | No SQL statement provided |
| `SYNTAX_ERROR` | 400 | SQL syntax error |
| `EXECUTION_ERROR` | 500 | Error executing query |
| `READ_ONLY_MODE` | 403 | Write operation in read-only mode |
| `MISSING_AUTH` | 401 | Authorization header required |
| `INVALID_API_KEY` | 403 | Invalid API key |
| `TABLE_NOT_FOUND` | 404 | Table does not exist |
| `METHOD_NOT_ALLOWED` | 405 | Invalid HTTP method |
| `TIMEOUT` | 408 | Query timeout exceeded |

---

## Parameterized Queries

Use `?` placeholders in your SQL and provide values in the `params` array:

```json
{
  "sql": "SELECT * FROM users WHERE name = ? AND age > ?",
  "params": ["Alice", 25]
}
```

**Supported Parameter Types:**
- `null` → `NULL`
- `string` → `'escaped''string'`
- `integer` → `123`
- `float` → `3.14`
- `boolean` → `1` (true) or `0` (false)

**SQL Injection Prevention:**
Strings are automatically escaped (single quotes are doubled).

```json
{
  "sql": "SELECT * FROM users WHERE name = ?",
  "params": ["O'Brien"]
}
// Becomes: SELECT * FROM users WHERE name = 'O''Brien'
```

---

## Response Compression

The server automatically compresses responses with gzip when the client sends:

```
Accept-Encoding: gzip
```

Example:
```bash
curl -H "Accept-Encoding: gzip" http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM large_table"}' \
  --compressed
```

---

## CORS Support

CORS is enabled by default (`-http-cors`), allowing requests from any origin. Headers sent:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
```

---

## Complete Usage Examples

### Create a Database Schema

```bash
# Create users table
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, created_at TEXT DEFAULT CURRENT_TIMESTAMP)"}'

# Create posts table with foreign key
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, content TEXT, FOREIGN KEY (user_id) REFERENCES users(id))"}'

# Create index
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE INDEX idx_posts_user ON posts(user_id)"}'
```

### CRUD Operations

```bash
# Create (INSERT)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "INSERT INTO users (name, email) VALUES (?, ?)",
    "params": ["John Doe", "john@example.com"]
  }'

# Read (SELECT)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE id = ?", "params": [1]}'

# Update
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "UPDATE users SET name = ? WHERE id = ?",
    "params": ["Jane Doe", 1]
  }'

# Delete
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "DELETE FROM users WHERE id = ?", "params": [1]}'
```

### Transaction Example

```bash
# Start transaction
curl -X POST http://localhost:8080/transaction/begin

# Execute multiple queries
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO accounts (name, balance) VALUES (?, ?)", "params": ["Alice", 1000]}'

curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO accounts (name, balance) VALUES (?, ?)", "params": ["Bob", 500]}'

# Commit if successful
curl -X POST http://localhost:8080/transaction/commit

# Or rollback on error
# curl -X POST http://localhost:8080/transaction/rollback
```

### Using with JavaScript/Node.js

```javascript
async function query(sql, params = []) {
  const response = await fetch('http://localhost:8080/query', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer your-api-key'  // if auth enabled
    },
    body: JSON.stringify({ sql, params })
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error.message);
  }

  return response.json();
}

// Usage
const users = await query('SELECT * FROM users WHERE active = ?', [true]);
console.log(users.rows);
```

### Using with Python

```python
import requests

def query(sql, params=None):
    response = requests.post(
        'http://localhost:8080/query',
        json={'sql': sql, 'params': params or []},
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer your-api-key'  # if auth enabled
        }
    )
    response.raise_for_status()
    return response.json()

# Usage
result = query('SELECT * FROM users WHERE name LIKE ?', ['%john%'])
for row in result['rows']:
    print(row)
```

---

## Test Coverage

The HTTP API has comprehensive test coverage for all endpoints:

| Test | Description |
|------|-------------|
| `TestQueryEndpoint` | Basic query execution (CREATE, INSERT, SELECT) |
| `TestExecuteEndpoint` | Batch execution with transactions |
| `TestSchemaEndpoints` | Table listing and schema introspection |
| `TestHealthEndpoint` | Health check response |
| `TestStatsEndpoint` | Statistics response |
| `TestMetricsEndpoint` | Prometheus metrics format |
| `TestReadOnlyMode` | Write rejection in readonly mode |
| `TestCORSMiddleware` | CORS headers on OPTIONS request |
| `TestAuthMiddleware` | API key authentication |
| `TestCompressionMiddleware` | Gzip compression |
| `TestParameterizedQuery` | Parameter substitution |
| `TestTransactionEndpoints` | BEGIN/COMMIT/ROLLBACK |
| `TestQueryEndpointErrors` | Error handling cases |
| `TestPrettyPrintOption` | Pretty JSON formatting |
| `TestSchemaTableNotFound` | 404 for missing tables |
