# PizzaSQL Go Client

Go client library for PizzaSQL - a simple, lightweight SQL database with HTTP API.

## Installation

```bash
go get github.com/pizzasql/pizzasql-go
```

## Usage

### Basic Example

```go
package main

import (
    "fmt"
    "log"

    "github.com/pizzasql/pizzasql-go"
)

func main() {
    // Connect to database
    db, err := pizzasql.Connect("http://localhost:8080/mydb", "")
    if err != nil {
        log.Fatal(err)
    }

    // Execute a query
    rows, err := db.SQL("SELECT * FROM users")
    if err != nil {
        log.Fatal(err)
    }

    // Process results
    for _, row := range rows {
        fmt.Printf("User ID: %v, Name: %v\n", row["id"], row["name"])
    }
}
```

### With API Key

```go
db, err := pizzasql.Connect(
    "https://pizzabase.cloud/my_org/sql/my_db:32131",
    "a78tsda68bdt6ad5afsd65saf5sd5a7d6sd87asy8d9aysnd7ay==",
)
if err != nil {
    log.Fatal(err)
}

rows, err := db.SQL("SELECT 42 as answer")
if err != nil {
    log.Fatal(err)
}
```

### Creating Tables and Inserting Data

```go
// Create table
_, err = db.SQL(`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    )
`)
if err != nil {
    log.Fatal(err)
}

// Insert data
_, err = db.SQL(`
    INSERT INTO users (id, name, email)
    VALUES (1, 'Alice', 'alice@example.com')
`)
if err != nil {
    log.Fatal(err)
}
```

### Filtering and Mapping Results

```go
rows, err := db.SQL("SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}

// Extract specific field
var userIDs []interface{}
for _, row := range rows {
    userIDs = append(userIDs, row["id"])
}

fmt.Println(userIDs) // [1, 2, 3, ...]
```

### Export Database

```go
// Export entire database as SQL
data, err := db.Export("", "sql")
if err != nil {
    log.Fatal(err)
}

// Export specific table as CSV
csvData, err := db.Export("users", "csv")
if err != nil {
    log.Fatal(err)
}

// Save to file
err = os.WriteFile("users.csv", csvData, 0644)
if err != nil {
    log.Fatal(err)
}
```

### Import Data

```go
// Read CSV file
data, err := os.ReadFile("users.csv")
if err != nil {
    log.Fatal(err)
}

// Import with table creation
err = db.Import(data, "csv", true)
if err != nil {
    log.Fatal(err)
}
```

## API Reference

### Connect(uri string, apiKey string) (*Client, error)

Creates a new PizzaSQL client connection.

**Parameters:**
- `uri` - Database URI (e.g., `http://localhost:8080/mydb`)
- `apiKey` - Optional API key for authentication

**Returns:**
- `*Client` - Connected client instance
- `error` - Error if connection fails

### (*Client) SQL(query string) ([]Row, error)

Executes a SQL query and returns the results.

**Parameters:**
- `query` - SQL query string

**Returns:**
- `[]Row` - Slice of rows (each row is `map[string]interface{}`)
- `error` - Error if query fails

### (*Client) Export(table string, format string) ([]byte, error)

Exports database or table data.

**Parameters:**
- `table` - Table name (empty string for entire database)
- `format` - Export format: `"sql"` or `"csv"`

**Returns:**
- `[]byte` - Exported data
- `error` - Error if export fails

### (*Client) Import(data []byte, format string, createTable bool) error

Imports data into the database.

**Parameters:**
- `data` - Data to import
- `format` - Import format: `"sql"` or `"csv"`
- `createTable` - Create table if it doesn't exist (CSV only)

**Returns:**
- `error` - Error if import fails

## Error Handling

All methods return errors following Go conventions. Always check for errors:

```go
rows, err := db.SQL("SELECT * FROM users")
if err != nil {
    log.Printf("Query failed: %v", err)
    return
}
```

## License

MIT
