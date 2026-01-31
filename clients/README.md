# PizzaSQL Client Libraries

Official client libraries for PizzaSQL in multiple programming languages.

## Available Clients

| Language | Directory | Package Manager | Status |
|----------|-----------|----------------|---------|
| JavaScript/TypeScript | [js/](js/) | npm | ✅ Ready |
| Python | [python/](python/) | pip | ✅ Ready |
| Go | [go/](go/) | go get | ✅ Ready |
| Ruby | [ruby/](ruby/) | gem | ✅ Ready |

## Quick Start

All client libraries follow a similar API design pattern for consistency across languages.

### JavaScript/TypeScript (Node.js/Bun)

```javascript
import { connect } from 'pizzasql';

const db = connect('http://localhost:8080/mydb', 'api-key');
const users = await db.sql('SELECT * FROM users');
const userIds = users.map(u => u.id);
```

[Full documentation](js/README.md)

### Python

```python
from pizzasql import connect

db = connect('http://localhost:8080/mydb', api_key='api-key')
users = db.sql('SELECT * FROM users')
user_ids = [u['id'] for u in users]
```

[Full documentation](python/README.md)

### Go

```go
import "github.com/pizzasql/pizzasql-go"

db, err := pizzasql.Connect("http://localhost:8080/mydb", "api-key")
if err != nil {
    log.Fatal(err)
}

rows, err := db.SQL("SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}

for _, row := range rows {
    fmt.Println(row["id"])
}
```

[Full documentation](go/README.md)

### Ruby

```ruby
require 'pizzasql'

db = PizzaSQL.connect('http://localhost:8080/mydb', 'api-key')
users = db.sql('SELECT * FROM users')
user_ids = users.map { |u| u['id'] }
```

[Full documentation](ruby/README.md)

## Common Features

All client libraries support:

- **SQL Queries** - Execute any SQL query and get JSON results
- **Export** - Export database or specific tables to SQL or CSV
- **Import** - Import data from SQL or CSV files
- **Authentication** - Optional API key authentication
- **Error Handling** - Proper error handling with meaningful messages

## Installation

### JavaScript/TypeScript

```bash
npm install pizzasql
# or
bun add pizzasql
```

### Python

```bash
pip install pizzasql
```

### Go

```bash
go get github.com/pizzasql/pizzasql-go
```

### Ruby

```bash
gem install pizzasql
```

## API Consistency

All clients implement the same core methods:

| Method | JavaScript | Python | Go | Ruby |
|--------|------------|--------|-----|------|
| Connect | `connect(uri, apiKey)` | `connect(uri, api_key)` | `Connect(uri, apiKey)` | `connect(uri, api_key)` |
| Query | `sql(query)` | `sql(query)` | `SQL(query)` | `sql(query)` |
| Export | `export(table, format)` | `export(table, format)` | `Export(table, format)` | `export(table:, format:)` |
| Import | `import(data, format, createTable)` | `import_data(data, format, create_table)` | `Import(data, format, createTable)` | `import(data, format:, create_table:)` |

## URI Format

All clients accept URIs in the following formats:

```
http://localhost:8080/mydb
https://pizzabase.cloud/my_org/sql/my_db:32131
```

The database name is extracted from the last segment of the URI path.

## Authentication

All clients support optional API key authentication via Bearer token:

```javascript
// With API key
const db = connect('http://localhost:8080/mydb', 'your-api-key-here');

// Without API key
const db = connect('http://localhost:8080/mydb');
```

## Response Format

All clients return query results as an array of objects/dictionaries/maps:

```json
[
  {"id": 1, "name": "Alice", "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "email": "bob@example.com"}
]
```

## Export/Import Formats

Supported formats:
- `sql` - SQL statements (default)
- `csv` - Comma-separated values

### Export Examples

```javascript
// Export entire database as SQL
const sqlData = await db.export('', 'sql');

// Export specific table as CSV
const csvData = await db.export('users', 'csv');
```

### Import Examples

```javascript
// Import SQL file
await db.import(sqlData, 'sql', false);

// Import CSV with auto table creation
await db.import(csvData, 'csv', true);
```

## Contributing

Each client library is maintained independently. See individual client directories for development setup and contribution guidelines.

## License

MIT
