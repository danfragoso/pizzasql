# PizzaSQL Ruby Client

Ruby client library for PizzaSQL - a simple, lightweight SQL database with HTTP API.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pizzasql'
```

And then execute:

```bash
bundle install
```

Or install it yourself:

```bash
gem install pizzasql
```

## Usage

### Basic Example

```ruby
require 'pizzasql'

# Connect to database
db = PizzaSQL.connect('http://localhost:8080/mydb')

# Execute a query
rows = db.sql('SELECT * FROM users')

# Process results
rows.each do |row|
  puts "User ID: #{row['id']}, Name: #{row['name']}"
end
```

### With API Key

```ruby
db = PizzaSQL.connect(
  'https://pizzabase.cloud/my_org/sql/my_db:32131',
  'a78tsda68bdt6ad5afsd65saf5sd5a7d6sd87asy8d9aysnd7ay=='
)

rows = db.sql('SELECT 42 as answer')
puts rows.first['answer'] # => 42
```

### Creating Tables and Inserting Data

```ruby
# Create table
db.sql(<<~SQL)
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  )
SQL

# Insert data
db.sql(<<~SQL)
  INSERT INTO users (id, name, email)
  VALUES (1, 'Alice', 'alice@example.com')
SQL
```

### Filtering and Mapping Results

```ruby
rows = db.sql('SELECT * FROM users')

# Extract specific field
user_ids = rows.map { |row| row['id'] }
puts user_ids.inspect # => [1, 2, 3, ...]

# Filter results
adults = rows.select { |row| row['age'] >= 18 }
```

### Export Database

```ruby
# Export entire database as SQL
sql_data = db.export(format: 'sql')

# Export specific table as CSV
csv_data = db.export(table: 'users', format: 'csv')

# Save to file
File.write('users.csv', csv_data)
```

### Import Data

```ruby
# Read CSV file
csv_data = File.read('users.csv')

# Import with table creation
db.import(csv_data, format: 'csv', create_table: true)

# Import SQL file
sql_data = File.read('backup.sql')
db.import(sql_data, format: 'sql')
```

## API Reference

### PizzaSQL.connect(uri, api_key = nil)

Creates a new PizzaSQL client connection.

**Parameters:**
- `uri` (String) - Database URI (e.g., `http://localhost:8080/mydb`)
- `api_key` (String, optional) - API key for authentication

**Returns:**
- `Client` - Connected client instance

**Example:**
```ruby
db = PizzaSQL.connect('http://localhost:8080/mydb')
```

### Client#sql(query)

Executes a SQL query and returns the results.

**Parameters:**
- `query` (String) - SQL query string

**Returns:**
- `Array<Hash>` - Array of rows (each row is a hash)

**Raises:**
- `RuntimeError` - If query fails

**Example:**
```ruby
rows = db.sql('SELECT * FROM users WHERE age > 18')
```

### Client#export(table: nil, format: 'sql')

Exports database or table data.

**Parameters:**
- `table` (String, optional) - Table name (nil for entire database)
- `format` (String) - Export format: `'sql'` or `'csv'`

**Returns:**
- `String` - Exported data

**Raises:**
- `RuntimeError` - If export fails

**Example:**
```ruby
data = db.export(table: 'users', format: 'csv')
```

### Client#import(data, format: 'sql', create_table: false)

Imports data into the database.

**Parameters:**
- `data` (String) - Data to import
- `format` (String) - Import format: `'sql'` or `'csv'`
- `create_table` (Boolean) - Create table if it doesn't exist (CSV only)

**Returns:**
- `nil`

**Raises:**
- `RuntimeError` - If import fails

**Example:**
```ruby
csv_data = File.read('users.csv')
db.import(csv_data, format: 'csv', create_table: true)
```

## Error Handling

All methods raise `RuntimeError` on failure. Use standard Ruby error handling:

```ruby
begin
  rows = db.sql('SELECT * FROM users')
rescue => e
  puts "Query failed: #{e.message}"
end
```

## License

MIT
