# PizzaSQL Python Client

A simple, Pythonic client for PizzaSQL.

## Installation

```bash
pip install pizzasql

# With httpx for better performance (optional)
pip install pizzasql[httpx]
```

## Quick Start

```python
from pizzasql import connect

db = connect('http://localhost:8080/mydb', api_key='your-api-key')

# Simple query
users = db.sql('SELECT * FROM users')
print(users)
# [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]

# Query with parameters
user = db.sql('SELECT * FROM users WHERE id = ?', [1])

# Iterate over results
for user in db.sql('SELECT * FROM users'):
    print(user['name'])
```

## API Reference

### `connect(uri, api_key=None)`

Create a new database connection.

```python
db = connect('http://localhost:8080/mydb', api_key='optional-api-key')
```

### `db.sql(query, params=None)`

Execute a query and return rows as dictionaries.

```python
# Simple query
rows = db.sql('SELECT * FROM users')

# With parameters (prevents SQL injection)
rows = db.sql('SELECT * FROM users WHERE age > ?', [18])

# Use with list comprehensions
names = [u['name'] for u in db.sql('SELECT name FROM users')]
```

### `db.query(query, params=None)`

Execute a query and return full result with metadata.

```python
result = db.query('SELECT * FROM users')
print(result.columns)        # [Column(name='id', type='INTEGER'), ...]
print(result.rows)           # [{'id': 1, 'name': 'Alice'}, ...]
print(result.execution_time) # '1.234ms'
print(len(result))           # Number of rows

# QueryResult is iterable
for row in result:
    print(row)
```

### `db.execute(statements, transaction=True)`

Execute multiple statements in a batch.

```python
result = db.execute([
    {'sql': 'INSERT INTO users (name) VALUES (?)', 'params': ['Alice']},
    {'sql': 'INSERT INTO users (name) VALUES (?)', 'params': ['Bob']},
], transaction=True)

print(result['totalRowsAffected'])  # 2
```

### `db.tables()`

List all tables.

```python
tables = db.tables()
# ['users', 'posts', 'comments']
```

### `db.schema(table_name)`

Get table schema.

```python
columns = db.schema('users')
# [Column(name='id', type='INTEGER'), Column(name='name', type='TEXT')]
```

### `db.use(database)`

Switch to a different database.

```python
other_db = db.use('other_database')
rows = other_db.sql('SELECT * FROM other_table')
```

## Examples

### CRUD Operations

```python
# Create
db.sql('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com'])

# Read
users = db.sql('SELECT * FROM users WHERE active = ?', [True])

# Update
db.sql('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 1])

# Delete
db.sql('DELETE FROM users WHERE id = ?', [1])
```

### Transactions

```python
db.execute([
    {'sql': 'UPDATE accounts SET balance = balance - ? WHERE id = ?', 'params': [100, 1]},
    {'sql': 'UPDATE accounts SET balance = balance + ? WHERE id = ?', 'params': [100, 2]},
], transaction=True)
```

### Context Manager

```python
with connect('http://localhost:8080/mydb') as db:
    users = db.sql('SELECT * FROM users')
    # Connection automatically closed
```

### Error Handling

```python
from pizzasql import connect, PizzaSQLError

try:
    db.sql('SELECT * FROM nonexistent_table')
except PizzaSQLError as e:
    print(e.code)     # 'TABLE_NOT_FOUND'
    print(e.message)  # "Table 'nonexistent_table' does not exist"
```

### Data Processing

```python
# List comprehension
emails = [u['email'] for u in db.sql('SELECT email FROM users')]

# Filter
active_users = [u for u in db.sql('SELECT * FROM users') if u['active']]

# Aggregation
from collections import Counter
status_counts = Counter(u['status'] for u in db.sql('SELECT status FROM users'))
```

## Configuration

```python
from pizzasql import PizzaSQL

db = PizzaSQL(
    'http://localhost:8080/mydb',
    api_key='your-api-key',
    timeout=60.0  # 60 seconds
)
```

## License

MIT
