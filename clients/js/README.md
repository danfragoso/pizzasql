# PizzaSQL JavaScript/TypeScript Client

A lightweight client for PizzaSQL that works with Node.js, Bun, and browsers.

## Installation

```bash
# npm
npm install pizzasql

# bun
bun add pizzasql

# pnpm
pnpm add pizzasql
```

## Quick Start

```typescript
import { connect } from 'pizzasql';

const db = connect('http://localhost:8080/mydb', 'your-api-key');

// Simple query
const users = await db.sql('SELECT * FROM users');
console.log(users);
// [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]

// Query with parameters
const user = await db.sql('SELECT * FROM users WHERE id = ?', [1]);

// Chain operations
const names = (await db.sql('SELECT name FROM users')).map(u => u.name);
```

## API Reference

### `connect(uri, apiKey?)`

Create a new database connection.

```typescript
const db = connect('http://localhost:8080/mydb', 'optional-api-key');
```

### `db.sql(query, params?)`

Execute a query and return rows as objects.

```typescript
// Simple query
const rows = await db.sql('SELECT * FROM users');

// With parameters (prevents SQL injection)
const rows = await db.sql('SELECT * FROM users WHERE age > ?', [18]);

// With TypeScript types
interface User {
  id: number;
  name: string;
  email: string;
}
const users = await db.sql<User>('SELECT * FROM users');
```

### `db.query(query, params?)`

Execute a query and return full result with metadata.

```typescript
const result = await db.query('SELECT * FROM users');
console.log(result.columns);      // [{ name: 'id', type: 'INTEGER' }, ...]
console.log(result.rows);         // [{ id: 1, name: 'Alice' }, ...]
console.log(result.executionTime); // '1.234ms'
```

### `db.execute(statements, transaction?)`

Execute multiple statements in a batch.

```typescript
const result = await db.execute([
  { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
  { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Bob'] },
], true); // true = wrap in transaction

console.log(result.totalRowsAffected); // 2
```

### `db.tables()`

List all tables.

```typescript
const tables = await db.tables();
// ['users', 'posts', 'comments']
```

### `db.schema(tableName)`

Get table schema.

```typescript
const columns = await db.schema('users');
// [{ name: 'id', type: 'INTEGER' }, { name: 'name', type: 'TEXT' }]
```

### `db.use(database)`

Switch to a different database.

```typescript
const otherDb = db.use('other_database');
const rows = await otherDb.sql('SELECT * FROM other_table');
```

## Examples

### CRUD Operations

```typescript
// Create
await db.sql('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);

// Read
const users = await db.sql('SELECT * FROM users WHERE active = ?', [true]);

// Update
await db.sql('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 1]);

// Delete
await db.sql('DELETE FROM users WHERE id = ?', [1]);
```

### Transactions

```typescript
await db.execute([
  { sql: 'UPDATE accounts SET balance = balance - ? WHERE id = ?', params: [100, 1] },
  { sql: 'UPDATE accounts SET balance = balance + ? WHERE id = ?', params: [100, 2] },
], true);
```

### Error Handling

```typescript
try {
  await db.sql('SELECT * FROM nonexistent_table');
} catch (error) {
  console.error(error.message);
  // [TABLE_NOT_FOUND] Table 'nonexistent_table' does not exist
}
```

## Configuration

```typescript
import { createClient } from 'pizzasql';

const db = createClient('http://localhost:8080/mydb', {
  apiKey: 'your-api-key',
  timeout: 60000, // 60 seconds
});
```

## License

MIT
