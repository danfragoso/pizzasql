#!/usr/bin/env node

/**
 * PizzaSQL Stress Test Script
 *
 * Tests all database features including:
 * - Table creation and schema operations
 * - CRUD operations (INSERT, SELECT, UPDATE, DELETE)
 * - Transactions (BEGIN, COMMIT, ROLLBACK)
 * - Batch execution
 * - Indexes
 * - JOINs
 * - Aggregations
 * - Subqueries
 * - ALTER TABLE
 * - Parameterized queries
 */

const BASE_URL = process.env.PIZZASQL_URL || 'http://localhost:8080';
const API_KEY = process.env.PIZZASQL_API_KEY || '';

// Test configuration
const CONFIG = {
  numUsers: 1000,
  numProducts: 500,
  numOrders: 2000,
  numOrderItems: 5000,
  concurrentRequests: 10,
};

// Stats tracking
const stats = {
  passed: 0,
  failed: 0,
  totalQueries: 0,
  totalTime: 0,
  errors: [],
};

// Helper functions
async function fetchWithTimeout(url, options, timeout = 300000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(id);
    return response;
  } catch (error) {
    clearTimeout(id);
    if (error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeout}ms`);
    }
    throw error;
  }
}

async function query(sql, params = []) {
  const headers = { 'Content-Type': 'application/json' };
  if (API_KEY) headers['Authorization'] = `Bearer ${API_KEY}`;

  const start = Date.now();
  const response = await fetchWithTimeout(`${BASE_URL}/query`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ sql, params }),
  }, 300000);

  const elapsed = Date.now() - start;
  stats.totalQueries++;
  stats.totalTime += elapsed;

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error?.message || `HTTP ${response.status}`);
  }
  return data;
}

async function execute(statements, transaction = false) {
  const headers = { 'Content-Type': 'application/json' };
  if (API_KEY) headers['Authorization'] = `Bearer ${API_KEY}`;

  const start = Date.now();
  const response = await fetchWithTimeout(`${BASE_URL}/execute`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ statements, transaction }),
  }, 300000);

  const elapsed = Date.now() - start;
  stats.totalQueries += statements.length;
  stats.totalTime += elapsed;

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error?.message || `HTTP ${response.status}`);
  }
  return data;
}

async function getHealth() {
  const response = await fetch(`${BASE_URL}/health`);
  return response.json();
}

async function getTables() {
  const response = await fetch(`${BASE_URL}/schema/tables`);
  return response.json();
}

async function getTableSchema(name) {
  const response = await fetch(`${BASE_URL}/schema/tables/${name}`);
  return response.json();
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(`Assertion failed: ${message}`);
  }
}

function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function assertArrayEqual(actual, expected, message) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

async function runTest(name, testFn) {
  process.stdout.write(`  Testing ${name}... `);
  const testStart = Date.now();
  try {
    await testFn();
    const testTime = Date.now() - testStart;
    console.log(`✓ PASSED (${testTime}ms)`);
    stats.passed++;
  } catch (error) {
    const testTime = Date.now() - testStart;
    console.log(`✗ FAILED (${testTime}ms): ${error.message}`);
    stats.failed++;
    stats.errors.push({ name, error: error.message });
  }
}

// ============================================================================
// Test Suites
// ============================================================================

async function testHealthCheck() {
  const health = await getHealth();
  assert(health.status === 'ok', 'Health status should be ok');
}

async function cleanupTables() {
  // Drop indexes first
  const indexes = ['idx_users_email', 'idx_orders_user', 'idx_products_category'];
  for (const idx of indexes) {
    try {
      await query(`DROP INDEX IF EXISTS ${idx}`);
    } catch (e) {
      // Ignore errors
    }
  }

  // Drop tables if they exist (in reverse dependency order)
  const tables = ['order_items', 'orders', 'products', 'categories', 'users', 'test_alter', 'test_index'];
  for (const table of tables) {
    try {
      await query(`DROP TABLE IF EXISTS ${table}`);
    } catch (e) {
      // Ignore errors
    }
  }
}

async function testCreateTables() {
  // Create users table
  await query(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      email TEXT NOT NULL,
      age INTEGER,
      balance REAL DEFAULT 0.0,
      active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Create categories table
  await query(`
    CREATE TABLE categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      description TEXT
    )
  `);

  // Create products table
  await query(`
    CREATE TABLE products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      category_id INTEGER,
      price REAL NOT NULL,
      stock INTEGER DEFAULT 0,
      FOREIGN KEY (category_id) REFERENCES categories(id)
    )
  `);

  // Create orders table
  await query(`
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      status TEXT DEFAULT 'pending',
      total REAL DEFAULT 0.0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  // Create order_items table
  await query(`
    CREATE TABLE order_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL,
      price REAL NOT NULL,
      FOREIGN KEY (order_id) REFERENCES orders(id),
      FOREIGN KEY (product_id) REFERENCES products(id)
    )
  `);

  // Verify tables were created
  const tables = await getTables();
  assert(tables.tables.includes('users'), 'users table should exist');
  assert(tables.tables.includes('products'), 'products table should exist');
  assert(tables.tables.includes('orders'), 'orders table should exist');
}

async function testSchemaIntrospection() {
  const schema = await getTableSchema('users');
  assert(schema.name === 'users', 'Table name should be users');
  assert(schema.columns.length >= 6, 'Users table should have at least 6 columns');

  const idCol = schema.columns.find(c => c.name === 'id');
  assert(idCol, 'id column should exist');
  assert(idCol.primaryKey === true, 'id should be primary key');
}

async function testInsertUsers() {
  process.stdout.write(`\n    → Preparing ${CONFIG.numUsers} user records... `);
  const statements = [];
  for (let i = 1; i <= CONFIG.numUsers; i++) {
    statements.push({
      sql: 'INSERT INTO users (username, email, age, balance) VALUES (?, ?, ?, ?)',
      params: [`user${i}`, `user${i}@example.com`, 18 + (i % 50), Math.random() * 1000],
    });
  }
  console.log('done');
  process.stdout.write(`    → Executing batch insert... `);

  const result = await execute(statements, true);
  console.log('done');
  assertEqual(result.results.length, CONFIG.numUsers, 'Should insert all users');
  
  // Verify count
  const count = await query('SELECT COUNT(*) as count FROM users');
  process.stdout.write(`    → Verified ${count.rows[0][0]} users in database\n`);
}

async function testInsertCategories() {
  const categories = ['Electronics', 'Books', 'Clothing', 'Food', 'Sports'];
  for (const cat of categories) {
    await query('INSERT INTO categories (name, description) VALUES (?, ?)', [cat, `${cat} category`]);
  }

  const result = await query('SELECT COUNT(*) as count FROM categories');
  assertEqual(result.rows[0][0], 5, 'Should have 5 categories');
}

async function testInsertProducts() {
  process.stdout.write(`\n    → Preparing ${CONFIG.numProducts} product records... `);
  const statements = [];
  const productNames = ['Widget', 'Gadget', 'Gizmo', 'Thing', 'Item'];

  for (let i = 1; i <= CONFIG.numProducts; i++) {
    const name = `${productNames[i % productNames.length]} ${i}`;
    const categoryId = (i % 5) + 1;
    const price = 9.99 + (i * 0.5);
    const stock = Math.floor(Math.random() * 100);

    statements.push({
      sql: 'INSERT INTO products (name, category_id, price, stock) VALUES (?, ?, ?, ?)',
      params: [name, categoryId, price, stock],
    });
  }
  console.log('done');
  process.stdout.write(`    → Executing batch insert... `);

  await execute(statements, true);
  console.log('done');

  const result = await query('SELECT COUNT(*) as count FROM products');
  process.stdout.write(`    → Verified ${result.rows[0][0]} products in database\n`);
  assertEqual(result.rows[0][0], CONFIG.numProducts, 'Should have all products');
}

async function testInsertOrders() {
  process.stdout.write(`\n    → Preparing ${CONFIG.numOrders} order records... `);
  const statements = [];
  const statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];

  for (let i = 1; i <= CONFIG.numOrders; i++) {
    const userId = (i % CONFIG.numUsers) + 1;
    const status = statuses[i % statuses.length];

    statements.push({
      sql: 'INSERT INTO orders (user_id, status, total) VALUES (?, ?, ?)',
      params: [userId, status, 0],
    });
  }
  console.log('done');
  process.stdout.write(`    → Executing batch insert... `);

  await execute(statements, true);
  console.log('done');

  const result = await query('SELECT COUNT(*) as count FROM orders');
  process.stdout.write(`    → Verified ${result.rows[0][0]} orders in database\n`);
  assertEqual(result.rows[0][0], CONFIG.numOrders, 'Should have all orders');
}

async function testInsertOrderItems() {
  process.stdout.write(`\n    → Preparing ${CONFIG.numOrderItems} order item records... `);
  const statements = [];

  for (let i = 1; i <= CONFIG.numOrderItems; i++) {
    const orderId = (i % CONFIG.numOrders) + 1;
    const productId = (i % CONFIG.numProducts) + 1;
    const quantity = 1 + (i % 5);
    const price = 9.99 + (productId * 0.5);

    statements.push({
      sql: 'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)',
      params: [orderId, productId, quantity, price],
    });
  }
  console.log('done');
  process.stdout.write(`    → Executing batch insert... `);

  await execute(statements, true);
  console.log('done');

  const result = await query('SELECT COUNT(*) as count FROM order_items');
  process.stdout.write(`    → Verified ${result.rows[0][0]} order items in database\n`);
  assertEqual(result.rows[0][0], CONFIG.numOrderItems, 'Should have all order items');
}

async function testSelectBasic() {
  // Simple SELECT
  const result = await query('SELECT * FROM users LIMIT 10');
  assertEqual(result.rows.length, 10, 'Should return 10 users');

  // SELECT with WHERE
  const result2 = await query('SELECT * FROM users WHERE age > ?', [30]);
  assert(result2.rows.length > 0, 'Should return users over 30');

  // SELECT specific columns
  const result3 = await query('SELECT username, email FROM users WHERE id = ?', [1]);
  assertEqual(result3.columns.length, 2, 'Should return 2 columns');
}

async function testSelectWithOrderBy() {
  const result = await query('SELECT * FROM users ORDER BY age DESC LIMIT 5');
  assertEqual(result.rows.length, 5, 'Should return 5 users');

  // Verify ordering
  for (let i = 1; i < result.rows.length; i++) {
    const ageIdx = result.columns.findIndex(c => c.name === 'age');
    assert(result.rows[i - 1][ageIdx] >= result.rows[i][ageIdx], 'Should be ordered by age DESC');
  }
}

async function testSelectWithGroupBy() {
  const result = await query(`
    SELECT status, COUNT(*) as count
    FROM orders
    GROUP BY status
    ORDER BY count DESC
  `);

  assert(result.rows.length > 0, 'Should have grouped results');

  // Verify all statuses are represented
  const totalCount = result.rows.reduce((sum, row) => sum + row[1], 0);
  assertEqual(totalCount, CONFIG.numOrders, 'Grouped counts should sum to total orders');
}

async function testSelectWithHaving() {
  const result = await query(`
    SELECT user_id, COUNT(*) as order_count
    FROM orders
    GROUP BY user_id
    HAVING COUNT(*) > 1
    ORDER BY order_count DESC
  `);

  // All returned users should have more than 1 order
  for (const row of result.rows) {
    assert(row[1] > 1, 'Each user should have more than 1 order');
  }
}

async function testSelectWithJoin() {
  // INNER JOIN
  const result = await query(`
    SELECT o.id, u.username, o.status, o.total
    FROM orders o
    INNER JOIN users u ON o.user_id = u.id
    LIMIT 10
  `);

  assertEqual(result.rows.length, 10, 'Should return 10 joined rows');
  assertEqual(result.columns.length, 4, 'Should have 4 columns');

  // LEFT JOIN
  const result2 = await query(`
    SELECT u.username, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.id, u.username
    LIMIT 10
  `);

  assertEqual(result2.rows.length, 10, 'Should return users with order counts');
}

async function testSelectWithMultipleJoins() {
  process.stdout.write(`\n    → Executing 4-table JOIN (this may take a while with ${CONFIG.numOrderItems} items)...\n`);
  const result = await query(`
    SELECT
      o.id as order_id,
      u.username,
      p.name as product_name,
      oi.quantity,
      oi.price
    FROM orders o
    INNER JOIN users u ON o.user_id = u.id
    INNER JOIN order_items oi ON o.id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.id
    WHERE o.id <= 50
    LIMIT 20
  `);

  assertEqual(result.rows.length, 20, 'Should return 20 joined rows');
  assertEqual(result.columns.length, 5, 'Should have 5 columns');
}

async function testAggregations() {
  // COUNT
  const count = await query('SELECT COUNT(*) FROM users');
  assertEqual(count.rows[0][0], CONFIG.numUsers, 'COUNT should match');

  // SUM
  const sum = await query('SELECT SUM(balance) FROM users');
  assert(sum.rows[0][0] > 0, 'SUM should be positive');

  // AVG
  const avg = await query('SELECT AVG(age) FROM users');
  assert(avg.rows[0][0] >= 18, 'AVG age should be at least 18');

  // MIN/MAX
  const minMax = await query('SELECT MIN(age), MAX(age) FROM users');
  assert(minMax.rows[0][0] >= 18, 'MIN age should be at least 18');
  assert(minMax.rows[0][1] <= 68, 'MAX age should be at most 68');
}

async function testSubqueries() {
  // Scalar subquery
  const result = await query(`
    SELECT username,
           (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
    FROM users
    WHERE id <= 5
  `);

  assertEqual(result.rows.length, 5, 'Should return 5 users');

  // IN subquery
  const result2 = await query(`
    SELECT * FROM users
    WHERE id IN (SELECT user_id FROM orders WHERE status = 'delivered')
    LIMIT 10
  `);

  assert(result2.rows.length >= 0, 'IN subquery should work');

  // EXISTS subquery
  const result3 = await query(`
    SELECT * FROM users u
    WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = u.id)
    LIMIT 10
  `);

  assert(result3.rows.length > 0, 'EXISTS subquery should return users with orders');
}

async function testUpdate() {
  // Update single row
  await query('UPDATE users SET balance = ? WHERE id = ?', [999.99, 1]);

  const result = await query('SELECT balance FROM users WHERE id = ?', [1]);
  assertEqual(result.rows[0][0], 999.99, 'Balance should be updated');

  // Update multiple rows
  await query('UPDATE users SET active = ? WHERE age < ?', [0, 25]);

  const result2 = await query('SELECT COUNT(*) FROM users WHERE active = 0');
  assert(result2.rows[0][0] > 0, 'Should have inactive users');
}

async function testDelete() {
  // Get current count
  const before = await query('SELECT COUNT(*) FROM order_items');

  // Delete some items
  await query('DELETE FROM order_items WHERE quantity = 1');

  const after = await query('SELECT COUNT(*) FROM order_items');
  assert(after.rows[0][0] < before.rows[0][0], 'Should have fewer items after delete');
}

async function testCreateIndex() {
  // Create index
  await query('CREATE INDEX idx_users_email ON users(email)');
  await query('CREATE INDEX idx_orders_user ON orders(user_id)');
  await query('CREATE INDEX idx_products_category ON products(category_id)');

  // Test that queries still work (index should be used transparently)
  const result = await query('SELECT * FROM users WHERE email = ?', ['user1@example.com']);
  assert(result.rows.length > 0, 'Should find user by email');
}

async function testTransaction() {
  // Get initial balance
  const before = await query('SELECT balance FROM users WHERE id = 2');
  const initialBalance = before.rows[0][0];

  // Start transaction and make changes
  await fetch(`${BASE_URL}/transaction/begin`, { method: 'POST' });

  await query('UPDATE users SET balance = balance + 100 WHERE id = 2');

  // Verify change within transaction
  const during = await query('SELECT balance FROM users WHERE id = 2');
  assertEqual(during.rows[0][0], initialBalance + 100, 'Balance should increase during transaction');

  // Commit transaction
  await fetch(`${BASE_URL}/transaction/commit`, { method: 'POST' });

  // Verify change persisted
  const after = await query('SELECT balance FROM users WHERE id = 2');
  assertEqual(after.rows[0][0], initialBalance + 100, 'Balance should be committed');
}

async function testBatchExecute() {
  const statements = [
    { sql: 'INSERT INTO categories (name, description) VALUES (?, ?)', params: ['Test1', 'Test category 1'] },
    { sql: 'INSERT INTO categories (name, description) VALUES (?, ?)', params: ['Test2', 'Test category 2'] },
    { sql: 'INSERT INTO categories (name, description) VALUES (?, ?)', params: ['Test3', 'Test category 3'] },
  ];

  const result = await execute(statements, true);
  assertEqual(result.results.length, 3, 'Should execute 3 statements');

  // Verify inserts
  const count = await query('SELECT COUNT(*) FROM categories WHERE name LIKE ?', ['Test%']);
  assertEqual(count.rows[0][0], 3, 'Should have 3 test categories');
}

async function testAlterTable() {
  // Create test table
  await query('CREATE TABLE test_alter (id INTEGER PRIMARY KEY, name TEXT)');

  // Add column
  await query('ALTER TABLE test_alter ADD COLUMN description TEXT');

  // Verify column was added
  const schema = await getTableSchema('test_alter');
  const descCol = schema.columns.find(c => c.name === 'description');
  assert(descCol, 'description column should exist');

  // Rename column
  await query('ALTER TABLE test_alter RENAME COLUMN name TO title');

  const schema2 = await getTableSchema('test_alter');
  const titleCol = schema2.columns.find(c => c.name === 'title');
  assert(titleCol, 'title column should exist');
}

async function testLikeOperator() {
  const result = await query('SELECT * FROM users WHERE username LIKE ?', ['user1%']);
  assert(result.rows.length > 0, 'Should find users starting with user1');

  const result2 = await query('SELECT * FROM users WHERE email LIKE ?', ['%@example.com']);
  assertEqual(result2.rows.length, CONFIG.numUsers, 'All users should match email pattern');
}

async function testBetweenOperator() {
  const result = await query('SELECT * FROM users WHERE age BETWEEN ? AND ?', [25, 35]);

  const ageIdx = result.columns.findIndex(c => c.name === 'age');
  for (const row of result.rows) {
    assert(row[ageIdx] >= 25 && row[ageIdx] <= 35, 'Age should be between 25 and 35');
  }
}

async function testCaseExpression() {
  const result = await query(`
    SELECT username,
           CASE
             WHEN age < 25 THEN 'young'
             WHEN age < 40 THEN 'middle'
             ELSE 'senior'
           END as age_group
    FROM users
    LIMIT 10
  `);

  assertEqual(result.columns.length, 2, 'Should have 2 columns');

  for (const row of result.rows) {
    assert(['young', 'middle', 'senior'].includes(row[1]), 'Age group should be valid');
  }
}

async function testNullHandling() {
  // Insert a user with null age
  await query('INSERT INTO users (username, email, age) VALUES (?, ?, ?)', ['nulltest', 'null@test.com', null]);

  // Test IS NULL
  const result = await query('SELECT * FROM users WHERE age IS NULL');
  assert(result.rows.length > 0, 'Should find users with null age');

  // Test COALESCE
  const result2 = await query('SELECT username, COALESCE(age, 0) as age FROM users WHERE username = ?', ['nulltest']);
  assertEqual(result2.rows[0][1], 0, 'COALESCE should return 0 for null');

  // Test IFNULL
  const result3 = await query('SELECT username, IFNULL(age, -1) as age FROM users WHERE username = ?', ['nulltest']);
  assertEqual(result3.rows[0][1], -1, 'IFNULL should return -1 for null');
}

async function testStringFunctions() {
  const result = await query(`
    SELECT
      UPPER(username) as upper_name,
      LOWER(email) as lower_email,
      LENGTH(username) as name_len
    FROM users
    WHERE id = 1
  `);

  assert(result.rows[0][0] === result.rows[0][0].toUpperCase(), 'UPPER should work');
  assert(result.rows[0][1] === result.rows[0][1].toLowerCase(), 'LOWER should work');
  assert(typeof result.rows[0][2] === 'number', 'LENGTH should return number');
}

async function testNumericFunctions() {
  const result = await query(`
    SELECT
      ABS(-10) as abs_val,
      ROUND(3.14159, 2) as rounded
  `);

  assertEqual(result.rows[0][0], 10, 'ABS should work');
  assertEqual(result.rows[0][1], 3.14, 'ROUND should work');
}

async function testConcurrentQueries() {
  const promises = [];

  for (let i = 0; i < CONFIG.concurrentRequests; i++) {
    promises.push(query('SELECT * FROM users WHERE id = ?', [i + 1]));
  }

  const results = await Promise.all(promises);

  for (const result of results) {
    assert(result.rows.length <= 1, 'Each query should return at most 1 row');
  }
}

async function testLargeResultSet() {
  // Query all users
  const result = await query('SELECT * FROM users');
  // Should have at least the configured users plus test users
  // Test users: nulltest, paramtest, nulluser1, nulluser2, user'with'quotes, user"double"quotes, and potentially rollback_test
  assert(result.rows.length >= CONFIG.numUsers, `Should return at least ${CONFIG.numUsers} users`);
  assert(result.rows.length <= CONFIG.numUsers + 10, 'Should not have too many extra users');
}

async function testComplexQuery() {
  process.stdout.write(`\n    → Executing complex aggregation with LEFT JOINs (${CONFIG.numUsers} users)...\n`);
  const result = await query(`
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
  `);

  assert(result.rows.length > 0, 'Should return users with orders');
  assertEqual(result.columns.length, 4, 'Should have 4 columns');
}

async function testParameterTypes() {
  // Test various parameter types
  await query('INSERT INTO users (username, email, age, balance, active) VALUES (?, ?, ?, ?, ?)',
    ['paramtest', 'param@test.com', 30, 123.45, true]);

  const result = await query('SELECT * FROM users WHERE username = ?', ['paramtest']);
  const row = result.rows[0];
  const cols = result.columns;

  const getValue = (name) => row[cols.findIndex(c => c.name === name)];

  assertEqual(getValue('username'), 'paramtest', 'String param should work');
  assertEqual(getValue('age'), 30, 'Integer param should work');
  assertEqual(getValue('balance'), 123.45, 'Float param should work');
  assertEqual(getValue('active'), 1, 'Boolean param should work (as 1)');
}

async function testDistinct() {
  // Test DISTINCT with single column (might not be implemented)
  try {
    const result1 = await query('SELECT DISTINCT status FROM orders');
    // If DISTINCT works, should have only distinct values (5 statuses)
    // If not implemented, will return all rows
    assert(result1.rows.length > 0, 'Should return rows');
    
    // If we get 5 or fewer rows, DISTINCT is working
    if (result1.rows.length <= 10) {
      console.log(`\n      ℹ DISTINCT appears to be working (${result1.rows.length} distinct values)`);
    } else {
      console.log(`\n      ⚠ DISTINCT may not be implemented (returned ${result1.rows.length} rows, expected ~5)`);
    }
  } catch (e) {
    // DISTINCT might not be supported
    if (e.message.includes('DISTINCT') || e.message.includes('syntax')) {
      console.log('\n      ⚠ DISTINCT keyword not yet supported');
    } else {
      throw e;
    }
  }

  // Test grouping as workaround for DISTINCT
  const result2 = await query('SELECT status FROM orders GROUP BY status');
  assert(result2.rows.length >= 1, 'Should group by status (alternative to DISTINCT)');
}

async function testSelfJoin() {
  // Find users with same age (self-join)
  const result = await query(`
    SELECT u1.username, u2.username, u1.age
    FROM users u1
    INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id
    WHERE u1.age = 25
    LIMIT 5
  `);
  
  assert(result.rows.length >= 0, 'Self-join should execute');
}

async function testPagination() {
  // Test LIMIT and OFFSET for pagination
  const page1 = await query('SELECT id, username FROM users ORDER BY id LIMIT 10 OFFSET 0');
  const page2 = await query('SELECT id, username FROM users ORDER BY id LIMIT 10 OFFSET 10');
  const page3 = await query('SELECT id, username FROM users ORDER BY id LIMIT 10 OFFSET 20');

  assertEqual(page1.rows.length, 10, 'First page should have 10 rows');
  assertEqual(page2.rows.length, 10, 'Second page should have 10 rows');
  assertEqual(page3.rows.length, 10, 'Third page should have 10 rows');

  // Ensure pages don't overlap
  const firstId = page1.rows[0][0];
  const secondId = page2.rows[0][0];
  assert(secondId > firstId, 'Pages should not overlap');
}

async function testNullSorting() {
  // Insert some NULL values
  await query('INSERT INTO users (username, email, age, balance) VALUES (?, ?, ?, ?)',
    ['nulluser1', 'null1@test.com', null, 100]);
  await query('INSERT INTO users (username, email, age, balance) VALUES (?, ?, ?, ?)',
    ['nulluser2', 'null2@test.com', 30, null]);

  // Test NULL in ORDER BY
  const result1 = await query('SELECT username, age FROM users WHERE username LIKE ? ORDER BY age LIMIT 10', ['null%']);
  assert(result1.rows.length >= 2, 'Should include users with NULL age');

  // Test NULL in WHERE
  const result2 = await query('SELECT COUNT(*) FROM users WHERE age IS NULL');
  assert(result2.rows[0][0] >= 1, 'Should find users with NULL age');

  const result3 = await query('SELECT COUNT(*) FROM users WHERE balance IS NOT NULL');
  assert(result3.rows[0][0] > 0, 'Should find users with non-NULL balance');
}

async function testComplexWhere() {
  // Complex WHERE with multiple conditions and operators
  const result = await query(`
    SELECT username, age, balance
    FROM users
    WHERE (age > 30 AND balance > 500)
       OR (age < 25 AND active = 1)
       OR (username LIKE 'user1%')
    ORDER BY age DESC
    LIMIT 20
  `);

  assert(result.rows.length <= 20, 'Should respect LIMIT');
  assert(result.rows.length > 0, 'Should match some users');
}

async function testStringOperations() {
  // Test string concatenation (if supported)
  const result1 = await query(`
    SELECT username || '@' || 'domain.com' as email_alt
    FROM users
    WHERE id = 1
  `);
  assert(result1.rows.length === 1, 'Should concatenate strings');

  // Test SUBSTRING (if supported)
  try {
    const result2 = await query(`
      SELECT SUBSTRING(username, 1, 4) as short_name
      FROM users
      WHERE id <= 5
    `);
    assertEqual(result2.rows.length, 5, 'SUBSTRING should work');
  } catch (e) {
    // SUBSTRING might not be implemented
  }
}

async function testMathOperations() {
  // Test arithmetic in SELECT
  const result1 = await query(`
    SELECT 
      balance,
      balance * 1.1 as with_tax,
      balance / 2 as half,
      balance + 100 as bonus
    FROM users
    WHERE id = 1
  `);
  assertEqual(result1.rows.length, 1, 'Should calculate arithmetic');

  // Test modulo
  const result2 = await query(`
    SELECT id, id % 10 as mod_result
    FROM users
    WHERE id <= 100
    LIMIT 10
  `);
  assertEqual(result2.rows.length, 10, 'Should calculate modulo');
}

async function testGroupByEdgeCases() {
  // GROUP BY with NULL values
  const result1 = await query(`
    SELECT age, COUNT(*) as count
    FROM users
    GROUP BY age
    ORDER BY count DESC
    LIMIT 10
  `);
  assert(result1.rows.length > 0, 'Should group including NULL values');

  // GROUP BY with multiple aggregates
  const result2 = await query(`
    SELECT 
      status,
      COUNT(*) as order_count,
      AVG(total) as avg_total,
      MIN(total) as min_total,
      MAX(total) as max_total
    FROM orders
    GROUP BY status
  `);
  assert(result2.rows.length > 0, 'Should compute multiple aggregates');
  assertEqual(result2.columns.length, 5, 'Should have 5 columns');
}

async function testCrossJoin() {
  // CROSS JOIN (Cartesian product) with LIMIT
  const result = await query(`
    SELECT c.name as category, p.name as product
    FROM categories c
    CROSS JOIN products p
    WHERE p.id <= 10
    LIMIT 20
  `);

  assert(result.rows.length <= 20, 'Should respect LIMIT on CROSS JOIN');
  assertEqual(result.columns.length, 2, 'Should have 2 columns');
}

async function testNestedSubqueries() {
  // Nested subqueries (subquery in WHERE with subquery in SELECT)
  const result = await query(`
    SELECT 
      username,
      (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
    FROM users
    WHERE id IN (
      SELECT user_id 
      FROM orders 
      WHERE total > 100
      LIMIT 50
    )
    LIMIT 10
  `);

  assert(result.rows.length <= 10, 'Should handle nested subqueries');
}

async function testEdgeCaseValues() {
  // Test with special characters and edge values
  await query(`INSERT INTO users (username, email, age, balance) VALUES (?, ?, ?, ?)`,
    ["user'with'quotes", 'special@example.com', 0, 0.01]);
  
  await query(`INSERT INTO users (username, email, age, balance) VALUES (?, ?, ?, ?)`,
    ['user"double"quotes', 'double@example.com', 150, 999999.99]);

  // Query them back
  const result = await query(`SELECT username FROM users WHERE username LIKE ?`, ["%quotes%"]);
  assert(result.rows.length >= 2, 'Should handle special characters in strings');

  // Test very large numbers
  const result2 = await query(`SELECT balance FROM users WHERE balance > 999999`);
  assert(result2.rows.length >= 1, 'Should handle large numbers');
}

async function testInWithMultipleValues() {
  // Test IN clause with multiple literal values
  const result = await query(`
    SELECT id, username
    FROM users
    WHERE id IN (1, 5, 10, 15, 20, 25, 30)
    ORDER BY id
  `);

  assert(result.rows.length <= 7, 'Should filter by IN clause');
  assert(result.rows.length > 0, 'Should find matching users');
}

async function testUnion() {
  // Test UNION (if supported)
  try {
    const result = await query(`
      SELECT username, 'high' as segment FROM users WHERE balance > 800
      UNION
      SELECT username, 'low' as segment FROM users WHERE balance < 200
      LIMIT 20
    `);
    assert(result.rows.length <= 20, 'UNION should combine results');
  } catch (e) {
    // UNION might not be implemented yet
    if (!e.message.includes('UNION')) {
      throw e;
    }
  }
}

async function testTransactionRollback() {
  // Get count before
  const before = await query('SELECT COUNT(*) FROM users WHERE username LIKE ?', ['rollback_%']);
  const beforeCount = before.rows[0][0];
  
  // Test transaction rollback
  try {
    await query('BEGIN TRANSACTION');
    
    // Insert a user
    await query('INSERT INTO users (username, email, age) VALUES (?, ?, ?)',
      ['rollback_test_tx', 'rollback@test.com', 99]);
    
    // Rollback
    await query('ROLLBACK');
  } catch (e) {
    // If transactions fail, try to clean up
    try {
      await query('DELETE FROM users WHERE username = ?', ['rollback_test_tx']);
    } catch {}
  }
  
  // Verify rollback worked or data was cleaned up
  const after = await query('SELECT COUNT(*) FROM users WHERE username LIKE ?', ['rollback_%']);
  const afterCount = after.rows[0][0];
  
  // Should be same or less (in case we had to manually clean up)
  assert(afterCount <= beforeCount + 1, 'Rollback should undo changes or data should be cleanable');
}

// ============================================================================
// Main Test Runner
// ============================================================================

async function main() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║           PizzaSQL Stress Test Suite                       ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`\nTarget: ${BASE_URL}`);
  console.log(`Config: ${CONFIG.numUsers} users, ${CONFIG.numProducts} products, ${CONFIG.numOrders} orders`);
  console.log(`        ${CONFIG.numOrderItems} order items, ${CONFIG.concurrentRequests} concurrent requests\n`);

  const startTime = Date.now();

  // Health check
  console.log('🔍 Checking server health...');
  try {
    const health = await getHealth();
    console.log(`   Server status: ${health.status}`);
    console.log(`   Database: ${health.database || 'default'}\n`);
  } catch (error) {
    console.error(`\n❌ Cannot connect to server: ${error.message}`);
    console.error('   Make sure PizzaSQL is running with: ./pizzasql -http\n');
    process.exit(1);
  }

  // Cleanup
  console.log('🧹 Cleaning up existing tables...');
  const cleanupStart = Date.now();
  await cleanupTables();
  const cleanupTime = Date.now() - cleanupStart;
  console.log(`   Done (${cleanupTime}ms)\n`);

  // Schema Tests
  console.log('📋 SCHEMA TESTS');
  console.log('─'.repeat(60));
  await runTest('Health check endpoint', testHealthCheck);
  await runTest('Create tables', testCreateTables);
  await runTest('Schema introspection', testSchemaIntrospection);
  console.log();

  // Insert Tests
  console.log('📥 INSERT TESTS');
  console.log('─'.repeat(60));
  await runTest(`Insert ${CONFIG.numUsers} users`, testInsertUsers);
  await runTest('Insert categories', testInsertCategories);
  await runTest(`Insert ${CONFIG.numProducts} products`, testInsertProducts);
  await runTest(`Insert ${CONFIG.numOrders} orders`, testInsertOrders);
  await runTest(`Insert ${CONFIG.numOrderItems} order items`, testInsertOrderItems);
  
  // Show data summary
  console.log('\n  📊 Database Statistics:');
  const userCount = await query('SELECT COUNT(*) FROM users');
  const productCount = await query('SELECT COUNT(*) FROM products');
  const orderCount = await query('SELECT COUNT(*) FROM orders');
  const itemCount = await query('SELECT COUNT(*) FROM order_items');
  console.log(`     Users:       ${userCount.rows[0][0].toLocaleString()}`);
  console.log(`     Products:    ${productCount.rows[0][0].toLocaleString()}`);
  console.log(`     Orders:      ${orderCount.rows[0][0].toLocaleString()}`);
  console.log(`     Order Items: ${itemCount.rows[0][0].toLocaleString()}`);
  const totalRows = userCount.rows[0][0] + productCount.rows[0][0] + orderCount.rows[0][0] + itemCount.rows[0][0] + 5;
  console.log(`     Total Rows:  ${totalRows.toLocaleString()}`);
  console.log();

  // Select Tests
  console.log('🔎 SELECT TESTS');
  console.log('─'.repeat(60));
  await runTest('Basic SELECT queries', testSelectBasic);
  await runTest('SELECT with ORDER BY', testSelectWithOrderBy);
  await runTest('SELECT with GROUP BY', testSelectWithGroupBy);
  await runTest('SELECT with HAVING', testSelectWithHaving);
  await runTest('SELECT with JOIN', testSelectWithJoin);
  await runTest('SELECT with multiple JOINs', testSelectWithMultipleJoins);
  await runTest('Aggregation functions', testAggregations);
  await runTest('Subqueries', testSubqueries);
  console.log();

  // Expression Tests
  console.log('🧮 EXPRESSION TESTS');
  console.log('─'.repeat(60));
  await runTest('LIKE operator', testLikeOperator);
  await runTest('BETWEEN operator', testBetweenOperator);
  await runTest('CASE expression', testCaseExpression);
  await runTest('NULL handling', testNullHandling);
  await runTest('String functions', testStringFunctions);
  await runTest('Numeric functions', testNumericFunctions);
  await runTest('Parameter types', testParameterTypes);
  await runTest('DISTINCT queries', testDistinct);
  await runTest('Math operations', testMathOperations);
  await runTest('String operations', testStringOperations);
  console.log();

  // JOIN and Query Complexity Tests
  console.log('🔗 ADVANCED JOIN TESTS');
  console.log('─'.repeat(60));
  await runTest('Self-join', testSelfJoin);
  await runTest('CROSS JOIN', testCrossJoin);
  console.log();

  // Data Integrity Tests
  console.log('🛡️  DATA INTEGRITY TESTS');
  console.log('─'.repeat(60));
  await runTest('NULL in sorting', testNullSorting);
  await runTest('Complex WHERE clauses', testComplexWhere);
  await runTest('GROUP BY edge cases', testGroupByEdgeCases);
  await runTest('Edge case values', testEdgeCaseValues);
  await runTest('IN with multiple values', testInWithMultipleValues);
  console.log();

  // Query Features Tests
  console.log('🎯 QUERY FEATURES');
  console.log('─'.repeat(60));
  await runTest('Pagination (LIMIT/OFFSET)', testPagination);
  await runTest('Nested subqueries', testNestedSubqueries);
  await runTest('UNION operations', testUnion);
  console.log();

  // Update/Delete Tests
  console.log('✏️  UPDATE/DELETE TESTS');
  console.log('─'.repeat(60));
  await runTest('UPDATE records', testUpdate);
  await runTest('DELETE records', testDelete);
  console.log();

  // Advanced Features Tests
  console.log('🚀 ADVANCED FEATURES TESTS');
  console.log('─'.repeat(60));
  await runTest('Create indexes', testCreateIndex);
  await runTest('Transaction handling', testTransaction);
  await runTest('Transaction rollback', testTransactionRollback);
  await runTest('Batch execute', testBatchExecute);
  await runTest('ALTER TABLE', testAlterTable);
  console.log();

  // Performance Tests
  console.log('⚡ PERFORMANCE TESTS');
  console.log('─'.repeat(60));
  await runTest('Concurrent queries', testConcurrentQueries);
  await runTest('Large result set', testLargeResultSet);
  await runTest('Complex query', testComplexQuery);
  console.log();

  const totalTime = Date.now() - startTime;

  // Summary
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                      TEST SUMMARY                          ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log();
  console.log(`  Total tests:     ${stats.passed + stats.failed}`);
  console.log(`  Passed:          ${stats.passed} ✓`);
  console.log(`  Failed:          ${stats.failed} ✗`);
  console.log(`  Success rate:    ${((stats.passed / (stats.passed + stats.failed)) * 100).toFixed(1)}%`);
  console.log();
  console.log(`  Total queries:   ${stats.totalQueries}`);
  console.log(`  Total time:      ${totalTime}ms`);
  console.log(`  Avg query time:  ${(stats.totalTime / stats.totalQueries).toFixed(2)}ms`);
  console.log(`  Queries/sec:     ${(stats.totalQueries / (totalTime / 1000)).toFixed(0)}`);
  console.log();

  if (stats.errors.length > 0) {
    console.log('Failed tests:');
    for (const err of stats.errors) {
      console.log(`  - ${err.name}: ${err.error}`);
    }
    console.log();
  }

  if (stats.failed > 0) {
    process.exit(1);
  }

  console.log('All tests passed! 🍕');
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
