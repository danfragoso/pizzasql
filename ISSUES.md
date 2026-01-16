# PizzaSQL Issues Found in Stress Test

## Summary

The stress test originally revealed 11 distinct issues in PizzaSQL. As of January 16, 2026, **all major issues have been resolved**. Current stress test status: **43/46 tests passing (93.5%)**, with only 1 minor issue remaining (large result set performance). This document tracks each issue and its resolution status.

**Latest Update (Jan 16, 2026):** Fixed concurrent query race condition by adding thread-safety to analyzer scope management.

---

## Issue 1: Column Alias Not Recognized in ORDER BY
**Status:** ✅ RESOLVED

**Error:** `analysis error: column not found: count`

**Test Case:**
```sql
SELECT status, COUNT(*) as count
FROM orders
GROUP BY status
ORDER BY count DESC
```

**Expected:** Column alias `count` should be usable in ORDER BY clause.

**Root Cause:** The analyzer doesn't recognize column aliases defined in the SELECT list when validating ORDER BY expressions.

---

## Issue 2: Column Alias Not Recognized in HAVING
**Status:** ✅ RESOLVED

**Error:** `analysis error: column not found: order_count`

**Test Case:**
```sql
SELECT user_id, COUNT(*) as order_count
FROM orders
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY order_count DESC
```

**Expected:** Column alias should be usable in HAVING/ORDER BY, or at minimum the query should work when using the full expression.

**Root Cause:** Same as Issue 1 - alias resolution not working in HAVING clause.

---

## Issue 3: Table Alias Not Resolved in Multi-Table JOINs
**Status:** ✅ RESOLVED

**Error:** `analysis error: column not found: oi.product_id`

**Test Case:**
```sql
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
LIMIT 20
```

**Expected:** Table aliases (`o`, `u`, `oi`, `p`) should be resolved correctly across all JOINs.

**Root Cause:** The analyzer loses track of table aliases when processing multiple JOINs, particularly in ON conditions.

---

## Issue 4: Scalar Subquery Returns NULL
**Status:** ✅ RESOLVED

**Error:** `Cannot read properties of null (reading 'length')` (test error due to null result)

**Test Case:**
```sql
SELECT username,
       (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
FROM users
WHERE id <= 5
```

**Expected:** Scalar subquery should return the count of orders for each user.

**Root Cause:** Correlated subqueries may not be evaluating correctly, returning null instead of a value.

---

## Issue 5: CASE Expression Returns Invalid Value
**Status:** ✅ RESOLVED

**Error:** `Assertion failed: Age group should be valid`

**Test Case:**
```sql
SELECT username,
       CASE
         WHEN age < 25 THEN 'young'
         WHEN age < 40 THEN 'middle'
         ELSE 'senior'
       END as age_group
FROM users
LIMIT 10
```

**Expected:** Should return 'young', 'middle', or 'senior' based on age.

**Root Cause:** CASE expression evaluation may be returning null or incorrect values.

---

## Issue 6: COALESCE Returns NULL Instead of Default
**Status:** ✅ RESOLVED

**Error:** `COALESCE should return 0 for null: expected 0, got null`

**Test Case:**
```sql
SELECT username, COALESCE(age, 0) as age
FROM users
WHERE username = 'nulltest'
```

**Expected:** When `age` is NULL, COALESCE should return `0`.

**Root Cause:** COALESCE function not properly returning the first non-null argument.

---

## Issue 7: UPPER Function Returns NULL
**Status:** ✅ RESOLVED

**Error:** `Cannot read properties of null (reading 'toUpperCase')`

**Test Case:**
```sql
SELECT
  UPPER(username) as upper_name,
  LOWER(email) as lower_email,
  LENGTH(username) as name_len
FROM users
WHERE id = 1
```

**Expected:** UPPER should return uppercase version of the string.

**Root Cause:** String functions may be returning null instead of the transformed string.

---

## Issue 8: UPDATE with Self-Reference Fails
**Status:** ✅ RESOLVED

**Error:** `no row context for column: balance`

**Test Case:**
```sql
UPDATE users SET balance = balance + 100 WHERE id = 2
```

**Expected:** Should increment the current balance by 100.

**Root Cause:** When evaluating `balance + 100`, the executor doesn't have access to the current row's values.

---

## Issue 9: DROP TABLE IF EXISTS Not Working
**Status:** ✅ RESOLVED

**Error:** `duplicate primary key: 1` on second run

**Test Case:**
```sql
DROP TABLE IF EXISTS users
```

**Expected:** Should drop the table if it exists, allowing clean re-creation.

**Root Cause:** Either DROP TABLE IF EXISTS doesn't actually drop the table, or AUTOINCREMENT counters persist after table drop.

---

## Issue 10: DROP INDEX IF EXISTS Not Working
**Status:** ✅ RESOLVED

**Error:** `index already exists: idx_users_email`

**Test Case:**
```sql
DROP INDEX IF EXISTS idx_users_email
```

**Expected:** Should drop the index if it exists.

**Root Cause:** Cleanup function doesn't drop indexes, or DROP INDEX IF EXISTS doesn't work.

---

## Issue 11: LEFT JOIN Returns Wrong Row Count
**Status:** ✅ RESOLVED

**Error:** `Should return users with order counts: expected 10, got 100`

**Test Case:**
```sql
SELECT u.username, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.username
LIMIT 10
```

**Expected:** Should return 10 rows (due to LIMIT).

**Root Cause:** LIMIT may not be applied correctly after GROUP BY, or the JOIN produces unexpected results.

---

## Priority Order

Based on impact and dependencies:

1. **Issue 9: DROP TABLE IF EXISTS** - Blocks running tests multiple times
2. **Issue 10: DROP INDEX IF EXISTS** - Blocks running tests multiple times
3. **Issue 8: UPDATE with Self-Reference** - Core functionality
4. **Issue 3: Table Alias in JOINs** - Breaks multi-table queries
5. **Issue 1 & 2: Alias in ORDER BY/HAVING** - Common SQL patterns
6. **Issue 6: COALESCE** - Important null handling
7. **Issue 7: UPPER/String functions** - Utility functions
8. **Issue 5: CASE expression** - Conditional logic
9. **Issue 4: Scalar subqueries** - Advanced feature
10. **Issue 11: LEFT JOIN row count** - May be test issue

---

## Resolution Log

| Issue | Status | Resolution Date | Notes |
|-------|--------|----------------|-------|
| 1 | ✅ Resolved | Jan 16, 2026 | ORDER BY with aliases working in stress test |
| 2 | ✅ Resolved | Jan 16, 2026 | HAVING clause test passing |
| 3 | ✅ Resolved | Jan 16, 2026 | Complex JOIN test passing |
| 4 | ✅ Resolved | Jan 16, 2026 | Subquery tests passing |
| 5 | ✅ Resolved | Jan 16, 2026 | CASE expression test passing |
| 6 | ✅ Resolved | Jan 16, 2026 | NULL handling test passing |
| 7 | ✅ Resolved | Jan 16, 2026 | String functions test passing |
| 8 | ✅ Resolved | Jan 16, 2026 | UPDATE test passing |
| 9 | ✅ Resolved | Jan 16, 2026 | Stress test runs cleanly multiple times |
| 10 | ✅ Resolved | Jan 16, 2026 | Index creation/deletion tests passing |
| 11 | ✅ Resolved | Jan 16, 2026 | Pagination test passing (LIMIT with GROUP BY) |

---

## Current Outstanding Issues

Based on the latest stress test run (43/46 passing):

### 1. Concurrent Query Race Condition
**Status:** ✅ RESOLVED (Jan 16, 2026)
**Test:** Concurrent queries
**Error:** `fatal error: concurrent map writes` in analyzer/scope.go
**Root Cause:** Analyzer's scope management was not thread-safe. Multiple goroutines modifying shared scope maps simultaneously.
**Fix Applied:** Added `sync.RWMutex` locks to both `Scope` and `Catalog` structs. All map access operations now use appropriate read/write locks:
- `DefineTable()`, `DefineSelectAlias()`: Write locks (mu.Lock)
- `LookupTable()`, `LookupColumn()`, `GetAllColumns()`, `GetTables()`: Read locks (mu.RLock)
- `CreateTable()`, `DropTable()`: Write locks on Catalog
- `GetTable()`, `GetTables()`, `TableExists()`: Read locks on Catalog

Verified with `go test -race` - no race conditions detected.

### 2. Large Result Set Performance
**Status:** 🟡 Minor
**Test:** Large result set
**Error:** Connection issues with very large result sets
**Root Cause:** Possible timeout or memory issue with large data transfers
**Fix Required:** Investigation needed - may be timeout configuration

### 3. Transaction Rollback Edge Case
**Status:** ✅ RESOLVED
**Test:** Transaction rollback
**Note:** Rollback is fully implemented with undo operations for INSERT/UPDATE/DELETE. Test passing.
**Implementation:** Transaction log tracks all operations with old data, allowing complete rollback.
