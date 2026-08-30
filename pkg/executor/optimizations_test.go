package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// newOptExec creates an executor backed by PizzaKV, skipping the test when it
// is unavailable (mirroring the convention in executor_test.go).
func newOptExec(t *testing.T, db string) *Executor {
	t.Helper()
	pool, err := storage.NewKVPool("localhost:8085", 5, 5*time.Second)
	if err != nil {
		t.Skipf("PizzaKV not available: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	schema := storage.NewSchemaManager(pool, db)
	table := storage.NewTableManager(pool, schema, db)
	return New(schema, table)
}

// intColumn extracts a column of int64 values from a result.
func intColumn(t *testing.T, r *Result, col int) []int64 {
	t.Helper()
	out := make([]int64, 0, len(r.Rows))
	for _, row := range r.Rows {
		if col >= len(row) {
			t.Fatalf("row too short: %v", row)
		}
		out = append(out, row[col].(int64))
	}
	return out
}

// TestTopNOrderByLimitMatchesFullSort verifies bounded top-N execution returns
// exactly the same rows (order included) as a full sort followed by LIMIT/OFFSET.
func TestTopNOrderByLimitMatchesFullSort(t *testing.T) {
	exec := newOptExec(t, "test_topn_db")
	execSQL(exec, "DROP TABLE IF EXISTS nums")
	if _, err := execSQL(exec, "CREATE TABLE nums (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer execSQL(exec, "DROP TABLE IF EXISTS nums")

	const n = 500
	for i := 0; i < n; i++ {
		// Deterministic permutation of 0..n-1.
		v := (i*137 + 41) % n
		if _, err := execSQL(exec, fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i+1, v)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	for _, desc := range []bool{false, true} {
		for _, offset := range []int{0, 3, 47, n - 1} {
			for _, limit := range []int{1, 2, 17, 100, n + 5} {
				dir := "ASC"
				if desc {
					dir = "DESC"
				}
				fullRes, err := execSQL(exec, fmt.Sprintf("SELECT v FROM nums ORDER BY v %s", dir))
				if err != nil {
					t.Fatalf("full: %v", err)
				}
				full := intColumn(t, fullRes, 0)

				q := fmt.Sprintf("SELECT v FROM nums ORDER BY v %s LIMIT %d OFFSET %d", dir, limit, offset)
				limRes, err := execSQL(exec, q)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				got := intColumn(t, limRes, 0)

				want := sliceRange(full, offset, limit)
				if !equalInt64s(got, want) {
					t.Fatalf("%s: got %v want %v", q, got, want)
				}
			}
		}
	}
}

// TestTopNOrderByLimitTies verifies LIMIT/OFFSET with tied ORDER BY keys returns
// the correct multiset of values even though tie ordering is unspecified.
func TestTopNOrderByLimitTies(t *testing.T) {
	exec := newOptExec(t, "test_topn_ties_db")
	execSQL(exec, "DROP TABLE IF EXISTS ties")
	if _, err := execSQL(exec, "CREATE TABLE ties (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer execSQL(exec, "DROP TABLE IF EXISTS ties")

	// 4 rows with v=1, 2 rows with v=2, 1 row with v=3.
	vals := []int{1, 1, 1, 1, 2, 2, 3}
	for i, v := range vals {
		if _, err := execSQL(exec, fmt.Sprintf("INSERT INTO ties VALUES (%d, %d)", i+1, v)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// LIMIT 3: the three smallest, all v=1.
	res, err := execSQL(exec, "SELECT v FROM ties ORDER BY v LIMIT 3")
	if err != nil {
		t.Fatalf("limit 3: %v", err)
	}
	got := intColumn(t, res, 0)
	if len(got) != 3 || got[0] != 1 || got[1] != 1 || got[2] != 1 {
		t.Fatalf("LIMIT 3 got %v, want [1 1 1]", got)
	}

	// OFFSET 3 LIMIT 3: skip three v=1 rows, then one v=1 + two v=2.
	res, err = execSQL(exec, "SELECT v FROM ties ORDER BY v LIMIT 3 OFFSET 3")
	if err != nil {
		t.Fatalf("offset 3 limit 3: %v", err)
	}
	got = intColumn(t, res, 0)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("OFFSET 3 LIMIT 3 got %v, want [1 2 2]", got)
	}

	// OFFSET beyond the ties boundary.
	res, err = execSQL(exec, "SELECT v FROM ties ORDER BY v LIMIT 2 OFFSET 5")
	if err != nil {
		t.Fatalf("offset 5 limit 2: %v", err)
	}
	got = intColumn(t, res, 0)
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("OFFSET 5 LIMIT 2 got %v, want [2 3]", got)
	}
}

// TestTopNGroupByOrderLimit verifies the grouped/result-row top-N path.
func TestTopNGroupByOrderLimit(t *testing.T) {
	exec := newOptExec(t, "test_topn_group_db")
	execSQL(exec, "DROP TABLE IF EXISTS sales")
	if _, err := execSQL(exec, "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer execSQL(exec, "DROP TABLE IF EXISTS sales")

	for _, r := range []struct {
		id     int
		region string
		amount int
	}{
		{1, "east", 10}, {2, "west", 20}, {3, "north", 30},
		{4, "south", 40}, {5, "east", 50},
	} {
		if _, err := execSQL(exec, fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', %d)", r.id, r.region, r.amount)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	res, err := execSQL(exec, "SELECT region, COUNT(*) AS c FROM sales GROUP BY region ORDER BY region LIMIT 2")
	if err != nil {
		t.Fatalf("group topn: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(res.Rows), res.Rows)
	}
	if res.Rows[0][0] != "east" || res.Rows[1][0] != "north" {
		t.Fatalf("unexpected top-2 regions: %v", res.Rows)
	}
	if res.Rows[0][1] != int64(2) {
		t.Fatalf("unexpected east count: %v", res.Rows[0][1])
	}
}

// TestCountFastPathLifecycleAndRollback verifies the exact COUNT(*) fast path
// across writes and transaction rollback.
func TestCountFastPathLifecycleAndRollback(t *testing.T) {
	exec := newOptExec(t, "test_count_db")
	execSQL(exec, "DROP TABLE IF EXISTS items")
	if _, err := execSQL(exec, "CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer execSQL(exec, "DROP TABLE IF EXISTS items")

	countStar := func() int64 {
		t.Helper()
		res, err := execSQL(exec, "SELECT COUNT(*) FROM items")
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		return res.Rows[0][0].(int64)
	}

	if got := countStar(); got != 0 {
		t.Fatalf("initial count = %d, want 0", got)
	}
	for i := 1; i <= 5; i++ {
		if _, err := execSQL(exec, fmt.Sprintf("INSERT INTO items VALUES (%d, 'x%d')", i, i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if got := countStar(); got != 5 {
		t.Fatalf("after inserts = %d, want 5", got)
	}

	// UPDATE keeps the count exact.
	if _, err := execSQL(exec, "UPDATE items SET v = 'y' WHERE id = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := countStar(); got != 5 {
		t.Fatalf("after update = %d, want 5", got)
	}

	// Bulk insert via INSERT ... SELECT.
	if _, err := execSQL(exec, "INSERT INTO items (id, v) SELECT id + 100, v FROM items"); err != nil {
		t.Fatalf("insert-select: %v", err)
	}
	if got := countStar(); got != 10 {
		t.Fatalf("after insert-select = %d, want 10", got)
	}

	// DELETE decrements.
	if _, err := execSQL(exec, "DELETE FROM items WHERE id > 100"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := countStar(); got != 5 {
		t.Fatalf("after delete = %d, want 5", got)
	}

	// Transaction rollback restores the exact count.
	execSQL(exec, "BEGIN")
	if _, err := execSQL(exec, "INSERT INTO items VALUES (999, 'tmp')"); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if got := countStar(); got != 6 {
		t.Fatalf("inside tx = %d, want 6", got)
	}
	if _, err := execSQL(exec, "ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := countStar(); got != 5 {
		t.Fatalf("after rollback = %d, want 5", got)
	}
}

// TestCountFastPathRestart verifies a second executor (fresh process state)
// derives the same exact count from durable rows.
func TestCountFastPathRestart(t *testing.T) {
	exec := newOptExec(t, "test_count_restart_db")
	execSQL(exec, "DROP TABLE IF EXISTS r")
	if _, err := execSQL(exec, "CREATE TABLE r (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer execSQL(exec, "DROP TABLE IF EXISTS r")
	for i := 1; i <= 7; i++ {
		if _, err := execSQL(exec, fmt.Sprintf("INSERT INTO r VALUES (%d)", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// A brand-new executor over the same KV.
	exec2 := newOptExec(t, "test_count_restart_db")
	res, err := execSQL(exec2, "SELECT COUNT(*) FROM r")
	if err != nil {
		t.Fatalf("count after restart: %v", err)
	}
	if got := res.Rows[0][0].(int64); got != 7 {
		t.Fatalf("count after restart = %d, want 7", got)
	}
}

// TestCountFastPathUnsupportedShapesStillCorrect verifies shapes outside the
// fast path fall through to the normal scan and produce correct results.
func TestCountFastPathUnsupportedShapesStillCorrect(t *testing.T) {
	exec := newOptExec(t, "test_count_unsupported_db")
	execSQL(exec, "DROP TABLE IF EXISTS t2")
	execSQL(exec, "DROP TABLE IF EXISTS t1")
	if _, err := execSQL(exec, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)"); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	if _, err := execSQL(exec, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, y INTEGER)"); err != nil {
		t.Fatalf("create t2: %v", err)
	}
	defer execSQL(exec, "DROP TABLE IF EXISTS t2")
	defer execSQL(exec, "DROP TABLE IF EXISTS t1")

	for i := 1; i <= 4; i++ {
		execSQL(exec, fmt.Sprintf("INSERT INTO t1 VALUES (%d, %d)", i, i))
		execSQL(exec, fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i, i))
	}

	cases := []struct {
		q    string
		want int64
	}{
		{"SELECT COUNT(*) FROM t1 WHERE x > 2", 2},
		{"SELECT COUNT(DISTINCT x) FROM t1", 4},
		{"SELECT COUNT(*) FROM t1 t1a, t2 t2b", 16},
	}
	for _, c := range cases {
		res, err := execSQL(exec, c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if got := res.Rows[0][0].(int64); got != c.want {
			t.Fatalf("%s = %d, want %d", c.q, got, c.want)
		}
	}
}

func sliceRange(v []int64, offset, limit int) []int64 {
	if offset >= len(v) {
		return nil
	}
	end := offset + limit
	if end > len(v) {
		end = len(v)
	}
	return v[offset:end]
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
