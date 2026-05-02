package executor

import (
	"testing"

	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
)

func TestCollectColumnRefsWithExists(t *testing.T) {
	sql := "SELECT * FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)"
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	sel := stmt.(*parser.SelectStmt)
	refs := collectColumnRefs(sel.Where)

	t.Logf("WHERE type: %T", sel.Where)
	t.Logf("Column refs: %v", refs)
	t.Logf("Ref count: %d", len(refs))

	if len(refs) == 0 {
		t.Error("Expected non-empty refs for EXISTS clause, got 0")
	}

	hasSubquery := false
	for _, ref := range refs {
		if ref == "__subquery__" {
			hasSubquery = true
			break
		}
	}

	if !hasSubquery {
		t.Error("Expected __subquery__ sentinel in refs, but didn't find it")
	}
}

func TestCollectColumnRefsWithSubquery(t *testing.T) {
	sql := "SELECT c FROM t1 WHERE c>(SELECT avg(c) FROM t1)"
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	sel := stmt.(*parser.SelectStmt)
	refs := collectColumnRefs(sel.Where)

	t.Logf("WHERE type: %T", sel.Where)
	t.Logf("Column refs: %v", refs)
	t.Logf("Ref count: %d", len(refs))

	// Should have "c" and "__subquery__"
	if len(refs) < 2 {
		t.Errorf("Expected at least 2 refs (column and subquery sentinel), got %d", len(refs))
	}

	hasSubquery := false
	hasColumn := false
	for _, ref := range refs {
		if ref == "__subquery__" {
			hasSubquery = true
		}
		if ref == "c" {
			hasColumn = true
		}
	}

	if !hasSubquery {
		t.Error("Expected __subquery__ sentinel in refs")
	}
	if !hasColumn {
		t.Error("Expected column 'c' in refs")
	}
}
