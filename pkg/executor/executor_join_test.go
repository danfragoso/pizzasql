package executor

import (
	"testing"

	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

func TestMergeRowsDoesNotOverwriteColumns(t *testing.T) {
	e := &Executor{}

	// Create two rows with overlapping column names (like "id")
	left := storage.Row{
		"id":   "left-id-123",
		"name": "LeftName",
	}

	right := storage.Row{
		"id":    "right-id-456",
		"value": "RightValue",
	}

	// Merge with aliases
	merged := e.mergeRows(left, right, "o", "om")

	// Check that both qualified names exist and are correct
	if merged["o.id"] != "left-id-123" {
		t.Errorf("Expected o.id = 'left-id-123', got %v", merged["o.id"])
	}

	if merged["om.id"] != "right-id-456" {
		t.Errorf("Expected om.id = 'right-id-456', got %v", merged["om.id"])
	}

	// Check that the unqualified "id" is from the left table (first one wins)
	if merged["id"] != "left-id-123" {
		t.Errorf("Expected unqualified id = 'left-id-123' (from left table), got %v", merged["id"])
	}

	// Check other columns are present
	if merged["o.name"] != "LeftName" {
		t.Errorf("Expected o.name = 'LeftName', got %v", merged["o.name"])
	}

	if merged["om.value"] != "RightValue" {
		t.Errorf("Expected om.value = 'RightValue', got %v", merged["om.value"])
	}
}

func TestJoinConditionWithQualifiedNames(t *testing.T) {
	e := &Executor{}

	// Simulate two rows from different tables with the same column name "id"
	orgRow := storage.Row{
		"id":   "org-123",
		"name": "Organization 1",
	}

	memberRow := storage.Row{
		"id":     "member-456",
		"org_id": "org-123", // This should match orgRow's id
	}

	// Merge with table aliases
	merged := e.mergeRows(orgRow, memberRow, "o", "om")

	// Verify that o.id and om.org_id have the correct values for comparison
	// This is what the JOIN condition would use: o.id = om.org_id
	if merged["o.id"] != "org-123" {
		t.Errorf("Expected o.id = 'org-123', got %v", merged["o.id"])
	}

	if merged["om.org_id"] != "org-123" {
		t.Errorf("Expected om.org_id = 'org-123', got %v", merged["om.org_id"])
	}

	// The key fix: om.org_id should NOT have been overwritten by the right table's "id"
	// In the old buggy code, this would have been "member-456" instead of "org-123"
	if merged["om.org_id"] == merged["om.id"] {
		t.Log("✓ JOIN condition can correctly compare o.id with om.org_id")
	}
}

func TestLeftJoinWithNullConditions(t *testing.T) {
	e := &Executor{}

	// Create test rows
	leftRows := []storage.Row{
		{"col1": int64(1), "col2": "a"},
		{"col1": int64(2), "col2": "b"},
	}

	// Test merging left row with null right row
	left := leftRows[0]
	nullRight := storage.Row{
		"col3": nil,
		"col4": nil,
	}

	merged := e.mergeRows(left, nullRight, "cor0", "cor1")

	// Verify left columns are preserved
	if merged["cor0.col1"] != int64(1) {
		t.Errorf("Expected cor0.col1 = 1, got %v", merged["cor0.col1"])
	}
	if merged["cor0.col2"] != "a" {
		t.Errorf("Expected cor0.col2 = 'a', got %v", merged["cor0.col2"])
	}

	// Verify right columns are NULL
	if merged["cor1.col3"] != nil {
		t.Errorf("Expected cor1.col3 = nil, got %v", merged["cor1.col3"])
	}
	if merged["cor1.col4"] != nil {
		t.Errorf("Expected cor1.col4 = nil, got %v", merged["cor1.col4"])
	}

	// Verify unqualified columns exist
	if merged["col1"] != int64(1) {
		t.Errorf("Expected col1 = 1, got %v", merged["col1"])
	}
	if merged["col2"] != "a" {
		t.Errorf("Expected col2 = 'a', got %v", merged["col2"])
	}
	if merged["col3"] != nil {
		t.Errorf("Expected col3 = nil, got %v", merged["col3"])
	}
	if merged["col4"] != nil {
		t.Errorf("Expected col4 = nil, got %v", merged["col4"])
	}

	t.Log("✓ LEFT JOIN with NULL conditions creates proper null rows for unmatched right table")
}
