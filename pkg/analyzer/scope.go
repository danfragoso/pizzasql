package analyzer

import (
	"strings"
	"sync"
)

// Scope represents a symbol table scope for name resolution.
type Scope struct {
	mu      sync.RWMutex
	parent  *Scope
	tables  map[string]*TableInfo  // Tables in this scope
	columns map[string]*ColumnInfo // Direct column references (for single table queries)
}

// NewScope creates a new empty scope.
func NewScope(parent *Scope) *Scope {
	return &Scope{
		parent:  parent,
		tables:  make(map[string]*TableInfo),
		columns: make(map[string]*ColumnInfo),
	}
}

// DefineTable adds a table to this scope.
func (s *Scope) DefineTable(info *TableInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	name := strings.ToUpper(info.Name)
	s.tables[name] = info

	// If there's an alias, also register by alias
	if info.Alias != "" {
		s.tables[strings.ToUpper(info.Alias)] = info
	}

	// Add columns to direct reference if this is the only table
	// This allows unqualified column references
	for i := range info.Columns {
		col := &info.Columns[i]
		s.columns[strings.ToUpper(col.Name)] = col
	}
}

// DefineSelectAlias registers a SELECT column alias for ORDER BY/HAVING reference.
func (s *Scope) DefineSelectAlias(alias string, colType Type) {
	s.mu.Lock()
	defer s.mu.Unlock()

	upper := strings.ToUpper(alias)
	// Create a virtual column for the alias
	s.columns[upper] = &ColumnInfo{
		Name: alias,
		Type: colType,
	}
}

// LookupTable finds a table by name or alias.
func (s *Scope) LookupTable(name string) (*TableInfo, bool) {
	s.mu.RLock()
	upper := strings.ToUpper(name)
	t, ok := s.tables[upper]
	s.mu.RUnlock()

	if ok {
		return t, true
	}
	if s.parent != nil {
		return s.parent.LookupTable(name)
	}
	return nil, false
}

// LookupColumn finds a column, optionally qualified by table name.
func (s *Scope) LookupColumn(tableName, columnName string) (*ColumnInfo, *TableInfo, bool) {
	upperCol := strings.ToUpper(columnName)

	if tableName != "" {
		// Qualified reference: table.column
		table, ok := s.LookupTable(tableName)
		if !ok {
			return nil, nil, false
		}
		col, ok := table.GetColumn(columnName)
		if !ok {
			return nil, table, false
		}
		return col, table, true
	}

	s.mu.RLock()
	// Unqualified reference: try direct column lookup first
	if col, ok := s.columns[upperCol]; ok {
		// Find which table this column belongs to
		for _, t := range s.tables {
			if _, found := t.GetColumn(columnName); found {
				s.mu.RUnlock()
				return col, t, true
			}
		}
		s.mu.RUnlock()
		return col, nil, true
	}

	// Search all tables in scope
	var foundCol *ColumnInfo
	var foundTable *TableInfo
	ambiguous := false

	for _, table := range s.tables {
		if col, ok := table.GetColumn(columnName); ok {
			if foundCol != nil {
				ambiguous = true
			}
			foundCol = col
			foundTable = table
		}
	}
	s.mu.RUnlock()

	if ambiguous {
		// Return nil to indicate ambiguous reference
		return nil, nil, false
	}

	if foundCol != nil {
		return foundCol, foundTable, true
	}

	// Try parent scope
	if s.parent != nil {
		return s.parent.LookupColumn("", columnName)
	}

	return nil, nil, false
}

// GetAllColumns returns all columns available in this scope.
func (s *Scope) GetAllColumns() []*ColumnInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var cols []*ColumnInfo
	seen := make(map[string]bool)

	for _, table := range s.tables {
		for i := range table.Columns {
			col := &table.Columns[i]
			key := strings.ToUpper(table.Name + "." + col.Name)
			if !seen[key] {
				seen[key] = true
				cols = append(cols, col)
			}
		}
	}

	return cols
}

// GetTables returns all tables in this scope.
func (s *Scope) GetTables() []*TableInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var tables []*TableInfo
	seen := make(map[string]bool)

	for name, table := range s.tables {
		// Use actual table name to avoid duplicates from aliases
		key := strings.ToUpper(table.Name)
		if !seen[key] {
			seen[key] = true
			_ = name // Silence unused variable
			tables = append(tables, table)
		}
	}

	return tables
}

// Catalog represents the database schema catalog.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]*TableInfo
}

// NewCatalog creates a new empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		tables: make(map[string]*TableInfo),
	}
}

// CreateTable adds a table to the catalog.
func (c *Catalog) CreateTable(info *TableInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	name := strings.ToUpper(info.Name)
	if _, exists := c.tables[name]; exists {
		return &AnalysisError{
			Type:    ErrTableExists,
			Message: "table already exists: " + info.Name,
		}
	}
	c.tables[name] = info
	return nil
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	upper := strings.ToUpper(name)
	if _, exists := c.tables[upper]; !exists {
		return &AnalysisError{
			Type:    ErrTableNotFound,
			Message: "table not found: " + name,
		}
	}
	delete(c.tables, upper)
	return nil
}

// GetTable returns a table by name.
func (c *Catalog) GetTable(name string) (*TableInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, ok := c.tables[strings.ToUpper(name)]
	return t, ok
}

// GetTables returns all tables in the catalog.
func (c *Catalog) GetTables() []*TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var tables []*TableInfo
	for _, t := range c.tables {
		tables = append(tables, t)
	}
	return tables
}

// TableExists returns true if a table exists.
func (c *Catalog) TableExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.tables[strings.ToUpper(name)]
	return ok
}
