package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/danfragoso/pizzasql-next/pkg/analyzer"
)

// Schema represents a table schema.
type Schema struct {
	Name          string    `json:"name"`
	Columns       []Column  `json:"columns"`
	PrimaryKey    string    `json:"primary_key"`
	CreatedAt     time.Time `json:"created_at"`
	NextRowID     int64     `json:"next_rowid"`
	AutoIncrement bool      `json:"autoincrement"`
}

// Column represents a column definition.
type Column struct {
	Name       string      `json:"name"`
	Type       string      `json:"type"`
	Nullable   bool        `json:"nullable"`
	Default    interface{} `json:"default,omitempty"`
	PrimaryKey bool        `json:"primary_key"`
}

// Index represents an index definition.
type Index struct {
	Name      string        `json:"name"`
	Table     string        `json:"table"`
	Columns   []IndexColumn `json:"columns"`
	Unique    bool          `json:"unique"`
	CreatedAt time.Time     `json:"created_at"`
}

// IndexColumn represents a column in an index.
type IndexColumn struct {
	Name string `json:"name"`
	Desc bool   `json:"desc"`
}

// SchemaManager manages table schemas.
type SchemaManager struct {
	pool     *KVPool
	database string
	cache    map[string]*Schema
	mu       sync.RWMutex
}

// NewSchemaManager creates a new schema manager.
func NewSchemaManager(pool *KVPool, database string) *SchemaManager {
	return &SchemaManager{
		pool:     pool,
		database: database,
		cache:    make(map[string]*Schema),
	}
}

// GetDatabaseName returns the database name.
func (m *SchemaManager) GetDatabaseName() string {
	return m.database
}

// GetPool returns the KV pool.
func (m *SchemaManager) GetPool() *KVPool {
	return m.pool
}

// schemaKey returns the key for a table schema.
func (m *SchemaManager) schemaKey(table string) string {
	return fmt.Sprintf("%s:_schema:%s", m.database, strings.ToLower(table))
}

// catalogKey returns the key for the table catalog.
func (m *SchemaManager) catalogKey() string {
	return fmt.Sprintf("%s:_sys:tables", m.database)
}

// CreateTable creates a new table.
func (m *SchemaManager) CreateTable(schema *Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if table already exists
	key := m.schemaKey(schema.Name)
	err := m.pool.WithClient(func(c *KVClient) error {
		_, err := c.Read(key)
		return err
	})
	if err == nil {
		return fmt.Errorf("table already exists: %s", schema.Name)
	}

	// Set creation time
	schema.CreatedAt = time.Now()

	// Determine primary key if not set
	if schema.PrimaryKey == "" {
		for _, col := range schema.Columns {
			if col.PrimaryKey {
				schema.PrimaryKey = col.Name
				break
			}
		}
		// Default to first column if no primary key specified
		if schema.PrimaryKey == "" && len(schema.Columns) > 0 {
			schema.PrimaryKey = schema.Columns[0].Name
			schema.Columns[0].PrimaryKey = true
		}
	}

	// Serialize schema
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}

	// Write schema
	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
	if err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	// Update catalog
	if err := m.addToCatalog(schema.Name); err != nil {
		// Rollback schema write
		m.pool.WithClient(func(c *KVClient) error {
			return c.Delete(key)
		})
		return err
	}

	// Update cache
	m.cache[strings.ToLower(schema.Name)] = schema

	return nil
}

// DropTable drops a table.
func (m *SchemaManager) DropTable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.schemaKey(name)

	// Check if table exists
	err := m.pool.WithClient(func(c *KVClient) error {
		_, err := c.Read(key)
		return err
	})
	if err != nil {
		return fmt.Errorf("table not found: %s", name)
	}

	// Delete all rows
	dataPrefix := fmt.Sprintf("%s:_data:%s:", m.database, strings.ToLower(name))
	err = m.pool.WithClient(func(c *KVClient) error {
		// Get all keys with this prefix and delete them
		// Note: This is a simplified version - in production you'd want batch delete
		values, err := c.Reads(dataPrefix)
		if err != nil {
			return err
		}
		// The Reads command returns values, not keys, so we can't delete them directly
		// In a real implementation, we'd need a keys scan command
		_ = values
		return nil
	})

	// Delete schema
	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	// Update catalog
	if err := m.removeFromCatalog(name); err != nil {
		return err
	}

	// Update cache
	delete(m.cache, strings.ToLower(name))

	return nil
}

// GetSchema retrieves a table schema.
func (m *SchemaManager) GetSchema(name string) (*Schema, error) {
	m.mu.RLock()
	if schema, ok := m.cache[strings.ToLower(name)]; ok {
		m.mu.RUnlock()
		return schema, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if schema, ok := m.cache[strings.ToLower(name)]; ok {
		return schema, nil
	}

	key := m.schemaKey(name)
	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("table not found: %s", name)
		}
		return nil, err
	}

	var schema Schema
	if err := json.Unmarshal([]byte(data), &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	m.cache[strings.ToLower(name)] = &schema
	return &schema, nil
}

// TableExists checks if a table exists.
func (m *SchemaManager) TableExists(name string) bool {
	_, err := m.GetSchema(name)
	return err == nil
}

// ListTables returns all table names.
func (m *SchemaManager) ListTables() ([]string, error) {
	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(m.catalogKey())
		return err
	})
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	var tables []string
	if err := json.Unmarshal([]byte(data), &tables); err != nil {
		return nil, fmt.Errorf("failed to parse catalog: %w", err)
	}

	return tables, nil
}

// addToCatalog adds a table to the catalog.
func (m *SchemaManager) addToCatalog(name string) error {
	tables, err := m.ListTables()
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	// Check if already exists
	lowerName := strings.ToLower(name)
	for _, t := range tables {
		if strings.ToLower(t) == lowerName {
			return nil
		}
	}

	tables = append(tables, name)
	data, err := json.Marshal(tables)
	if err != nil {
		return err
	}

	return m.pool.WithClient(func(c *KVClient) error {
		return c.Write(m.catalogKey(), string(data))
	})
}

// removeFromCatalog removes a table from the catalog.
func (m *SchemaManager) removeFromCatalog(name string) error {
	tables, err := m.ListTables()
	if err != nil {
		return err
	}

	lowerName := strings.ToLower(name)
	newTables := make([]string, 0, len(tables))
	for _, t := range tables {
		if strings.ToLower(t) != lowerName {
			newTables = append(newTables, t)
		}
	}

	data, err := json.Marshal(newTables)
	if err != nil {
		return err
	}

	return m.pool.WithClient(func(c *KVClient) error {
		return c.Write(m.catalogKey(), string(data))
	})
}

// InvalidateCache clears the cache for a table.
func (m *SchemaManager) InvalidateCache(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cache, strings.ToLower(name))
}

// ToAnalyzerTableInfo converts a Schema to analyzer.TableInfo.
func (s *Schema) ToAnalyzerTableInfo() *analyzer.TableInfo {
	info := &analyzer.TableInfo{
		Name: s.Name,
	}

	for _, col := range s.Columns {
		info.Columns = append(info.Columns, analyzer.ColumnInfo{
			Name:       col.Name,
			Type:       analyzer.TypeFromName(col.Type),
			Nullable:   col.Nullable,
			PrimaryKey: col.PrimaryKey,
			TableName:  s.Name,
		})
	}

	return info
}

// GetColumn returns a column by name.
func (s *Schema) GetColumn(name string) (*Column, bool) {
	lowerName := strings.ToLower(name)
	for i := range s.Columns {
		if strings.ToLower(s.Columns[i].Name) == lowerName {
			return &s.Columns[i], true
		}
	}
	return nil, false
}

// GetNextRowID gets and increments the next ROWID for a table.
func (m *SchemaManager) GetNextRowID(table string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, err := m.getSchemaLocked(table)
	if err != nil {
		return 0, err
	}

	// Get current and increment
	rowid := schema.NextRowID
	if rowid == 0 {
		rowid = 1
	}
	schema.NextRowID = rowid + 1

	// Save updated schema
	if err := m.saveSchemaLocked(schema); err != nil {
		return 0, err
	}

	return rowid, nil
}

// UpdateMaxRowID updates the next ROWID if the provided value is higher.
func (m *SchemaManager) UpdateMaxRowID(table string, rowid int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, err := m.getSchemaLocked(table)
	if err != nil {
		return err
	}

	if rowid >= schema.NextRowID {
		schema.NextRowID = rowid + 1
		return m.saveSchemaLocked(schema)
	}

	return nil
}

// getSchemaLocked retrieves schema (must hold lock).
func (m *SchemaManager) getSchemaLocked(name string) (*Schema, error) {
	if schema, ok := m.cache[strings.ToLower(name)]; ok {
		return schema, nil
	}

	key := m.schemaKey(name)
	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("table not found: %s", name)
		}
		return nil, err
	}

	var schema Schema
	if err := json.Unmarshal([]byte(data), &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	m.cache[strings.ToLower(name)] = &schema
	return &schema, nil
}

// saveSchemaLocked saves schema (must hold lock).
func (m *SchemaManager) saveSchemaLocked(schema *Schema) error {
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}

	key := m.schemaKey(schema.Name)
	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
	if err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	m.cache[strings.ToLower(schema.Name)] = schema
	return nil
}

// Index management methods

// indexKey returns the key for an index.
func (m *SchemaManager) indexKey(name string) string {
	return fmt.Sprintf("%s:index:%s", m.database, strings.ToLower(name))
}

// indexListKey returns the key for the index list.
func (m *SchemaManager) indexListKey() string {
	return fmt.Sprintf("%s:indexes", m.database)
}

// CreateIndex creates a new index.
func (m *SchemaManager) CreateIndex(index *Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if index already exists
	key := m.indexKey(index.Name)
	err := m.pool.WithClient(func(c *KVClient) error {
		_, err := c.Read(key)
		return err
	})
	if err == nil {
		return fmt.Errorf("index already exists: %s", index.Name)
	}

	// Verify table exists
	if _, err := m.getSchemaLocked(index.Table); err != nil {
		return fmt.Errorf("table not found: %s", index.Table)
	}

	// Save index
	index.CreatedAt = time.Now()
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("failed to serialize index: %w", err)
	}

	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
	if err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Add to index list
	return m.addToIndexList(index.Name)
}

// DropIndex drops an index.
func (m *SchemaManager) DropIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.indexKey(name)
	err := m.pool.WithClient(func(c *KVClient) error {
		return c.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}

	return m.removeFromIndexList(name)
}

// IndexExists checks if an index exists.
func (m *SchemaManager) IndexExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.indexKey(name)
	err := m.pool.WithClient(func(c *KVClient) error {
		_, err := c.Read(key)
		return err
	})
	return err == nil
}

// GetIndex retrieves an index by name.
func (m *SchemaManager) GetIndex(name string) (*Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.indexKey(name)
	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("index not found: %s", name)
	}

	var index Index
	if err := json.Unmarshal([]byte(data), &index); err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	return &index, nil
}

// ListIndexes returns all index names.
func (m *SchemaManager) ListIndexes() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.indexListKey()
	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		return []string{}, nil
	}

	var indexes []string
	if err := json.Unmarshal([]byte(data), &indexes); err != nil {
		return []string{}, nil
	}

	return indexes, nil
}

// ListTableIndexes returns all indexes for a table.
func (m *SchemaManager) ListTableIndexes(table string) ([]*Index, error) {
	indexes, err := m.ListIndexes()
	if err != nil {
		return nil, err
	}

	var result []*Index
	for _, name := range indexes {
		idx, err := m.GetIndex(name)
		if err != nil {
			continue
		}
		if strings.EqualFold(idx.Table, table) {
			result = append(result, idx)
		}
	}

	return result, nil
}

// addToIndexList adds an index name to the list.
func (m *SchemaManager) addToIndexList(name string) error {
	key := m.indexListKey()
	var indexes []string

	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err == nil {
		json.Unmarshal([]byte(data), &indexes)
	}

	indexes = append(indexes, name)
	newData, _ := json.Marshal(indexes)

	return m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(newData))
	})
}

// removeFromIndexList removes an index name from the list.
func (m *SchemaManager) removeFromIndexList(name string) error {
	key := m.indexListKey()
	var indexes []string

	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		return nil
	}
	json.Unmarshal([]byte(data), &indexes)

	var newIndexes []string
	for _, idx := range indexes {
		if !strings.EqualFold(idx, name) {
			newIndexes = append(newIndexes, idx)
		}
	}

	newData, _ := json.Marshal(newIndexes)
	return m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(newData))
	})
}

// AddColumn adds a new column to a table.
func (m *SchemaManager) AddColumn(table string, column Column) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, err := m.getSchemaUnsafe(table)
	if err != nil {
		return err
	}

	// Check if column already exists
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, column.Name) {
			return fmt.Errorf("column already exists: %s", column.Name)
		}
	}

	// Add column
	schema.Columns = append(schema.Columns, column)

	// Update schema
	return m.updateSchemaUnsafe(schema)
}

// DropColumn removes a column from a table.
func (m *SchemaManager) DropColumn(table, columnName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, err := m.getSchemaUnsafe(table)
	if err != nil {
		return err
	}

	// Cannot drop primary key column
	if strings.EqualFold(schema.PrimaryKey, columnName) {
		return fmt.Errorf("cannot drop primary key column: %s", columnName)
	}

	// Find and remove column
	newColumns := make([]Column, 0, len(schema.Columns)-1)
	found := false
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, columnName) {
			found = true
			continue
		}
		newColumns = append(newColumns, col)
	}

	if !found {
		return fmt.Errorf("column not found: %s", columnName)
	}

	schema.Columns = newColumns

	// Update schema
	return m.updateSchemaUnsafe(schema)
}

// RenameTable renames a table.
func (m *SchemaManager) RenameTable(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if old table exists
	schema, err := m.getSchemaUnsafe(oldName)
	if err != nil {
		return err
	}

	// Check if new table name already exists
	_, err = m.getSchemaUnsafe(newName)
	if err == nil {
		return fmt.Errorf("table already exists: %s", newName)
	}

	// Update schema name
	schema.Name = newName

	// Delete old schema
	oldKey := m.schemaKey(oldName)
	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Delete(oldKey)
	})
	if err != nil {
		return err
	}

	// Remove from catalog
	m.removeFromCatalog(oldName)

	// Update cache
	delete(m.cache, strings.ToLower(oldName))

	// Write new schema
	newKey := m.schemaKey(newName)
	data, _ := json.Marshal(schema)
	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Write(newKey, string(data))
	})
	if err != nil {
		return err
	}

	// Add to catalog
	m.addToCatalog(newName)

	// Update cache
	m.cache[strings.ToLower(newName)] = schema

	return nil
}

// RenameColumn renames a column in a table.
func (m *SchemaManager) RenameColumn(table, oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, err := m.getSchemaUnsafe(table)
	if err != nil {
		return err
	}

	// Check if new column name already exists
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, newName) {
			return fmt.Errorf("column already exists: %s", newName)
		}
	}

	// Find and rename column
	found := false
	for i, col := range schema.Columns {
		if strings.EqualFold(col.Name, oldName) {
			schema.Columns[i].Name = newName
			found = true

			// Update primary key reference if needed
			if strings.EqualFold(schema.PrimaryKey, oldName) {
				schema.PrimaryKey = newName
			}
			break
		}
	}

	if !found {
		return fmt.Errorf("column not found: %s", oldName)
	}

	// Update schema
	return m.updateSchemaUnsafe(schema)
}

// getSchemaUnsafe gets a schema without locking (internal use).
func (m *SchemaManager) getSchemaUnsafe(table string) (*Schema, error) {
	tableLower := strings.ToLower(table)

	// Check cache
	if schema, ok := m.cache[tableLower]; ok {
		return schema, nil
	}

	// Read from storage
	key := m.schemaKey(table)
	var data string
	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	var schema Schema
	if err := json.Unmarshal([]byte(data), &schema); err != nil {
		return nil, err
	}

	m.cache[tableLower] = &schema
	return &schema, nil
}

// updateSchemaUnsafe updates a schema without locking (internal use).
func (m *SchemaManager) updateSchemaUnsafe(schema *Schema) error {
	key := m.schemaKey(schema.Name)
	data, _ := json.Marshal(schema)

	err := m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
	if err != nil {
		return err
	}

	// Update cache
	m.cache[strings.ToLower(schema.Name)] = schema
	return nil
}
