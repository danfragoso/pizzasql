package storage

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Row represents a database row.
type Row map[string]interface{}

// TableManager manages table data operations.
type TableManager struct {
	pool     *KVPool
	schema   *SchemaManager
	database string
}

// NewTableManager creates a new table manager.
func NewTableManager(pool *KVPool, schema *SchemaManager, database string) *TableManager {
	return &TableManager{
		pool:     pool,
		schema:   schema,
		database: database,
	}
}

// dataKey returns the key for a row.
func (m *TableManager) dataKey(table, pk string) string {
	return fmt.Sprintf("%s:_data:%s:%s", m.database, strings.ToLower(table), pk)
}

// dataPrefix returns the prefix for all rows in a table.
func (m *TableManager) dataPrefix(table string) string {
	return fmt.Sprintf("%s:_data:%s:", m.database, strings.ToLower(table))
}

// Insert inserts a new row.
func (m *TableManager) Insert(table string, row Row) error {
	schema, err := m.schema.GetSchema(table)
	if err != nil {
		return err
	}

	// Get primary key value
	pkValue, ok := row[schema.PrimaryKey]
	if !ok {
		// Try case-insensitive lookup
		for k, v := range row {
			if strings.EqualFold(k, schema.PrimaryKey) {
				pkValue = v
				ok = true
				break
			}
		}
	}

	// Check if PK is INTEGER PRIMARY KEY (implicit ROWID alias)
	pkCol, _ := schema.GetColumn(schema.PrimaryKey)
	isIntegerPK := pkCol != nil && isIntegerType(pkCol.Type)

	// Auto-generate ROWID if no primary key provided or if it's INTEGER PRIMARY KEY
	var rowid int64
	if !ok || pkValue == nil {
		if isIntegerPK || !ok {
			// Generate ROWID
			rowid, err = m.schema.GetNextRowID(table)
			if err != nil {
				return err
			}
			pkValue = rowid
			row[schema.PrimaryKey] = rowid
			ok = true
		} else {
			return fmt.Errorf("missing primary key: %s", schema.PrimaryKey)
		}
	} else if isIntegerPK {
		// User provided INTEGER PRIMARY KEY value - track it
		switch v := pkValue.(type) {
		case int64:
			rowid = v
		case float64:
			rowid = int64(v)
		case int:
			rowid = int64(v)
		default:
			rowid = 0
		}
		if rowid > 0 {
			m.schema.UpdateMaxRowID(table, rowid)
		}
	}

	pk := fmt.Sprintf("%v", pkValue)

	// Check for duplicate
	key := m.dataKey(table, pk)
	err = m.pool.WithClient(func(c *KVClient) error {
		_, err := c.Read(key)
		return err
	})
	if err == nil {
		return fmt.Errorf("duplicate primary key: %s", pk)
	}

	// Validate required columns
	for _, col := range schema.Columns {
		if !col.Nullable && col.Default == nil {
			val, hasVal := row[col.Name]
			if !hasVal {
				// Try case-insensitive lookup
				for k, v := range row {
					if strings.EqualFold(k, col.Name) {
						val = v
						hasVal = true
						break
					}
				}
			}
			if !hasVal || val == nil {
				return fmt.Errorf("missing required column: %s", col.Name)
			}
		}
	}

	// Normalize column names to match schema
	normalizedRow := make(Row)
	for _, col := range schema.Columns {
		for k, v := range row {
			if strings.EqualFold(k, col.Name) {
				normalizedRow[col.Name] = v
				break
			}
		}
	}

	// Apply defaults
	for _, col := range schema.Columns {
		if _, ok := normalizedRow[col.Name]; !ok && col.Default != nil {
			normalizedRow[col.Name] = col.Default
		}
	}

	// Store ROWID (use PK value for INTEGER PRIMARY KEY, otherwise generate)
	if rowid > 0 {
		normalizedRow["_rowid_"] = rowid
	} else {
		// Generate ROWID for non-integer primary keys
		newRowID, _ := m.schema.GetNextRowID(table)
		normalizedRow["_rowid_"] = newRowID
	}

	// Serialize row
	data, err := json.Marshal(normalizedRow)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	// Write row
	err = m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
	if err != nil {
		return err
	}

	// Update indexes
	m.updateIndexesForRow(table, normalizedRow, true)

	return nil
}

// updateIndexesForRow adds or removes index entries for a row.
func (m *TableManager) updateIndexesForRow(table string, row Row, add bool) {
	indexes, err := m.schema.ListTableIndexes(table)
	if err != nil || len(indexes) == 0 {
		return
	}

	rowid, ok := row["_rowid_"].(float64)
	if !ok {
		if rid, ok := row["_rowid_"].(int64); ok {
			rowid = float64(rid)
		} else {
			return
		}
	}

	for _, idx := range indexes {
		columns := make([]string, len(idx.Columns))
		for i, col := range idx.Columns {
			columns[i] = col.Name
		}
		colValue := m.buildIndexValue(row, columns)

		if add {
			m.AddIndexEntry(idx.Name, colValue, int64(rowid))
		} else {
			m.RemoveIndexEntry(idx.Name, colValue, int64(rowid))
		}
	}
}

// Select retrieves rows from a table.
func (m *TableManager) Select(table string, filter func(Row) bool) ([]Row, error) {
	if !m.schema.TableExists(table) {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	prefix := m.dataPrefix(table)
	var values []string

	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		values, err = c.Reads(prefix)
		fmt.Printf("[DEBUG] Select: table=%s, database=%s, prefix=%s, values_count=%d\n", table, m.database, prefix, len(values))
		return err
	})
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(values))
	for _, data := range values {
		var row Row
		if err := json.Unmarshal([]byte(data), &row); err != nil {
			fmt.Printf("[DEBUG] Select: failed to unmarshal row: %v\n", err)
			continue // Skip invalid rows
		}

		if filter == nil || filter(row) {
			rows = append(rows, row)
		}
	}

	fmt.Printf("[DEBUG] Select: returning %d rows\n", len(rows))
	return rows, nil
}

// SelectWithLimit retrieves rows with limit and offset.
func (m *TableManager) SelectWithLimit(table string, filter func(Row) bool, limit, offset int) ([]Row, error) {
	rows, err := m.Select(table, filter)
	if err != nil {
		return nil, err
	}

	// Apply offset
	if offset > 0 {
		if offset >= len(rows) {
			return nil, nil
		}
		rows = rows[offset:]
	}

	// Apply limit
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}

	return rows, nil
}

// Update updates rows matching the filter.
func (m *TableManager) Update(table string, updates Row, filter func(Row) bool) (int, error) {
	schema, err := m.schema.GetSchema(table)
	if err != nil {
		return 0, err
	}

	// Get all rows
	rows, err := m.Select(table, filter)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, row := range rows {
		// Remove old index entries before update
		m.updateIndexesForRow(table, row, false)

		// Apply updates
		for k, v := range updates {
			// Normalize column name
			for _, col := range schema.Columns {
				if strings.EqualFold(k, col.Name) {
					row[col.Name] = v
					break
				}
			}
		}

		// Get primary key
		pkValue := row[schema.PrimaryKey]
		pk := fmt.Sprintf("%v", pkValue)

		// Serialize row
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}

		// Write back
		key := m.dataKey(table, pk)
		err = m.pool.WithClient(func(c *KVClient) error {
			return c.Write(key, string(data))
		})
		if err == nil {
			// Add new index entries after update
			m.updateIndexesForRow(table, row, true)
			count++
		}
	}

	return count, nil
}

// UpdateFunc updates rows matching the filter using a function to compute new values.
// The updateFn receives the current row and returns the updates to apply.
func (m *TableManager) UpdateFunc(table string, updateFn func(Row) (Row, error), filter func(Row) bool) (int, error) {
	schema, err := m.schema.GetSchema(table)
	if err != nil {
		return 0, err
	}

	// Get all rows
	rows, err := m.Select(table, filter)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, row := range rows {
		// Remove old index entries before update
		m.updateIndexesForRow(table, row, false)

		// Compute updates using the provided function
		updates, err := updateFn(row)
		if err != nil {
			return count, err
		}

		// Apply updates
		for k, v := range updates {
			// Normalize column name
			for _, col := range schema.Columns {
				if strings.EqualFold(k, col.Name) {
					row[col.Name] = v
					break
				}
			}
		}

		// Get primary key
		pkValue := row[schema.PrimaryKey]
		pk := fmt.Sprintf("%v", pkValue)

		// Serialize row
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}

		// Write back
		key := m.dataKey(table, pk)
		err = m.pool.WithClient(func(c *KVClient) error {
			return c.Write(key, string(data))
		})
		if err == nil {
			// Add new index entries after update
			m.updateIndexesForRow(table, row, true)
			count++
		}
	}

	return count, nil
}

// Delete deletes rows matching the filter.
func (m *TableManager) Delete(table string, filter func(Row) bool) (int, error) {
	schema, err := m.schema.GetSchema(table)
	if err != nil {
		return 0, err
	}

	// Get all rows
	rows, err := m.Select(table, filter)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, row := range rows {
		// Remove index entries before deleting row
		m.updateIndexesForRow(table, row, false)

		pkValue := row[schema.PrimaryKey]
		pk := fmt.Sprintf("%v", pkValue)
		key := m.dataKey(table, pk)

		err = m.pool.WithClient(func(c *KVClient) error {
			return c.Delete(key)
		})
		if err == nil {
			count++
		}
	}

	return count, nil
}

// GetByPK retrieves a row by primary key.
func (m *TableManager) GetByPK(table string, pk string) (Row, error) {
	if !m.schema.TableExists(table) {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	key := m.dataKey(table, pk)
	var data string

	err := m.pool.WithClient(func(c *KVClient) error {
		var err error
		data, err = c.Read(key)
		return err
	})
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("row not found: %s", pk)
		}
		return nil, err
	}

	var row Row
	if err := json.Unmarshal([]byte(data), &row); err != nil {
		return nil, fmt.Errorf("failed to parse row: %w", err)
	}

	return row, nil
}

// Count returns the number of rows in a table.
func (m *TableManager) Count(table string, filter func(Row) bool) (int, error) {
	rows, err := m.Select(table, filter)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// Truncate removes all rows from a table.
func (m *TableManager) Truncate(table string) (int, error) {
	return m.Delete(table, nil)
}

// isIntegerType checks if a type name is an integer type.
func isIntegerType(typeName string) bool {
	t := strings.ToUpper(typeName)
	switch t {
	case "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT":
		return true
	}
	return false
}

// IsRowIDColumn checks if a column name is a ROWID alias.
func IsRowIDColumn(name string) bool {
	n := strings.ToLower(name)
	return n == "rowid" || n == "oid" || n == "_rowid_"
}

// Index entry methods - leveraging radix trie for prefix-based lookups
// Format: {database}:idx:{index_name}:{column_value} → JSON array of rowids

// indexEntryKey returns the key for an index entry.
func (m *TableManager) indexEntryKey(indexName string, colValue interface{}) string {
	// Format the value without scientific notation
	var valueStr string
	switch v := colValue.(type) {
	case float64:
		// Check if it's actually an integer value
		if v == float64(int64(v)) {
			valueStr = fmt.Sprintf("%d", int64(v))
		} else {
			valueStr = fmt.Sprintf("%f", v)
		}
	case int64:
		valueStr = fmt.Sprintf("%d", v)
	case int:
		valueStr = fmt.Sprintf("%d", v)
	default:
		valueStr = fmt.Sprintf("%v", v)
	}
	return fmt.Sprintf("%s:idx:%s:%s", m.database, strings.ToLower(indexName), valueStr)
}

// indexPrefix returns the prefix for all entries of an index.
func (m *TableManager) indexPrefix(indexName string) string {
	return fmt.Sprintf("%s:idx:%s:", m.database, strings.ToLower(indexName))
}

// AddIndexEntry adds a rowid to an index entry.
func (m *TableManager) AddIndexEntry(indexName string, colValue interface{}, rowid int64) error {
	key := m.indexEntryKey(indexName, colValue)

	// Read existing rowids
	var rowids []int64
	err := m.pool.WithClient(func(c *KVClient) error {
		data, err := c.Read(key)
		if err == nil && data != "" {
			json.Unmarshal([]byte(data), &rowids)
		}
		return nil // Ignore not found errors
	})
	if err != nil {
		return err
	}

	// Add new rowid if not already present
	for _, r := range rowids {
		if r == rowid {
			return nil // Already exists
		}
	}
	rowids = append(rowids, rowid)

	// Write back
	data, _ := json.Marshal(rowids)
	return m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
}

// RemoveIndexEntry removes a rowid from an index entry.
func (m *TableManager) RemoveIndexEntry(indexName string, colValue interface{}, rowid int64) error {
	key := m.indexEntryKey(indexName, colValue)

	// Read existing rowids
	var rowids []int64
	err := m.pool.WithClient(func(c *KVClient) error {
		data, err := c.Read(key)
		if err != nil {
			return err
		}
		json.Unmarshal([]byte(data), &rowids)
		return nil
	})
	if err != nil {
		return nil // Entry doesn't exist
	}

	// Remove rowid
	newRowids := make([]int64, 0, len(rowids))
	for _, r := range rowids {
		if r != rowid {
			newRowids = append(newRowids, r)
		}
	}

	if len(newRowids) == 0 {
		// Delete the entry entirely
		return m.pool.WithClient(func(c *KVClient) error {
			return c.Delete(key)
		})
	}

	// Write back
	data, _ := json.Marshal(newRowids)
	return m.pool.WithClient(func(c *KVClient) error {
		return c.Write(key, string(data))
	})
}

// LookupIndex returns rowids matching a column value using the index.
func (m *TableManager) LookupIndex(indexName string, colValue interface{}) ([]int64, error) {
	key := m.indexEntryKey(indexName, colValue)

	var rowids []int64
	err := m.pool.WithClient(func(c *KVClient) error {
		data, err := c.Read(key)
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(data), &rowids)
	})
	if err != nil {
		return nil, nil // Return empty if not found
	}

	return rowids, nil
}

// ClearIndex removes all entries for an index by scanning table and removing entries.
func (m *TableManager) ClearIndex(indexName, tableName string, columns []string) error {
	rows, err := m.Select(tableName, nil)
	if err != nil {
		return err
	}

	for _, row := range rows {
		colValue := m.buildIndexValue(row, columns)
		key := m.indexEntryKey(indexName, colValue)
		m.pool.WithClient(func(c *KVClient) error {
			return c.Delete(key)
		})
	}

	return nil
}

// BuildIndex builds index entries for all existing rows in a table.
func (m *TableManager) BuildIndex(indexName, tableName string, columns []string) error {
	rows, err := m.Select(tableName, nil)
	if err != nil {
		return err
	}

	for _, row := range rows {
		rowid, ok := row["_rowid_"].(float64)
		if !ok {
			continue
		}

		// Build composite key value for multi-column indexes
		colValue := m.buildIndexValue(row, columns)
		if err := m.AddIndexEntry(indexName, colValue, int64(rowid)); err != nil {
			return err
		}
	}

	return nil
}

// buildIndexValue creates the index key value from row columns.
func (m *TableManager) buildIndexValue(row Row, columns []string) string {
	formatValue := func(v interface{}) string {
		switch val := v.(type) {
		case float64:
			// Check if it's actually an integer value
			if val == float64(int64(val)) {
				return fmt.Sprintf("%d", int64(val))
			}
			return fmt.Sprintf("%f", val)
		case int64:
			return fmt.Sprintf("%d", val)
		case int:
			return fmt.Sprintf("%d", val)
		default:
			return fmt.Sprintf("%v", val)
		}
	}

	if len(columns) == 1 {
		return formatValue(row[columns[0]])
	}

	// Multi-column index: concatenate values with separator
	var parts []string
	for _, col := range columns {
		parts = append(parts, formatValue(row[col]))
	}
	return strings.Join(parts, "\x00")
}

// SelectByIndex retrieves rows using an index lookup.
func (m *TableManager) SelectByIndex(table, indexName string, colValue interface{}) ([]Row, error) {
	rowids, err := m.LookupIndex(indexName, colValue)
	if err != nil {
		return nil, err
	}

	// If no rowids found, return empty result
	if len(rowids) == 0 {
		return []Row{}, nil
	}

	schema, err := m.schema.GetSchema(table)
	if err != nil {
		return nil, err
	}

	// Check if primary key is INTEGER type (in which case rowid == pk)
	pkCol, _ := schema.GetColumn(schema.PrimaryKey)
	isPKInteger := pkCol != nil && isIntegerType(pkCol.Type)

	rows := make([]Row, 0, len(rowids))
	for _, rowid := range rowids {
		var row Row

		// For INTEGER PRIMARY KEY, the rowid IS the primary key
		if isPKInteger {
			row, err = m.GetByPK(table, fmt.Sprintf("%d", rowid))
			if err == nil {
				rows = append(rows, row)
				continue
			}
		}

		// For non-INTEGER primary keys or if PK lookup fails, look up by _rowid_
		allRows, _ := m.Select(table, func(r Row) bool {
			if rid, ok := r["_rowid_"].(float64); ok {
				return int64(rid) == rowid
			}
			if rid, ok := r["_rowid_"].(int64); ok {
				return rid == rowid
			}
			return false
		})
		if len(allRows) > 0 {
			rows = append(rows, allRows[0])
		}
	}

	return rows, nil
}
