package storage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/goccy/go-json"
)

// Row represents a database row.
type Row map[string]interface{}

// TableManager manages table data operations.
type TableManager struct {
	pool     *KVPool
	schema   *SchemaManager
	database string

	cacheMu  sync.RWMutex
	rowCache map[string][]Row // table name → all rows (nil means not loaded)
	rowIDMap map[string]map[int64]Row

	indexCache map[string]map[string][]int64 // index name → indexed value → rowids
	indexTable map[string]string             // index name → table name
}

// NewTableManager creates a new table manager.
func NewTableManager(pool *KVPool, schema *SchemaManager, database string) *TableManager {
	return &TableManager{
		pool:       pool,
		schema:     schema,
		database:   database,
		rowCache:   make(map[string][]Row),
		rowIDMap:   make(map[string]map[int64]Row),
		indexCache: make(map[string]map[string][]int64),
		indexTable: make(map[string]string),
	}
}

// invalidateCache removes a table's rows from the in-memory cache.
func (m *TableManager) invalidateCache(table string) {
	m.cacheMu.Lock()
	key := strings.ToLower(table)
	delete(m.rowCache, key)
	delete(m.rowIDMap, key)
	for indexName, tableName := range m.indexTable {
		if tableName == key {
			delete(m.indexCache, indexName)
			delete(m.indexTable, indexName)
		}
	}
	m.cacheMu.Unlock()
}

// InvalidateCache is the exported version for use by the executor.
func (m *TableManager) InvalidateCache(table string) {
	m.invalidateCache(table)
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

	// Update in-memory indexes only. Durable index entries are derived from rows.
	m.updateIndexesForRow(table, normalizedRow, true)

	m.invalidateCache(table)
	return nil
}

// InsertBulk inserts multiple rows efficiently, parallelizing KV writes across
// the connection pool. Skips per-row duplicate checks (caller must ensure
// uniqueness). Used by INSERT ... SELECT.
func (m *TableManager) InsertBulk(table string, rows []Row) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	schema, err := m.schema.GetSchema(table)
	if err != nil {
		return 0, err
	}

	pkCol, _ := schema.GetColumn(schema.PrimaryKey)
	isIntegerPK := pkCol != nil && isIntegerType(pkCol.Type)

	// Normalize rows and assign _rowid_.
	normalized := make([]Row, 0, len(rows))
	var maxRowID int64
	for _, row := range rows {
		nr := make(Row)
		for _, col := range schema.Columns {
			for k, v := range row {
				if strings.EqualFold(k, col.Name) {
					nr[col.Name] = v
					break
				}
			}
		}
		for _, col := range schema.Columns {
			if _, ok := nr[col.Name]; !ok && col.Default != nil {
				nr[col.Name] = col.Default
			}
		}
		var rowid int64
		var hasRowid bool
		if isIntegerPK {
			switch v := nr[schema.PrimaryKey].(type) {
			case float64:
				rowid = int64(v)
				hasRowid = true
			case int64:
				rowid = v
				hasRowid = true
			case int:
				rowid = int64(v)
				hasRowid = true
			}
		}
		if !hasRowid {
			// Fall back to sequential insert for non-integer-pk rows.
			if err := m.Insert(table, row); err != nil {
				return len(normalized), err
			}
			continue
		}
		nr["_rowid_"] = rowid
		if rowid > maxRowID {
			maxRowID = rowid
		}
		normalized = append(normalized, nr)
	}

	if maxRowID > 0 {
		m.schema.UpdateMaxRowID(table, maxRowID)
	}

	// Serialize all rows.
	type kv struct{ key, val string }
	rowKVs := make([]kv, 0, len(normalized))
	for _, nr := range normalized {
		pk := fmt.Sprintf("%v", nr[schema.PrimaryKey])
		data, err := json.Marshal(nr)
		if err != nil {
			return 0, err
		}
		rowKVs = append(rowKVs, kv{m.dataKey(table, pk), string(data)})
	}

	// Write rows concurrently.
	errs := make([]error, len(rowKVs))
	var wg sync.WaitGroup
	for i, w := range rowKVs {
		wg.Add(1)
		i, w := i, w
		go func() {
			defer wg.Done()
			errs[i] = m.pool.WithClient(func(c *KVClient) error {
				return c.Write(w.key, w.val)
			})
		}()
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return 0, e
		}
	}

	m.invalidateCache(table)
	return len(normalized), nil
}

// updateIndexesForRow adds or removes entries from already-built in-memory
// indexes. Index entries are rebuildable from durable row data, so this method
// intentionally does not write idx:* keys to KV.
func (m *TableManager) updateIndexesForRow(table string, row Row, add bool) {
	indexes, err := m.schema.ListTableIndexes(table)
	if err != nil || len(indexes) == 0 {
		return
	}

	rowid, ok := rowIDFromRow(row)
	if !ok {
		return
	}

	for _, idx := range indexes {
		indexName := strings.ToLower(idx.Name)
		m.cacheMu.RLock()
		_, initialized := m.indexCache[indexName]
		m.cacheMu.RUnlock()
		if !initialized {
			continue
		}

		columns := make([]string, len(idx.Columns))
		for i, col := range idx.Columns {
			columns[i] = col.Name
		}
		colValue := m.buildIndexValue(row, columns)

		if add {
			m.AddIndexEntry(idx.Name, colValue, rowid)
		} else {
			m.RemoveIndexEntry(idx.Name, colValue, rowid)
		}
	}
}

// Select retrieves rows from a table.
func (m *TableManager) Select(table string, filter func(Row) bool) ([]Row, error) {
	if !m.schema.TableExists(table) {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	key := strings.ToLower(table)

	m.cacheMu.RLock()
	cached, ok := m.rowCache[key]
	m.cacheMu.RUnlock()

	if !ok {
		prefix := m.dataPrefix(table)
		var values []string
		err := m.pool.WithClient(func(c *KVClient) error {
			var err error
			values, err = c.Reads(prefix)
			return err
		})
		if err != nil {
			return nil, err
		}

		loaded := make([]Row, 0, len(values))
		byRowID := make(map[int64]Row, len(values))
		for _, data := range values {
			var row Row
			if err := json.Unmarshal([]byte(data), &row); err != nil {
				continue
			}
			loaded = append(loaded, row)
			if rowid, ok := valueAsInt64(row["_rowid_"]); ok {
				byRowID[rowid] = row
			}
		}

		m.cacheMu.Lock()
		m.rowCache[key] = loaded
		m.rowIDMap[key] = byRowID
		m.cacheMu.Unlock()

		cached = loaded
	}

	if filter == nil {
		result := make([]Row, len(cached))
		copy(result, cached)
		return result, nil
	}

	rows := make([]Row, 0, len(cached))
	for _, row := range cached {
		if filter(row) {
			rows = append(rows, row)
		}
	}
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

	m.invalidateCache(table)
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

	m.invalidateCache(table)
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

	m.invalidateCache(table)
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
	return fmt.Sprintf("%s:idx:%s:%s", m.database, strings.ToLower(indexName), formatIndexValue(colValue))
}

// indexPrefix returns the prefix for all entries of an index.
func (m *TableManager) indexPrefix(indexName string) string {
	return fmt.Sprintf("%s:idx:%s:", m.database, strings.ToLower(indexName))
}

func formatIndexValue(value interface{}) string {
	switch v := value.(type) {
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%f", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func rowIDFromRow(row Row) (int64, bool) {
	switch v := row["_rowid_"].(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func (m *TableManager) ensureIndex(index *Index) error {
	indexKey := strings.ToLower(index.Name)
	m.cacheMu.RLock()
	_, initialized := m.indexCache[indexKey]
	m.cacheMu.RUnlock()
	if initialized {
		return nil
	}

	columns := make([]string, len(index.Columns))
	for i, col := range index.Columns {
		columns[i] = col.Name
	}

	rows, err := m.Select(index.Table, nil)
	if err != nil {
		return err
	}

	values := make(map[string][]int64)
	for _, row := range rows {
		rowid, ok := rowIDFromRow(row)
		if !ok {
			continue
		}
		colValue := m.buildIndexValue(row, columns)
		valueKey := formatIndexValue(colValue)
		values[valueKey] = append(values[valueKey], rowid)
	}

	m.cacheMu.Lock()
	if _, initialized := m.indexCache[indexKey]; !initialized {
		m.indexCache[indexKey] = values
		m.indexTable[indexKey] = strings.ToLower(index.Table)
	}
	m.cacheMu.Unlock()

	return nil
}

// AddIndexEntry adds a rowid to an in-memory index entry.
func (m *TableManager) AddIndexEntry(indexName string, colValue interface{}, rowid int64) error {
	indexKey := strings.ToLower(indexName)
	valueKey := formatIndexValue(colValue)

	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	values, ok := m.indexCache[indexKey]
	if !ok {
		return nil
	}
	rowids := values[valueKey]
	for _, r := range rowids {
		if r == rowid {
			return nil
		}
	}
	values[valueKey] = append(rowids, rowid)
	return nil
}

// RemoveIndexEntry removes a rowid from an in-memory index entry.
func (m *TableManager) RemoveIndexEntry(indexName string, colValue interface{}, rowid int64) error {
	indexKey := strings.ToLower(indexName)
	valueKey := formatIndexValue(colValue)

	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	values, ok := m.indexCache[indexKey]
	if !ok {
		return nil
	}
	rowids := values[valueKey]
	newRowids := make([]int64, 0, len(rowids))
	for _, r := range rowids {
		if r != rowid {
			newRowids = append(newRowids, r)
		}
	}

	if len(newRowids) == 0 {
		delete(values, valueKey)
		return nil
	}

	values[valueKey] = newRowids
	return nil
}

// LookupIndex returns rowids matching a column value using the index.
func (m *TableManager) LookupIndex(indexName string, colValue interface{}) ([]int64, error) {
	index, err := m.schema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}
	if err := m.ensureIndex(index); err != nil {
		return nil, err
	}

	indexKey := strings.ToLower(indexName)
	valueKey := formatIndexValue(colValue)

	m.cacheMu.RLock()
	rowids := append([]int64(nil), m.indexCache[indexKey][valueKey]...)
	m.cacheMu.RUnlock()

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
	index, err := m.schema.GetIndex(indexName)
	if err == nil {
		return m.ensureIndex(index)
	}

	rows, err := m.Select(tableName, nil)
	if err != nil {
		return err
	}

	values := make(map[string][]int64)
	for _, row := range rows {
		rowid, ok := rowIDFromRow(row)
		if !ok {
			continue
		}

		colValue := m.buildIndexValue(row, columns)
		values[formatIndexValue(colValue)] = append(values[formatIndexValue(colValue)], rowid)
	}

	indexKey := strings.ToLower(indexName)
	m.cacheMu.Lock()
	m.indexCache[indexKey] = values
	m.indexTable[indexKey] = strings.ToLower(tableName)
	m.cacheMu.Unlock()

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

	// Build a set of target rowids for O(1) lookup.
	key := strings.ToLower(table)
	m.cacheMu.RLock()
	byRowID, ok := m.rowIDMap[key]
	m.cacheMu.RUnlock()
	if !ok {
		if _, err := m.Select(table, nil); err != nil {
			return nil, err
		}
		m.cacheMu.RLock()
		byRowID = m.rowIDMap[key]
		m.cacheMu.RUnlock()
	}

	rows := make([]Row, 0, len(rowids))
	seen := make(map[int64]struct{}, len(rowids))
	for _, rid := range rowids {
		if _, duplicate := seen[rid]; duplicate {
			continue
		}
		seen[rid] = struct{}{}
		if row, ok := byRowID[rid]; ok {
			rows = append(rows, row)
		}
	}

	return rows, nil
}
