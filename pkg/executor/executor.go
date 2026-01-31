package executor

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/danfragoso/pizzasql-next/pkg/analyzer"
	"github.com/danfragoso/pizzasql-next/pkg/lexer"
	"github.com/danfragoso/pizzasql-next/pkg/parser"
	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// Executor executes SQL statements.
type Executor struct {
	schema   *storage.SchemaManager
	table    *storage.TableManager
	analyzer *analyzer.Analyzer
	catalog  *analyzer.Catalog

	// Multi-database support
	attachedDatabases map[string]*DatabaseConnection // alias -> connection
	currentDatabase   string                         // current database alias (default is "main")

	// Transaction state
	inTransaction bool
	savepoints    []string     // stack of savepoint names
	txLog         []txLogEntry // transaction log for rollback

	// Subquery context for correlated subqueries
	outerRow storage.Row
}

// DatabaseConnection represents an attached database.
type DatabaseConnection struct {
	Alias  string
	Path   string // Database path or identifier
	Schema *storage.SchemaManager
	Table  *storage.TableManager
}

// txLogEntry represents a transaction log entry for rollback support.
type txLogEntry struct {
	operation string // "INSERT", "UPDATE", "DELETE"
	table     string
	key       string
	oldData   storage.Row // for UPDATE/DELETE, the original row data
}

// New creates a new executor.
func New(schema *storage.SchemaManager, table *storage.TableManager) *Executor {
	catalog := analyzer.NewCatalog()
	executor := &Executor{
		schema:            schema,
		table:             table,
		analyzer:          analyzer.New(catalog),
		catalog:           catalog,
		attachedDatabases: make(map[string]*DatabaseConnection),
		currentDatabase:   "main",
	}

	// Register the main database
	executor.attachedDatabases["main"] = &DatabaseConnection{
		Alias:  "main",
		Path:   schema.GetDatabaseName(),
		Schema: schema,
		Table:  table,
	}

	return executor
}

// SyncCatalog synchronizes the analyzer catalog with the storage schema.
func (e *Executor) SyncCatalog() error {
	tables, err := e.schema.ListTables()
	if err != nil {
		return err
	}

	for _, tableName := range tables {
		schema, err := e.schema.GetSchema(tableName)
		if err != nil {
			continue
		}
		// Drop table from catalog if it exists, then recreate with updated schema
		e.catalog.DropTable(tableName)
		e.catalog.CreateTable(schema.ToAnalyzerTableInfo())
	}

	return nil
}

// Execute executes a SQL statement.
func (e *Executor) Execute(stmt parser.Statement) (*Result, error) {
	// PRAGMA doesn't need analysis
	if pragma, ok := stmt.(*parser.PragmaStmt); ok {
		return e.executePragma(pragma)
	}

	// EXPLAIN doesn't need analysis
	if explain, ok := stmt.(*parser.ExplainStmt); ok {
		return e.executeExplain(explain)
	}

	// Transaction statements don't need analysis
	switch s := stmt.(type) {
	case *parser.BeginStmt:
		return e.executeBegin(s)
	case *parser.CommitStmt:
		return e.executeCommit(s)
	case *parser.RollbackStmt:
		return e.executeRollback(s)
	case *parser.SavepointStmt:
		return e.executeSavepoint(s)
	case *parser.ReleaseStmt:
		return e.executeRelease(s)
	case *parser.CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *parser.DropIndexStmt:
		return e.executeDropIndex(s)
	case *parser.AttachStmt:
		return e.executeAttach(s)
	case *parser.DetachStmt:
		return e.executeDetach(s)
	}

	// Analyze first
	if err := e.analyzer.Analyze(stmt); err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return e.executeSelect(s)
	case *parser.InsertStmt:
		return e.executeInsert(s)
	case *parser.UpdateStmt:
		return e.executeUpdate(s)
	case *parser.DeleteStmt:
		return e.executeDelete(s)
	case *parser.CreateTableStmt:
		return e.executeCreateTable(s)
	case *parser.DropTableStmt:
		return e.executeDropTable(s)
	case *parser.CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *parser.DropIndexStmt:
		return e.executeDropIndex(s)
	case *parser.AlterTableStmt:
		return e.executeAlterTable(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeSelect executes a SELECT statement.
func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	if len(stmt.From) == 0 {
		// SELECT without FROM (e.g., SELECT 1+1)
		return e.executeSelectExpr(stmt)
	}

	// Check if FROM clause is a subquery (derived table)
	if stmt.From[0].Subquery != nil {
		return e.executeSelectFromSubquery(stmt)
	}

	tableName := stmt.From[0].Name
	schema, err := e.schema.GetSchema(tableName)
	if err != nil {
		return nil, err
	}

	// Try to use index for WHERE clause
	var rows []storage.Row
	usedIndex := false

	if stmt.Where != nil {
		// Check if we can use an index
		colName, colValue, isEquality := e.extractIndexableCondition(stmt.Where)
		if isEquality {
			// Look for an index on this column
			indexes, _ := e.schema.ListTableIndexes(tableName)
			for _, idx := range indexes {
				if len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0].Name, colName) {
					// Use this index
					rows, err = e.table.SelectByIndex(tableName, idx.Name, colValue)
					if err == nil {
						usedIndex = true
					}
					break
				}
			}
		}
	}

	// Fall back to full table scan if no index used
	if !usedIndex {
		// Build filter function from WHERE clause
		// Only use filter during scan if there's NO alias (otherwise the filter won't have the right column names)
		var filter func(storage.Row) bool
		if stmt.Where != nil && stmt.From[0].Alias == "" {
			filter = func(row storage.Row) bool {
				val, err := e.evalExpr(stmt.Where, row)
				if err != nil {
					return false
				}
				return toBool(val)
			}
		}

		rows, err = e.table.Select(tableName, filter)
	}
	if err != nil {
		return nil, err
	}

	// Add table alias to rows if there's an explicit alias
	// This needs to happen BEFORE filtering so that the WHERE clause can reference the alias
	if stmt.From[0].Alias != "" {
		for i := range rows {
			rows[i] = e.addTableAlias(rows[i], stmt.From[0].Alias)
		}
	}

	// Apply WHERE clause filter if we have an alias (we couldn't filter during scan)
	if stmt.Where != nil && stmt.From[0].Alias != "" {
		var filtered []storage.Row
		for _, row := range rows {
			val, err := e.evalExpr(stmt.Where, row)
			if err != nil {
				continue // Skip rows that error
			}
			if toBool(val) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	// Handle JOINs
	if len(stmt.From) > 0 && stmt.From[0].Join != nil {
		rows, err = e.executeJoins(stmt.From[0], rows)
		if err != nil {
			return nil, err
		}
	}

	// Handle GROUP BY
	if len(stmt.GroupBy) > 0 {
		return e.executeGroupBy(stmt, rows, schema)
	}

	// Check for aggregate functions without GROUP BY
	hasAggregate := e.hasAggregates(stmt.Columns)
	if hasAggregate {
		return e.executeAggregateSelect(stmt, rows, schema)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.sortRows(rows, stmt.OrderBy)
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil {
		offset := e.evalIntExpr(stmt.Offset)
		if offset < len(rows) {
			rows = rows[offset:]
		} else {
			rows = nil
		}
	}
	if stmt.Limit != nil {
		limit := e.evalIntExpr(stmt.Limit)
		if limit < len(rows) {
			rows = rows[:limit]
		}
	}

	// Build result
	result := NewResult("SELECT")

	// Determine columns
	for i, col := range stmt.Columns {
		if col.Alias != "" {
			result.AddColumn(col.Alias)
		} else if ref, ok := col.Expr.(*parser.ColumnRef); ok {
			result.AddColumn(ref.Column)
		} else if col.Star {
			// Handle SELECT * - add all columns from schema
			for _, c := range schema.Columns {
				result.AddColumn(c.Name)
			}
		} else {
			result.AddColumn(fmt.Sprintf("column%d", i+1))
		}
	}

	// Add rows - evaluate each select expression
	for _, row := range rows {
		values := make([]interface{}, 0)
		for _, col := range stmt.Columns {
			if col.Star {
				// For SELECT *, add all columns in order
				for _, c := range schema.Columns {
					if storage.IsRowIDColumn(c.Name) {
						values = append(values, row["_rowid_"])
					} else {
						values = append(values, row[c.Name])
					}
				}
			} else {
				// Evaluate the expression
				val, err := e.evalExpr(col.Expr, row)
				if err != nil {
					return nil, err
				}
				values = append(values, val)
			}
		}
		result.AddRow(values...)
	}

	// Apply DISTINCT if specified
	if stmt.Distinct {
		result.Rows = e.applyDistinct(result.Rows)
	}

	return result, nil
}

// executeSelectExpr executes a SELECT without FROM.
func (e *Executor) executeSelectExpr(stmt *parser.SelectStmt) (*Result, error) {
	result := NewResult("SELECT")

	// Determine columns
	for i, col := range stmt.Columns {
		if col.Alias != "" {
			result.AddColumn(col.Alias)
		} else {
			result.AddColumn(fmt.Sprintf("column%d", i+1))
		}
	}

	// Evaluate expressions
	values := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		val, err := e.evalExpr(col.Expr, nil)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	result.AddRow(values...)

	return result, nil
}

// executeSelectFromSubquery executes a SELECT with a subquery in FROM clause.
func (e *Executor) executeSelectFromSubquery(stmt *parser.SelectStmt) (*Result, error) {
	// Execute the subquery to get the derived table
	subqueryResult, err := e.executeSelect(stmt.From[0].Subquery)
	if err != nil {
		return nil, fmt.Errorf("subquery error: %w", err)
	}

	// Convert subquery result to rows for further processing
	derivedRows := make([]storage.Row, 0, subqueryResult.RowCount)
	for _, rowValues := range subqueryResult.Rows {
		row := make(storage.Row)
		for i, col := range subqueryResult.Columns {
			row[col] = rowValues[i]
		}
		derivedRows = append(derivedRows, row)
	}

	// Handle JOINs if present
	if stmt.From[0].Join != nil {
		derivedRows, err = e.executeJoin(stmt.From[0], derivedRows)
		if err != nil {
			return nil, err
		}
	}

	// Apply WHERE clause on derived table
	if stmt.Where != nil {
		filteredRows := make([]storage.Row, 0)
		for _, row := range derivedRows {
			val, err := e.evalExpr(stmt.Where, row)
			if err != nil {
				continue
			}
			if toBool(val) {
				filteredRows = append(filteredRows, row)
			}
		}
		derivedRows = filteredRows
	}

	// Handle GROUP BY
	if len(stmt.GroupBy) > 0 {
		// Create a temporary schema from subquery columns
		tempSchema := &storage.Schema{
			Name:    "derived",
			Columns: make([]storage.Column, len(subqueryResult.Columns)),
		}
		for i, col := range subqueryResult.Columns {
			tempSchema.Columns[i] = storage.Column{
				Name: col,
				Type: "ANY",
			}
		}
		return e.executeGroupBy(stmt, derivedRows, tempSchema)
	}

	// Check for aggregate functions without GROUP BY
	hasAggregate := e.hasAggregates(stmt.Columns)
	if hasAggregate {
		tempSchema := &storage.Schema{
			Name:    "derived",
			Columns: make([]storage.Column, len(subqueryResult.Columns)),
		}
		for i, col := range subqueryResult.Columns {
			tempSchema.Columns[i] = storage.Column{
				Name: col,
				Type: "ANY",
			}
		}
		return e.executeAggregateSelect(stmt, derivedRows, tempSchema)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.sortRows(derivedRows, stmt.OrderBy)
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil {
		offset := e.evalIntExpr(stmt.Offset)
		if offset < len(derivedRows) {
			derivedRows = derivedRows[offset:]
		} else {
			derivedRows = nil
		}
	}
	if stmt.Limit != nil {
		limit := e.evalIntExpr(stmt.Limit)
		if limit < len(derivedRows) {
			derivedRows = derivedRows[:limit]
		}
	}

	// Build result
	result := NewResult("SELECT")

	// Determine output columns
	if stmt.Columns[0].Star {
		// SELECT * from derived table
		for _, col := range subqueryResult.Columns {
			result.AddColumn(col)
		}
	} else {
		// Specific columns
		for _, col := range stmt.Columns {
			if col.Alias != "" {
				result.AddColumn(col.Alias)
			} else if colRef, ok := col.Expr.(*parser.ColumnRef); ok {
				result.AddColumn(colRef.Column)
			} else {
				result.AddColumn("column")
			}
		}
	}

	// Add rows
	for _, row := range derivedRows {
		if stmt.Columns[0].Star {
			// SELECT * - use all columns
			values := make([]interface{}, len(subqueryResult.Columns))
			for i, col := range subqueryResult.Columns {
				values[i] = row[col]
			}
			result.AddRow(values...)
		} else {
			// Specific columns - evaluate expressions
			values := make([]interface{}, len(stmt.Columns))
			for i, col := range stmt.Columns {
				val, err := e.evalExpr(col.Expr, row)
				if err != nil {
					return nil, err
				}
				values[i] = val
			}
			result.AddRow(values...)
		}
	}

	return result, nil
}

// executeAggregateSelect executes a SELECT with aggregate functions.
func (e *Executor) executeAggregateSelect(stmt *parser.SelectStmt, rows []storage.Row, schema *storage.Schema) (*Result, error) {
	result := NewResult("SELECT")

	// Determine columns and evaluate aggregates
	for i, col := range stmt.Columns {
		if col.Alias != "" {
			result.AddColumn(col.Alias)
		} else if col.Star {
			result.AddColumn("*")
		} else {
			result.AddColumn(fmt.Sprintf("column%d", i+1))
		}
	}

	// Calculate values
	values := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		val, err := e.evalAggregateExpr(col.Expr, rows)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	result.AddRow(values...)

	return result, nil
}

// executeGroupBy executes a GROUP BY query.
func (e *Executor) executeGroupBy(stmt *parser.SelectStmt, rows []storage.Row, schema *storage.Schema) (*Result, error) {
	// Group rows
	groups := make(map[string][]storage.Row)
	for _, row := range rows {
		key := e.buildGroupKey(stmt.GroupBy, row)
		groups[key] = append(groups[key], row)
	}

	result := NewResult("SELECT")

	// Determine columns
	columnNames := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		if col.Alias != "" {
			columnNames[i] = col.Alias
			result.AddColumn(col.Alias)
		} else if ref, ok := col.Expr.(*parser.ColumnRef); ok {
			columnNames[i] = ref.Column
			result.AddColumn(ref.Column)
		} else {
			columnNames[i] = fmt.Sprintf("column%d", i+1)
			result.AddColumn(columnNames[i])
		}
	}

	// Process each group
	for _, groupRows := range groups {
		// Apply HAVING
		if stmt.Having != nil {
			val, err := e.evalAggregateExpr(stmt.Having, groupRows)
			if err != nil {
				continue
			}
			if !toBool(val) {
				continue
			}
		}

		values := make([]interface{}, len(stmt.Columns))
		for i, col := range stmt.Columns {
			if e.isAggregate(col.Expr) {
				val, err := e.evalAggregateExpr(col.Expr, groupRows)
				if err != nil {
					return nil, err
				}
				values[i] = val
			} else {
				// Use first row's value for non-aggregate columns
				val, err := e.evalExpr(col.Expr, groupRows[0])
				if err != nil {
					return nil, err
				}
				values[i] = val
			}
		}
		result.AddRow(values...)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.sortResultRows(result, stmt.OrderBy, stmt.Columns, columnNames)
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil {
		offset := e.evalIntExpr(stmt.Offset)
		if offset < len(result.Rows) {
			result.Rows = result.Rows[offset:]
		} else {
			result.Rows = nil
		}
		result.RowCount = len(result.Rows)
	}
	if stmt.Limit != nil {
		limit := e.evalIntExpr(stmt.Limit)
		if limit < len(result.Rows) {
			result.Rows = result.Rows[:limit]
		}
		result.RowCount = len(result.Rows)
	}

	return result, nil
}

// executeJoins recursively processes all JOIN clauses in a table reference.
func (e *Executor) executeJoins(tableRef parser.TableRef, leftRows []storage.Row) ([]storage.Row, error) {
	if tableRef.Join == nil || tableRef.Join.Table == nil {
		return leftRows, nil
	}

	// Get the right table name and its data
	rightTableRef := tableRef.Join.Table
	rightTable := rightTableRef.Name
	rightRows, err := e.table.Select(rightTable, nil)
	if err != nil {
		return nil, err
	}

	// Perform the join between left and right
	var result []storage.Row
	leftTableName := tableRef.Name
	leftAlias := tableRef.Alias
	rightAlias := rightTableRef.Alias

	// If leftAlias is empty, use the table name
	if leftAlias == "" {
		leftAlias = leftTableName
	}
	if rightAlias == "" {
		rightAlias = rightTable
	}

	switch tableRef.Join.Type {
	case parser.JoinInner:
		for _, left := range leftRows {
			for _, right := range rightRows {
				merged := e.mergeRows(left, right, leftAlias, rightAlias)
				if tableRef.Join.Condition != nil {
					match, _ := e.evalExpr(tableRef.Join.Condition, merged)
					if toBool(match) {
						result = append(result, merged)
					}
				} else {
					result = append(result, merged)
				}
			}
		}

	case parser.JoinLeft:
		for _, left := range leftRows {
			matched := false
			for _, right := range rightRows {
				merged := e.mergeRows(left, right, leftAlias, rightAlias)
				if tableRef.Join.Condition != nil {
					match, _ := e.evalExpr(tableRef.Join.Condition, merged)
					if toBool(match) {
						result = append(result, merged)
						matched = true
					}
				}
			}
			if !matched {
				// Add left row with nulls for right
				result = append(result, left)
			}
		}

	case parser.JoinCross:
		for _, left := range leftRows {
			for _, right := range rightRows {
				result = append(result, e.mergeRows(left, right, leftAlias, rightAlias))
			}
		}
	}

	// Recursively process any additional joins
	if rightTableRef.Join != nil {
		return e.executeJoins(*rightTableRef, result)
	}

	return result, nil
}

// executeJoin executes a JOIN operation.
func (e *Executor) executeJoin(tableRef parser.TableRef, leftRows []storage.Row) ([]storage.Row, error) {
	join := tableRef.Join
	if join == nil || join.Table == nil {
		return leftRows, nil
	}

	rightTable := join.Table.Name
	rightRows, err := e.table.Select(rightTable, nil)
	if err != nil {
		return nil, err
	}

	var result []storage.Row

	switch join.Type {
	case parser.JoinInner:
		for _, left := range leftRows {
			for _, right := range rightRows {
				merged := e.mergeRows(left, right, tableRef.Alias, join.Table.Alias)
				if join.Condition != nil {
					match, _ := e.evalExpr(join.Condition, merged)
					if toBool(match) {
						result = append(result, merged)
					}
				} else {
					result = append(result, merged)
				}
			}
		}

	case parser.JoinLeft:
		for _, left := range leftRows {
			matched := false
			for _, right := range rightRows {
				merged := e.mergeRows(left, right, tableRef.Alias, join.Table.Alias)
				if join.Condition != nil {
					match, _ := e.evalExpr(join.Condition, merged)
					if toBool(match) {
						result = append(result, merged)
						matched = true
					}
				}
			}
			if !matched {
				// Add left row with nulls for right
				result = append(result, left)
			}
		}

	case parser.JoinCross:
		for _, left := range leftRows {
			for _, right := range rightRows {
				result = append(result, e.mergeRows(left, right, tableRef.Alias, join.Table.Alias))
			}
		}
	}

	return result, nil
}

// mergeRows merges two rows with optional table aliases.
func (e *Executor) mergeRows(left, right storage.Row, leftAlias, rightAlias string) storage.Row {
	result := make(storage.Row)
	for k, v := range left {
		// Copy the key as-is (it might already be qualified)
		result[k] = v
		// Only add qualified name if the key is NOT already qualified and we have an alias
		if leftAlias != "" && !strings.Contains(k, ".") {
			result[leftAlias+"."+k] = v
		}
	}
	for k, v := range right {
		// For unqualified names, only add if they don't already exist
		// This prevents right table columns from overwriting left table columns
		if !strings.Contains(k, ".") {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
			// Add qualified name for right table
			if rightAlias != "" {
				result[rightAlias+"."+k] = v
			}
		} else {
			// Already qualified, just copy it
			result[k] = v
		}
	}
	return result
}

// addTableAlias adds table-qualified names to a row.
func (e *Executor) addTableAlias(row storage.Row, alias string) storage.Row {
	result := make(storage.Row)
	for k, v := range row {
		result[k] = v
		// Don't add alias to already-qualified names
		if !strings.Contains(k, ".") {
			result[alias+"."+k] = v
		}
	}
	return result
}

// executeInsert executes an INSERT statement.
func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	tableName := stmt.Table.Name
	schema, err := e.schema.GetSchema(tableName)
	if err != nil {
		return nil, err
	}

	count := 0
	for _, values := range stmt.Values {
		row := make(storage.Row)

		if len(stmt.Columns) > 0 {
			// Named columns
			for i, col := range stmt.Columns {
				if i < len(values) {
					val, err := e.evalExpr(values[i], nil)
					if err != nil {
						return nil, err
					}
					row[col] = val
				}
			}
		} else {
			// All columns in order
			for i, col := range schema.Columns {
				if i < len(values) {
					val, err := e.evalExpr(values[i], nil)
					if err != nil {
						return nil, err
					}
					row[col.Name] = val
				}
			}
		}

		err := e.table.Insert(tableName, row)
		if err != nil {
			// Handle conflict based on OnConflict action
			if strings.Contains(err.Error(), "duplicate") {
				switch stmt.OnConflict {
				case parser.ConflictIgnore:
					// Silently ignore the duplicate
					continue
				case parser.ConflictReplace:
					// Delete existing row and insert new one
					pkValue := row[schema.PrimaryKey]
					if pkValue != nil {
						e.table.Delete(tableName, func(r storage.Row) bool {
							return fmt.Sprintf("%v", r[schema.PrimaryKey]) == fmt.Sprintf("%v", pkValue)
						})
						// Try insert again
						if err := e.table.Insert(tableName, row); err != nil {
							return nil, err
						}
					}
				case parser.ConflictAbort, parser.ConflictFail:
					return nil, err
				case parser.ConflictRollback:
					// In a real implementation, this would rollback the transaction
					return nil, err
				default:
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		count++
	}

	result := NewResult("INSERT")
	result.SetRowCount(count)
	return result, nil
}

// executeUpdate executes an UPDATE statement.
func (e *Executor) executeUpdate(stmt *parser.UpdateStmt) (*Result, error) {
	tableName := stmt.Table.Name

	// Build filter
	var filter func(storage.Row) bool
	if stmt.Where != nil {
		filter = func(row storage.Row) bool {
			val, err := e.evalExpr(stmt.Where, row)
			if err != nil {
				return false
			}
			return toBool(val)
		}
	}

	// Use UpdateFunc to evaluate expressions per-row (supports self-referencing like balance = balance + 100)
	updateFn := func(row storage.Row) (storage.Row, error) {
		updates := make(storage.Row)
		for _, assign := range stmt.Set {
			val, err := e.evalExpr(assign.Value, row)
			if err != nil {
				return nil, err
			}
			updates[assign.Column] = val
		}
		return updates, nil
	}

	count, err := e.table.UpdateFunc(tableName, updateFn, filter)
	if err != nil {
		return nil, err
	}

	result := NewResult("UPDATE")
	result.SetRowCount(count)
	return result, nil
}

// executeDelete executes a DELETE statement.
func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*Result, error) {
	tableName := stmt.Table.Name

	// Build filter
	var filter func(storage.Row) bool
	if stmt.Where != nil {
		filter = func(row storage.Row) bool {
			val, err := e.evalExpr(stmt.Where, row)
			if err != nil {
				return false
			}
			return toBool(val)
		}
	}

	count, err := e.table.Delete(tableName, filter)
	if err != nil {
		return nil, err
	}

	result := NewResult("DELETE")
	result.SetRowCount(count)
	return result, nil
}

// executeCreateTable executes a CREATE TABLE statement.
func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
	// Check if exists
	if e.schema.TableExists(stmt.Table.Name) {
		if stmt.IfNotExists {
			result := NewResult("CREATE TABLE")
			return result, nil
		}
		return nil, fmt.Errorf("table already exists: %s", stmt.Table.Name)
	}

	// Build schema
	schema := &storage.Schema{
		Name: stmt.Table.Name,
	}

	for _, colDef := range stmt.Columns {
		col := storage.Column{
			Name:     colDef.Name,
			Type:     colDef.Type.Name,
			Nullable: true,
		}

		for _, constraint := range colDef.Constraints {
			switch constraint.Type {
			case parser.ConstraintPrimaryKey:
				col.PrimaryKey = true
				col.Nullable = false
				schema.PrimaryKey = col.Name
			case parser.ConstraintNotNull:
				col.Nullable = false
			case parser.ConstraintDefault:
				if constraint.Default != nil {
					val, _ := e.evalExpr(constraint.Default, nil)
					col.Default = val
				}
			case parser.ConstraintAutoIncrement:
				schema.AutoIncrement = true
			}
		}

		schema.Columns = append(schema.Columns, col)
	}

	// Handle table-level constraints
	for _, constraint := range stmt.Constraints {
		if constraint.Type == parser.ConstraintPrimaryKey && len(constraint.Columns) > 0 {
			schema.PrimaryKey = constraint.Columns[0]
			for i := range schema.Columns {
				if strings.EqualFold(schema.Columns[i].Name, schema.PrimaryKey) {
					schema.Columns[i].PrimaryKey = true
					schema.Columns[i].Nullable = false
				}
			}
		}
	}

	if err := e.schema.CreateTable(schema); err != nil {
		return nil, err
	}

	// Update analyzer catalog
	e.catalog.CreateTable(schema.ToAnalyzerTableInfo())

	result := NewResult("CREATE TABLE")
	return result, nil
}

// executeDropTable executes a DROP TABLE statement.
func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	for _, tableRef := range stmt.Tables {
		if !e.schema.TableExists(tableRef.Name) {
			if stmt.IfExists {
				continue
			}
			return nil, fmt.Errorf("table not found: %s", tableRef.Name)
		}

		// First, drop all indexes associated with this table
		indexes, _ := e.schema.ListTableIndexes(tableRef.Name)
		for _, idx := range indexes {
			// Clear index entries
			columns := make([]string, len(idx.Columns))
			for i, col := range idx.Columns {
				columns[i] = col.Name
			}
			e.table.ClearIndex(idx.Name, tableRef.Name, columns)
			// Drop the index schema
			e.schema.DropIndex(idx.Name)
		}

		// Then, truncate all data rows
		e.table.Truncate(tableRef.Name)

		// Finally, drop the table schema
		if err := e.schema.DropTable(tableRef.Name); err != nil {
			return nil, err
		}

		// Update analyzer catalog
		e.catalog.DropTable(tableRef.Name)
	}

	result := NewResult("DROP TABLE")
	return result, nil
}

// executeCreateIndex creates a new index.
func (e *Executor) executeCreateIndex(stmt *parser.CreateIndexStmt) (*Result, error) {
	// Check if index already exists
	if e.schema.IndexExists(stmt.Name) {
		if stmt.IfNotExists {
			result := NewResult("CREATE INDEX")
			return result, nil
		}
		return nil, fmt.Errorf("index already exists: %s", stmt.Name)
	}

	// Verify table exists
	if !e.schema.TableExists(stmt.Table) {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Verify columns exist
	schema, err := e.schema.GetSchema(stmt.Table)
	if err != nil {
		return nil, err
	}

	for _, col := range stmt.Columns {
		if _, found := schema.GetColumn(col.Name); !found {
			return nil, fmt.Errorf("column not found: %s", col.Name)
		}
	}

	// Create storage index
	index := &storage.Index{
		Name:   stmt.Name,
		Table:  stmt.Table,
		Unique: stmt.Unique,
	}

	for _, col := range stmt.Columns {
		index.Columns = append(index.Columns, storage.IndexColumn{
			Name: col.Name,
			Desc: col.Desc,
		})
	}

	if err := e.schema.CreateIndex(index); err != nil {
		return nil, err
	}

	// Build index entries for existing rows
	columns := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = col.Name
	}
	if err := e.table.BuildIndex(stmt.Name, stmt.Table, columns); err != nil {
		// Rollback index creation on failure
		e.schema.DropIndex(stmt.Name)
		return nil, fmt.Errorf("failed to build index: %w", err)
	}

	result := NewResult("CREATE INDEX")
	return result, nil
}

// executeDropIndex drops an index.
func (e *Executor) executeDropIndex(stmt *parser.DropIndexStmt) (*Result, error) {
	if !e.schema.IndexExists(stmt.Name) {
		if stmt.IfExists {
			result := NewResult("DROP INDEX")
			return result, nil
		}
		return nil, fmt.Errorf("index not found: %s", stmt.Name)
	}

	// Get index info to clear entries
	index, err := e.schema.GetIndex(stmt.Name)
	if err == nil && index != nil {
		columns := make([]string, len(index.Columns))
		for i, col := range index.Columns {
			columns[i] = col.Name
		}
		e.table.ClearIndex(stmt.Name, index.Table, columns)
	}

	if err := e.schema.DropIndex(stmt.Name); err != nil {
		return nil, err
	}

	result := NewResult("DROP INDEX")
	return result, nil
}

// executeAlterTable executes an ALTER TABLE statement.
func (e *Executor) executeAlterTable(stmt *parser.AlterTableStmt) (*Result, error) {
	switch action := stmt.Action.(type) {
	case *parser.AddColumnAction:
		return e.executeAlterTableAddColumn(stmt.Table, action)
	case *parser.DropColumnAction:
		return e.executeAlterTableDropColumn(stmt.Table, action)
	case *parser.RenameTableAction:
		return e.executeAlterTableRename(stmt.Table, action)
	case *parser.RenameColumnAction:
		return e.executeAlterTableRenameColumn(stmt.Table, action)
	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action: %T", action)
	}
}

// executeAlterTableAddColumn adds a column to a table.
func (e *Executor) executeAlterTableAddColumn(table string, action *parser.AddColumnAction) (*Result, error) {
	col := storage.Column{
		Name:     action.Column.Name,
		Type:     action.Column.Type.Name,
		Nullable: true,
	}

	// Process column constraints
	for _, constraint := range action.Column.Constraints {
		switch constraint.Type {
		case parser.ConstraintPrimaryKey:
			col.PrimaryKey = true
			col.Nullable = false
		case parser.ConstraintNotNull:
			col.Nullable = false
		case parser.ConstraintDefault:
			if constraint.Default != nil {
				val, _ := e.evalExpr(constraint.Default, nil)
				col.Default = val
			}
		}
	}

	if err := e.schema.AddColumn(table, col); err != nil {
		return nil, err
	}

	// Update catalog
	e.SyncCatalog()

	result := NewResult("ALTER TABLE")
	return result, nil
}

// executeAlterTableDropColumn drops a column from a table.
func (e *Executor) executeAlterTableDropColumn(table string, action *parser.DropColumnAction) (*Result, error) {
	if err := e.schema.DropColumn(table, action.Column); err != nil {
		return nil, err
	}

	// Update catalog
	e.SyncCatalog()

	result := NewResult("ALTER TABLE")
	return result, nil
}

// executeAlterTableRename renames a table.
func (e *Executor) executeAlterTableRename(table string, action *parser.RenameTableAction) (*Result, error) {
	if err := e.schema.RenameTable(table, action.NewName); err != nil {
		return nil, err
	}

	// Update catalog
	e.SyncCatalog()

	result := NewResult("ALTER TABLE")
	return result, nil
}

// executeAlterTableRenameColumn renames a column.
func (e *Executor) executeAlterTableRenameColumn(table string, action *parser.RenameColumnAction) (*Result, error) {
	if err := e.schema.RenameColumn(table, action.OldName, action.NewName); err != nil {
		return nil, err
	}

	// Update catalog
	e.SyncCatalog()

	result := NewResult("ALTER TABLE")
	return result, nil
}

// Transaction execution methods

// executeBegin starts a new transaction.
func (e *Executor) executeBegin(stmt *parser.BeginStmt) (*Result, error) {
	if e.inTransaction {
		return nil, fmt.Errorf("cannot start a transaction within a transaction")
	}

	e.inTransaction = true
	e.savepoints = nil
	e.txLog = nil

	result := NewResult("BEGIN")
	return result, nil
}

// executeCommit commits the current transaction.
func (e *Executor) executeCommit(stmt *parser.CommitStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("cannot commit: no transaction in progress")
	}

	// Clear transaction state
	e.inTransaction = false
	e.savepoints = nil
	e.txLog = nil

	result := NewResult("COMMIT")
	return result, nil
}

// executeRollback rolls back the current transaction or to a savepoint.
func (e *Executor) executeRollback(stmt *parser.RollbackStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("cannot rollback: no transaction in progress")
	}

	if stmt.Savepoint != "" {
		// Rollback to savepoint
		return e.rollbackToSavepoint(stmt.Savepoint)
	}

	// Full rollback - undo all operations in reverse order
	for i := len(e.txLog) - 1; i >= 0; i-- {
		entry := e.txLog[i]
		if err := e.undoOperation(entry); err != nil {
			// Log error but continue with rollback
			continue
		}
	}

	// Clear transaction state
	e.inTransaction = false
	e.savepoints = nil
	e.txLog = nil

	result := NewResult("ROLLBACK")
	return result, nil
}

// executeSavepoint creates a savepoint.
func (e *Executor) executeSavepoint(stmt *parser.SavepointStmt) (*Result, error) {
	if !e.inTransaction {
		// SQLite allows SAVEPOINT outside transaction (starts implicit transaction)
		e.inTransaction = true
		e.txLog = nil
	}

	// Add savepoint marker
	e.savepoints = append(e.savepoints, stmt.Name)

	result := NewResult("SAVEPOINT")
	return result, nil
}

// executeRelease releases a savepoint.
func (e *Executor) executeRelease(stmt *parser.ReleaseStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("cannot release savepoint: no transaction in progress")
	}

	// Find and remove the savepoint
	found := false
	for i := len(e.savepoints) - 1; i >= 0; i-- {
		if e.savepoints[i] == stmt.Name {
			e.savepoints = e.savepoints[:i]
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("no such savepoint: %s", stmt.Name)
	}

	result := NewResult("RELEASE")
	return result, nil
}

// executeAttach attaches a database.
func (e *Executor) executeAttach(stmt *parser.AttachStmt) (*Result, error) {
	// Check if alias already exists
	if _, exists := e.attachedDatabases[stmt.Alias]; exists {
		return nil, fmt.Errorf("database alias already exists: %s", stmt.Alias)
	}

	// Reserved alias check
	if strings.EqualFold(stmt.Alias, "temp") || strings.EqualFold(stmt.Alias, "temporary") {
		return nil, fmt.Errorf("reserved database alias: %s", stmt.Alias)
	}

	// Get the pool from the main schema manager
	pool := e.schema.GetPool()

	// Create new schema and table managers for the attached database
	// In PizzaKV, each database is just a different namespace/prefix
	schema := storage.NewSchemaManager(pool, stmt.FilePath)
	table := storage.NewTableManager(pool, schema, stmt.FilePath)

	// Register the database connection
	e.attachedDatabases[stmt.Alias] = &DatabaseConnection{
		Alias:  stmt.Alias,
		Path:   stmt.FilePath,
		Schema: schema,
		Table:  table,
	}

	// Sync the catalog with the attached database's tables
	tables, _ := schema.ListTables()
	for _, tableName := range tables {
		tSchema, err := schema.GetSchema(tableName)
		if err != nil {
			continue
		}
		// Add with database prefix
		tableInfo := tSchema.ToAnalyzerTableInfo()
		tableInfo.Name = stmt.Alias + "." + tableInfo.Name
		e.catalog.CreateTable(tableInfo)
	}

	result := NewResult("ATTACH")
	return result, nil
}

// executeDetach detaches a database.
func (e *Executor) executeDetach(stmt *parser.DetachStmt) (*Result, error) {
	// Cannot detach main database
	if strings.EqualFold(stmt.Alias, "main") {
		return nil, fmt.Errorf("cannot detach main database")
	}

	// Check if database exists
	if _, exists := e.attachedDatabases[stmt.Alias]; !exists {
		return nil, fmt.Errorf("no such database: %s", stmt.Alias)
	}

	// Remove from attached databases
	delete(e.attachedDatabases, stmt.Alias)

	// Note: We don't remove from catalog as that would be more complex
	// In a production system, we'd need to track which tables belong to which database

	result := NewResult("DETACH")
	return result, nil
}

// rollbackToSavepoint rolls back to a specific savepoint.
func (e *Executor) rollbackToSavepoint(name string) (*Result, error) {
	// Find savepoint index
	savepointIdx := -1
	for i := len(e.savepoints) - 1; i >= 0; i-- {
		if e.savepoints[i] == name {
			savepointIdx = i
			break
		}
	}

	if savepointIdx == -1 {
		return nil, fmt.Errorf("no such savepoint: %s", name)
	}

	// Count operations to undo (operations after the savepoint)
	// For simplicity, we track savepoint positions by counting log entries
	// In a real implementation, we'd track log positions per savepoint

	// Undo operations in reverse order
	for i := len(e.txLog) - 1; i >= 0; i-- {
		entry := e.txLog[i]
		if err := e.undoOperation(entry); err != nil {
			continue
		}
	}

	// Remove savepoints after the target
	e.savepoints = e.savepoints[:savepointIdx+1]

	result := NewResult("ROLLBACK")
	return result, nil
}

// undoOperation reverses a single operation.
func (e *Executor) undoOperation(entry txLogEntry) error {
	switch entry.operation {
	case "INSERT":
		// Delete the inserted row
		_, err := e.table.Delete(entry.table, func(r storage.Row) bool {
			// Match by primary key stored in entry.key
			pk := e.getPrimaryKey(entry.table)
			if pk == "" {
				return false
			}
			return fmt.Sprintf("%v", r[pk]) == entry.key
		})
		return err

	case "DELETE":
		// Re-insert the deleted row
		if entry.oldData != nil {
			return e.table.Insert(entry.table, entry.oldData)
		}

	case "UPDATE":
		// Restore the old data
		if entry.oldData != nil {
			pk := e.getPrimaryKey(entry.table)
			if pk != "" {
				// Delete current row and insert old data
				e.table.Delete(entry.table, func(r storage.Row) bool {
					return fmt.Sprintf("%v", r[pk]) == entry.key
				})
				return e.table.Insert(entry.table, entry.oldData)
			}
		}
	}
	return nil
}

// getPrimaryKey returns the primary key column name for a table.
func (e *Executor) getPrimaryKey(tableName string) string {
	schema, err := e.schema.GetSchema(tableName)
	if err != nil {
		return ""
	}
	return schema.PrimaryKey
}

// extractIndexableCondition extracts column name and value from a simple equality condition.
// Returns (column, value, true) if the expression is column = literal.
func (e *Executor) extractIndexableCondition(expr parser.Expr) (string, interface{}, bool) {
	binExpr, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return "", nil, false
	}

	// Only handle equality for now
	if binExpr.Op != lexer.TokenEq {
		return "", nil, false
	}

	// Check for column = literal pattern
	colRef, leftIsCol := binExpr.Left.(*parser.ColumnRef)
	litExpr, rightIsLit := binExpr.Right.(*parser.LiteralExpr)

	if leftIsCol && rightIsLit {
		val, _ := e.evalLiteral(litExpr)
		return colRef.Column, val, true
	}

	// Check for literal = column pattern
	litExpr, leftIsLit := binExpr.Left.(*parser.LiteralExpr)
	colRef, rightIsCol := binExpr.Right.(*parser.ColumnRef)

	if leftIsLit && rightIsCol {
		val, _ := e.evalLiteral(litExpr)
		return colRef.Column, val, true
	}

	return "", nil, false
}

// executePragma executes a PRAGMA statement.
func (e *Executor) executePragma(stmt *parser.PragmaStmt) (*Result, error) {
	switch stmt.Name {
	case "table_info":
		return e.pragmaTableInfo(stmt.Arg)
	case "table_list":
		return e.pragmaTableList()
	case "database_list":
		return e.pragmaDatabaseList()
	case "version":
		return e.pragmaVersion()
	default:
		return nil, fmt.Errorf("unknown pragma: %s", stmt.Name)
	}
}

// pragmaTableInfo returns column information for a table.
func (e *Executor) pragmaTableInfo(tableName string) (*Result, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table_info requires a table name")
	}

	schema, err := e.schema.GetSchema(tableName)
	if err != nil {
		return nil, err
	}

	result := NewResult("PRAGMA")
	result.AddColumn("cid")
	result.AddColumn("name")
	result.AddColumn("type")
	result.AddColumn("notnull")
	result.AddColumn("dflt_value")
	result.AddColumn("pk")

	for i, col := range schema.Columns {
		notnull := 0
		if !col.Nullable {
			notnull = 1
		}
		pk := 0
		if col.PrimaryKey {
			pk = 1
		}
		result.AddRow(int64(i), col.Name, col.Type, int64(notnull), col.Default, int64(pk))
	}

	return result, nil
}

// pragmaTableList returns a list of all tables.
func (e *Executor) pragmaTableList() (*Result, error) {
	tables, err := e.schema.ListTables()
	if err != nil {
		return nil, err
	}

	result := NewResult("PRAGMA")
	result.AddColumn("schema")
	result.AddColumn("name")
	result.AddColumn("type")

	for _, t := range tables {
		result.AddRow("main", t, "table")
	}

	return result, nil
}

// pragmaDatabaseList returns a list of databases.
func (e *Executor) pragmaDatabaseList() (*Result, error) {
	result := NewResult("PRAGMA")
	result.AddColumn("seq")
	result.AddColumn("name")
	result.AddColumn("file")

	// We only have one database
	result.AddRow(int64(0), "main", "")

	return result, nil
}

// pragmaVersion returns the PizzaSQL version.
func (e *Executor) pragmaVersion() (*Result, error) {
	result := NewResult("PRAGMA")
	result.AddColumn("version")
	result.AddRow("PizzaSQL 1.0.0")
	return result, nil
}

// executeExplain executes an EXPLAIN statement.
func (e *Executor) executeExplain(stmt *parser.ExplainStmt) (*Result, error) {
	result := NewResult("EXPLAIN")

	if stmt.QueryPlan {
		// EXPLAIN QUERY PLAN format
		result.AddColumn("id")
		result.AddColumn("parent")
		result.AddColumn("notused")
		result.AddColumn("detail")

		plan := e.generateQueryPlan(stmt.Statement)
		for i, step := range plan {
			result.AddRow(int64(i), int64(0), int64(0), step)
		}
	} else {
		// Simple EXPLAIN format
		result.AddColumn("addr")
		result.AddColumn("opcode")
		result.AddColumn("p1")
		result.AddColumn("p2")
		result.AddColumn("p3")
		result.AddColumn("p4")
		result.AddColumn("p5")
		result.AddColumn("comment")

		ops := e.generateOpcodes(stmt.Statement)
		for i, op := range ops {
			result.AddRow(int64(i), op, int64(0), int64(0), int64(0), "", int64(0), "")
		}
	}

	return result, nil
}

// generateQueryPlan generates a simple query plan description.
func (e *Executor) generateQueryPlan(stmt parser.Statement) []string {
	var plan []string

	switch s := stmt.(type) {
	case *parser.SelectStmt:
		if len(s.From) > 0 {
			plan = append(plan, fmt.Sprintf("SCAN TABLE %s", s.From[0].Name))
			if s.Where != nil {
				plan = append(plan, "FILTER")
			}
			if len(s.OrderBy) > 0 {
				plan = append(plan, "SORT")
			}
			if s.Limit != nil {
				plan = append(plan, "LIMIT")
			}
		} else {
			plan = append(plan, "SCALAR EXPRESSION")
		}
	case *parser.InsertStmt:
		plan = append(plan, fmt.Sprintf("INSERT INTO %s", s.Table.Name))
	case *parser.UpdateStmt:
		plan = append(plan, fmt.Sprintf("SCAN TABLE %s", s.Table.Name))
		plan = append(plan, "UPDATE")
	case *parser.DeleteStmt:
		plan = append(plan, fmt.Sprintf("SCAN TABLE %s", s.Table.Name))
		plan = append(plan, "DELETE")
	default:
		plan = append(plan, "EXECUTE")
	}

	return plan
}

// generateOpcodes generates simplified opcodes for EXPLAIN.
func (e *Executor) generateOpcodes(stmt parser.Statement) []string {
	var ops []string

	switch s := stmt.(type) {
	case *parser.SelectStmt:
		ops = append(ops, "Init")
		if len(s.From) > 0 {
			ops = append(ops, "OpenRead")
			ops = append(ops, "Rewind")
			ops = append(ops, "Column")
			ops = append(ops, "ResultRow")
			ops = append(ops, "Next")
			ops = append(ops, "Close")
		} else {
			ops = append(ops, "Integer")
			ops = append(ops, "ResultRow")
		}
		ops = append(ops, "Halt")
	case *parser.InsertStmt:
		ops = append(ops, "Init")
		ops = append(ops, "OpenWrite")
		ops = append(ops, "NewRowid")
		ops = append(ops, "Insert")
		ops = append(ops, "Close")
		ops = append(ops, "Halt")
	case *parser.UpdateStmt:
		ops = append(ops, "Init")
		ops = append(ops, "OpenWrite")
		ops = append(ops, "Rewind")
		ops = append(ops, "Column")
		ops = append(ops, "Update")
		ops = append(ops, "Next")
		ops = append(ops, "Close")
		ops = append(ops, "Halt")
	case *parser.DeleteStmt:
		ops = append(ops, "Init")
		ops = append(ops, "OpenWrite")
		ops = append(ops, "Rewind")
		ops = append(ops, "Delete")
		ops = append(ops, "Next")
		ops = append(ops, "Close")
		ops = append(ops, "Halt")
	default:
		ops = append(ops, "Init")
		ops = append(ops, "Halt")
	}

	return ops
}

// evalExpr evaluates an expression.
func (e *Executor) evalExpr(expr parser.Expr, row storage.Row) (interface{}, error) {
	switch ex := expr.(type) {
	case *parser.LiteralExpr:
		return e.evalLiteral(ex)
	case *parser.ColumnRef:
		return e.evalColumnRef(ex, row)
	case *parser.BinaryExpr:
		return e.evalBinaryExpr(ex, row)
	case *parser.UnaryExpr:
		return e.evalUnaryExpr(ex, row)
	case *parser.FunctionCall:
		return e.evalFunctionCall(ex, row)
	case *parser.ParenExpr:
		return e.evalExpr(ex.Expr, row)
	case *parser.CaseExpr:
		return e.evalCaseExpr(ex, row)
	case *parser.InExpr:
		return e.evalInExpr(ex, row)
	case *parser.BetweenExpr:
		return e.evalBetweenExpr(ex, row)
	case *parser.LikeExpr:
		return e.evalLikeExpr(ex, row)
	case *parser.IsNullExpr:
		return e.evalIsNullExpr(ex, row)
	case *parser.CastExpr:
		return e.evalCastExpr(ex, row)
	case *parser.SubqueryExpr:
		return e.evalSubqueryExpr(ex, row)
	case *parser.ExistsExpr:
		return e.evalExistsExpr(ex, row)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) evalLiteral(lit *parser.LiteralExpr) (interface{}, error) {
	switch lit.Type {
	case lexer.TokenNumber:
		// Check for scientific notation (e.g., 1e+06) or decimal point
		if strings.Contains(lit.Value, ".") || strings.ContainsAny(lit.Value, "eE") {
			f, err := strconv.ParseFloat(lit.Value, 64)
			if err != nil {
				return nil, err
			}
			// If it's a whole number (no fractional part), return as int64
			if f == float64(int64(f)) {
				return int64(f), nil
			}
			return f, nil
		}
		return strconv.ParseInt(lit.Value, 10, 64)
	case lexer.TokenString:
		return lit.Value, nil
	case lexer.TokenNULL:
		return nil, nil
	case lexer.TokenTRUE:
		return true, nil
	case lexer.TokenFALSE:
		return false, nil
	default:
		return lit.Value, nil
	}
}

func (e *Executor) evalColumnRef(ref *parser.ColumnRef, row storage.Row) (interface{}, error) {
	if row == nil {
		return nil, fmt.Errorf("no row context for column: %s", ref.Column)
	}

	// Check for ROWID aliases (rowid, oid, _rowid_)
	if storage.IsRowIDColumn(ref.Column) {
		if val, ok := row["_rowid_"]; ok {
			return val, nil
		}
		return nil, nil
	}

	// For qualified column references (table.column), check outer row first
	// This handles correlated subqueries where the qualifier refers to an outer table
	if ref.Table != "" && e.outerRow != nil {
		// Try qualified name in outer row first
		if val, ok := e.outerRow[ref.Table+"."+ref.Column]; ok {
			return val, nil
		}
		// Try case-insensitive in outer row
		for k, v := range e.outerRow {
			if strings.EqualFold(k, ref.Table+"."+ref.Column) {
				return v, nil
			}
		}
	}

	// Try qualified name in current row
	if ref.Table != "" {
		if val, ok := row[ref.Table+"."+ref.Column]; ok {
			return val, nil
		}
	}

	// Try direct column name
	if val, ok := row[ref.Column]; ok {
		return val, nil
	}

	// Case-insensitive search in current row
	for k, v := range row {
		if strings.EqualFold(k, ref.Column) {
			return v, nil
		}
		if ref.Table != "" && strings.EqualFold(k, ref.Table+"."+ref.Column) {
			return v, nil
		}
	}

	// For unqualified references, also check the outer row context
	if e.outerRow != nil {
		// Try direct column name in outer row
		if val, ok := e.outerRow[ref.Column]; ok {
			return val, nil
		}

		// Case-insensitive search in outer row
		for k, v := range e.outerRow {
			if strings.EqualFold(k, ref.Column) {
				return v, nil
			}
		}
	}

	return nil, nil // Column not found, return NULL
}

func (e *Executor) evalBinaryExpr(expr *parser.BinaryExpr, row storage.Row) (interface{}, error) {
	left, err := e.evalExpr(expr.Left, row)
	if err != nil {
		return nil, err
	}
	right, err := e.evalExpr(expr.Right, row)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case lexer.TokenPlus:
		return toFloat(left) + toFloat(right), nil
	case lexer.TokenMinus:
		return toFloat(left) - toFloat(right), nil
	case lexer.TokenStar:
		return toFloat(left) * toFloat(right), nil
	case lexer.TokenSlash:
		r := toFloat(right)
		if r == 0 {
			return nil, nil // Division by zero returns NULL
		}
		return toFloat(left) / r, nil
	case lexer.TokenPercent:
		return int64(toFloat(left)) % int64(toFloat(right)), nil
	case lexer.TokenEq:
		return compare(left, right) == 0, nil
	case lexer.TokenNeq:
		return compare(left, right) != 0, nil
	case lexer.TokenLt:
		return compare(left, right) < 0, nil
	case lexer.TokenLte:
		return compare(left, right) <= 0, nil
	case lexer.TokenGt:
		return compare(left, right) > 0, nil
	case lexer.TokenGte:
		return compare(left, right) >= 0, nil
	case lexer.TokenAND:
		return toBool(left) && toBool(right), nil
	case lexer.TokenOR:
		return toBool(left) || toBool(right), nil
	case lexer.TokenConcat:
		return toString(left) + toString(right), nil
	default:
		return nil, fmt.Errorf("unsupported operator: %v", expr.Op)
	}
}

func (e *Executor) evalUnaryExpr(expr *parser.UnaryExpr, row storage.Row) (interface{}, error) {
	val, err := e.evalExpr(expr.Operand, row)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case lexer.TokenMinus:
		return -toFloat(val), nil
	case lexer.TokenPlus:
		return toFloat(val), nil
	case lexer.TokenNOT:
		return !toBool(val), nil
	default:
		return val, nil
	}
}

func (e *Executor) evalFunctionCall(fn *parser.FunctionCall, row storage.Row) (interface{}, error) {
	name := strings.ToUpper(fn.Name)

	// Evaluate arguments
	args := make([]interface{}, len(fn.Args))
	for i, arg := range fn.Args {
		val, err := e.evalExpr(arg, row)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	switch name {
	case "UPPER":
		if len(args) > 0 {
			if args[0] == nil {
				return nil, nil // NULL propagation
			}
			return strings.ToUpper(toString(args[0])), nil
		}
	case "LOWER":
		if len(args) > 0 {
			if args[0] == nil {
				return nil, nil // NULL propagation
			}
			return strings.ToLower(toString(args[0])), nil
		}
	case "LENGTH":
		if len(args) > 0 {
			if args[0] == nil {
				return nil, nil // NULL propagation
			}
			return int64(len(toString(args[0]))), nil
		}
	case "ABS":
		if len(args) > 0 {
			v := toFloat(args[0])
			if v < 0 {
				return -v, nil
			}
			return v, nil
		}
	case "COALESCE":
		for _, arg := range args {
			if arg != nil {
				return arg, nil
			}
		}
		return nil, nil
	case "NULLIF":
		if len(args) >= 2 && compare(args[0], args[1]) == 0 {
			return nil, nil
		}
		if len(args) > 0 {
			return args[0], nil
		}
	case "IFNULL":
		if len(args) >= 2 {
			if args[0] == nil {
				return args[1], nil
			}
			return args[0], nil
		}
	case "TYPEOF":
		if len(args) > 0 {
			switch args[0].(type) {
			case nil:
				return "null", nil
			case int64, int:
				return "integer", nil
			case float64:
				return "real", nil
			case string:
				return "text", nil
			case []byte:
				return "blob", nil
			default:
				return "text", nil
			}
		}
	case "SUBSTR", "SUBSTRING":
		if len(args) >= 2 {
			s := toString(args[0])
			start := int(toFloat(args[1])) - 1 // SQL is 1-indexed
			if start < 0 {
				start = 0
			}
			if start >= len(s) {
				return "", nil
			}
			if len(args) >= 3 {
				length := int(toFloat(args[2]))
				if start+length > len(s) {
					length = len(s) - start
				}
				return s[start : start+length], nil
			}
			return s[start:], nil
		}
	case "TRIM":
		if len(args) > 0 {
			return strings.TrimSpace(toString(args[0])), nil
		}
	case "REPLACE":
		if len(args) >= 3 {
			return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2])), nil
		}

	// Additional SQLite functions
	case "PRINTF":
		if len(args) > 0 {
			format := toString(args[0])
			fmtArgs := make([]interface{}, len(args)-1)
			for i := 1; i < len(args); i++ {
				fmtArgs[i-1] = args[i]
			}
			return fmt.Sprintf(format, fmtArgs...), nil
		}
	case "HEX":
		if len(args) > 0 {
			s := toString(args[0])
			return strings.ToUpper(fmt.Sprintf("%x", []byte(s))), nil
		}
	case "UNHEX":
		if len(args) > 0 {
			s := toString(args[0])
			var result []byte
			for i := 0; i < len(s)-1; i += 2 {
				var b byte
				fmt.Sscanf(s[i:i+2], "%x", &b)
				result = append(result, b)
			}
			return string(result), nil
		}
	case "RANDOM":
		return rand.Int63(), nil
	case "RANDOMBLOB":
		if len(args) > 0 {
			n := int(toFloat(args[0]))
			if n <= 0 {
				n = 1
			}
			if n > 1000000 {
				n = 1000000
			}
			blob := make([]byte, n)
			rand.Read(blob)
			return string(blob), nil
		}
	case "ZEROBLOB":
		if len(args) > 0 {
			n := int(toFloat(args[0]))
			if n <= 0 {
				n = 1
			}
			if n > 1000000 {
				n = 1000000
			}
			return string(make([]byte, n)), nil
		}
	case "INSTR":
		if len(args) >= 2 {
			s := toString(args[0])
			substr := toString(args[1])
			idx := strings.Index(s, substr)
			if idx < 0 {
				return int64(0), nil
			}
			return int64(idx + 1), nil // SQL is 1-indexed
		}
	case "GLOB":
		if len(args) >= 2 {
			pattern := toString(args[0])
			s := toString(args[1])
			return matchGlob(pattern, s), nil
		}
	case "ROUND":
		if len(args) > 0 {
			v := toFloat(args[0])
			decimals := 0
			if len(args) >= 2 {
				decimals = int(toFloat(args[1]))
			}
			mult := 1.0
			for i := 0; i < decimals; i++ {
				mult *= 10
			}
			return float64(int64(v*mult+0.5)) / mult, nil
		}
	case "MAX":
		if len(args) > 0 {
			max := args[0]
			for _, arg := range args[1:] {
				if compare(arg, max) > 0 {
					max = arg
				}
			}
			return max, nil
		}
	case "MIN":
		if len(args) > 0 {
			min := args[0]
			for _, arg := range args[1:] {
				if compare(arg, min) < 0 {
					min = arg
				}
			}
			return min, nil
		}
	case "CONCAT":
		var result strings.Builder
		for _, arg := range args {
			result.WriteString(toString(arg))
		}
		return result.String(), nil
	}

	return nil, nil
}

func (e *Executor) evalCaseExpr(expr *parser.CaseExpr, row storage.Row) (interface{}, error) {
	var operand interface{}
	if expr.Operand != nil {
		var err error
		operand, err = e.evalExpr(expr.Operand, row)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range expr.Whens {
		cond, err := e.evalExpr(when.Condition, row)
		if err != nil {
			return nil, err
		}

		var match bool
		if operand != nil {
			match = compare(operand, cond) == 0
		} else {
			match = toBool(cond)
		}

		if match {
			return e.evalExpr(when.Result, row)
		}
	}

	if expr.Else != nil {
		return e.evalExpr(expr.Else, row)
	}

	return nil, nil
}

func (e *Executor) evalInExpr(expr *parser.InExpr, row storage.Row) (interface{}, error) {
	left, err := e.evalExpr(expr.Left, row)
	if err != nil {
		return nil, err
	}

	// Handle subquery: IN (SELECT ...)
	if expr.Subquery != nil {
		result, err := e.executeSelect(expr.Subquery)
		if err != nil {
			return nil, fmt.Errorf("IN subquery error: %w", err)
		}

		// Check each row's first column value
		for _, resultRow := range result.Rows {
			if len(resultRow) > 0 {
				if compare(left, resultRow[0]) == 0 {
					return !expr.Not, nil
				}
			}
		}
		return expr.Not, nil
	}

	// Handle value list: IN (1, 2, 3)
	for _, val := range expr.Values {
		v, err := e.evalExpr(val, row)
		if err != nil {
			return nil, err
		}
		if compare(left, v) == 0 {
			return !expr.Not, nil
		}
	}

	return expr.Not, nil
}

func (e *Executor) evalBetweenExpr(expr *parser.BetweenExpr, row storage.Row) (interface{}, error) {
	val, err := e.evalExpr(expr.Left, row)
	if err != nil {
		return nil, err
	}
	low, err := e.evalExpr(expr.Low, row)
	if err != nil {
		return nil, err
	}
	high, err := e.evalExpr(expr.High, row)
	if err != nil {
		return nil, err
	}

	inRange := compare(val, low) >= 0 && compare(val, high) <= 0
	if expr.Not {
		return !inRange, nil
	}
	return inRange, nil
}

func (e *Executor) evalLikeExpr(expr *parser.LikeExpr, row storage.Row) (interface{}, error) {
	val, err := e.evalExpr(expr.Left, row)
	if err != nil {
		return nil, err
	}
	pattern, err := e.evalExpr(expr.Pattern, row)
	if err != nil {
		return nil, err
	}

	s := toString(val)
	p := toString(pattern)

	// Convert SQL LIKE pattern to simple matching
	// % matches any sequence, _ matches single character
	matched := matchLike(s, p)
	if expr.Not {
		return !matched, nil
	}
	return matched, nil
}

func (e *Executor) evalIsNullExpr(expr *parser.IsNullExpr, row storage.Row) (interface{}, error) {
	val, err := e.evalExpr(expr.Left, row)
	if err != nil {
		return nil, err
	}

	isNull := val == nil
	if expr.Not {
		return !isNull, nil
	}
	return isNull, nil
}

func (e *Executor) evalCastExpr(expr *parser.CastExpr, row storage.Row) (interface{}, error) {
	val, err := e.evalExpr(expr.Expr, row)
	if err != nil {
		return nil, err
	}

	typeName := strings.ToUpper(expr.Type.Name)
	switch {
	case strings.Contains(typeName, "INT"):
		return int64(toFloat(val)), nil
	case strings.Contains(typeName, "REAL"), strings.Contains(typeName, "FLOAT"), strings.Contains(typeName, "DOUBLE"):
		return toFloat(val), nil
	case strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CHAR"):
		return toString(val), nil
	default:
		return val, nil
	}
}

// evalSubqueryExpr executes a scalar subquery and returns its value.
// A scalar subquery must return exactly one column. It returns:
// - The single value if the subquery returns one row
// - NULL if the subquery returns no rows
// - Error if the subquery returns more than one row (for strict SQL compliance)
func (e *Executor) evalSubqueryExpr(expr *parser.SubqueryExpr, row storage.Row) (interface{}, error) {
	// Save and set outer row context for correlated subqueries
	savedOuter := e.outerRow
	e.outerRow = row
	defer func() { e.outerRow = savedOuter }()

	// Execute the subquery
	result, err := e.executeSelect(expr.Query)
	if err != nil {
		return nil, fmt.Errorf("subquery error: %w", err)
	}

	// Check for empty result
	if result.RowCount == 0 {
		return nil, nil // Return NULL for empty subquery
	}

	// Check column count
	if len(result.Columns) == 0 {
		return nil, fmt.Errorf("subquery must return at least one column")
	}

	// For scalar subquery, return first column of first row
	// Note: Strict SQL would error if more than one row is returned
	// but we follow SQLite behavior which just returns the first value
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		return result.Rows[0][0], nil
	}

	return nil, nil
}

// evalExistsExpr evaluates an EXISTS expression.
// Returns true if the subquery returns at least one row, false otherwise.
func (e *Executor) evalExistsExpr(expr *parser.ExistsExpr, row storage.Row) (interface{}, error) {
	// Save and set outer row context for correlated subqueries
	savedOuter := e.outerRow
	e.outerRow = row
	defer func() { e.outerRow = savedOuter }()

	// Execute the subquery
	result, err := e.executeSelect(expr.Subquery)
	if err != nil {
		return nil, fmt.Errorf("EXISTS subquery error: %w", err)
	}

	// EXISTS returns true if any rows are returned
	return len(result.Rows) > 0, nil
}

// evalAggregateExpr evaluates an aggregate expression over multiple rows.
func (e *Executor) evalAggregateExpr(expr parser.Expr, rows []storage.Row) (interface{}, error) {
	fn, ok := expr.(*parser.FunctionCall)
	if !ok {
		// Not a function call - could be a binary expression with aggregates inside
		// Evaluate it with the aggregate evaluation context
		return e.evalExprWithAggregates(expr, rows)
	}

	name := strings.ToUpper(fn.Name)

	switch name {
	case "COUNT":
		if fn.Star {
			return int64(len(rows)), nil
		}
		count := int64(0)
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, _ := e.evalExpr(fn.Args[0], row)
				if val != nil {
					count++
				}
			}
		}
		return count, nil

	case "SUM":
		var sum float64
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, _ := e.evalExpr(fn.Args[0], row)
				if val != nil {
					sum += toFloat(val)
				}
			}
		}
		return sum, nil

	case "AVG":
		var sum float64
		count := 0
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, _ := e.evalExpr(fn.Args[0], row)
				if val != nil {
					sum += toFloat(val)
					count++
				}
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case "MIN":
		var min interface{}
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, _ := e.evalExpr(fn.Args[0], row)
				if val != nil && (min == nil || compare(val, min) < 0) {
					min = val
				}
			}
		}
		return min, nil

	case "MAX":
		var max interface{}
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, _ := e.evalExpr(fn.Args[0], row)
				if val != nil && (max == nil || compare(val, max) > 0) {
					max = val
				}
			}
		}
		return max, nil

	default:
		// Try scalar function
		if len(rows) > 0 {
			return e.evalFunctionCall(fn, rows[0])
		}
		return nil, nil
	}
}

// evalExprWithAggregates evaluates an expression that may contain aggregate functions
func (e *Executor) evalExprWithAggregates(expr parser.Expr, rows []storage.Row) (interface{}, error) {
	switch ex := expr.(type) {
	case *parser.BinaryExpr:
		left, err := e.evalExprWithAggregates(ex.Left, rows)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExprWithAggregates(ex.Right, rows)
		if err != nil {
			return nil, err
		}

		// Apply the binary operator
		switch ex.Op {
		case lexer.TokenPlus:
			return toFloat(left) + toFloat(right), nil
		case lexer.TokenMinus:
			return toFloat(left) - toFloat(right), nil
		case lexer.TokenStar:
			return toFloat(left) * toFloat(right), nil
		case lexer.TokenSlash:
			r := toFloat(right)
			if r == 0 {
				return nil, nil
			}
			return toFloat(left) / r, nil
		case lexer.TokenPercent:
			return int64(toFloat(left)) % int64(toFloat(right)), nil
		case lexer.TokenEq:
			return compare(left, right) == 0, nil
		case lexer.TokenNeq:
			return compare(left, right) != 0, nil
		case lexer.TokenLt:
			return compare(left, right) < 0, nil
		case lexer.TokenLte:
			return compare(left, right) <= 0, nil
		case lexer.TokenGt:
			return compare(left, right) > 0, nil
		case lexer.TokenGte:
			return compare(left, right) >= 0, nil
		case lexer.TokenAND:
			return toBool(left) && toBool(right), nil
		case lexer.TokenOR:
			return toBool(left) || toBool(right), nil
		case lexer.TokenConcat:
			return toString(left) + toString(right), nil
		default:
			return nil, fmt.Errorf("unsupported operator: %v", ex.Op)
		}
	case *parser.FunctionCall:
		return e.evalAggregateExpr(expr, rows)
	default:
		// Non-aggregate expression, use first row
		if len(rows) > 0 {
			return e.evalExpr(expr, rows[0])
		}
		return nil, nil
	}
}

// Helper functions

func (e *Executor) getSelectColumns(stmt *parser.SelectStmt, schema *storage.Schema) []string {
	var columns []string
	for _, col := range stmt.Columns {
		if col.Star {
			for _, c := range schema.Columns {
				columns = append(columns, c.Name)
			}
		} else if col.Alias != "" {
			columns = append(columns, col.Alias)
		} else if ref, ok := col.Expr.(*parser.ColumnRef); ok {
			columns = append(columns, ref.Column)
		} else {
			columns = append(columns, fmt.Sprintf("column%d", len(columns)+1))
		}
	}
	return columns
}

func (e *Executor) hasAggregates(columns []parser.SelectColumn) bool {
	for _, col := range columns {
		if e.isAggregate(col.Expr) {
			return true
		}
	}
	return false
}

func (e *Executor) isAggregate(expr parser.Expr) bool {
	if fn, ok := expr.(*parser.FunctionCall); ok {
		name := strings.ToUpper(fn.Name)
		switch name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL", "GROUP_CONCAT":
			return true
		}
	}
	return false
}

func (e *Executor) buildGroupKey(groupBy []parser.Expr, row storage.Row) string {
	var parts []string
	for _, expr := range groupBy {
		val, _ := e.evalExpr(expr, row)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "|")
}

func (e *Executor) sortRows(rows []storage.Row, orderBy []parser.OrderByItem) {
	sort.Slice(rows, func(i, j int) bool {
		for _, item := range orderBy {
			vi, _ := e.evalExpr(item.Expr, rows[i])
			vj, _ := e.evalExpr(item.Expr, rows[j])
			cmp := compare(vi, vj)
			if cmp != 0 {
				if item.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// sortResultRows sorts Result.Rows based on ORDER BY clauses.
// It handles column aliases by matching them against the select columns.
func (e *Executor) sortResultRows(result *Result, orderBy []parser.OrderByItem, selectColumns []parser.SelectColumn, columnNames []string) {
	sort.Slice(result.Rows, func(i, j int) bool {
		for _, item := range orderBy {
			var vi, vj interface{}
			var rowI, rowJ storage.Row

			// Check if ORDER BY references a column alias
			if ref, ok := item.Expr.(*parser.ColumnRef); ok && ref.Table == "" {
				// Look for matching alias in select columns
				for idx, name := range columnNames {
					if strings.EqualFold(name, ref.Column) {
						if idx < len(result.Rows[i]) {
							vi = result.Rows[i][idx]
							vj = result.Rows[j][idx]
							goto compare
						}
					}
				}
			}

			// If not found as alias, try to evaluate the expression
			// Create temporary rows from result rows for evaluation
			rowI = e.resultRowToStorageRow(result, i)
			rowJ = e.resultRowToStorageRow(result, j)
			vi, _ = e.evalExpr(item.Expr, rowI)
			vj, _ = e.evalExpr(item.Expr, rowJ)

		compare:
			cmp := compare(vi, vj)
			if cmp != 0 {
				if item.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// resultRowToStorageRow converts a Result row back to storage.Row for expression evaluation.
func (e *Executor) resultRowToStorageRow(result *Result, rowIdx int) storage.Row {
	row := make(storage.Row)
	for colIdx, colName := range result.Columns {
		if colIdx < len(result.Rows[rowIdx]) {
			row[colName] = result.Rows[rowIdx][colIdx]
		}
	}
	return row
}

func (e *Executor) evalIntExpr(expr parser.Expr) int {
	val, _ := e.evalExpr(expr, nil)
	return int(toFloat(val))
}

// Type conversion helpers

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case nil:
		return 0
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case float64:
		return val
	case bool:
		if val {
			return 1
		}
		return 0
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	switch val := v.(type) {
	case nil:
		return false
	case bool:
		return val
	case int64:
		return val != 0
	case int:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "0" && strings.ToLower(val) != "false"
	default:
		return false
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func compare(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	fa, oka := toNumeric(a)
	fb, okb := toNumeric(b)
	if oka && okb {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	// String comparison
	sa := toString(a)
	sb := toString(b)
	return strings.Compare(sa, sb)
}

func toNumeric(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// matchLike matches a string against a SQL LIKE pattern.
func matchLike(s, pattern string) bool {
	// Simple implementation - convert to lowercase for case-insensitive matching
	s = strings.ToLower(s)
	pattern = strings.ToLower(pattern)

	return matchLikeHelper(s, pattern)
}

func matchLikeHelper(s, p string) bool {
	if p == "" {
		return s == ""
	}

	if p[0] == '%' {
		// % matches any sequence
		for i := 0; i <= len(s); i++ {
			if matchLikeHelper(s[i:], p[1:]) {
				return true
			}
		}
		return false
	}

	if s == "" {
		return false
	}

	if p[0] == '_' || p[0] == s[0] {
		return matchLikeHelper(s[1:], p[1:])
	}

	return false
}

// matchGlob matches a string against a GLOB pattern.
// GLOB uses * for any sequence and ? for single character (case-sensitive).
func matchGlob(pattern, s string) bool {
	return matchGlobHelper(pattern, s)
}

func matchGlobHelper(p, s string) bool {
	if p == "" {
		return s == ""
	}

	if p[0] == '*' {
		// * matches any sequence
		for i := 0; i <= len(s); i++ {
			if matchGlobHelper(p[1:], s[i:]) {
				return true
			}
		}
		return false
	}

	if s == "" {
		return false
	}

	if p[0] == '?' || p[0] == s[0] {
		return matchGlobHelper(p[1:], s[1:])
	}

	// Handle character classes [...]
	if p[0] == '[' {
		end := strings.Index(p, "]")
		if end > 0 {
			class := p[1:end]
			match := false
			negate := false
			if len(class) > 0 && class[0] == '^' {
				negate = true
				class = class[1:]
			}
			for _, c := range class {
				if byte(c) == s[0] {
					match = true
					break
				}
			}
			if negate {
				match = !match
			}
			if match {
				return matchGlobHelper(p[end+1:], s[1:])
			}
		}
	}

	return false
}

// applyDistinct removes duplicate rows from the result
func (e *Executor) applyDistinct(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}

	seen := make(map[string]bool)
	uniqueRows := make([][]interface{}, 0)

	for _, row := range rows {
		// Create a key from all column values
		key := ""
		for i, val := range row {
			if i > 0 {
				key += "\x00" // Use null byte as separator
			}
			key += fmt.Sprintf("%v", val)
		}

		if !seen[key] {
			seen[key] = true
			uniqueRows = append(uniqueRows, row)
		}
	}

	return uniqueRows
}
