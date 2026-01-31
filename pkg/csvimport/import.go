package csvimport

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// ImportOptions configures CSV import behavior
type ImportOptions struct {
	TableName    string            // Target table name (required)
	HasHeader    bool              // First row is header (default: true)
	CreateTable  bool              // Create table if not exists
	IgnoreErrors bool              // Continue on row errors
	NullValue    string            // String that represents NULL (default: "")
	Delimiter    rune              // CSV delimiter (default: ',')
	ColumnTypes  map[string]string // Explicit column types for table creation
}

// DefaultImportOptions returns sensible defaults
func DefaultImportOptions() ImportOptions {
	return ImportOptions{
		HasHeader:   true,
		NullValue:   "",
		Delimiter:   ',',
		ColumnTypes: make(map[string]string),
	}
}

// ImportResult contains import statistics
type ImportResult struct {
	RowsImported int64    `json:"rowsImported"`
	RowsSkipped  int64    `json:"rowsSkipped"`
	TableCreated bool     `json:"tableCreated"`
	Errors       []string `json:"errors,omitempty"`
}

// ImportCSV imports CSV data into a table
func ImportCSV(r io.Reader, schema *storage.SchemaManager, table *storage.TableManager, opts ImportOptions) (*ImportResult, error) {
	if opts.TableName == "" {
		return nil, fmt.Errorf("table name is required for CSV import")
	}

	result := &ImportResult{}

	// Create CSV reader
	csvReader := csv.NewReader(r)
	if opts.Delimiter != 0 {
		csvReader.Comma = opts.Delimiter
	}
	csvReader.FieldsPerRecord = -1 // Allow variable field count

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		return result, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return result, nil
	}

	// Determine column names
	var columnNames []string
	startRow := 0

	if opts.HasHeader {
		columnNames = records[0]
		startRow = 1
	} else {
		// Generate column names if no header
		for i := range records[0] {
			columnNames = append(columnNames, fmt.Sprintf("column%d", i+1))
		}
	}

	// Check if table exists
	tableSchema, err := schema.GetSchema(opts.TableName)
	tableExists := err == nil

	if !tableExists {
		if !opts.CreateTable {
			return result, fmt.Errorf("table %s does not exist (use CreateTable option to auto-create)", opts.TableName)
		}

		// Create table with inferred schema
		tableSchema = inferSchema(opts.TableName, columnNames, records[startRow:], opts.ColumnTypes)
		if err := schema.CreateTable(tableSchema); err != nil {
			return result, fmt.Errorf("failed to create table %s: %w", opts.TableName, err)
		}
		result.TableCreated = true
	}

	// Build column type map for parsing
	columnTypeMap := make(map[string]string)
	for _, col := range tableSchema.Columns {
		columnTypeMap[strings.ToLower(col.Name)] = strings.ToUpper(col.Type)
	}

	// Import rows
	for i := startRow; i < len(records); i++ {
		record := records[i]
		row := make(storage.Row)

		for j, colName := range columnNames {
			if j >= len(record) {
				break
			}

			value := record[j]
			colType := columnTypeMap[strings.ToLower(colName)]

			// Handle NULL values
			if value == opts.NullValue {
				row[colName] = nil
				continue
			}

			// Parse value based on column type
			parsedValue, err := parseValue(value, colType)
			if err != nil {
				if opts.IgnoreErrors {
					result.Errors = append(result.Errors, fmt.Sprintf("row %d, column %s: %v", i+1, colName, err))
					row[colName] = value // Store as string
				} else {
					return result, fmt.Errorf("row %d, column %s: %w", i+1, colName, err)
				}
			} else {
				row[colName] = parsedValue
			}
		}

		// Insert row
		if err := table.Insert(opts.TableName, row); err != nil {
			if opts.IgnoreErrors {
				result.Errors = append(result.Errors, fmt.Sprintf("row %d: %v", i+1, err))
				result.RowsSkipped++
			} else {
				return result, fmt.Errorf("failed to insert row %d: %w", i+1, err)
			}
		} else {
			result.RowsImported++
		}
	}

	return result, nil
}

// inferSchema creates a schema based on column names and sample data
func inferSchema(tableName string, columnNames []string, sampleData [][]string, explicitTypes map[string]string) *storage.Schema {
	columns := make([]storage.Column, len(columnNames))

	for i, name := range columnNames {
		colType := "TEXT" // Default type

		// Check for explicit type
		if explicit, ok := explicitTypes[name]; ok {
			colType = explicit
		} else {
			// Infer type from sample data
			colType = inferColumnType(sampleData, i)
		}

		columns[i] = storage.Column{
			Name:     name,
			Type:     colType,
			Nullable: true,
		}
	}

	// Use first column as primary key if it looks like an ID
	if len(columns) > 0 {
		firstCol := strings.ToLower(columns[0].Name)
		if firstCol == "id" || strings.HasSuffix(firstCol, "_id") || strings.HasSuffix(firstCol, "id") {
			columns[0].PrimaryKey = true
			columns[0].Nullable = false
		}
	}

	return &storage.Schema{
		Name:       tableName,
		Columns:    columns,
		PrimaryKey: columns[0].Name,
	}
}

// inferColumnType infers the column type from sample data
func inferColumnType(sampleData [][]string, colIndex int) string {
	if len(sampleData) == 0 {
		return "TEXT"
	}

	allInts := true
	allFloats := true
	hasData := false

	for _, row := range sampleData {
		if colIndex >= len(row) {
			continue
		}

		value := strings.TrimSpace(row[colIndex])
		if value == "" {
			continue // Skip empty values for type inference
		}

		hasData = true

		// Try parsing as integer
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			allInts = false
		}

		// Try parsing as float
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			allFloats = false
		}
	}

	if !hasData {
		return "TEXT"
	}

	if allInts {
		return "INTEGER"
	}
	if allFloats {
		return "REAL"
	}
	return "TEXT"
}

// parseValue parses a string value based on the column type
func parseValue(value string, colType string) (interface{}, error) {
	colType = strings.ToUpper(colType)

	switch {
	case strings.Contains(colType, "INT"):
		// Handle INTEGER, INT, BIGINT, SMALLINT
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value: %s", value)
		}
		return i, nil

	case strings.Contains(colType, "REAL") || strings.Contains(colType, "FLOAT") || strings.Contains(colType, "DOUBLE"):
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float value: %s", value)
		}
		return f, nil

	case strings.Contains(colType, "BLOB"):
		// Handle hex-encoded blob (0xABCD or just ABCD)
		hexStr := value
		if strings.HasPrefix(hexStr, "0x") || strings.HasPrefix(hexStr, "0X") {
			hexStr = hexStr[2:]
		}
		data, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid hex blob value: %s", value)
		}
		return data, nil

	case strings.Contains(colType, "BOOL"):
		lower := strings.ToLower(value)
		if lower == "1" || lower == "true" || lower == "yes" {
			return true, nil
		}
		if lower == "0" || lower == "false" || lower == "no" {
			return false, nil
		}
		return nil, fmt.Errorf("invalid boolean value: %s", value)

	default:
		// TEXT, VARCHAR, CHAR, etc.
		return value, nil
	}
}
