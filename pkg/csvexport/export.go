package csvexport

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"sort"

	"github.com/danfragoso/pizzasql-next/pkg/storage"
)

// ExportOptions configures CSV export behavior
type ExportOptions struct {
	Table         string // Required: specific table to export
	IncludeHeader bool   // Include column names as first row (default: true)
	NullValue     string // String representation of NULL (default: "")
	Delimiter     rune   // CSV delimiter (default: ',')
}

// DefaultExportOptions returns sensible defaults
func DefaultExportOptions() ExportOptions {
	return ExportOptions{
		IncludeHeader: true,
		NullValue:     "",
		Delimiter:     ',',
	}
}

// ExportTable exports a single table to CSV format
func ExportTable(w io.Writer, schema *storage.SchemaManager, table *storage.TableManager, opts ExportOptions) error {
	if opts.Table == "" {
		return fmt.Errorf("table name is required for CSV export")
	}

	// Get table schema
	tableSchema, err := schema.GetSchema(opts.Table)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", opts.Table, err)
	}

	// Get all rows
	rows, err := table.Select(opts.Table, nil)
	if err != nil {
		return fmt.Errorf("failed to select rows from table %s: %w", opts.Table, err)
	}

	// Create CSV writer
	csvWriter := csv.NewWriter(w)
	if opts.Delimiter != 0 {
		csvWriter.Comma = opts.Delimiter
	}
	defer csvWriter.Flush()

	// Get column names (excluding internal _rowid_)
	var columns []string
	for _, col := range tableSchema.Columns {
		if col.Name != "_rowid_" {
			columns = append(columns, col.Name)
		}
	}

	// Write header if requested
	if opts.IncludeHeader {
		if err := csvWriter.Write(columns); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	// Write data rows
	for _, row := range rows {
		record := make([]string, len(columns))
		for i, colName := range columns {
			value := row[colName]
			record[i] = formatValue(value, opts.NullValue)
		}
		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return csvWriter.Error()
}

// ExportTableToBytes exports a single table and returns bytes
func ExportTableToBytes(schema *storage.SchemaManager, table *storage.TableManager, opts ExportOptions) ([]byte, error) {
	var buf bytes.Buffer
	if err := ExportTable(&buf, schema, table, opts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ExportMultipleTables exports multiple tables as a map of table name to CSV bytes
func ExportMultipleTables(schema *storage.SchemaManager, table *storage.TableManager, tables []string, opts ExportOptions) (map[string][]byte, error) {
	// If no tables specified, export all
	if len(tables) == 0 {
		var err error
		tables, err = schema.ListTables()
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		sort.Strings(tables)
	}

	result := make(map[string][]byte)
	for _, tableName := range tables {
		tableOpts := opts
		tableOpts.Table = tableName
		data, err := ExportTableToBytes(schema, table, tableOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to export table %s: %w", tableName, err)
		}
		result[tableName] = data
	}

	return result, nil
}

// formatValue converts a value to its CSV string representation
func formatValue(value interface{}, nullValue string) string {
	if value == nil {
		return nullValue
	}

	switch v := value.(type) {
	case string:
		return v
	case float64:
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case []byte:
		return "0x" + hex.EncodeToString(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
