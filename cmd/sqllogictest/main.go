// sqllogictest runner — sends SQL via HTTP to a running PizzaSQL server and
// compares results against the expected output in .test files.
//
// File format: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//
// Usage:
//
//	go run ./cmd/sqllogictest -url http://localhost:8080 -dir testdata/sqllogictest
package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const engineName = "pizzasql"

// ANSI color helpers
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

// ── types ────────────────────────────────────────────────────────────────────

type lineInfo struct {
	text string
	num  int
}

type record struct {
	isStatement bool
	isQuery     bool
	expectOK    bool     // statement: true → expect success
	typeStr     string   // query: column type chars (I/R/T)
	sortMode    string   // nosort | rowsort | valuesort
	label       string
	sql         string
	expected    []string // flattened expected values, one per line
	skip        bool
	file        string
	line        int
}

type queryRequest struct {
	SQL string `json:"sql"`
}

type queryResponse struct {
	Columns []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"columns"`
	Rows  [][]interface{} `json:"rows"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// ── runner ───────────────────────────────────────────────────────────────────

type runner struct {
	baseURL    string
	client     *http.Client
	verbose    bool
	stopOnFail bool
	passed     int
	failed     int
	skipped    int
	total      int    // total files to run
	filesDone  int    // files completed
	logW       *bufio.Writer
	logPath    string
}

func main() {
	urlFlag     := flag.String("url", "http://localhost:8080", "PizzaSQL server URL")
	dirFlag     := flag.String("dir", "testdata/sqllogictest", "Directory containing .test files")
	fileFlag    := flag.String("file", "", "Single .test file to run (overrides -dir)")
	verboseFlag := flag.Bool("v", false, "Print each passing record")
	stopFlag    := flag.Bool("stop", false, "Stop on first failure")
	logFlag     := flag.String("log", "sqllogictest-failures.log", "File to write failures to ('' to disable)")
	flag.Parse()

	r := &runner{
		baseURL:    strings.TrimRight(*urlFlag, "/"),
		client:     &http.Client{Timeout: 120 * time.Second},
		verbose:    *verboseFlag,
		stopOnFail: *stopFlag,
		logPath:    *logFlag,
	}

	if *logFlag != "" {
		lf, err := os.Create(*logFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot open log file: %v\n", err)
			os.Exit(1)
		}
		defer lf.Close()
		r.logW = bufio.NewWriter(lf)
		defer r.logW.Flush()
	}

	var files []string
	if *fileFlag != "" {
		files = []string{*fileFlag}
	} else {
		err := filepath.WalkDir(*dirFlag, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(path, ".test") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil || len(files) == 0 {
			fmt.Fprintf(os.Stderr, "no .test files found in %s\n", *dirFlag)
			os.Exit(1)
		}
		sort.Strings(files)
	}

	r.total = len(files)
	start := time.Now()
	for _, f := range files {
		if err := r.runFile(f, start); err != nil {
			fmt.Fprintf(os.Stderr, "error in %s: %v\n", f, err)
		}
		if r.stopOnFail && r.failed > 0 {
			break
		}
	}

	// clear the progress line
	fmt.Print("\r\033[K")

	total := r.passed + r.failed
	elapsed := time.Since(start).Round(time.Millisecond)

	passColor, failColor := colorGreen, colorDim
	if r.failed > 0 {
		failColor = colorRed
	}
	pct := 0.0
	if total > 0 {
		pct = 100.0 * float64(r.passed) / float64(total)
	}

	fmt.Printf("%s--- Summary ---%s\n", colorBold, colorReset)
	var summaryQPS string
	if secs := elapsed.Seconds(); secs > 0 && total > 0 {
		qps := float64(total) / secs
		switch {
		case qps >= 1_000_000:
			summaryQPS = fmt.Sprintf("%.2fM q/s", qps/1_000_000)
		case qps >= 1_000:
			summaryQPS = fmt.Sprintf("%.2fk q/s", qps/1_000)
		default:
			summaryQPS = fmt.Sprintf("%.0f q/s", qps)
		}
	}

	fmt.Printf("passed:  %s%d/%d (%.1f%%)%s\n", passColor, r.passed, total, pct, colorReset)
	fmt.Printf("failed:  %s%d%s\n", failColor, r.failed, colorReset)
	fmt.Printf("skipped: %d\n", r.skipped)
	fmt.Printf("time:    %s\n", elapsed)
	fmt.Printf("thru:    %s%s%s\n", colorCyan, summaryQPS, colorReset)
	if r.failed > 0 && *logFlag != "" {
		fmt.Printf("log:     %s%s%s\n", colorCyan, *logFlag, colorReset)
	}
	if r.failed > 0 {
		os.Exit(1)
	}
}

func (r *runner) runFile(path string, start time.Time) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	records, err := parseFile(path, f)
	if err != nil {
		return err
	}

	// Drop any tables/views this file creates so it always runs against a clean state.
	for _, tbl := range collectCreatedTables(records) {
		r.execQuery("DROP TABLE IF EXISTS " + tbl) //nolint:errcheck
	}
	for _, v := range collectCreatedViews(records) {
		r.execQuery("DROP VIEW IF EXISTS " + v) //nolint:errcheck
	}

	labelCache := make(map[string][]string) // label → first result
	failsBefore := r.failed
	for _, rec := range records {
		if r.stopOnFail && r.failed > 0 {
			break
		}
		if rec.skip {
			r.skipped++
			continue
		}
		r.runRecord(rec, labelCache)
		r.printProgress(path, start)
	}

	r.filesDone++
	newFails := r.failed - failsBefore
	rel, _ := filepath.Rel("testdata/sqllogictest", path)
	if rel == "" {
		rel = filepath.Base(path)
	}
	var statusStr string
	if newFails == 0 {
		statusStr = colorGreen + "ok" + colorReset
	} else {
		statusStr = fmt.Sprintf("%s%d FAILED%s", colorRed, newFails, colorReset)
	}
	fmt.Printf("\r\033[K%s[%d/%d]%s %-52s %s\n", colorDim, r.filesDone, r.total, colorReset, rel, statusStr)
	return nil
}

func (r *runner) printProgress(currentFile string, start time.Time) {
	rel, _ := filepath.Rel("testdata/sqllogictest", currentFile)
	if rel == "" {
		rel = filepath.Base(currentFile)
	}

	elapsed := time.Since(start)
	elapsedStr := elapsed.Round(time.Second).String()

	var etaStr string
	if r.filesDone > 0 {
		rate := float64(r.filesDone) / elapsed.Seconds()
		eta := time.Duration(float64(r.total-r.filesDone)/rate * float64(time.Second)).Round(time.Second)
		etaStr = "eta " + eta.String()
	} else {
		etaStr = "eta --"
	}

	checked := r.passed + r.failed
	var rateStr string
	if checked > 0 {
		pct := 100.0 * float64(r.passed) / float64(checked)
		color := colorRed
		if r.failed == 0 {
			color = colorGreen
		} else if pct >= 90 {
			color = colorYellow
		}
		rateStr = fmt.Sprintf("%s%.1f%%%s", color, pct, colorReset)
	} else {
		rateStr = "  --.--%"
	}

	var throughputStr string
	if secs := elapsed.Seconds(); secs > 0 && checked > 0 {
		qps := float64(checked) / secs
		switch {
		case qps >= 1_000_000:
			throughputStr = fmt.Sprintf("%.1fM q/s", qps/1_000_000)
		case qps >= 1_000:
			throughputStr = fmt.Sprintf("%.1fk q/s", qps/1_000)
		default:
			throughputStr = fmt.Sprintf("%.0f q/s", qps)
		}
	} else {
		throughputStr = "-- q/s"
	}

	fmt.Printf("\r\033[K%s[%d/%d]%s %-40s  %s  pass=%-6d %sfail=%-5d%s skip=%-5d  %s / %s  %s%s%s",
		colorDim, r.filesDone+1, r.total, colorReset,
		rel, rateStr,
		r.passed,
		colorRed, r.failed, colorReset,
		r.skipped,
		elapsedStr, etaStr,
		colorCyan, throughputStr, colorReset,
	)
}

// collectCreatedTables scans records for CREATE TABLE statements and returns
// the table names so they can be pre-dropped before each test file runs.
func collectCreatedViews(records []*record) []string {
	seen := map[string]bool{}
	var views []string
	for _, rec := range records {
		if !rec.isStatement {
			continue
		}
		fields := strings.Fields(rec.sql)
		if len(fields) < 3 {
			continue
		}
		if !strings.EqualFold(fields[0], "CREATE") || !strings.EqualFold(fields[1], "VIEW") {
			continue
		}
		idx := 2
		if strings.EqualFold(fields[idx], "IF") && len(fields) > idx+2 {
			idx = 5
		}
		if idx < len(fields) {
			name := strings.TrimSuffix(fields[idx], ";")
			if name != "" && !seen[name] {
				seen[name] = true
				views = append(views, name)
			}
		}
	}
	return views
}

func collectCreatedTables(records []*record) []string {
	seen := map[string]bool{}
	var tables []string
	for _, rec := range records {
		if !rec.isStatement {
			continue
		}
		fields := strings.Fields(rec.sql)
		if len(fields) < 3 {
			continue
		}
		if !strings.EqualFold(fields[0], "CREATE") || !strings.EqualFold(fields[1], "TABLE") {
			continue
		}
		idx := 2
		if strings.EqualFold(fields[idx], "IF") && len(fields) > idx+2 {
			idx = 5 // CREATE TABLE IF NOT EXISTS <name>
		}
		if idx < len(fields) {
			name := strings.TrimSuffix(strings.TrimSuffix(fields[idx], "("), ";")
			if name != "" && !seen[name] {
				seen[name] = true
				tables = append(tables, name)
			}
		}
	}
	return tables
}

func (r *runner) runRecord(rec *record, labelCache map[string][]string) {
	resp, err := r.execQuery(rec.sql)
	if err != nil {
		r.fail(rec, "http error: %v", err)
		return
	}

	if rec.isStatement {
		if rec.expectOK {
			if resp.Error != nil {
				r.fail(rec, "expected ok, got error: %s", resp.Error.Message)
			} else {
				r.pass(rec)
			}
		} else {
			if resp.Error == nil {
				r.fail(rec, "expected error, got ok")
			} else {
				r.pass(rec)
			}
		}
		return
	}

	// query record
	if resp.Error != nil {
		r.fail(rec, "unexpected error: %s", resp.Error.Message)
		return
	}

	got := r.formatResults(resp, rec.typeStr)
	ncols := len(rec.typeStr)
	if ncols == 0 {
		ncols = 1
	}

	// Apply sort before any comparison.
	switch rec.sortMode {
	case "rowsort":
		got = sortRows(got, ncols)
	case "valuesort":
		g := append([]string(nil), got...)
		sort.Strings(g)
		got = g
	}

	// Label caching: if this query has a label, compare against first occurrence.
	if rec.label != "" {
		if cached, seen := labelCache[rec.label]; seen {
			if !equalSlices(got, cached) {
				r.fail(rec, "label %q result mismatch\n    want: %v\n    got:  %v", rec.label, cached, got)
			} else {
				r.pass(rec)
			}
			return
		}
		// First occurrence: store and fall through to normal expected-value check.
		labelCache[rec.label] = got
	}

	// hash format: "N values hashing to <md5>"
	if len(rec.expected) == 1 {
		parts := strings.Fields(rec.expected[0])
		if len(parts) == 5 && parts[1] == "values" && parts[2] == "hashing" && parts[3] == "to" {
			wantCount, _ := strconv.Atoi(parts[0])
			wantHash := parts[4]
			if len(got) != wantCount {
				r.fail(rec, "hash record: want %d values got %d", wantCount, len(got))
				return
			}
			h := md5.Sum([]byte(strings.Join(got, "\n") + "\n"))
			gotHash := fmt.Sprintf("%x", h)
			if gotHash != wantHash {
				r.fail(rec, "hash mismatch: want %s got %s", wantHash, gotHash)
				return
			}
			r.pass(rec)
			return
		}
	}

	exp := rec.expected
	switch rec.sortMode {
	case "rowsort":
		exp = sortRows(exp, ncols)
	case "valuesort":
		e := append([]string(nil), exp...)
		sort.Strings(e)
		exp = e
	}

	if !equalSlices(got, exp) {
		r.fail(rec, "result mismatch\n    want: %v\n    got:  %v", exp, got)
	} else {
		r.pass(rec)
	}
}

// ── formatting ───────────────────────────────────────────────────────────────

func (r *runner) formatResults(resp *queryResponse, typeStr string) []string {
	var vals []string
	for _, row := range resp.Rows {
		for i, v := range row {
			ct := byte('T')
			if i < len(typeStr) {
				ct = typeStr[i]
			}
			vals = append(vals, formatValue(v, ct))
		}
	}
	return vals
}

// formatValue converts a JSON value to the string representation expected by
// the sqllogictest format. Type chars: I=integer, R=real (%.3g), T=text.
func formatValue(v interface{}, colType byte) string {
	if v == nil {
		return "NULL"
	}
	switch colType {
	case 'I':
		switch n := v.(type) {
		case float64:
			return strconv.FormatInt(int64(n), 10)
		case int64:
			return strconv.FormatInt(n, 10)
		case int:
			return strconv.Itoa(n)
		case bool:
			if n {
				return "1"
			}
			return "0"
		case string:
			if i, err := strconv.ParseInt(n, 10, 64); err == nil {
				return strconv.FormatInt(i, 10)
			}
			return n
		default:
			return fmt.Sprintf("%v", v)
		}
	case 'R':
		switch n := v.(type) {
		case float64:
			return strconv.FormatFloat(n, 'g', 3, 64)
		case int64:
			return strconv.FormatFloat(float64(n), 'g', 3, 64)
		case int:
			return strconv.FormatFloat(float64(n), 'g', 3, 64)
		case string:
			if f, err := strconv.ParseFloat(n, 64); err == nil {
				return strconv.FormatFloat(f, 'g', 3, 64)
			}
			return n
		default:
			return fmt.Sprintf("%v", v)
		}
	default: // T
		switch s := v.(type) {
		case string:
			return s
		case bool:
			if s {
				return "1"
			}
			return "0"
		case float64:
			if s == math.Trunc(s) && !math.IsInf(s, 0) {
				return strconv.FormatInt(int64(s), 10)
			}
			return fmt.Sprintf("%g", s)
		default:
			return fmt.Sprintf("%v", v)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func sortRows(vals []string, ncols int) []string {
	if ncols <= 0 || len(vals) == 0 {
		return vals
	}
	nrows := len(vals) / ncols
	rows := make([][]string, nrows)
	for i := range rows {
		s, e := i*ncols, i*ncols+ncols
		if e > len(vals) {
			e = len(vals)
		}
		rows[i] = vals[s:e]
	}
	sort.Slice(rows, func(i, j int) bool {
		for k := 0; k < len(rows[i]) && k < len(rows[j]); k++ {
			if rows[i][k] != rows[j][k] {
				return rows[i][k] < rows[j][k]
			}
		}
		return len(rows[i]) < len(rows[j])
	})
	out := make([]string, 0, len(vals))
	for _, row := range rows {
		out = append(out, row...)
	}
	return out
}

func equalSlices(a, b []string) bool {
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

func (r *runner) execQuery(sql string) (*queryResponse, error) {
	body, _ := json.Marshal(queryRequest{SQL: sql})
	resp, err := r.client.Post(r.baseURL+"/query", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var qr queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &qr, nil
}

func (r *runner) pass(rec *record) {
	r.passed++
	if r.verbose && r.logW != nil {
		fmt.Fprintf(r.logW, "  ok   %s:%d\n", rec.file, rec.line)
	}
}

func (r *runner) fail(rec *record, format string, args ...interface{}) {
	r.failed++
	msg := fmt.Sprintf(format, args...)
	sql := strings.ReplaceAll(strings.TrimSpace(rec.sql), "\n", " ")
	if len(sql) > 120 {
		sql = sql[:117] + "..."
	}
	line := fmt.Sprintf("FAIL %s:%d: %s\n     SQL: %s\n", rec.file, rec.line, msg, sql)
	if r.logW != nil {
		fmt.Fprint(r.logW, line)
		r.logW.Flush()
	} else {
		fmt.Print(line)
	}
}

// ── parser ────────────────────────────────────────────────────────────────────

// parseFile reads a sqllogictest file and returns all records.
func parseFile(path string, f *os.File) ([]*record, error) {
	scanner := bufio.NewScanner(f)

	var lines []lineInfo
	n := 0
	for scanner.Scan() {
		n++
		text := scanner.Text()
		if !strings.HasPrefix(strings.TrimSpace(text), "#") {
			lines = append(lines, lineInfo{text: text, num: n})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// split into blocks separated by blank lines
	var blocks [][]lineInfo
	var cur []lineInfo
	for _, li := range lines {
		if strings.TrimSpace(li.text) == "" {
			if len(cur) > 0 {
				blocks = append(blocks, cur)
				cur = nil
			}
		} else {
			cur = append(cur, li)
		}
	}
	if len(cur) > 0 {
		blocks = append(blocks, cur)
	}

	var records []*record
	haltSeen := false
	skipNext := false

	for _, block := range blocks {
		if haltSeen {
			break
		}

		// consume skipif / onlyif lines at the top of the block
		i := 0
		for i < len(block) {
			lower := strings.ToLower(strings.TrimSpace(block[i].text))
			if strings.HasPrefix(lower, "skipif ") {
				engine := strings.TrimSpace(block[i].text[7:])
				if strings.EqualFold(engine, engineName) {
					skipNext = true
				}
				i++
			} else if strings.HasPrefix(lower, "onlyif ") {
				engine := strings.TrimSpace(block[i].text[7:])
				if !strings.EqualFold(engine, engineName) {
					skipNext = true
				}
				i++
			} else {
				break
			}
		}

		if i >= len(block) {
			continue
		}

		directiveLine := block[i]
		parts := strings.Fields(directiveLine.text)
		if len(parts) == 0 {
			continue
		}

		rec := &record{file: path, line: directiveLine.num, skip: skipNext}
		skipNext = false
		body := block[i+1:]

		switch parts[0] {
		case "halt":
			haltSeen = true
			continue

		case "statement":
			rec.isStatement = true
			rec.expectOK = len(parts) > 1 && parts[1] == "ok"
			var sqlLines []string
			for _, li := range body {
				sqlLines = append(sqlLines, li.text)
			}
			rec.sql = strings.Join(sqlLines, "\n")

		case "query":
			rec.isQuery = true
			if len(parts) > 1 {
				rec.typeStr = strings.ToUpper(parts[1])
			}
			if len(parts) > 2 {
				rec.sortMode = parts[2]
			} else {
				rec.sortMode = "nosort"
			}
			if len(parts) > 3 {
				rec.label = parts[3]
			}
			inResults := false
			var sqlLines []string
			for _, li := range body {
				if strings.TrimSpace(li.text) == "----" {
					inResults = true
					continue
				}
				if inResults {
					rec.expected = append(rec.expected, strings.TrimSpace(li.text))
				} else {
					sqlLines = append(sqlLines, li.text)
				}
			}
			rec.sql = strings.Join(sqlLines, "\n")

		default:
			continue
		}

		if strings.TrimSpace(rec.sql) == "" {
			continue
		}
		records = append(records, rec)
	}

	return records, nil
}
