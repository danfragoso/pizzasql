package pizzasql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Client represents a connection to a PizzaSQL database
type Client struct {
	baseURL string
	dbName  string
	apiKey  string
	client  *http.Client
}

// Row represents a single row in the result set
type Row map[string]interface{}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Rows []Row `json:"rows"`
}

// Connect creates a new PizzaSQL client connection
// URI format: http://host:port/dbname or https://pizzabase.cloud/my_org/sql/my_db:32131
func Connect(uri string, apiKey string) (*Client, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI: %w", err)
	}

	// Extract database name from path
	path := strings.Trim(parsedURL.Path, "/")
	if path == "" {
		return nil, fmt.Errorf("database name not found in URI path")
	}

	// Split path to get database name (last segment)
	pathParts := strings.Split(path, "/")
	dbName := pathParts[len(pathParts)-1]

	// Reconstruct base URL without the database path
	baseURL := fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host)

	return &Client{
		baseURL: baseURL,
		dbName:  dbName,
		apiKey:  apiKey,
		client:  &http.Client{},
	}, nil
}

// SQL executes a SQL query and returns the results as a slice of rows
func (c *Client) SQL(query string) ([]Row, error) {
	// Prepare request body
	body := map[string]string{"query": query}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/%s/query", c.baseURL, c.dbName)
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	}

	// Execute request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response
	var result QueryResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result.Rows, nil
}

// Export exports a database or table to SQL or CSV format
func (c *Client) Export(table string, format string) ([]byte, error) {
	params := url.Values{}
	if table != "" {
		params.Set("table", table)
	}
	if format != "" {
		params.Set("format", format)
	}

	url := fmt.Sprintf("%s/%s/export?%s", c.baseURL, c.dbName, params.Encode())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.apiKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("export failed with status %d: %s", resp.StatusCode, string(data))
	}

	return data, nil
}

// Import imports data from SQL or CSV format
func (c *Client) Import(data []byte, format string, createTable bool) error {
	params := url.Values{}
	if format != "" {
		params.Set("format", format)
	}
	if createTable {
		params.Set("create_table", "true")
	}

	url := fmt.Sprintf("%s/%s/import?%s", c.baseURL, c.dbName, params.Encode())
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	if c.apiKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("import failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
