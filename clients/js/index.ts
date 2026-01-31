/**
 * PizzaSQL Client for JavaScript/TypeScript
 * Works with Node.js, Bun, and browsers
 */

export interface PizzaSQLConfig {
  apiKey?: string;
  timeout?: number;
}

export interface Column {
  name: string;
  type: string;
}

export interface QueryResult<T = Record<string, unknown>> {
  columns: Column[];
  rows: T[];
  rowsAffected: number;
  lastInsertId: number;
  executionTime: string;
}

export interface ExecuteResult {
  results: { rowsAffected: number; lastInsertId: number }[];
  totalRowsAffected: number;
  executionTime: string;
}

export interface TableInfo {
  tables: string[];
  count: number;
}

export interface PizzaSQLError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

class PizzaSQLClient {
  private baseUrl: string;
  private apiKey?: string;
  private timeout: number;
  private database?: string;

  constructor(uri: string, config: PizzaSQLConfig = {}) {
    // Parse URI: https://host:port/database or https://host:port
    const url = new URL(uri);
    this.baseUrl = `${url.protocol}//${url.host}`;
    this.database = url.pathname.slice(1) || undefined;
    this.apiKey = config.apiKey;
    this.timeout = config.timeout || 30000;
  }

  private async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<T> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(options.headers as Record<string, string>),
    };

    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    if (this.database) {
      headers['X-Database'] = this.database;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await fetch(`${this.baseUrl}${path}`, {
        ...options,
        headers,
        signal: controller.signal,
      });

      const data = await response.json();

      if (!response.ok) {
        const error = data.error as PizzaSQLError;
        throw new Error(`[${error.code}] ${error.message}`);
      }

      return data as T;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Execute a SQL query with optional parameters
   */
  async query<T = Record<string, unknown>>(
    sql: string,
    params: unknown[] = []
  ): Promise<QueryResult<T>> {
    const result = await this.request<{
      columns: Column[];
      rows: unknown[][];
      rowsAffected: number;
      lastInsertId: number;
      executionTime: string;
    }>('/query', {
      method: 'POST',
      body: JSON.stringify({ sql, params }),
    });

    // Transform rows from arrays to objects
    const rows = result.rows.map((row) => {
      const obj: Record<string, unknown> = {};
      result.columns.forEach((col, i) => {
        obj[col.name] = row[i];
      });
      return obj as T;
    });

    return {
      ...result,
      rows,
    };
  }

  /**
   * Shorthand for query - returns rows directly
   */
  async sql<T = Record<string, unknown>>(
    sql: string,
    params: unknown[] = []
  ): Promise<T[]> {
    const result = await this.query<T>(sql, params);
    return result.rows;
  }

  /**
   * Execute multiple statements in a batch
   */
  async execute(
    statements: { sql: string; params?: unknown[] }[],
    transaction = true
  ): Promise<ExecuteResult> {
    return this.request<ExecuteResult>('/execute', {
      method: 'POST',
      body: JSON.stringify({
        statements: statements.map((s) => ({
          sql: s.sql,
          params: s.params || [],
        })),
        transaction,
      }),
    });
  }

  /**
   * List all tables in the database
   */
  async tables(): Promise<string[]> {
    const result = await this.request<TableInfo>('/schema/tables');
    return result.tables;
  }

  /**
   * Get schema for a specific table
   */
  async schema(tableName: string): Promise<Column[]> {
    const result = await this.request<{ columns: Column[] }>(
      `/schema/tables/${encodeURIComponent(tableName)}`
    );
    return result.columns;
  }

  /**
   * Health check
   */
  async health(): Promise<{ status: string; database: string }> {
    return this.request('/health');
  }

  /**
   * Use a different database
   */
  use(database: string): PizzaSQLClient {
    const client = new PizzaSQLClient(this.baseUrl, {
      apiKey: this.apiKey,
      timeout: this.timeout,
    });
    client.database = database;
    return client;
  }
}

/**
 * Create a new PizzaSQL connection
 */
export function connect(uri: string, apiKey?: string): PizzaSQLClient {
  return new PizzaSQLClient(uri, { apiKey });
}

/**
 * Create a new PizzaSQL connection with full config
 */
export function createClient(
  uri: string,
  config: PizzaSQLConfig = {}
): PizzaSQLClient {
  return new PizzaSQLClient(uri, config);
}

// Default export
export default { connect, createClient };
