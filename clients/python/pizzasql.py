"""
PizzaSQL Client for Python

A simple, Pythonic client for PizzaSQL.
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from urllib.parse import urlparse
import json

try:
    import httpx
    _client_class = httpx.Client
    _async_client_class = httpx.AsyncClient
except ImportError:
    import urllib.request
    import urllib.error
    _client_class = None
    _async_client_class = None


@dataclass
class Column:
    """Represents a column in a query result."""
    name: str
    type: str


@dataclass
class QueryResult:
    """Result of a SQL query."""
    columns: List[Column]
    rows: List[Dict[str, Any]]
    rows_affected: int
    last_insert_id: int
    execution_time: str

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, index):
        return self.rows[index]


class PizzaSQLError(Exception):
    """Exception raised for PizzaSQL errors."""
    def __init__(self, code: str, message: str, details: Optional[Dict] = None):
        self.code = code
        self.message = message
        self.details = details
        super().__init__(f"[{code}] {message}")


class PizzaSQL:
    """
    PizzaSQL client for Python.

    Usage:
        db = PizzaSQL('http://localhost:8080/mydb', api_key='your-key')
        rows = db.sql('SELECT * FROM users')
    """

    def __init__(
        self,
        uri: str,
        api_key: Optional[str] = None,
        timeout: float = 30.0
    ):
        """
        Create a new PizzaSQL connection.

        Args:
            uri: Database URI (e.g., 'http://localhost:8080/mydb')
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
        """
        parsed = urlparse(uri)
        self._base_url = f"{parsed.scheme}://{parsed.netloc}"
        self._database = parsed.path.lstrip('/') or None
        self._api_key = api_key
        self._timeout = timeout

        if _client_class:
            self._client = _client_class(timeout=timeout)
        else:
            self._client = None

    def _headers(self) -> Dict[str, str]:
        headers = {'Content-Type': 'application/json'}
        if self._api_key:
            headers['Authorization'] = f'Bearer {self._api_key}'
        if self._database:
            headers['X-Database'] = self._database
        return headers

    def _request(self, method: str, path: str, data: Optional[Dict] = None) -> Dict:
        url = f"{self._base_url}{path}"
        headers = self._headers()

        if self._client:
            # Use httpx
            if method == 'GET':
                response = self._client.get(url, headers=headers)
            else:
                response = self._client.post(url, headers=headers, json=data)

            result = response.json()
            if response.status_code >= 400:
                error = result.get('error', {})
                raise PizzaSQLError(
                    error.get('code', 'UNKNOWN'),
                    error.get('message', 'Unknown error'),
                    error.get('details')
                )
            return result
        else:
            # Fallback to urllib
            req = urllib.request.Request(url, headers=headers)
            if data:
                req.data = json.dumps(data).encode('utf-8')

            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as response:
                    return json.loads(response.read().decode('utf-8'))
            except urllib.error.HTTPError as e:
                result = json.loads(e.read().decode('utf-8'))
                error = result.get('error', {})
                raise PizzaSQLError(
                    error.get('code', 'UNKNOWN'),
                    error.get('message', str(e)),
                    error.get('details')
                )

    def _transform_rows(self, columns: List[Dict], rows: List[List]) -> List[Dict[str, Any]]:
        """Transform array rows to dictionaries."""
        return [
            {col['name']: row[i] for i, col in enumerate(columns)}
            for row in rows
        ]

    def query(self, sql: str, params: Optional[List] = None) -> QueryResult:
        """
        Execute a SQL query and return full result.

        Args:
            sql: SQL query string
            params: Optional list of parameters

        Returns:
            QueryResult with columns, rows, and metadata
        """
        result = self._request('POST', '/query', {
            'sql': sql,
            'params': params or []
        })

        columns = [Column(**col) for col in result.get('columns', [])]
        rows = self._transform_rows(result.get('columns', []), result.get('rows', []))

        return QueryResult(
            columns=columns,
            rows=rows,
            rows_affected=result.get('rowsAffected', 0),
            last_insert_id=result.get('lastInsertId', 0),
            execution_time=result.get('executionTime', '')
        )

    def sql(self, sql: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return rows.

        Args:
            sql: SQL query string
            params: Optional list of parameters

        Returns:
            List of row dictionaries
        """
        return self.query(sql, params).rows

    def execute(
        self,
        statements: List[Dict[str, Any]],
        transaction: bool = True
    ) -> Dict[str, Any]:
        """
        Execute multiple statements in a batch.

        Args:
            statements: List of {'sql': ..., 'params': [...]} dicts
            transaction: Whether to wrap in a transaction

        Returns:
            Execution result with affected rows
        """
        return self._request('POST', '/execute', {
            'statements': [
                {'sql': s['sql'], 'params': s.get('params', [])}
                for s in statements
            ],
            'transaction': transaction
        })

    def tables(self) -> List[str]:
        """
        List all tables in the database.

        Returns:
            List of table names
        """
        result = self._request('GET', '/schema/tables')
        return result.get('tables', [])

    def schema(self, table_name: str) -> List[Column]:
        """
        Get schema for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            List of Column objects
        """
        result = self._request('GET', f'/schema/tables/{table_name}')
        return [Column(**col) for col in result.get('columns', [])]

    def health(self) -> Dict[str, str]:
        """
        Check database health.

        Returns:
            Health status dict
        """
        return self._request('GET', '/health')

    def use(self, database: str) -> 'PizzaSQL':
        """
        Create a new client for a different database.

        Args:
            database: Database name

        Returns:
            New PizzaSQL client
        """
        client = PizzaSQL(self._base_url, self._api_key, self._timeout)
        client._database = database
        return client

    def close(self):
        """Close the underlying HTTP client."""
        if self._client and hasattr(self._client, 'close'):
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# Convenience function
def connect(uri: str, api_key: Optional[str] = None) -> PizzaSQL:
    """
    Create a new PizzaSQL connection.

    Args:
        uri: Database URI (e.g., 'http://localhost:8080/mydb')
        api_key: Optional API key for authentication

    Returns:
        PizzaSQL client instance
    """
    return PizzaSQL(uri, api_key)
