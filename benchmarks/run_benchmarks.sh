#!/bin/bash
set -e

echo "========================================"
echo "PizzaSQL Benchmarks"
echo "========================================"
echo ""

# Configuration
PIZZASQL_HTTP_URL="http://localhost:8080"
PIZZASQL_PG_HOST="localhost"
PIZZASQL_PG_PORT="5432"
SQLITE_DB="benchmark.db"
NUM_ROWS=10000
NUM_ITERATIONS=1000

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check dependencies
command -v sqlite3 >/dev/null 2>&1 || { echo "sqlite3 is required but not installed. Aborting." >&2; exit 1; }
command -v psql >/dev/null 2>&1 || { echo "psql is required but not installed. Aborting." >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl is required but not installed. Aborting." >&2; exit 1; }

echo -e "${BLUE}Configuration:${NC}"
echo "  Rows to insert: $NUM_ROWS"
echo "  Query iterations: $NUM_ITERATIONS"
echo "  PizzaSQL HTTP: $PIZZASQL_HTTP_URL"
echo "  PizzaSQL PostgreSQL: $PIZZASQL_PG_HOST:$PIZZASQL_PG_PORT"
echo "  SQLite DB: $SQLITE_DB"
echo ""

# Function to time a command
time_command() {
    local name=$1
    shift
    local start=$(date +%s%N)
    "$@" > /dev/null 2>&1
    local end=$(date +%s%N)
    local duration=$(( (end - start) / 1000000 ))
    echo "$name: ${duration}ms"
    echo $duration
}

# Function to calculate throughput
calc_throughput() {
    local operations=$1
    local time_ms=$2
    local ops_per_sec=$(echo "scale=2; $operations * 1000 / $time_ms" | bc)
    echo "$ops_per_sec"
}

# Cleanup
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    rm -f $SQLITE_DB
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"DROP TABLE IF EXISTS benchmark_users"}' > /dev/null 2>&1 || true
    psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "DROP TABLE IF EXISTS benchmark_users" > /dev/null 2>&1 || true
}

trap cleanup EXIT

echo -e "${GREEN}=== Benchmark 1: CREATE TABLE ===${NC}"
echo ""

# SQLite
sqlite3 $SQLITE_DB "CREATE TABLE benchmark_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, created_at TEXT)"
sqlite_create_time=$(time_command "SQLite CREATE" sqlite3 $SQLITE_DB "CREATE TABLE IF NOT EXISTS benchmark_test (id INTEGER PRIMARY KEY, value TEXT)")

# PizzaSQL HTTP
pizzasql_http_create_time=$(time_command "PizzaSQL HTTP CREATE" curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"CREATE TABLE benchmark_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, created_at TEXT)"}')

# PizzaSQL PostgreSQL
pizzasql_pg_create_time=$(time_command "PizzaSQL PostgreSQL CREATE" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "CREATE TABLE IF NOT EXISTS benchmark_test (id INTEGER PRIMARY KEY, value TEXT)")

echo ""
echo -e "${GREEN}=== Benchmark 2: INSERT $NUM_ROWS Rows ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 $NUM_ROWS); do
    sqlite3 $SQLITE_DB "INSERT INTO benchmark_users (name, email, age, created_at) VALUES ('User$i', 'user$i@example.com', $((20 + $i % 50)), '2024-01-01')" 2>/dev/null
done
end=$(date +%s%N)
sqlite_insert_time=$(( (end - start) / 1000000 ))
sqlite_insert_ops=$(calc_throughput $NUM_ROWS $sqlite_insert_time)
echo "SQLite INSERT: ${sqlite_insert_time}ms (${sqlite_insert_ops} ops/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 $NUM_ROWS); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"sql\":\"INSERT INTO benchmark_users (name, email, age, created_at) VALUES ('User$i', 'user$i@example.com', $((20 + $i % 50)), '2024-01-01')\"}" > /dev/null
done
end=$(date +%s%N)
pizzasql_http_insert_time=$(( (end - start) / 1000000 ))
pizzasql_http_insert_ops=$(calc_throughput $NUM_ROWS $pizzasql_http_insert_time)
echo "PizzaSQL HTTP INSERT: ${pizzasql_http_insert_time}ms (${pizzasql_http_insert_ops} ops/sec)"

# PizzaSQL PostgreSQL
start=$(date +%s%N)
for i in $(seq 1 $NUM_ROWS); do
    psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "INSERT INTO benchmark_users (name, email, age, created_at) VALUES ('User$i', 'user$i@example.com', $((20 + $i % 50)), '2024-01-01')" > /dev/null 2>&1
done
end=$(date +%s%N)
pizzasql_pg_insert_time=$(( (end - start) / 1000000 ))
pizzasql_pg_insert_ops=$(calc_throughput $NUM_ROWS $pizzasql_pg_insert_time)
echo "PizzaSQL PostgreSQL INSERT: ${pizzasql_pg_insert_time}ms (${pizzasql_pg_insert_ops} ops/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 3: SELECT (Full Table Scan) ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 100); do
    sqlite3 $SQLITE_DB "SELECT * FROM benchmark_users WHERE age > 30" > /dev/null
done
end=$(date +%s%N)
sqlite_select_time=$(( (end - start) / 1000000 ))
sqlite_select_ops=$(calc_throughput 100 $sqlite_select_time)
echo "SQLite SELECT: ${sqlite_select_time}ms (${sqlite_select_ops} queries/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 100); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT * FROM benchmark_users WHERE age > 30"}' > /dev/null
done
end=$(date +%s%N)
pizzasql_http_select_time=$(( (end - start) / 1000000 ))
pizzasql_http_select_ops=$(calc_throughput 100 $pizzasql_http_select_time)
echo "PizzaSQL HTTP SELECT: ${pizzasql_http_select_time}ms (${pizzasql_http_select_ops} queries/sec)"

# PizzaSQL PostgreSQL
start=$(date +%s%N)
for i in $(seq 1 100); do
    psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "SELECT * FROM benchmark_users WHERE age > 30" > /dev/null 2>&1
done
end=$(date +%s%N)
pizzasql_pg_select_time=$(( (end - start) / 1000000 ))
pizzasql_pg_select_ops=$(calc_throughput 100 $pizzasql_pg_select_time)
echo "PizzaSQL PostgreSQL SELECT: ${pizzasql_pg_select_time}ms (${pizzasql_pg_select_ops} queries/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 4: CREATE INDEX ===${NC}"
echo ""

# SQLite
sqlite_index_time=$(time_command "SQLite CREATE INDEX" sqlite3 $SQLITE_DB "CREATE INDEX idx_age ON benchmark_users(age)")

# PizzaSQL HTTP
pizzasql_http_index_time=$(time_command "PizzaSQL HTTP CREATE INDEX" curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"CREATE INDEX idx_age ON benchmark_users(age)"}')

# PizzaSQL PostgreSQL
pizzasql_pg_index_time=$(time_command "PizzaSQL PostgreSQL CREATE INDEX" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "CREATE INDEX idx_age_pg ON benchmark_users(age)")

echo ""
echo -e "${GREEN}=== Benchmark 5: SELECT with INDEX ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 100); do
    sqlite3 $SQLITE_DB "SELECT * FROM benchmark_users WHERE age = 35" > /dev/null
done
end=$(date +%s%N)
sqlite_indexed_time=$(( (end - start) / 1000000 ))
sqlite_indexed_ops=$(calc_throughput 100 $sqlite_indexed_time)
echo "SQLite SELECT (indexed): ${sqlite_indexed_time}ms (${sqlite_indexed_ops} queries/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 100); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT * FROM benchmark_users WHERE age = 35"}' > /dev/null
done
end=$(date +%s%N)
pizzasql_http_indexed_time=$(( (end - start) / 1000000 ))
pizzasql_http_indexed_ops=$(calc_throughput 100 $pizzasql_http_indexed_time)
echo "PizzaSQL HTTP SELECT (indexed): ${pizzasql_http_indexed_time}ms (${pizzasql_http_indexed_ops} queries/sec)"

# PizzaSQL PostgreSQL
start=$(date +%s%N)
for i in $(seq 1 100); do
    psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "SELECT * FROM benchmark_users WHERE age = 35" > /dev/null 2>&1
done
end=$(date +%s%N)
pizzasql_pg_indexed_time=$(( (end - start) / 1000000 ))
pizzasql_pg_indexed_ops=$(calc_throughput 100 $pizzasql_pg_indexed_time)
echo "PizzaSQL PostgreSQL SELECT (indexed): ${pizzasql_pg_indexed_time}ms (${pizzasql_pg_indexed_ops} queries/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 6: UPDATE ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 100); do
    sqlite3 $SQLITE_DB "UPDATE benchmark_users SET age = age + 1 WHERE id = $i" > /dev/null
done
end=$(date +%s%N)
sqlite_update_time=$(( (end - start) / 1000000 ))
sqlite_update_ops=$(calc_throughput 100 $sqlite_update_time)
echo "SQLite UPDATE: ${sqlite_update_time}ms (${sqlite_update_ops} ops/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 100); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"sql\":\"UPDATE benchmark_users SET age = age + 1 WHERE id = $i\"}" > /dev/null
done
end=$(date +%s%N)
pizzasql_http_update_time=$(( (end - start) / 1000000 ))
pizzasql_http_update_ops=$(calc_throughput 100 $pizzasql_http_update_time)
echo "PizzaSQL HTTP UPDATE: ${pizzasql_http_update_time}ms (${pizzasql_http_update_ops} ops/sec)"

# PizzaSQL PostgreSQL
start=$(date +%s%N)
for i in $(seq 1 100); do
    psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "UPDATE benchmark_users SET age = age + 1 WHERE id = $i" > /dev/null 2>&1
done
end=$(date +%s%N)
pizzasql_pg_update_time=$(( (end - start) / 1000000 ))
pizzasql_pg_update_ops=$(calc_throughput 100 $pizzasql_pg_update_time)
echo "PizzaSQL PostgreSQL UPDATE: ${pizzasql_pg_update_time}ms (${pizzasql_pg_update_ops} ops/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 7: Aggregate Query ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 100); do
    sqlite3 $SQLITE_DB "SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM benchmark_users GROUP BY age" > /dev/null
done
end=$(date +%s%N)
sqlite_agg_time=$(( (end - start) / 1000000 ))
sqlite_agg_ops=$(calc_throughput 100 $sqlite_agg_time)
echo "SQLite AGGREGATE: ${sqlite_agg_time}ms (${sqlite_agg_ops} queries/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 100); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM benchmark_users GROUP BY age"}' > /dev/null
done
end=$(date +%s%N)
pizzasql_http_agg_time=$(( (end - start) / 1000000 ))
pizzasql_http_agg_ops=$(calc_throughput 100 $pizzasql_http_agg_time)
echo "PizzaSQL HTTP AGGREGATE: ${pizzasql_http_agg_time}ms (${pizzasql_http_agg_ops} queries/sec)"

# PizzaSQL PostgreSQL
start=$(date +%s%N)
for i in $(seq 1 100); do
    psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -d pizzasql -c "SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM benchmark_users GROUP BY age" > /dev/null 2>&1
done
end=$(date +%s%N)
pizzasql_pg_agg_time=$(( (end - start) / 1000000 ))
pizzasql_pg_agg_ops=$(calc_throughput 100 $pizzasql_pg_agg_time)
echo "PizzaSQL PostgreSQL AGGREGATE: ${pizzasql_pg_agg_time}ms (${pizzasql_pg_agg_ops} queries/sec)"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

printf "%-30s %-15s %-15s %-15s\n" "Operation" "SQLite" "PizzaSQL HTTP" "PizzaSQL PG"
printf "%-30s %-15s %-15s %-15s\n" "------------------------------" "---------------" "---------------" "---------------"
printf "%-30s %-15s %-15s %-15s\n" "INSERT ($NUM_ROWS rows)" "${sqlite_insert_ops} ops/s" "${pizzasql_http_insert_ops} ops/s" "${pizzasql_pg_insert_ops} ops/s"
printf "%-30s %-15s %-15s %-15s\n" "SELECT (no index)" "${sqlite_select_ops} q/s" "${pizzasql_http_select_ops} q/s" "${pizzasql_pg_select_ops} q/s"
printf "%-30s %-15s %-15s %-15s\n" "SELECT (indexed)" "${sqlite_indexed_ops} q/s" "${pizzasql_http_indexed_ops} q/s" "${pizzasql_pg_indexed_ops} q/s"
printf "%-30s %-15s %-15s %-15s\n" "UPDATE" "${sqlite_update_ops} ops/s" "${pizzasql_http_update_ops} ops/s" "${pizzasql_pg_update_ops} ops/s"
printf "%-30s %-15s %-15s %-15s\n" "AGGREGATE" "${sqlite_agg_ops} q/s" "${pizzasql_http_agg_ops} q/s" "${pizzasql_pg_agg_ops} q/s"

echo ""
echo -e "${GREEN}Benchmarks completed!${NC}"
