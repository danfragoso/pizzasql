#!/bin/bash
set -e

echo "========================================"
echo "PizzaSQL Quick Benchmarks"
echo "========================================"
echo ""

# Configuration
PIZZASQL_HTTP_URL="http://localhost:8080"
PIZZASQL_PG_HOST="localhost"
PIZZASQL_PG_PORT="5433"
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_DB="postgres"
POSTGRES_USER="$USER"
SQLITE_DB="/tmp/pizzasql_benchmark.db"
NUM_ROWS=1000
NUM_QUERIES=100

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Configuration:${NC}"
echo "  Rows to insert: $NUM_ROWS"
echo "  Query iterations: $NUM_QUERIES"
echo "  PizzaSQL HTTP: $PIZZASQL_HTTP_URL"
echo "  PizzaSQL PostgreSQL: $PIZZASQL_PG_HOST:$PIZZASQL_PG_PORT"
echo "  PostgreSQL: $POSTGRES_HOST:$POSTGRES_PORT"
echo "  SQLite DB: $SQLITE_DB"
echo ""

# Cleanup
rm -f $SQLITE_DB
curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"DROP TABLE IF EXISTS users"}' > /dev/null 2>&1 || true
PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -c "DROP TABLE IF EXISTS users_pg" > /dev/null 2>&1 || true
PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "DROP TABLE IF EXISTS users_bench" > /dev/null 2>&1 || true

echo -e "${GREEN}=== Setup: Creating Tables ===${NC}"
echo ""

# SQLite
sqlite3 $SQLITE_DB "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)"
echo "✓ SQLite table created"

# PizzaSQL HTTP
curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"DROP TABLE IF EXISTS users"}' > /dev/null 2>&1 || true
curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)"}' > /dev/null
echo "✓ PizzaSQL HTTP table created"

# PizzaSQL PostgreSQL
PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -c "DROP TABLE IF EXISTS users_pg" > /dev/null 2>&1 || true
PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -c "CREATE TABLE users_pg (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)" > /dev/null 2>&1
echo "✓ PizzaSQL PostgreSQL table created"

# PostgreSQL
PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "DROP TABLE IF EXISTS users_bench" > /dev/null 2>&1 || true
PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "CREATE TABLE users_bench (id SERIAL PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)" > /dev/null 2>&1
echo "✓ PostgreSQL table created"

echo ""
echo -e "${GREEN}=== Benchmark 1: INSERT $NUM_ROWS Rows ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 $NUM_ROWS); do
    sqlite3 $SQLITE_DB "INSERT INTO users (name, email, age, active) VALUES ('User$i', 'user$i@test.com', $((20 + $i % 50)), 1)" 2>/dev/null
done
end=$(date +%s%N)
sqlite_insert_ms=$(( (end - start) / 1000000 ))
sqlite_insert_ops=$(echo "scale=0; $NUM_ROWS * 1000 / $sqlite_insert_ms" | bc)
echo "  SQLite:              ${sqlite_insert_ms}ms total (${sqlite_insert_ops} inserts/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 $NUM_ROWS); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"sql\":\"INSERT INTO users (name, email, age, active) VALUES ('User$i', 'user$i@test.com', $((20 + $i % 50)), 1)\"}" > /dev/null
done
end=$(date +%s%N)
http_insert_ms=$(( (end - start) / 1000000 ))
http_insert_ops=$(echo "scale=0; $NUM_ROWS * 1000 / $http_insert_ms" | bc)
echo "  PizzaSQL (HTTP):     ${http_insert_ms}ms total (${http_insert_ops} inserts/sec)"

# PizzaSQL PostgreSQL (using single connection with transaction)
start=$(date +%s%N)
{
    echo "BEGIN;"
    for i in $(seq 1 $NUM_ROWS); do
        echo "INSERT INTO users_pg (name, email, age, active) VALUES ('User$i', 'user$i@test.com', $((20 + $i % 50)), 1);"
    done
    echo "COMMIT;"
} | PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -q > /dev/null 2>&1
end=$(date +%s%N)
pg_insert_ms=$(( (end - start) / 1000000 ))
pg_insert_ops=$(echo "scale=0; $NUM_ROWS * 1000 / $pg_insert_ms" | bc)
echo "  PizzaSQL (Postgres): ${pg_insert_ms}ms total (${pg_insert_ops} inserts/sec)"

# PostgreSQL
start=$(date +%s%N)
{
    echo "BEGIN;"
    for i in $(seq 1 $NUM_ROWS); do
        echo "INSERT INTO users_bench (name, email, age, active) VALUES ('User$i', 'user$i@test.com', $((20 + $i % 50)), 1);"
    done
    echo "COMMIT;"
} | PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -q > /dev/null 2>&1
end=$(date +%s%N)
postgres_insert_ms=$(( (end - start) / 1000000 ))
postgres_insert_ops=$(echo "scale=0; $NUM_ROWS * 1000 / $postgres_insert_ms" | bc)
echo "  PostgreSQL:          ${postgres_insert_ms}ms total (${postgres_insert_ops} inserts/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 2: SELECT (Full Scan) ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 $NUM_QUERIES); do
    sqlite3 $SQLITE_DB "SELECT * FROM users WHERE age > 30" > /dev/null
done
end=$(date +%s%N)
sqlite_select_ms=$(( (end - start) / 1000000 ))
sqlite_select_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $sqlite_select_ms" | bc)
echo "  SQLite:              ${sqlite_select_ms}ms total (${sqlite_select_ops} queries/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 $NUM_QUERIES); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT * FROM users WHERE age > 30"}' > /dev/null
done
end=$(date +%s%N)
http_select_ms=$(( (end - start) / 1000000 ))
http_select_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $http_select_ms" | bc)
echo "  PizzaSQL (HTTP):     ${http_select_ms}ms total (${http_select_ops} queries/sec)"

# PizzaSQL PostgreSQL (using single connection)
start=$(date +%s%N)
{
    for i in $(seq 1 $NUM_QUERIES); do
        echo "SELECT * FROM users_pg WHERE age > 30;"
    done
} | PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -q > /dev/null 2>&1
end=$(date +%s%N)
pg_select_ms=$(( (end - start) / 1000000 ))
pg_select_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $pg_select_ms" | bc)
echo "  PizzaSQL (Postgres): ${pg_select_ms}ms total (${pg_select_ops} queries/sec)"

# PostgreSQL
start=$(date +%s%N)
{
    for i in $(seq 1 $NUM_QUERIES); do
        echo "SELECT * FROM users_bench WHERE age > 30;"
    done
} | PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -q > /dev/null 2>&1
end=$(date +%s%N)
postgres_select_ms=$(( (end - start) / 1000000 ))
postgres_select_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $postgres_select_ms" | bc)
echo "  PostgreSQL:          ${postgres_select_ms}ms total (${postgres_select_ops} queries/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 3: CREATE INDEX ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
sqlite3 $SQLITE_DB "CREATE INDEX idx_age ON users(age)" > /dev/null
end=$(date +%s%N)
sqlite_index_ms=$(( (end - start) / 1000000 ))
echo "  SQLite:              ${sqlite_index_ms}ms"

# PizzaSQL HTTP
start=$(date +%s%N)
curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"CREATE INDEX idx_age ON users(age)"}' > /dev/null
end=$(date +%s%N)
http_index_ms=$(( (end - start) / 1000000 ))
echo "  PizzaSQL (HTTP):     ${http_index_ms}ms"

# PizzaSQL PostgreSQL
start=$(date +%s%N)
PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -c "CREATE INDEX idx_age_pg ON users_pg(age)" > /dev/null 2>&1
end=$(date +%s%N)
pg_index_ms=$(( (end - start) / 1000000 ))
echo "  PizzaSQL (Postgres): ${pg_index_ms}ms"

# PostgreSQL
start=$(date +%s%N)
PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "CREATE INDEX idx_age_bench ON users_bench(age)" > /dev/null 2>&1
end=$(date +%s%N)
postgres_index_ms=$(( (end - start) / 1000000 ))
echo "  PostgreSQL:          ${postgres_index_ms}ms"

echo ""
echo -e "${GREEN}=== Benchmark 4: SELECT with INDEX ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 $NUM_QUERIES); do
    sqlite3 $SQLITE_DB "SELECT * FROM users WHERE age = 35" > /dev/null
done
end=$(date +%s%N)
sqlite_indexed_ms=$(( (end - start) / 1000000 ))
sqlite_indexed_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $sqlite_indexed_ms" | bc)
echo "  SQLite:              ${sqlite_indexed_ms}ms total (${sqlite_indexed_ops} queries/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 $NUM_QUERIES); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT * FROM users WHERE age = 35"}' > /dev/null
done
end=$(date +%s%N)
http_indexed_ms=$(( (end - start) / 1000000 ))
http_indexed_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $http_indexed_ms" | bc)
echo "  PizzaSQL (HTTP):     ${http_indexed_ms}ms total (${http_indexed_ops} queries/sec)"

# PizzaSQL PostgreSQL (using single connection)
start=$(date +%s%N)
{
    for i in $(seq 1 $NUM_QUERIES); do
        echo "SELECT * FROM users_pg WHERE age = 35;"
    done
} | PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -q > /dev/null 2>&1
end=$(date +%s%N)
pg_indexed_ms=$(( (end - start) / 1000000 ))
pg_indexed_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $pg_indexed_ms" | bc)
echo "  PizzaSQL (Postgres): ${pg_indexed_ms}ms total (${pg_indexed_ops} queries/sec)"

# PostgreSQL
start=$(date +%s%N)
{
    for i in $(seq 1 $NUM_QUERIES); do
        echo "SELECT * FROM users_bench WHERE age = 35;"
    done
} | PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -q > /dev/null 2>&1
end=$(date +%s%N)
postgres_indexed_ms=$(( (end - start) / 1000000 ))
postgres_indexed_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $postgres_indexed_ms" | bc)
echo "  PostgreSQL:          ${postgres_indexed_ms}ms total (${postgres_indexed_ops} queries/sec)"

echo ""
echo -e "${GREEN}=== Benchmark 5: Aggregate Query ===${NC}"
echo ""

# SQLite
start=$(date +%s%N)
for i in $(seq 1 $NUM_QUERIES); do
    sqlite3 $SQLITE_DB "SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM users GROUP BY age" > /dev/null
done
end=$(date +%s%N)
sqlite_agg_ms=$(( (end - start) / 1000000 ))
sqlite_agg_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $sqlite_agg_ms" | bc)
echo "  SQLite:              ${sqlite_agg_ms}ms total (${sqlite_agg_ops} queries/sec)"

# PizzaSQL HTTP
start=$(date +%s%N)
for i in $(seq 1 $NUM_QUERIES); do
    curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM users GROUP BY age"}' > /dev/null
done
end=$(date +%s%N)
http_agg_ms=$(( (end - start) / 1000000 ))
http_agg_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $http_agg_ms" | bc)
echo "  PizzaSQL (HTTP):     ${http_agg_ms}ms total (${http_agg_ops} queries/sec)"

# PizzaSQL PostgreSQL (using single connection)
start=$(date +%s%N)
{
    for i in $(seq 1 $NUM_QUERIES); do
        echo "SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM users_pg GROUP BY age;"
    done
} | PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -q > /dev/null 2>&1
end=$(date +%s%N)
pg_agg_ms=$(( (end - start) / 1000000 ))
pg_agg_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $pg_agg_ms" | bc)
echo "  PizzaSQL (Postgres): ${pg_agg_ms}ms total (${pg_agg_ops} queries/sec)"

# PostgreSQL
start=$(date +%s%N)
{
    for i in $(seq 1 $NUM_QUERIES); do
        echo "SELECT age, COUNT(*) as count, AVG(id) as avg_id FROM users_bench GROUP BY age;"
    done
} | PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -q > /dev/null 2>&1
end=$(date +%s%N)
postgres_agg_ms=$(( (end - start) / 1000000 ))
postgres_agg_ops=$(echo "scale=0; $NUM_QUERIES * 1000 / $postgres_agg_ms" | bc)
echo "  PostgreSQL:          ${postgres_agg_ms}ms total (${postgres_agg_ops} queries/sec)"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

printf "%-25s %-20s %-20s %-20s %-20s\n" "Operation" "SQLite" "PizzaSQL (HTTP)" "PizzaSQL (PG Wire)" "PostgreSQL"
printf "%-25s %-20s %-20s %-20s %-20s\n" "-------------------------" "--------------------" "--------------------" "--------------------" "--------------------"
printf "%-25s %-20s %-20s %-20s %-20s\n" "INSERT ($NUM_ROWS rows)" "${sqlite_insert_ops} ops/s" "${http_insert_ops} ops/s" "${pg_insert_ops} ops/s" "${postgres_insert_ops} ops/s"
printf "%-25s %-20s %-20s %-20s %-20s\n" "SELECT (no index)" "${sqlite_select_ops} q/s" "${http_select_ops} q/s" "${pg_select_ops} q/s" "${postgres_select_ops} q/s"
printf "%-25s %-20s %-20s %-20s %-20s\n" "CREATE INDEX" "${sqlite_index_ms} ms" "${http_index_ms} ms" "${pg_index_ms} ms" "${postgres_index_ms} ms"
printf "%-25s %-20s %-20s %-20s %-20s\n" "SELECT (indexed)" "${sqlite_indexed_ops} q/s" "${http_indexed_ops} q/s" "${pg_indexed_ops} q/s" "${postgres_indexed_ops} q/s"
printf "%-25s %-20s %-20s %-20s %-20s\n" "AGGREGATE" "${sqlite_agg_ops} q/s" "${http_agg_ops} q/s" "${pg_agg_ops} q/s" "${postgres_agg_ops} q/s"

echo ""
echo -e "${GREEN}Benchmarks completed!${NC}"

# Cleanup
echo ""
echo -e "${YELLOW}Cleaning up...${NC}"
rm -f $SQLITE_DB
curl -s -X POST "$PIZZASQL_HTTP_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"DROP TABLE IF EXISTS users"}' > /dev/null 2>&1 || true
PGPASSWORD="" psql -h $PIZZASQL_PG_HOST -p $PIZZASQL_PG_PORT -U postgres -d pizzasql -c "DROP TABLE IF EXISTS users_pg" > /dev/null 2>&1 || true
PGPASSWORD="" psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "DROP TABLE IF EXISTS users_bench" > /dev/null 2>&1 || true
echo "✓ Cleanup complete"
