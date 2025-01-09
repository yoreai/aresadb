#!/bin/bash
# AresaDB Benchmark Comparison Suite
# Runs benchmarks against SQLite, DuckDB, and Pandas

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║       AresaDB Benchmark Comparison Suite                   ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULTS_DIR/benchmark_$TIMESTAMP.json"

# Check dependencies
echo -e "${YELLOW}Checking dependencies...${NC}"

check_command() {
    if command -v "$1" &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} $1 found"
        return 0
    else
        echo -e "  ${RED}✗${NC} $1 not found"
        return 1
    fi
}

check_command "cargo" || { echo "Rust/Cargo required"; exit 1; }
check_command "python3" || { echo "Python3 required"; exit 1; }
check_command "sqlite3" || echo "SQLite3 not found (will skip)"
check_command "duckdb" || echo "DuckDB not found (will skip)"

echo ""

# Build AresaDB in release mode
echo -e "${YELLOW}Building AresaDB (release mode)...${NC}"
cd "$PROJECT_DIR"
cargo build --release --quiet
echo -e "  ${GREEN}✓${NC} Build complete"
echo ""

# Run AresaDB benchmarks
echo -e "${YELLOW}Running AresaDB benchmarks...${NC}"
cargo bench --quiet 2>/dev/null || echo "  Benchmarks skipped (criterion not configured)"
echo ""

# Run comparison scripts
echo -e "${YELLOW}Running comparison benchmarks...${NC}"

# SQLite comparison
if command -v sqlite3 &> /dev/null; then
    echo -e "  ${BLUE}→${NC} SQLite comparison..."
    python3 "$SCRIPT_DIR/compare_sqlite.py" >> "$RESULT_FILE" 2>&1 || echo "  SQLite comparison failed"
fi

# DuckDB comparison
if command -v duckdb &> /dev/null || python3 -c "import duckdb" 2>/dev/null; then
    echo -e "  ${BLUE}→${NC} DuckDB comparison..."
    python3 "$SCRIPT_DIR/compare_duckdb.py" >> "$RESULT_FILE" 2>&1 || echo "  DuckDB comparison failed"
fi

# Pandas comparison
if python3 -c "import pandas" 2>/dev/null; then
    echo -e "  ${BLUE}→${NC} Pandas comparison..."
    python3 "$SCRIPT_DIR/compare_pandas.py" >> "$RESULT_FILE" 2>&1 || echo "  Pandas comparison failed"
fi

echo ""
echo -e "${GREEN}Benchmarks complete!${NC}"
echo -e "Results saved to: $RESULT_FILE"
echo ""

# Print summary
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                        Summary                             ${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"

if [ -f "$RESULT_FILE" ]; then
    cat "$RESULT_FILE"
fi

