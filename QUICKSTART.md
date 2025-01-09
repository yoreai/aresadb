# AresaDB Quick Start

Get up and running in 5 minutes.

---

## Install

```bash
git clone https://github.com/yoreai/aresadb.git
cd aresadb
cargo build --release
export PATH="$PATH:$(pwd)/target/release"
aresadb --version
```

Or with Docker:

```bash
docker build -t aresadb .
docker run -it -v $(pwd)/data:/data aresadb
```

---

## Your First Database

### 1. Initialize

```bash
aresadb init ./mydata --name "My First DB"
```

### 2. Insert Data

```bash
aresadb -d ./mydata insert user --props '{"name": "Alice", "email": "alice@example.com", "role": "admin"}'
aresadb -d ./mydata insert user --props '{"name": "Bob", "email": "bob@example.com", "role": "user"}'
```

### 3. Query

```bash
aresadb -d ./mydata query "SELECT * FROM user"
aresadb -d ./mydata query "SELECT * FROM user WHERE role = 'admin'"
```

### 4. Check Status

```bash
aresadb -d ./mydata status
```

---

## Output Formats

```bash
aresadb -d ./mydata query "SELECT * FROM user"                    # Table (default)
aresadb -d ./mydata -f json query "SELECT * FROM user"            # JSON
aresadb -d ./mydata -f csv query "SELECT * FROM user" > users.csv # CSV
```

---

## Interactive REPL

```bash
aresadb -d ./mydata repl
```

```
aresadb> SELECT * FROM user
aresadb> .status
aresadb> .help
aresadb> .exit
```

---

## Next Steps

- [README.md](README.md) — Full documentation
- [ARCHITECTURE.md](ARCHITECTURE.md) — Technical deep-dive
- [EXAMPLES.md](EXAMPLES.md) — Real-world use cases

```bash
aresadb --help
aresadb <command> --help
```

Questions? [Open an issue](https://github.com/yoreai/aresadb/issues).
