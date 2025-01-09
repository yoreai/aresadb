//! AresaDB CLI Entry Point
//!
//! High-performance multi-model database engine with SQL queries.

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cli;
mod distributed;
mod output;
mod query;
mod rag;
mod schema;
mod storage;

use cli::commands::OutputFormat;
use cli::repl::Repl;

/// Database format version for compatibility checking
pub const FORMAT_VERSION: u32 = 1;

/// AresaDB - High-Performance Multi-Model Database Engine
///
/// A blazing-fast database that supports Key/Value, Graph, and Relational
/// models with SQL queries and a unified property graph architecture.
#[derive(Parser)]
#[command(name = "aresadb")]
#[command(author = "Yevheniy Chuba <yevheniyc@gmail.com>")]
#[command(version)]
#[command(about = "High-performance multi-model database - KV, Graph, and Relational in one", long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Natural language query (if no subcommand provided)
    #[arg(trailing_var_arg = true)]
    query: Vec<String>,

    /// Database path (defaults to current directory)
    #[arg(short, long, global = true)]
    database: Option<String>,

    /// Output format
    #[arg(short, long, default_value = "table", global = true)]
    format: OutputFormat,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Limit number of results
    #[arg(short, long, global = true)]
    limit: Option<usize>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new database
    Init {
        /// Path to create the database
        path: String,
        /// Database name
        #[arg(short, long)]
        name: Option<String>,
    },

    /// Start interactive REPL mode
    Repl,

    /// Execute a SQL query
    Query {
        /// SQL query string
        sql: String,
    },

    /// Schema management commands
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },

    /// View data in different modes
    View {
        /// Table/schema name to view
        name: String,
        /// View mode: table, graph, or kv
        #[arg(long, default_value = "table")]
        r#as: ViewMode,
        /// Number of rows to show
        #[arg(short, long)]
        limit: Option<usize>,
    },

    /// Graph traversal from a node
    Traverse {
        /// Starting node (e.g., "users/1")
        node: String,
        /// Maximum traversal depth
        #[arg(short, long, default_value = "2")]
        depth: u32,
        /// Edge types to follow (comma-separated)
        #[arg(short, long)]
        edges: Option<String>,
    },

    /// Push database to cloud storage
    Push {
        /// Cloud storage URL (s3://... or gs://...)
        url: String,
    },

    /// Connect to a remote database
    Connect {
        /// Cloud storage URL
        url: String,
        /// Open in read-only mode
        #[arg(long)]
        readonly: bool,
    },

    /// Sync local database with remote
    Sync {
        /// Cloud storage URL
        url: String,
    },

    /// Configuration commands
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Show database status
    Status,

    /// Insert a node
    Insert {
        /// Node type (table name)
        node_type: String,
        /// Properties as JSON
        #[arg(short, long)]
        props: String,
    },

    /// Get a node by ID
    Get {
        /// Node ID
        id: String,
    },

    /// Delete a node
    Delete {
        /// Node ID
        id: String,
    },

    /// Vector similarity search (RAG)
    Search {
        /// Node type to search in
        node_type: String,
        /// Query vector as JSON array [0.1, 0.2, ...]
        #[arg(short, long)]
        vector: String,
        /// Field containing embeddings (default: "embedding")
        #[arg(short, long, default_value = "embedding")]
        field: String,
        /// Number of results to return
        #[arg(short, long, default_value = "10")]
        k: usize,
        /// Distance metric: cosine, euclidean, dot, manhattan
        #[arg(short, long, default_value = "cosine")]
        metric: String,
    },

    /// Insert a node with vector embedding
    Embed {
        /// Node type
        node_type: String,
        /// Properties as JSON (including content to embed)
        #[arg(short, long)]
        props: String,
        /// Vector embedding as JSON array [0.1, 0.2, ...]
        #[arg(short, long)]
        vector: String,
        /// Field name for embedding (default: "embedding")
        #[arg(short, long, default_value = "embedding")]
        field: String,
    },

    /// Chunk a document for RAG (split into embeddable pieces)
    Chunk {
        /// Text content to chunk (or use --file)
        #[arg(short, long)]
        text: Option<String>,
        /// File path to read content from
        #[arg(short = 'F', long)]
        file: Option<String>,
        /// Document ID (for tracking chunks)
        #[arg(short, long, default_value = "doc")]
        document_id: String,
        /// Chunking strategy: fixed, sentence, paragraph, semantic
        #[arg(short, long, default_value = "fixed")]
        strategy: String,
        /// Chunk size (chars for fixed, tokens for sentence)
        #[arg(short = 'S', long, default_value = "512")]
        size: usize,
        /// Overlap between chunks (for fixed strategy)
        #[arg(short, long, default_value = "50")]
        overlap: usize,
        /// Store chunks in database (requires --props for base properties)
        #[arg(long)]
        store: bool,
        /// Base properties for stored chunks (JSON)
        #[arg(long)]
        props: Option<String>,
    },

    /// Retrieve context for RAG query
    Context {
        /// Query text
        query: String,
        /// Query vector as JSON array
        #[arg(short, long)]
        vector: String,
        /// Node type to search
        #[arg(short, long, default_value = "chunk")]
        node_type: String,
        /// Embedding field name
        #[arg(short, long, default_value = "embedding")]
        field: String,
        /// Maximum tokens to retrieve
        #[arg(short = 'M', long, default_value = "4096")]
        max_tokens: usize,
        /// Minimum similarity score
        #[arg(short = 's', long, default_value = "0.0")]
        min_score: f64,
        /// Output format: llm, json, text
        #[arg(short, long, default_value = "llm")]
        output: String,
    },

    /// Ingest document: chunk + embed + store in one step
    Ingest {
        /// Text content to ingest (or use --file)
        #[arg(short, long)]
        text: Option<String>,
        /// File path to read content from
        #[arg(short = 'F', long)]
        file: Option<String>,
        /// Document ID for tracking
        #[arg(short, long, default_value = "doc")]
        document_id: String,
        /// Embedding provider: openai, local
        #[arg(short, long, default_value = "local")]
        provider: String,
        /// OpenAI API key (or set OPENAI_API_KEY)
        #[arg(long)]
        api_key: Option<String>,
        /// Chunk size
        #[arg(short = 'S', long, default_value = "512")]
        chunk_size: usize,
        /// Chunk overlap
        #[arg(short = 'O', long, default_value = "50")]
        overlap: usize,
        /// Additional properties (JSON)
        #[arg(long)]
        props: Option<String>,
    },
}

#[derive(Subcommand)]
enum SchemaAction {
    /// Create a new schema/table
    Create {
        /// Schema name
        name: String,
        /// Field definitions (e.g., "name:string, age:int")
        #[arg(short, long)]
        fields: String,
    },
    /// Create a relationship between schemas
    Link {
        /// Source schema
        from: String,
        /// Target schema
        to: String,
        /// Relation type: has_one, has_many, belongs_to
        #[arg(short, long)]
        relation: String,
        /// Alias for the relationship
        #[arg(long)]
        r#as: Option<String>,
    },
    /// List all schemas
    List,
    /// Show schema details
    Show {
        /// Schema name
        name: String,
    },
    /// Drop a schema
    Drop {
        /// Schema name
        name: String,
        /// Force drop even if data exists
        #[arg(long)]
        force: bool,
    },
    /// Run pending migrations
    Migrate,
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },
    /// List all configuration
    List,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
enum ViewMode {
    #[default]
    Table,
    Graph,
    Kv,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "aresadb=info".into()),
        ))
        .with(tracing_subscriber::fmt::layer().without_time())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Init { path, name }) => {
            handle_init(&path, name.as_deref()).await?;
        }
        Some(Commands::Repl) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            let mut repl = Repl::new(db_path).await?;
            repl.run().await?;
        }
        Some(Commands::Query { sql }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_query(db_path, &sql, cli.format, cli.limit).await?;
        }
        Some(Commands::Schema { action }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_schema(db_path, action).await?;
        }
        Some(Commands::View { name, r#as, limit }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_view(db_path, &name, r#as, limit.or(cli.limit), cli.format).await?;
        }
        Some(Commands::Traverse { node, depth, edges }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_traverse(db_path, &node, depth, edges.as_deref(), cli.format).await?;
        }
        Some(Commands::Push { url }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_push(db_path, &url).await?;
        }
        Some(Commands::Connect { url, readonly }) => {
            handle_connect(&url, readonly).await?;
        }
        Some(Commands::Sync { url }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_sync(db_path, &url).await?;
        }
        Some(Commands::Config { action }) => {
            handle_config(action).await?;
        }
        Some(Commands::Status) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_status(db_path).await?;
        }
        Some(Commands::Insert { node_type, props }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_insert(db_path, &node_type, &props, cli.format).await?;
        }
        Some(Commands::Get { id }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_get(db_path, &id, cli.format).await?;
        }
        Some(Commands::Delete { id }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_delete(db_path, &id).await?;
        }
        Some(Commands::Search {
            node_type,
            vector,
            field,
            k,
            metric,
        }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_vector_search(db_path, &node_type, &vector, &field, k, &metric, cli.format)
                .await?;
        }
        Some(Commands::Embed {
            node_type,
            props,
            vector,
            field,
        }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_embed(db_path, &node_type, &props, &vector, &field, cli.format).await?;
        }
        Some(Commands::Chunk {
            text,
            file,
            document_id,
            strategy,
            size,
            overlap,
            store,
            props,
        }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_chunk(
                db_path,
                text.as_deref(),
                file.as_deref(),
                &document_id,
                &strategy,
                size,
                overlap,
                store,
                props.as_deref(),
                cli.format,
            )
            .await?;
        }
        Some(Commands::Context {
            query,
            vector,
            node_type,
            field,
            max_tokens,
            min_score,
            output,
        }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_context(
                db_path, &query, &vector, &node_type, &field, max_tokens, min_score, &output,
                cli.format,
            )
            .await?;
        }
        Some(Commands::Ingest {
            text,
            file,
            document_id,
            provider,
            api_key,
            chunk_size,
            overlap,
            props,
        }) => {
            let db_path = cli.database.as_deref().unwrap_or(".");
            handle_ingest(
                db_path,
                text.as_deref(),
                file.as_deref(),
                &document_id,
                &provider,
                api_key.as_deref(),
                chunk_size,
                overlap,
                props.as_deref(),
                cli.format,
            )
            .await?;
        }
        None => {
            if cli.query.is_empty() {
                print_welcome();
            } else {
                let query_text = cli.query.join(" ");
                let db_path = cli.database.as_deref().unwrap_or(".");
                handle_natural_language(db_path, &query_text, cli.format, cli.limit).await?;
            }
        }
    }

    Ok(())
}

fn print_welcome() {
    println!();
    println!(
        "{}",
        "╭───────────────────────────────────────────────────────────────╮".bright_cyan()
    );
    println!(
        "{}",
        "│                                                               │".bright_cyan()
    );
    println!(
        "│  {}  │",
        " █████╗ ██████╗ ███████╗███████╗ █████╗ ██████╗ ██████╗  "
            .bright_cyan()
            .bold()
    );
    println!(
        "│  {}  │",
        "██╔══██╗██╔══██╗██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗ "
            .bright_cyan()
            .bold()
    );
    println!(
        "│  {}  │",
        "███████║██████╔╝█████╗  ███████╗███████║██║  ██║██████╔╝ "
            .bright_cyan()
            .bold()
    );
    println!(
        "│  {}  │",
        "██╔══██║██╔══██╗██╔══╝  ╚════██║██╔══██║██║  ██║██╔══██╗ "
            .bright_cyan()
            .bold()
    );
    println!(
        "│  {}  │",
        "██║  ██║██║  ██║███████╗███████║██║  ██║██████╔╝██████╔╝ "
            .bright_cyan()
            .bold()
    );
    println!(
        "│  {}  │",
        "╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═════╝ ╚═════╝  "
            .bright_cyan()
            .bold()
    );
    println!(
        "{}",
        "│                                                               │".bright_cyan()
    );
    println!(
        "│     {}      │",
        "High-Performance Multi-Model Database".white().bold()
    );
    println!(
        "│         {}          │",
        "KV • Graph • Relational".bright_yellow()
    );
    println!(
        "{}",
        "│                                                               │".bright_cyan()
    );
    println!(
        "{}",
        "╰───────────────────────────────────────────────────────────────╯".bright_cyan()
    );
    println!();
    println!("{}", "Usage:".bright_yellow().bold());
    println!(
        "  {} {}",
        "aresadb init ./mydata".bright_green(),
        "               # Create new database".white()
    );
    println!(
        "  {} {}",
        "aresadb query".bright_green(),
        "\"SELECT * FROM users\" # SQL query".white()
    );
    println!(
        "  {} {}",
        "aresadb repl".bright_green(),
        "                       # Interactive mode".white()
    );
    println!(
        "  {} {}",
        "aresadb status".bright_green(),
        "                     # Database status".white()
    );
    println!();
    println!("{}", "Quick Start:".bright_yellow().bold());
    println!(
        "  1. Initialize:    {}",
        "aresadb init ./mydb --name myapp".bright_green()
    );
    println!(
        "  2. Create schema: {}",
        "aresadb schema create users --fields \"name:string, age:int\"".bright_green()
    );
    println!(
        "  3. Insert data:   {}",
        "aresadb insert users --props '{\"name\": \"John\", \"age\": 30}'".bright_green()
    );
    println!(
        "  4. Query data:    {}",
        "aresadb query \"SELECT * FROM users\"".bright_green()
    );
    println!();
    println!("Run {} for more options.", "aresadb --help".bright_green());
    println!();
}

async fn handle_init(path: &str, name: Option<&str>) -> Result<()> {
    use storage::Database;

    let db_name = name.unwrap_or_else(|| {
        std::path::Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("aresadb")
    });

    println!(
        "{} Initializing database '{}' at {}...",
        "●".bright_blue(),
        db_name.bright_yellow(),
        path.bright_cyan()
    );

    Database::create(path, db_name).await?;

    println!(
        "{} Database initialized successfully!",
        "✓".bright_green().bold()
    );
    println!();
    println!("{}", "Next steps:".bright_yellow().bold());
    println!(
        "  {}",
        format!(
            "cd {} && aresadb schema create users --fields \"name:string, email:string\"",
            path
        )
        .bright_green()
    );

    Ok(())
}

async fn handle_query(
    db_path: &str,
    sql: &str,
    format: OutputFormat,
    limit: Option<usize>,
) -> Result<()> {
    use output::Renderer;
    use query::QueryEngine;
    use storage::Database;

    let db = Database::open(db_path).await?;
    let engine = QueryEngine::new(db);
    let results = engine.execute_sql(sql, limit).await?;

    let renderer = Renderer::new(format);
    renderer.render_results(&results)?;

    Ok(())
}

async fn handle_schema(db_path: &str, action: SchemaAction) -> Result<()> {
    use output::Renderer;
    use schema::SchemaManager;
    use storage::Database;

    let db = Database::open(db_path).await?;
    let manager = SchemaManager::new(db);

    match action {
        SchemaAction::Create { name, fields } => {
            manager.create_schema(&name, &fields).await?;
            println!(
                "{} Created schema '{}'",
                "✓".bright_green().bold(),
                name.bright_yellow()
            );
        }
        SchemaAction::Link {
            from,
            to,
            relation,
            r#as,
        } => {
            manager
                .create_relationship(&from, &to, &relation, r#as.as_deref())
                .await?;
            println!(
                "{} Created relationship {} -> {}",
                "✓".bright_green().bold(),
                from.bright_cyan(),
                to.bright_cyan()
            );
        }
        SchemaAction::List => {
            let schemas = manager.list_schemas().await?;
            let renderer = Renderer::new(OutputFormat::Table);
            renderer.render_schemas(&schemas)?;
        }
        SchemaAction::Show { name } => {
            let schema = manager.get_schema(&name).await?;
            let renderer = Renderer::new(OutputFormat::Table);
            renderer.render_schema_details(&schema)?;
        }
        SchemaAction::Drop { name, force } => {
            manager.drop_schema(&name, force).await?;
            println!(
                "{} Dropped schema '{}'",
                "✓".bright_green().bold(),
                name.bright_yellow()
            );
        }
        SchemaAction::Migrate => {
            let migrations = manager.run_migrations().await?;
            println!(
                "{} Applied {} migrations",
                "✓".bright_green().bold(),
                migrations.len()
            );
        }
    }

    Ok(())
}

async fn handle_view(
    db_path: &str,
    name: &str,
    mode: ViewMode,
    limit: Option<usize>,
    format: OutputFormat,
) -> Result<()> {
    use output::Renderer;
    use storage::Database;

    let db = Database::open(db_path).await?;
    let renderer = Renderer::new(format);

    match mode {
        ViewMode::Table => {
            let rows = db.get_all_by_type(name, limit).await?;
            renderer.render_as_table(&rows)?;
        }
        ViewMode::Graph => {
            let graph = db.get_as_graph(name, limit).await?;
            renderer.render_as_graph(&graph)?;
        }
        ViewMode::Kv => {
            let kvs = db.get_as_kv(name, limit).await?;
            renderer.render_as_kv(&kvs)?;
        }
    }

    Ok(())
}

async fn handle_traverse(
    db_path: &str,
    node_id: &str,
    depth: u32,
    edges: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    use output::Renderer;
    use query::QueryEngine;
    use storage::Database;

    let db = Database::open(db_path).await?;
    let engine = QueryEngine::new(db);

    let edge_types: Option<Vec<&str>> = edges.map(|e| e.split(',').collect());
    let results = engine.traverse(node_id, depth, edge_types).await?;

    let renderer = Renderer::new(format);
    renderer.render_traversal(&results)?;

    Ok(())
}

async fn handle_push(db_path: &str, url: &str) -> Result<()> {
    use storage::Database;

    println!(
        "{} Pushing database to {}...",
        "●".bright_blue(),
        url.bright_cyan()
    );

    let db = Database::open(db_path).await?;
    db.push_to_bucket(url).await?;

    println!(
        "{} Database pushed successfully!",
        "✓".bright_green().bold()
    );

    Ok(())
}

async fn handle_connect(url: &str, readonly: bool) -> Result<()> {
    use storage::Database;

    println!(
        "{} Connecting to {}...",
        "●".bright_blue(),
        url.bright_cyan()
    );

    let _db = Database::connect_bucket(url, readonly).await?;

    println!(
        "{} Connected! Use {} to start querying.",
        "✓".bright_green().bold(),
        "aresadb repl".bright_green()
    );

    Ok(())
}

async fn handle_sync(db_path: &str, url: &str) -> Result<()> {
    use storage::Database;

    println!(
        "{} Syncing with {}...",
        "●".bright_blue(),
        url.bright_cyan()
    );

    let db = Database::open(db_path).await?;
    let stats = db.sync_with_bucket(url).await?;

    println!(
        "{} Synced: {} uploaded, {} downloaded",
        "✓".bright_green().bold(),
        stats.uploaded,
        stats.downloaded
    );

    Ok(())
}

async fn handle_config(action: ConfigAction) -> Result<()> {
    use cli::config::Config;

    let config = Config::load()?;

    match action {
        ConfigAction::Set { key, value } => {
            config.set(&key, &value)?;
            println!(
                "{} Set {} = {}",
                "✓".bright_green().bold(),
                key.bright_cyan(),
                value.bright_yellow()
            );
        }
        ConfigAction::Get { key } => {
            if let Some(value) = config.get(&key) {
                println!("{}: {}", key.bright_cyan(), value.bright_yellow());
            } else {
                println!("{} Key not found: {}", "!".bright_red(), key);
            }
        }
        ConfigAction::List => {
            config.print_all()?;
        }
    }

    Ok(())
}

async fn handle_status(db_path: &str) -> Result<()> {
    use storage::Database;

    let db = Database::open(db_path).await?;
    let status = db.status().await?;

    println!("{}", "Database Status".bright_yellow().bold());
    println!("─────────────────────────────────────");
    println!("  {} {}", "Name:".bright_cyan(), status.name);
    println!("  {} {}", "Path:".bright_cyan(), status.path);
    println!("  {} {}", "Nodes:".bright_cyan(), status.node_count);
    println!("  {} {}", "Edges:".bright_cyan(), status.edge_count);
    println!("  {} {}", "Schemas:".bright_cyan(), status.schema_count);
    println!(
        "  {} {}",
        "Size:".bright_cyan(),
        humansize::format_size(status.size_bytes, humansize::BINARY)
    );

    Ok(())
}

async fn handle_insert(
    db_path: &str,
    node_type: &str,
    props_json: &str,
    format: OutputFormat,
) -> Result<()> {
    use output::Renderer;
    use storage::Database;

    let db = Database::open(db_path).await?;
    let props: serde_json::Value = serde_json::from_str(props_json)?;

    let node = db.insert_node(node_type, props).await?;

    let renderer = Renderer::new(format);
    renderer.render_node(&node)?;

    println!(
        "{} Inserted node {}",
        "✓".bright_green().bold(),
        node.id.to_string().bright_yellow()
    );

    Ok(())
}

async fn handle_get(db_path: &str, id: &str, format: OutputFormat) -> Result<()> {
    use output::Renderer;
    use storage::Database;

    let db = Database::open(db_path).await?;

    if let Some(node) = db.get_node(id).await? {
        let renderer = Renderer::new(format);
        renderer.render_node(&node)?;
    } else {
        println!("{} Node not found: {}", "!".bright_red(), id);
    }

    Ok(())
}

async fn handle_delete(db_path: &str, id: &str) -> Result<()> {
    use storage::Database;

    let db = Database::open(db_path).await?;
    db.delete_node(id).await?;

    println!(
        "{} Deleted node {}",
        "✓".bright_green().bold(),
        id.bright_yellow()
    );

    Ok(())
}

async fn handle_natural_language(
    db_path: &str,
    query: &str,
    format: OutputFormat,
    limit: Option<usize>,
) -> Result<()> {
    use output::Renderer;
    use query::QueryEngine;
    use storage::Database;

    // Try to parse as SQL directly
    let db = Database::open(db_path).await?;
    let engine = QueryEngine::new(db);
    let results = engine.execute_sql(query, limit).await?;

    let renderer = Renderer::new(format);
    renderer.render_results(&results)?;

    Ok(())
}

async fn handle_vector_search(
    db_path: &str,
    node_type: &str,
    vector_json: &str,
    field: &str,
    k: usize,
    metric_str: &str,
    format: OutputFormat,
) -> Result<()> {
    use output::Renderer;
    use storage::{Database, DistanceMetric};

    let db = Database::open(db_path).await?;

    // Parse the query vector
    let query_vector: Vec<f32> = serde_json::from_str(vector_json).map_err(|e| {
        anyhow::anyhow!(
            "Invalid vector JSON: {}. Expected format: [0.1, 0.2, ...]",
            e
        )
    })?;

    // Parse distance metric
    let metric = match metric_str.to_lowercase().as_str() {
        "cosine" => DistanceMetric::Cosine,
        "euclidean" | "l2" => DistanceMetric::Euclidean,
        "dot" | "dotproduct" | "inner" => DistanceMetric::DotProduct,
        "manhattan" | "l1" => DistanceMetric::Manhattan,
        _ => {
            println!(
                "{} Unknown metric '{}', using cosine",
                "!".bright_yellow(),
                metric_str
            );
            DistanceMetric::Cosine
        }
    };

    println!(
        "{} Searching {} nodes by {} similarity...",
        "●".bright_blue(),
        node_type.bright_cyan(),
        metric_str.bright_yellow()
    );

    let results = db
        .similarity_search(&query_vector, node_type, field, k, metric)
        .await?;

    if results.is_empty() {
        println!(
            "{} No results found. Make sure nodes have '{}' field with vector embeddings.",
            "!".bright_yellow(),
            field
        );
    } else {
        println!(
            "{} Found {} similar nodes:",
            "✓".bright_green().bold(),
            results.len()
        );
        println!();

        let renderer = Renderer::new(format);
        renderer.render_similarity_results(&results, &db).await?;
    }

    Ok(())
}

async fn handle_embed(
    db_path: &str,
    node_type: &str,
    props_json: &str,
    vector_json: &str,
    field: &str,
    format: OutputFormat,
) -> Result<()> {
    use output::Renderer;
    use storage::Database;

    let db = Database::open(db_path).await?;

    // Parse properties
    let props: serde_json::Value = serde_json::from_str(props_json)?;

    // Parse vector
    let vector: Vec<f32> = serde_json::from_str(vector_json).map_err(|e| {
        anyhow::anyhow!(
            "Invalid vector JSON: {}. Expected format: [0.1, 0.2, ...]",
            e
        )
    })?;

    let node = db
        .insert_with_embedding(node_type, props, field, vector)
        .await?;

    let renderer = Renderer::new(format);
    renderer.render_node(&node)?;

    println!(
        "{} Inserted node {} with {}-dimensional embedding",
        "✓".bright_green().bold(),
        node.id.to_string().bright_yellow(),
        node.properties
            .get(field)
            .and_then(|v| v.vector_dimension())
            .unwrap_or(0)
    );

    Ok(())
}

/// Handle chunk command - split document for RAG
#[allow(clippy::too_many_arguments)]
async fn handle_chunk(
    db_path: &str,
    text: Option<&str>,
    file_path: Option<&str>,
    document_id: &str,
    strategy: &str,
    size: usize,
    overlap: usize,
    store: bool,
    props_json: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    use storage::Database;

    // Get content from text or file
    let content = if let Some(text) = text {
        text.to_string()
    } else if let Some(path) = file_path {
        std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read file {}: {}", path, e))?
    } else {
        anyhow::bail!("Must provide --text or --file");
    };

    // Create chunker with strategy
    let chunk_strategy = match strategy.to_lowercase().as_str() {
        "fixed" => rag::ChunkStrategy::FixedSize {
            chunk_size: size,
            overlap,
        },
        "sentence" => rag::ChunkStrategy::Sentence { max_tokens: size },
        "paragraph" => rag::ChunkStrategy::Paragraph { max_size: size },
        "semantic" => rag::ChunkStrategy::Semantic { max_size: size },
        _ => anyhow::bail!(
            "Unknown strategy: {}. Use: fixed, sentence, paragraph, semantic",
            strategy
        ),
    };

    let chunker = rag::Chunker::new(chunk_strategy);
    let chunks = chunker.chunk(document_id, &content);

    println!(
        "{} Split document into {} chunks using {} strategy",
        "●".bright_blue(),
        chunks.len().to_string().bright_yellow(),
        strategy.bright_cyan()
    );

    // Display chunks
    match format {
        OutputFormat::Json => {
            let json: Vec<serde_json::Value> = chunks
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "id": c.id,
                        "document_id": c.document_id,
                        "chunk_index": c.chunk_index,
                        "total_chunks": c.total_chunks,
                        "content": c.content,
                        "start_offset": c.start_offset,
                        "end_offset": c.end_offset,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            for chunk in &chunks {
                println!();
                println!(
                    "{} Chunk {}/{} (chars: {}-{})",
                    "─".repeat(40),
                    chunk.chunk_index + 1,
                    chunk.total_chunks,
                    chunk.start_offset,
                    chunk.end_offset
                );
                // Truncate display if too long
                let preview = if chunk.content.len() > 200 {
                    format!("{}...", &chunk.content[..200])
                } else {
                    chunk.content.clone()
                };
                println!("{}", preview.bright_white());
            }
        }
    }

    // Store chunks if requested
    if store {
        let db = Database::open(db_path).await?;
        let base_props: serde_json::Value = if let Some(json) = props_json {
            serde_json::from_str(json)?
        } else {
            serde_json::json!({})
        };

        for chunk in &chunks {
            let mut props = base_props.clone();
            if let Some(obj) = props.as_object_mut() {
                obj.insert("content".to_string(), serde_json::json!(chunk.content));
                obj.insert(
                    "document_id".to_string(),
                    serde_json::json!(chunk.document_id),
                );
                obj.insert(
                    "chunk_index".to_string(),
                    serde_json::json!(chunk.chunk_index),
                );
                obj.insert(
                    "total_chunks".to_string(),
                    serde_json::json!(chunk.total_chunks),
                );
                obj.insert(
                    "start_offset".to_string(),
                    serde_json::json!(chunk.start_offset),
                );
                obj.insert(
                    "end_offset".to_string(),
                    serde_json::json!(chunk.end_offset),
                );
            }
            db.insert_node("chunk", props).await?;
        }

        println!(
            "{} Stored {} chunks in database",
            "✓".bright_green().bold(),
            chunks.len()
        );
    }

    Ok(())
}

/// Handle context retrieval for RAG
#[allow(clippy::too_many_arguments)]
async fn handle_context(
    db_path: &str,
    query_text: &str,
    vector_json: &str,
    node_type: &str,
    field: &str,
    max_tokens: usize,
    min_score: f64,
    output_format: &str,
    _format: OutputFormat,
) -> Result<()> {
    use storage::Database;

    println!(
        "{} Retrieving context for: \"{}\"",
        "●".bright_blue(),
        query_text.bright_cyan()
    );

    let db = Database::open(db_path).await?;

    // Parse query vector
    let query_vector: Vec<f32> = serde_json::from_str(vector_json).map_err(|e| {
        anyhow::anyhow!(
            "Invalid vector JSON: {}. Expected format: [0.1, 0.2, ...]",
            e
        )
    })?;

    // Create context retriever
    let retriever = rag::ContextRetriever::new(&db)
        .node_type(node_type)
        .embedding_field(field)
        .content_field("content")
        .max_tokens(max_tokens)
        .min_score(min_score);

    let context = retriever.retrieve(&query_vector, query_text).await?;

    println!(
        "{} Found {} chunks ({} estimated tokens)",
        "✓".bright_green().bold(),
        context.chunks.len(),
        context.estimated_tokens
    );

    // Output in requested format
    match output_format.to_lowercase().as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&context.to_json())?);
        }
        "text" => {
            println!("\n{}", context.text_only());
        }
        _ => {
            println!("\n{}", context.format_for_llm());
        }
    }

    Ok(())
}

/// Handle ingest command - chunk + embed + store
#[allow(clippy::too_many_arguments)]
async fn handle_ingest(
    db_path: &str,
    text: Option<&str>,
    file_path: Option<&str>,
    document_id: &str,
    provider_name: &str,
    api_key: Option<&str>,
    chunk_size: usize,
    overlap: usize,
    props_json: Option<&str>,
    _format: OutputFormat,
) -> Result<()> {
    use storage::Database;

    // Get content
    let content = if let Some(text) = text {
        text.to_string()
    } else if let Some(path) = file_path {
        std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read file {}: {}", path, e))?
    } else {
        anyhow::bail!("Must provide --text or --file");
    };

    println!(
        "{} Ingesting document '{}' ({} chars)",
        "●".bright_blue(),
        document_id.bright_yellow(),
        content.len()
    );

    // Create embedding manager
    let embedder = rag::EmbeddingManager::from_name(provider_name, api_key)?;
    println!(
        "  Provider: {} ({}D)",
        embedder.name().bright_cyan(),
        embedder.dimension()
    );

    // Chunk the document
    let chunker = rag::Chunker::new(rag::ChunkStrategy::FixedSize {
        chunk_size,
        overlap,
    });
    let chunks = chunker.chunk(document_id, &content);
    println!(
        "  Chunks: {} (size: {}, overlap: {})",
        chunks.len().to_string().bright_yellow(),
        chunk_size,
        overlap
    );

    // Open database
    let db = Database::open(db_path).await?;

    // Parse base properties
    let base_props: serde_json::Value = if let Some(json) = props_json {
        serde_json::from_str(json)?
    } else {
        serde_json::json!({})
    };

    // Process each chunk
    let mut inserted = 0;
    let start = std::time::Instant::now();

    for (i, chunk) in chunks.iter().enumerate() {
        // Generate embedding
        let embedding = embedder.embed(&chunk.content).await?;

        // Build properties
        let mut props = base_props.clone();
        if let Some(obj) = props.as_object_mut() {
            obj.insert("content".to_string(), serde_json::json!(chunk.content));
            obj.insert(
                "document_id".to_string(),
                serde_json::json!(chunk.document_id),
            );
            obj.insert(
                "chunk_index".to_string(),
                serde_json::json!(chunk.chunk_index),
            );
            obj.insert(
                "total_chunks".to_string(),
                serde_json::json!(chunk.total_chunks),
            );
        }

        // Insert with embedding
        db.insert_with_embedding("chunk", props, "embedding", embedding)
            .await?;
        inserted += 1;

        // Progress indicator
        if (i + 1) % 10 == 0 || i + 1 == chunks.len() {
            print!(
                "\r  Progress: {}/{} chunks embedded...",
                i + 1,
                chunks.len()
            );
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }

    let elapsed = start.elapsed();
    let rate = inserted as f64 / elapsed.as_secs_f64();

    println!();
    println!(
        "{} Ingested {} chunks in {:.2}s ({:.1} chunks/sec)",
        "✓".bright_green().bold(),
        inserted,
        elapsed.as_secs_f64(),
        rate
    );

    Ok(())
}
