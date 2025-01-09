//! Schema Management
//!
//! Provides schema definitions, validation, and migrations.

#![allow(dead_code)]
#![allow(unused_imports)]

mod migration;
mod registry;

pub use migration::{Migration, MigrationAction, MigrationGenerator};
pub use registry::{FieldType, RelationType, Schema, SchemaField, SchemaRelation};

use crate::storage::Database;
use anyhow::Result;

/// Schema manager for database
pub struct SchemaManager {
    db: Database,
}

impl SchemaManager {
    /// Create a new schema manager
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Create a new schema
    pub async fn create_schema(&self, name: &str, fields_str: &str) -> Result<Schema> {
        let fields = Self::parse_fields(fields_str)?;
        let schema = Schema::new(name, fields);

        // Store schema in database metadata
        self.save_schema(&schema).await?;

        Ok(schema)
    }

    /// Create a relationship between schemas
    pub async fn create_relationship(
        &self,
        from: &str,
        to: &str,
        relation_type: &str,
        alias: Option<&str>,
    ) -> Result<SchemaRelation> {
        let rel_type = match relation_type.to_lowercase().as_str() {
            "has_one" | "hasone" => RelationType::HasOne,
            "has_many" | "hasmany" => RelationType::HasMany,
            "belongs_to" | "belongsto" => RelationType::BelongsTo,
            "many_to_many" | "manytomany" => RelationType::ManyToMany,
            _ => anyhow::bail!("Unknown relation type: {}", relation_type),
        };

        let relation = SchemaRelation {
            from_schema: from.to_string(),
            to_schema: to.to_string(),
            relation_type: rel_type,
            alias: alias.map(|s| s.to_string()),
            edge_type: format!("{}_{}", from, to),
        };

        self.save_relation(&relation).await?;

        Ok(relation)
    }

    /// List all schemas
    pub async fn list_schemas(&self) -> Result<Vec<Schema>> {
        self.load_schemas().await
    }

    /// Get a schema by name
    pub async fn get_schema(&self, name: &str) -> Result<Schema> {
        let schemas = self.load_schemas().await?;
        schemas
            .into_iter()
            .find(|s| s.name == name)
            .ok_or_else(|| anyhow::anyhow!("Schema not found: {}", name))
    }

    /// Drop a schema
    pub async fn drop_schema(&self, name: &str, force: bool) -> Result<()> {
        if !force {
            // Check if there are any nodes of this type
            let nodes = self.db.get_all_by_type(name, Some(1)).await?;
            if !nodes.is_empty() {
                anyhow::bail!("Schema '{}' has data. Use --force to drop anyway.", name);
            }
        }

        self.remove_schema(name).await?;
        Ok(())
    }

    /// Run pending migrations
    pub async fn run_migrations(&self) -> Result<Vec<Migration>> {
        let migrations = self.detect_migrations().await?;

        for migration in &migrations {
            self.apply_migration(migration).await?;
        }

        Ok(migrations)
    }

    /// Parse field definitions from string
    fn parse_fields(fields_str: &str) -> Result<Vec<SchemaField>> {
        let mut fields = Vec::new();

        for field_def in fields_str.split(',') {
            let field_def = field_def.trim();
            if field_def.is_empty() {
                continue;
            }

            let parts: Vec<&str> = field_def.split(':').collect();
            if parts.is_empty() {
                continue;
            }

            let name = parts[0].trim().to_string();
            let field_type = parts
                .get(1)
                .map(|t| FieldType::parse(t.trim()))
                .unwrap_or(FieldType::String);

            let mut field = SchemaField::new(&name, field_type);

            // Check for modifiers
            for part in parts.iter().skip(2) {
                match part.trim().to_lowercase().as_str() {
                    "unique" => field.unique = true,
                    "required" | "notnull" => field.nullable = false,
                    "indexed" | "index" => field.indexed = true,
                    _ => {}
                }
            }

            fields.push(field);
        }

        Ok(fields)
    }

    // Internal methods for schema persistence

    async fn save_schema(&self, schema: &Schema) -> Result<()> {
        let schema_json = serde_json::to_value(schema)?;
        let props = serde_json::json!({
            "name": schema.name,
            "schema_data": schema_json,
        });

        // Check if schema already exists
        let existing = self.db.get_all_by_type("__schema__", None).await?;
        for node in existing {
            if let Some(crate::storage::Value::String(n)) = node.properties.get("name") {
                if n == &schema.name {
                    // Update existing
                    self.db.update_node(&node.id.to_string(), props).await?;
                    return Ok(());
                }
            }
        }

        // Create new
        self.db.insert_node("__schema__", props).await?;
        Ok(())
    }

    async fn save_relation(&self, relation: &SchemaRelation) -> Result<()> {
        let relation_json = serde_json::to_value(relation)?;
        let props = serde_json::json!({
            "from": relation.from_schema,
            "to": relation.to_schema,
            "relation_data": relation_json,
        });

        self.db.insert_node("__relation__", props).await?;
        Ok(())
    }

    async fn load_schemas(&self) -> Result<Vec<Schema>> {
        let nodes = self.db.get_all_by_type("__schema__", None).await?;
        let mut schemas = Vec::new();

        for node in nodes {
            if let Some(crate::storage::Value::Object(data)) = node.properties.get("schema_data") {
                let json = crate::storage::Value::Object(data.clone()).to_json();
                if let Ok(schema) = serde_json::from_value::<Schema>(json) {
                    schemas.push(schema);
                }
            }
        }

        Ok(schemas)
    }

    async fn remove_schema(&self, name: &str) -> Result<()> {
        let nodes = self.db.get_all_by_type("__schema__", None).await?;

        for node in nodes {
            if let Some(crate::storage::Value::String(n)) = node.properties.get("name") {
                if n == name {
                    self.db.delete_node(&node.id.to_string()).await?;
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn detect_migrations(&self) -> Result<Vec<Migration>> {
        // Compare current schemas with stored schemas
        // For now, return empty - full implementation would diff schemas
        Ok(Vec::new())
    }

    async fn apply_migration(&self, _migration: &Migration) -> Result<()> {
        // Apply migration changes
        // Full implementation would execute migration actions
        Ok(())
    }
}
