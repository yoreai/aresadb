//! Schema Migrations
//!
//! Handles schema versioning and migrations.

#![allow(dead_code)]

use super::{Schema, SchemaField};
use serde::{Deserialize, Serialize};

/// A migration to apply to the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Migration ID
    pub id: String,
    /// Migration version
    pub version: u32,
    /// Description of the migration
    pub description: String,
    /// Actions to perform
    pub actions: Vec<MigrationAction>,
    /// Created timestamp
    pub created_at: i64,
    /// Whether migration was applied
    pub applied: bool,
    /// Applied timestamp
    pub applied_at: Option<i64>,
}

impl Migration {
    /// Create a new migration
    pub fn new(description: &str, actions: Vec<MigrationAction>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            version: 1,
            description: description.to_string(),
            actions,
            created_at: chrono::Utc::now().timestamp_millis(),
            applied: false,
            applied_at: None,
        }
    }

    /// Generate SQL for this migration
    pub fn to_sql(&self) -> String {
        self.actions
            .iter()
            .map(|action| action.to_sql())
            .collect::<Vec<_>>()
            .join(";\n")
    }
}

/// A single migration action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationAction {
    /// Create a new schema/table
    CreateSchema(Schema),

    /// Drop a schema/table
    DropSchema(String),

    /// Add a field to a schema
    AddField {
        /// Target schema name.
        schema: String,
        /// Field definition to add.
        field: SchemaField,
    },

    /// Remove a field from a schema
    RemoveField {
        /// Target schema name.
        schema: String,
        /// Name of the field to remove.
        field_name: String,
    },

    /// Modify a field's type or constraints
    ModifyField {
        /// Target schema name.
        schema: String,
        /// Original field definition.
        old_field: SchemaField,
        /// Updated field definition.
        new_field: SchemaField,
    },

    /// Rename a field
    RenameField {
        /// Target schema name.
        schema: String,
        /// Current field name.
        old_name: String,
        /// New field name.
        new_name: String,
    },

    /// Rename a schema
    RenameSchema {
        /// Current schema name.
        old_name: String,
        /// New schema name.
        new_name: String,
    },

    /// Add an index
    AddIndex {
        /// Target schema name.
        schema: String,
        /// Field to index.
        field: String,
        /// Whether the index enforces uniqueness.
        unique: bool,
    },

    /// Remove an index
    RemoveIndex {
        /// Target schema name.
        schema: String,
        /// Indexed field to remove.
        field: String,
    },

    /// Raw SQL for custom migrations
    RawSql(String),
}

impl MigrationAction {
    /// Generate SQL for this action
    pub fn to_sql(&self) -> String {
        match self {
            MigrationAction::CreateSchema(schema) => schema.to_sql(),

            MigrationAction::DropSchema(name) => {
                format!("DROP TABLE IF EXISTS {}", name)
            }

            MigrationAction::AddField { schema, field } => {
                let mut sql = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    schema,
                    field.name,
                    field.field_type.to_sql()
                );

                if !field.nullable {
                    sql.push_str(" NOT NULL");
                }

                if let Some(ref default) = field.default {
                    sql.push_str(&format!(" DEFAULT {}", default));
                }

                sql
            }

            MigrationAction::RemoveField { schema, field_name } => {
                format!("ALTER TABLE {} DROP COLUMN {}", schema, field_name)
            }

            MigrationAction::ModifyField {
                schema,
                old_field: _,
                new_field,
            } => {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                    schema,
                    new_field.name,
                    new_field.field_type.to_sql()
                )
            }

            MigrationAction::RenameField {
                schema,
                old_name,
                new_name,
            } => {
                format!(
                    "ALTER TABLE {} RENAME COLUMN {} TO {}",
                    schema, old_name, new_name
                )
            }

            MigrationAction::RenameSchema { old_name, new_name } => {
                format!("ALTER TABLE {} RENAME TO {}", old_name, new_name)
            }

            MigrationAction::AddIndex {
                schema,
                field,
                unique,
            } => {
                let index_type = if *unique { "UNIQUE INDEX" } else { "INDEX" };
                format!(
                    "CREATE {} {}_{}_idx ON {} ({})",
                    index_type, schema, field, schema, field
                )
            }

            MigrationAction::RemoveIndex { schema, field } => {
                format!("DROP INDEX IF EXISTS {}_{}_idx", schema, field)
            }

            MigrationAction::RawSql(sql) => sql.clone(),
        }
    }
}

/// Migration generator - compares schemas and generates migrations
pub struct MigrationGenerator;

impl MigrationGenerator {
    /// Generate migrations to transform old schema into new schema
    pub fn generate(old: &Schema, new: &Schema) -> Vec<MigrationAction> {
        let mut actions = Vec::new();

        // Check for renamed schema
        if old.name != new.name {
            actions.push(MigrationAction::RenameSchema {
                old_name: old.name.clone(),
                new_name: new.name.clone(),
            });
        }

        // Find added fields
        for new_field in &new.fields {
            if old.get_field(&new_field.name).is_none() {
                actions.push(MigrationAction::AddField {
                    schema: new.name.clone(),
                    field: new_field.clone(),
                });
            }
        }

        // Find removed fields
        for old_field in &old.fields {
            if new.get_field(&old_field.name).is_none() {
                actions.push(MigrationAction::RemoveField {
                    schema: new.name.clone(),
                    field_name: old_field.name.clone(),
                });
            }
        }

        // Find modified fields
        for new_field in &new.fields {
            if let Some(old_field) = old.get_field(&new_field.name) {
                if Self::field_changed(old_field, new_field) {
                    actions.push(MigrationAction::ModifyField {
                        schema: new.name.clone(),
                        old_field: old_field.clone(),
                        new_field: new_field.clone(),
                    });
                }

                // Check index changes
                if new_field.indexed && !old_field.indexed {
                    actions.push(MigrationAction::AddIndex {
                        schema: new.name.clone(),
                        field: new_field.name.clone(),
                        unique: new_field.unique,
                    });
                } else if !new_field.indexed && old_field.indexed {
                    actions.push(MigrationAction::RemoveIndex {
                        schema: new.name.clone(),
                        field: new_field.name.clone(),
                    });
                }
            }
        }

        actions
    }

    /// Check if a field has changed
    fn field_changed(old: &SchemaField, new: &SchemaField) -> bool {
        old.field_type != new.field_type
            || old.nullable != new.nullable
            || old.unique != new.unique
            || old.default != new.default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FieldType;

    #[test]
    fn test_migration_generation() {
        let old = Schema::new(
            "users",
            vec![
                SchemaField::new("name", FieldType::String),
                SchemaField::new("email", FieldType::String),
            ],
        );

        let new = Schema::new(
            "users",
            vec![
                SchemaField::new("name", FieldType::String),
                SchemaField::new("email", FieldType::String),
                SchemaField::new("age", FieldType::Int),
            ],
        );

        let actions = MigrationGenerator::generate(&old, &new);

        assert_eq!(actions.len(), 1);
        if let MigrationAction::AddField { field, .. } = &actions[0] {
            assert_eq!(field.name, "age");
        } else {
            panic!("Expected AddField action");
        }
    }

    #[test]
    fn test_migration_sql() {
        let action = MigrationAction::AddField {
            schema: "users".to_string(),
            field: SchemaField::new("age", FieldType::Int).nullable(false),
        };

        let sql = action.to_sql();
        assert!(sql.contains("ALTER TABLE users ADD COLUMN age BIGINT NOT NULL"));
    }
}
