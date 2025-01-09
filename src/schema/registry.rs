//! Schema Registry
//!
//! Defines schema structures and types.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A schema definition (like a table schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name (node type)
    pub name: String,
    /// Field definitions
    pub fields: Vec<SchemaField>,
    /// Schema version for migrations
    pub version: u32,
    /// Created timestamp
    pub created_at: i64,
    /// Updated timestamp
    pub updated_at: i64,
}

impl Schema {
    /// Create a new schema
    pub fn new(name: &str, fields: Vec<SchemaField>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            name: name.to_string(),
            fields,
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    /// Get a field by name
    pub fn get_field(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get all indexed fields
    pub fn indexed_fields(&self) -> Vec<&SchemaField> {
        self.fields
            .iter()
            .filter(|f| f.indexed || f.unique)
            .collect()
    }

    /// Get all required fields
    pub fn required_fields(&self) -> Vec<&SchemaField> {
        self.fields.iter().filter(|f| !f.nullable).collect()
    }

    /// Validate a node's properties against this schema
    pub fn validate(
        &self,
        properties: &BTreeMap<String, crate::storage::Value>,
    ) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Check required fields
        for field in &self.fields {
            if !field.nullable && !properties.contains_key(&field.name) {
                errors.push(format!("Missing required field: {}", field.name));
            }
        }

        // Check field types
        for (key, value) in properties {
            if let Some(field) = self.get_field(key) {
                if !field.field_type.matches(value) {
                    errors.push(format!(
                        "Field '{}' type mismatch: expected {:?}, got {:?}",
                        key, field.field_type, value
                    ));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Convert to SQL CREATE TABLE statement (for reference)
    pub fn to_sql(&self) -> String {
        let mut columns = Vec::new();

        columns.push("id UUID PRIMARY KEY".to_string());

        for field in &self.fields {
            let mut col = format!("{} {}", field.name, field.field_type.to_sql());

            if !field.nullable {
                col.push_str(" NOT NULL");
            }

            if field.unique {
                col.push_str(" UNIQUE");
            }

            if let Some(ref default) = field.default {
                col.push_str(&format!(" DEFAULT {}", default));
            }

            columns.push(col);
        }

        columns.push("created_at TIMESTAMP NOT NULL DEFAULT NOW()".to_string());
        columns.push("updated_at TIMESTAMP NOT NULL DEFAULT NOW()".to_string());

        format!(
            "CREATE TABLE {} (\n  {}\n);",
            self.name,
            columns.join(",\n  ")
        )
    }
}

/// A field in a schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: FieldType,
    /// Whether the field can be null
    pub nullable: bool,
    /// Whether the field has a unique constraint
    pub unique: bool,
    /// Whether the field is indexed
    pub indexed: bool,
    /// Default value (as JSON string)
    pub default: Option<String>,
    /// Field description
    pub description: Option<String>,
}

impl SchemaField {
    /// Create a new field
    pub fn new(name: &str, field_type: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type,
            nullable: true,
            unique: false,
            indexed: false,
            default: None,
            description: None,
        }
    }

    /// Set nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set unique
    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Set indexed
    pub fn indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }

    /// Set default value
    pub fn default(mut self, default: &str) -> Self {
        self.default = Some(default.to_string());
        self
    }
}

/// Field types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// UTF-8 text.
    String,
    /// 64-bit signed integer.
    Int,
    /// 64-bit floating-point number.
    Float,
    /// Boolean value.
    Bool,
    /// Date and time (UTC).
    DateTime,
    /// Arbitrary JSON value.
    Json,
    /// Raw byte array.
    Bytes,
    /// Universally unique identifier.
    Uuid,
    /// Enumeration with the given allowed values.
    Enum(Vec<String>),
    /// Ordered array of a single element type.
    Array(Box<FieldType>),
    /// Foreign-key reference to another schema.
    Reference(String),
}

impl FieldType {
    /// Parse a field type from string
    pub fn parse(s: &str) -> Self {
        let s_lower = s.to_lowercase();

        // Check for enum
        if s_lower.starts_with("enum(") && s_lower.ends_with(')') {
            let values = s[5..s.len() - 1]
                .split(',')
                .map(|v| v.trim().to_string())
                .collect();
            return FieldType::Enum(values);
        }

        // Check for array
        if s_lower.starts_with("array<") && s_lower.ends_with('>') {
            let inner = &s[6..s.len() - 1];
            return FieldType::Array(Box::new(FieldType::parse(inner)));
        }

        // Check for reference
        if s_lower.starts_with("ref:") || s_lower.starts_with("reference:") {
            let target = s.split(':').nth(1).unwrap_or("unknown");
            return FieldType::Reference(target.to_string());
        }

        match s_lower.as_str() {
            "string" | "text" | "varchar" => FieldType::String,
            "int" | "integer" | "bigint" | "i64" => FieldType::Int,
            "float" | "double" | "decimal" | "f64" => FieldType::Float,
            "bool" | "boolean" => FieldType::Bool,
            "datetime" | "timestamp" | "date" => FieldType::DateTime,
            "json" | "jsonb" | "object" => FieldType::Json,
            "bytes" | "binary" | "blob" => FieldType::Bytes,
            "uuid" | "id" => FieldType::Uuid,
            _ => FieldType::String,
        }
    }

    /// Convert to SQL type
    pub fn to_sql(&self) -> &'static str {
        match self {
            FieldType::String => "TEXT",
            FieldType::Int => "BIGINT",
            FieldType::Float => "DOUBLE PRECISION",
            FieldType::Bool => "BOOLEAN",
            FieldType::DateTime => "TIMESTAMP",
            FieldType::Json => "JSONB",
            FieldType::Bytes => "BYTEA",
            FieldType::Uuid => "UUID",
            FieldType::Enum(_) => "TEXT",      // Enums stored as text
            FieldType::Array(_) => "JSONB",    // Arrays stored as JSON
            FieldType::Reference(_) => "UUID", // References are UUIDs
        }
    }

    /// Check if a value matches this type
    pub fn matches(&self, value: &crate::storage::Value) -> bool {
        use crate::storage::Value;

        match (self, value) {
            (_, Value::Null) => true, // Null matches any type (nullable check is separate)
            (FieldType::String, Value::String(_)) => true,
            (FieldType::Int, Value::Int(_)) => true,
            (FieldType::Float, Value::Float(_)) => true,
            (FieldType::Float, Value::Int(_)) => true, // Int can be used as float
            (FieldType::Bool, Value::Bool(_)) => true,
            (FieldType::Json, Value::Object(_)) => true,
            (FieldType::Json, Value::Array(_)) => true,
            (FieldType::Bytes, Value::Bytes(_)) => true,
            (FieldType::Uuid, Value::String(s)) => uuid::Uuid::parse_str(s).is_ok(),
            (FieldType::Enum(values), Value::String(s)) => values.contains(s),
            (FieldType::Array(inner), Value::Array(arr)) => arr.iter().all(|v| inner.matches(v)),
            (FieldType::Reference(_), Value::String(s)) => uuid::Uuid::parse_str(s).is_ok(),
            _ => false,
        }
    }
}

/// A relationship between schemas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRelation {
    /// Source schema
    pub from_schema: String,
    /// Target schema
    pub to_schema: String,
    /// Relation type
    pub relation_type: RelationType,
    /// Alias for the relationship
    pub alias: Option<String>,
    /// Edge type name
    pub edge_type: String,
}

/// Relationship types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationType {
    /// One-to-one relationship
    HasOne,
    /// One-to-many relationship
    HasMany,
    /// Many-to-one relationship (reverse of HasMany)
    BelongsTo,
    /// Many-to-many relationship
    ManyToMany,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let fields = vec![
            SchemaField::new("name", FieldType::String).nullable(false),
            SchemaField::new("email", FieldType::String).unique(true),
            SchemaField::new("age", FieldType::Int),
        ];

        let schema = Schema::new("users", fields);

        assert_eq!(schema.name, "users");
        assert_eq!(schema.fields.len(), 3);
        assert!(schema.get_field("email").unwrap().unique);
    }

    #[test]
    fn test_field_type_parsing() {
        assert_eq!(FieldType::parse("string"), FieldType::String);
        assert_eq!(FieldType::parse("int"), FieldType::Int);
        assert_eq!(FieldType::parse("bool"), FieldType::Bool);

        let enum_type = FieldType::parse("enum(pending,active,inactive)");
        if let FieldType::Enum(values) = enum_type {
            assert_eq!(values, vec!["pending", "active", "inactive"]);
        } else {
            panic!("Expected Enum type");
        }
    }

    #[test]
    fn test_schema_validation() {
        let fields = vec![
            SchemaField::new("name", FieldType::String).nullable(false),
            SchemaField::new("age", FieldType::Int),
        ];

        let schema = Schema::new("users", fields);

        // Valid
        let mut props = std::collections::BTreeMap::new();
        props.insert(
            "name".to_string(),
            crate::storage::Value::String("John".to_string()),
        );
        props.insert("age".to_string(), crate::storage::Value::Int(30));
        assert!(schema.validate(&props).is_ok());

        // Missing required field
        let mut props = std::collections::BTreeMap::new();
        props.insert("age".to_string(), crate::storage::Value::Int(30));
        assert!(schema.validate(&props).is_err());
    }

    #[test]
    fn test_to_sql() {
        let fields = vec![
            SchemaField::new("name", FieldType::String).nullable(false),
            SchemaField::new("email", FieldType::String).unique(true),
        ];

        let schema = Schema::new("users", fields);
        let sql = schema.to_sql();

        assert!(sql.contains("CREATE TABLE users"));
        assert!(sql.contains("name TEXT NOT NULL"));
        assert!(sql.contains("email TEXT UNIQUE"));
    }
}
