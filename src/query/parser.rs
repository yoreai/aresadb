//! SQL and natural language query parser
//!
//! Converts SQL strings to parsed queries using sqlparser-rs.

#![allow(dead_code)]

use anyhow::{bail, Result};
use sqlparser::ast::{
    BinaryOperator, Expr, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;

use super::{
    Condition, FullTextSearchParams, Operator, OrderBy, ParsedQuery, QueryOperation,
    VectorSearchParams,
};
use crate::storage::{DistanceMetric, Value};

/// SQL query parser
pub struct QueryParser {
    dialect: GenericDialect,
}

impl QueryParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Parse a SQL query string
    pub fn parse(&self, sql: &str) -> Result<ParsedQuery> {
        // First, try to parse as vector search syntax
        if let Some(vector_query) = self.parse_vector_search(sql) {
            return Ok(vector_query);
        }

        // Try FULLTEXT SEARCH syntax
        if let Some(ft_query) = self.parse_fulltext_search(sql) {
            return Ok(ft_query);
        }

        // Try CREATE INDEX / DROP INDEX / CREATE FULLTEXT INDEX (custom syntax)
        if let Some(index_query) = self.parse_index_command(sql) {
            return Ok(index_query);
        }

        // Fall back to standard SQL parsing
        let statements = Parser::parse_sql(&self.dialect, sql)?;

        if statements.is_empty() {
            bail!("No SQL statement found");
        }

        if statements.len() > 1 {
            bail!("Multiple statements not supported");
        }

        self.convert_statement(&statements[0])
    }

    /// Convert a SQL AST statement to ParsedQuery
    fn convert_statement(&self, stmt: &Statement) -> Result<ParsedQuery> {
        match stmt {
            Statement::Query(query) => self.convert_query(query),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let target = table_name.to_string();
                let column_names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();

                // Extract values from source
                let data = if let Some(source) = source {
                    self.extract_insert_values(&column_names, source)?
                } else {
                    None
                };

                Ok(ParsedQuery {
                    operation: QueryOperation::Insert,
                    target,
                    columns: column_names,
                    conditions: Vec::new(),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                    data,
                    vector_search: None,
                    fulltext_search: None,
                })
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let target = match &table.relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => bail!("Complex table references not supported"),
                };

                let mut data = BTreeMap::new();
                for assignment in assignments {
                    let column = assignment
                        .id
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(".");
                    let value = self.convert_expr(&assignment.value)?;
                    data.insert(column, value);
                }

                let conditions = selection
                    .as_ref()
                    .map(|expr| self.extract_conditions(expr))
                    .transpose()?
                    .unwrap_or_default();

                Ok(ParsedQuery {
                    operation: QueryOperation::Update,
                    target,
                    columns: Vec::new(),
                    conditions,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                    data: Some(data),
                    vector_search: None,
                    fulltext_search: None,
                })
            }
            Statement::Delete {
                from, selection, ..
            } => {
                let target = from
                    .first()
                    .map(|t| match &t.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => "unknown".to_string(),
                    })
                    .unwrap_or_else(|| "unknown".to_string());

                let conditions = selection
                    .as_ref()
                    .map(|expr| self.extract_conditions(expr))
                    .transpose()?
                    .unwrap_or_default();

                Ok(ParsedQuery {
                    operation: QueryOperation::Delete,
                    target,
                    columns: Vec::new(),
                    conditions,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                    data: None,
                    vector_search: None,
                    fulltext_search: None,
                })
            }
            _ => bail!("Unsupported SQL statement type"),
        }
    }

    /// Convert a SELECT query
    fn convert_query(&self, query: &Query) -> Result<ParsedQuery> {
        let select = match &*query.body {
            SetExpr::Select(select) => select,
            _ => bail!("Only SELECT queries are supported"),
        };

        self.convert_select(select, query)
    }

    /// Convert a SELECT statement
    fn convert_select(&self, select: &Select, query: &Query) -> Result<ParsedQuery> {
        // Extract table name
        let target = select
            .from
            .first()
            .map(|table| match &table.relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => "unknown".to_string(),
            })
            .unwrap_or_else(|| "unknown".to_string());

        // Extract columns
        let columns: Vec<String> = select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.to_string()),
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(ident),
                    ..
                } => Some(ident.to_string()),
                SelectItem::Wildcard(_) => None, // Will select all columns
                _ => None,
            })
            .collect();

        // Extract conditions from WHERE clause
        let conditions = select
            .selection
            .as_ref()
            .map(|expr| self.extract_conditions(expr))
            .transpose()?
            .unwrap_or_default();

        // Extract ORDER BY
        let order_by: Vec<OrderBy> = query
            .order_by
            .iter()
            .filter_map(|o| {
                if let OrderByExpr {
                    expr: Expr::Identifier(ident),
                    asc,
                    ..
                } = o
                {
                    Some(OrderBy {
                        column: ident.to_string(),
                        descending: !asc.unwrap_or(true),
                    })
                } else {
                    None
                }
            })
            .collect();

        // Extract LIMIT
        let limit = query.limit.as_ref().and_then(|expr| {
            if let Expr::Value(SqlValue::Number(n, _)) = expr {
                n.parse().ok()
            } else {
                None
            }
        });

        // Extract OFFSET
        let offset = query.offset.as_ref().and_then(|o| {
            if let Expr::Value(SqlValue::Number(n, _)) = &o.value {
                n.parse().ok()
            } else {
                None
            }
        });

        Ok(ParsedQuery {
            operation: QueryOperation::Select,
            target,
            columns,
            conditions,
            order_by,
            limit,
            offset,
            data: None,
            vector_search: None,
            fulltext_search: None,
        })
    }

    /// Extract conditions from a WHERE expression
    fn extract_conditions(&self, expr: &Expr) -> Result<Vec<Condition>> {
        let mut conditions = Vec::new();
        self.extract_conditions_recursive(expr, &mut conditions)?;
        Ok(conditions)
    }

    fn extract_conditions_recursive(
        &self,
        expr: &Expr,
        conditions: &mut Vec<Condition>,
    ) -> Result<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    self.extract_conditions_recursive(left, conditions)?;
                    self.extract_conditions_recursive(right, conditions)?;
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    if let Expr::Identifier(ident) = &**left {
                        let column = ident.to_string();
                        let operator = match op {
                            BinaryOperator::Eq => Operator::Eq,
                            BinaryOperator::NotEq => Operator::Ne,
                            BinaryOperator::Lt => Operator::Lt,
                            BinaryOperator::LtEq => Operator::Le,
                            BinaryOperator::Gt => Operator::Gt,
                            BinaryOperator::GtEq => Operator::Ge,
                            _ => unreachable!(),
                        };
                        let value = self.convert_expr(right)?;
                        conditions.push(Condition {
                            column,
                            operator,
                            value,
                        });
                    }
                }
                _ => {}
            },
            Expr::Like { expr, pattern, .. } => {
                if let Expr::Identifier(ident) = &**expr {
                    let column = ident.to_string();
                    let value = self.convert_expr(pattern)?;
                    conditions.push(Condition {
                        column,
                        operator: Operator::Like,
                        value,
                    });
                }
            }
            Expr::IsNull(expr) => {
                if let Expr::Identifier(ident) = &**expr {
                    conditions.push(Condition {
                        column: ident.to_string(),
                        operator: Operator::IsNull,
                        value: Value::Null,
                    });
                }
            }
            Expr::IsNotNull(expr) => {
                if let Expr::Identifier(ident) = &**expr {
                    conditions.push(Condition {
                        column: ident.to_string(),
                        operator: Operator::IsNotNull,
                        value: Value::Null,
                    });
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Expr::Identifier(ident) = &**expr {
                    let values: Result<Vec<Value>> =
                        list.iter().map(|e| self.convert_expr(e)).collect();
                    conditions.push(Condition {
                        column: ident.to_string(),
                        operator: Operator::In,
                        value: Value::Array(values?),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Convert a SQL expression to a Value
    fn convert_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Value(val) => self.convert_sql_value(val),
            Expr::Identifier(ident) => Ok(Value::String(ident.to_string())),
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => {
                let val = self.convert_expr(expr)?;
                match val {
                    Value::Int(i) => Ok(Value::Int(-i)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    _ => bail!("Cannot negate non-numeric value"),
                }
            }
            _ => bail!("Unsupported expression type: {:?}", expr),
        }
    }

    /// Convert a SQL value to a Value
    fn convert_sql_value(&self, val: &SqlValue) -> Result<Value> {
        match val {
            SqlValue::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Float(n.parse()?))
                } else {
                    Ok(Value::Int(n.parse()?))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::String(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
            SqlValue::Null => Ok(Value::Null),
            _ => bail!("Unsupported SQL value type"),
        }
    }

    /// Extract values from INSERT statement
    fn extract_insert_values(
        &self,
        columns: &[String],
        source: &Query,
    ) -> Result<Option<BTreeMap<String, Value>>> {
        if let SetExpr::Values(values) = &*source.body {
            if let Some(row) = values.rows.first() {
                let mut data = BTreeMap::new();
                for (i, expr) in row.iter().enumerate() {
                    let column = columns
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("col{}", i));
                    let value = self.convert_expr(expr)?;
                    data.insert(column, value);
                }
                return Ok(Some(data));
            }
        }
        Ok(None)
    }
}

impl Default for QueryParser {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryParser {
    /// Validate a SQL query without executing it
    pub fn validate(&self, sql: &str) -> bool {
        self.parse(sql).is_ok()
    }

    /// Parse CREATE INDEX / DROP INDEX / CREATE FULLTEXT INDEX commands.
    fn parse_index_command(&self, sql: &str) -> Option<ParsedQuery> {
        let sql_upper = sql.trim().to_uppercase();

        let (op, rest) = if sql_upper.starts_with("CREATE FULLTEXT INDEX") {
            ("fulltext", sql.trim()[21..].trim())
        } else if sql_upper.starts_with("CREATE INDEX") {
            ("create", sql.trim()[12..].trim())
        } else if sql_upper.starts_with("DROP INDEX") {
            ("drop", sql.trim()[10..].trim())
        } else {
            return None;
        };

        // Expect: ON <table> (<field>)
        let rest_upper = rest.to_uppercase();
        if !rest_upper.starts_with("ON ") {
            return None;
        }
        let rest = rest[3..].trim();

        // Extract table name and field
        let paren_start = rest.find('(')?;
        let paren_end = rest.find(')')?;
        if paren_end <= paren_start {
            return None;
        }

        let table = rest[..paren_start].trim().to_string();
        let field = rest[paren_start + 1..paren_end].trim().to_string();

        if table.is_empty() || field.is_empty() {
            return None;
        }

        let mut data = BTreeMap::new();
        data.insert("field".to_string(), Value::String(field));

        let operation = match op {
            "create" => QueryOperation::CreateIndex,
            "drop" => QueryOperation::DropIndex,
            "fulltext" => QueryOperation::CreateFullTextIndex,
            _ => return None,
        };

        Some(ParsedQuery {
            operation,
            target: table,
            columns: vec![],
            conditions: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            data: Some(data),
            vector_search: None,
            fulltext_search: None,
        })
    }

    /// Parse FULLTEXT SEARCH syntax:
    /// FULLTEXT SEARCH <table> FIELD <field> FOR '<query>' [LIMIT <n>]
    fn parse_fulltext_search(&self, sql: &str) -> Option<ParsedQuery> {
        let sql_upper = sql.to_uppercase();
        if !sql_upper.starts_with("FULLTEXT SEARCH") {
            return None;
        }

        let rest = sql[15..].trim();
        let tokens: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
        if tokens.len() < 2 {
            return None;
        }

        let target = tokens[0].to_string();
        let rest = tokens[1].trim();

        // Parse FIELD <field>
        let rest_upper = rest.to_uppercase();
        if !rest_upper.starts_with("FIELD ") {
            return None;
        }
        let rest = rest[6..].trim();

        let tokens: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
        if tokens.len() < 2 {
            return None;
        }
        let field = tokens[0].to_string();
        let rest = tokens[1].trim();

        // Parse FOR '<query>'
        let rest_upper = rest.to_uppercase();
        if !rest_upper.starts_with("FOR ") {
            return None;
        }
        let rest = rest[4..].trim();

        // Extract quoted string
        let (query_text, rest) = if let Some(stripped) = rest.strip_prefix('\'') {
            let end = stripped.find('\'')?;
            (&stripped[..end], stripped[end + 1..].trim())
        } else if let Some(stripped) = rest.strip_prefix('"') {
            let end = stripped.find('"')?;
            (&stripped[..end], stripped[end + 1..].trim())
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            (&rest[..end], rest[end..].trim())
        };

        // Parse optional LIMIT
        let rest_upper = rest.to_uppercase();
        let limit = if rest_upper.starts_with("LIMIT ") {
            rest[6..].trim().parse::<usize>().unwrap_or(10)
        } else {
            10
        };

        Some(ParsedQuery {
            operation: QueryOperation::FullTextSearch,
            target,
            columns: Vec::new(),
            conditions: Vec::new(),
            order_by: Vec::new(),
            limit: Some(limit),
            offset: None,
            data: None,
            vector_search: None,
            fulltext_search: Some(FullTextSearchParams {
                field,
                query: query_text.to_string(),
                limit,
            }),
        })
    }

    /// Try to parse a vector search query.
    ///
    /// Syntax: `VECTOR SEARCH <table> FIELD <field> FOR <vector> [METRIC <metric>] [LIMIT <n>]`
    ///
    /// Example: `VECTOR SEARCH documents FIELD embedding FOR [0.1, 0.2, 0.3] METRIC cosine LIMIT 10`
    pub fn parse_vector_search(&self, sql: &str) -> Option<ParsedQuery> {
        let sql_upper = sql.to_uppercase();

        if !sql_upper.starts_with("VECTOR SEARCH") {
            return None;
        }

        let parts: Vec<&str> = sql.split_whitespace().collect();

        if parts.len() < 6 {
            return None;
        }

        let target = parts.get(2)?.to_string();

        let field_idx = parts
            .iter()
            .position(|&p| p.eq_ignore_ascii_case("FIELD"))?;
        let embedding_field = parts.get(field_idx + 1)?.to_string();

        let vector_start = sql.find('[')? + 1;
        let vector_end = sql.find(']')?;
        let vector_str = &sql[vector_start..vector_end];

        let query_vector: Vec<f32> = vector_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        if query_vector.is_empty() {
            return None;
        }

        let metric = parts
            .iter()
            .position(|&p| p.eq_ignore_ascii_case("METRIC"))
            .and_then(|idx| parts.get(idx + 1))
            .map(|m| match m.to_lowercase().as_str() {
                "euclidean" | "l2" => DistanceMetric::Euclidean,
                "dot" | "dotproduct" => DistanceMetric::DotProduct,
                "manhattan" | "l1" => DistanceMetric::Manhattan,
                _ => DistanceMetric::Cosine,
            })
            .unwrap_or(DistanceMetric::Cosine);

        let k = parts
            .iter()
            .position(|&p| p.eq_ignore_ascii_case("LIMIT"))
            .and_then(|idx| parts.get(idx + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        // Parse optional WHERE clause
        // Syntax: VECTOR SEARCH table FIELD f FOR [...] WHERE col = 'val' AND col2 > 5 LIMIT k
        let filters = self.parse_vector_where(sql);

        Some(ParsedQuery {
            operation: QueryOperation::VectorSearch,
            target,
            columns: Vec::new(),
            conditions: filters.clone(),
            order_by: Vec::new(),
            limit: Some(k),
            offset: None,
            data: None,
            vector_search: Some(VectorSearchParams {
                query_vector,
                embedding_field,
                k,
                metric,
                filters,
            }),
            fulltext_search: None,
        })
    }

    /// Parse WHERE clause from a VECTOR SEARCH statement
    fn parse_vector_where(&self, sql: &str) -> Vec<Condition> {
        let sql_upper = sql.to_uppercase();

        // Find WHERE ... bounded by LIMIT, METRIC, or end of string
        let where_start = match sql_upper.find(" WHERE ") {
            Some(idx) => idx + 7,
            None => return Vec::new(),
        };

        // WHERE clause ends at LIMIT, METRIC, or end
        let where_end = [" LIMIT ", " METRIC "]
            .iter()
            .filter_map(|kw| {
                let upper_after = &sql_upper[where_start..];
                upper_after.find(kw).map(|i| where_start + i)
            })
            .min()
            .unwrap_or(sql.len());

        let where_clause = sql[where_start..where_end].trim();
        if where_clause.is_empty() {
            return Vec::new();
        }

        self.parse_simple_where(where_clause)
    }

    /// Parse simple WHERE conditions: col = 'val' AND col2 > 5
    fn parse_simple_where(&self, clause: &str) -> Vec<Condition> {
        let mut conditions = Vec::new();

        for part in clause.split(" AND ") {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some(cond) = self.parse_single_condition(part) {
                conditions.push(cond);
            }
        }

        conditions
    }

    /// Parse a single condition like "col = 'val'" or "score > 5"
    fn parse_single_condition(&self, s: &str) -> Option<Condition> {
        let operators = [
            ("!=", Operator::Ne),
            (">=", Operator::Ge),
            ("<=", Operator::Le),
            ("<>", Operator::Ne),
            ("=", Operator::Eq),
            (">", Operator::Gt),
            ("<", Operator::Lt),
        ];

        for (op_str, op) in &operators {
            if let Some(idx) = s.find(op_str) {
                let column = s[..idx].trim().to_string();
                let val_str = s[idx + op_str.len()..].trim();

                let value = if val_str.starts_with('\'') && val_str.ends_with('\'') {
                    Value::String(val_str[1..val_str.len() - 1].to_string())
                } else if let Ok(i) = val_str.parse::<i64>() {
                    Value::Int(i)
                } else if let Ok(f) = val_str.parse::<f64>() {
                    Value::Float(f)
                } else if val_str.eq_ignore_ascii_case("true") {
                    Value::Bool(true)
                } else if val_str.eq_ignore_ascii_case("false") {
                    Value::Bool(false)
                } else if val_str.eq_ignore_ascii_case("null") {
                    Value::Null
                } else {
                    Value::String(val_str.to_string())
                };

                return Some(Condition {
                    column,
                    operator: op.clone(),
                    value,
                });
            }
        }

        None
    }

    /// Parse a natural language query (falls back to SQL if not recognized)
    /// In production, this would use LLM APIs for NL processing
    pub fn parse_natural_language(&self, query: &str) -> Result<ParsedQuery> {
        // Simple pattern matching for common NL queries
        let query_lower = query.to_lowercase();

        if query_lower.starts_with("get all ") || query_lower.starts_with("find all ") {
            // "get all users" -> "SELECT * FROM users"
            let words: Vec<&str> = query.split_whitespace().collect();
            if words.len() >= 3 {
                let table = words[2];
                let sql = format!("SELECT * FROM {}", table);
                return self.parse(&sql);
            }
        }

        if query_lower.starts_with("show me ") {
            let words: Vec<&str> = query.split_whitespace().collect();
            if words.len() >= 4 {
                let table = words.last().unwrap();
                let sql = format!("SELECT * FROM {} LIMIT 10", table);
                return self.parse(&sql);
            }
        }

        if query_lower.starts_with("delete ") && query_lower.contains("from") {
            let words: Vec<&str> = query.split_whitespace().collect();
            if let Some(idx) = words.iter().position(|&w| w == "from") {
                if idx + 1 < words.len() {
                    let table = words[idx + 1];
                    let sql = format!("DELETE FROM {}", table);
                    return self.parse(&sql);
                }
            }
        }

        // Fall back to trying to parse as SQL
        self.parse(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let parser = QueryParser::new();

        let query = parser.parse("SELECT * FROM users WHERE age > 25").unwrap();
        assert_eq!(query.operation, QueryOperation::Select);
        assert_eq!(query.target, "users");
        assert_eq!(query.conditions.len(), 1);
        assert_eq!(query.conditions[0].column, "age");
    }

    #[test]
    fn test_parse_select_with_limit() {
        let parser = QueryParser::new();

        let query = parser
            .parse("SELECT name, email FROM users LIMIT 10")
            .unwrap();
        assert_eq!(query.columns, vec!["name", "email"]);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_parse_insert() {
        let parser = QueryParser::new();

        let query = parser
            .parse("INSERT INTO users (name, age) VALUES ('John', 30)")
            .unwrap();
        assert_eq!(query.operation, QueryOperation::Insert);
        assert_eq!(query.target, "users");

        let data = query.data.unwrap();
        assert_eq!(data.get("name").unwrap().as_str(), Some("John"));
        assert_eq!(data.get("age").unwrap().as_int(), Some(30));
    }

    #[test]
    fn test_parse_update() {
        let parser = QueryParser::new();

        let query = parser
            .parse("UPDATE users SET age = 31 WHERE name = 'John'")
            .unwrap();
        assert_eq!(query.operation, QueryOperation::Update);
        assert_eq!(query.conditions.len(), 1);
    }

    #[test]
    fn test_parse_delete() {
        let parser = QueryParser::new();

        let query = parser.parse("DELETE FROM users WHERE age < 18").unwrap();
        assert_eq!(query.operation, QueryOperation::Delete);
        assert_eq!(query.conditions.len(), 1);
    }

    #[test]
    fn test_parse_vector_search() {
        let parser = QueryParser::new();

        let query = parser.parse(
            "VECTOR SEARCH documents FIELD embedding FOR [0.1, 0.2, 0.3] METRIC cosine LIMIT 10"
        ).unwrap();

        assert_eq!(query.operation, QueryOperation::VectorSearch);
        assert_eq!(query.target, "documents");

        let params = query.vector_search.unwrap();
        assert_eq!(params.embedding_field, "embedding");
        assert_eq!(params.query_vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(params.k, 10);
    }

    #[test]
    fn test_parse_vector_search_euclidean() {
        let parser = QueryParser::new();

        let query = parser
            .parse("VECTOR SEARCH docs FIELD vec FOR [1.0, 2.0] METRIC euclidean LIMIT 5")
            .unwrap();

        let params = query.vector_search.unwrap();
        assert_eq!(params.metric, crate::storage::DistanceMetric::Euclidean);
        assert_eq!(params.k, 5);
    }

    #[test]
    fn test_parse_vector_search_with_where() {
        let parser = QueryParser::new();

        let query = parser.parse(
            "VECTOR SEARCH documents FIELD embedding FOR [0.1, 0.2, 0.3] WHERE topic = 'ai' AND score > 50 LIMIT 10"
        ).unwrap();

        assert_eq!(query.operation, QueryOperation::VectorSearch);
        assert_eq!(query.target, "documents");

        let params = query.vector_search.unwrap();
        assert_eq!(params.embedding_field, "embedding");
        assert_eq!(params.query_vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(params.k, 10);
        assert_eq!(params.filters.len(), 2);
        assert_eq!(params.filters[0].column, "topic");
        assert_eq!(params.filters[0].operator, Operator::Eq);
        assert_eq!(params.filters[1].column, "score");
        assert_eq!(params.filters[1].operator, Operator::Gt);
    }

    #[test]
    fn test_parse_vector_search_where_with_metric() {
        let parser = QueryParser::new();

        let query = parser
            .parse(
                "VECTOR SEARCH docs FIELD vec FOR [1.0] WHERE active = true METRIC cosine LIMIT 5",
            )
            .unwrap();

        let params = query.vector_search.unwrap();
        assert_eq!(params.filters.len(), 1);
        assert_eq!(params.filters[0].column, "active");
        assert_eq!(params.k, 5);
    }

    #[test]
    fn test_parse_create_index() {
        let parser = QueryParser::new();
        let query = parser.parse("CREATE INDEX ON users (email)").unwrap();
        assert_eq!(query.operation, QueryOperation::CreateIndex);
        assert_eq!(query.target, "users");
        assert_eq!(
            query.data.unwrap().get("field").unwrap(),
            &Value::String("email".to_string())
        );
    }

    #[test]
    fn test_parse_fulltext_search() {
        let parser = QueryParser::new();
        let query = parser
            .parse("FULLTEXT SEARCH articles FIELD content FOR 'machine learning' LIMIT 5")
            .unwrap();
        assert_eq!(query.operation, QueryOperation::FullTextSearch);
        assert_eq!(query.target, "articles");
        let ft = query.fulltext_search.unwrap();
        assert_eq!(ft.field, "content");
        assert_eq!(ft.query, "machine learning");
        assert_eq!(ft.limit, 5);
    }

    #[test]
    fn test_parse_create_fulltext_index() {
        let parser = QueryParser::new();
        let query = parser
            .parse("CREATE FULLTEXT INDEX ON articles (content)")
            .unwrap();
        assert_eq!(query.operation, QueryOperation::CreateFullTextIndex);
        assert_eq!(query.target, "articles");
    }

    #[test]
    fn test_parse_drop_index() {
        let parser = QueryParser::new();
        let query = parser.parse("DROP INDEX ON products (category)").unwrap();
        assert_eq!(query.operation, QueryOperation::DropIndex);
        assert_eq!(query.target, "products");
        assert_eq!(
            query.data.unwrap().get("field").unwrap(),
            &Value::String("category".to_string())
        );
    }
}
