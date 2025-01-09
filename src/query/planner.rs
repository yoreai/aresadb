//! Query planner and optimizer
//!
//! Plans query execution and optimizes based on available indexes.

#![allow(dead_code)]

use anyhow::Result;
use std::collections::HashSet;

use super::{Condition, ParsedQuery, QueryOperation};
use crate::schema::Schema;

/// A query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Steps to execute
    pub steps: Vec<PlanStep>,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Whether the plan uses indexes
    pub uses_index: bool,
}

/// A single step in the query plan
#[derive(Debug, Clone)]
pub enum PlanStep {
    /// Scan all nodes of a type
    FullScan {
        /// Node type to scan
        node_type: String,
    },
    /// Index lookup on a specific field
    IndexLookup {
        /// Node type containing the indexed field
        node_type: String,
        /// Indexed field name
        field: String,
        /// Value to look up in the index
        value: crate::storage::Value,
    },
    /// Filter results by conditions
    Filter {
        /// Conditions that rows must satisfy
        conditions: Vec<Condition>,
    },
    /// Sort results
    Sort {
        /// Field to sort by
        field: String,
        /// Sort in descending order when true
        descending: bool,
    },
    /// Limit results
    Limit {
        /// Maximum number of rows to return
        count: usize,
        /// Number of rows to skip
        offset: usize,
    },
    /// Project specific columns
    Project {
        /// Column names to include in the output
        columns: Vec<String>,
    },
    /// Graph traversal from a node
    Traverse {
        /// Node ID to start traversal from
        start_node: String,
        /// Maximum traversal depth
        depth: u32,
        /// Edge types to follow, or all if `None`
        edge_types: Option<Vec<String>>,
    },
    /// Insert a node
    InsertNode {
        /// Type of node to insert
        node_type: String,
        /// Property key-value pairs for the new node
        data: std::collections::BTreeMap<String, crate::storage::Value>,
    },
    /// Update nodes
    UpdateNodes {
        /// Property key-value pairs to set on matched nodes
        data: std::collections::BTreeMap<String, crate::storage::Value>,
    },
    /// Delete nodes
    DeleteNodes,
}

/// Query planner
pub struct QueryPlanner {
    /// Available schemas for optimization hints
    #[allow(dead_code)]
    schemas: Vec<Schema>,
    /// Known indexed fields
    indexed_fields: HashSet<(String, String)>, // (node_type, field)
}

impl QueryPlanner {
    /// Create a new planner
    pub fn new() -> Self {
        Self {
            schemas: Vec::new(),
            indexed_fields: HashSet::new(),
        }
    }

    /// Create with schema information
    pub fn with_schemas(schemas: Vec<Schema>) -> Self {
        let mut indexed_fields = HashSet::new();

        // Extract indexed fields from schemas
        for schema in &schemas {
            for field in &schema.fields {
                if field.indexed || field.unique {
                    indexed_fields.insert((schema.name.clone(), field.name.clone()));
                }
            }
        }

        Self {
            schemas,
            indexed_fields,
        }
    }

    /// Register an indexed field for the planner
    pub fn add_indexed_field(&mut self, node_type: String, field: String) {
        self.indexed_fields.insert((node_type, field));
    }

    /// Plan a query
    pub fn plan(&self, query: &ParsedQuery) -> Result<QueryPlan> {
        let mut steps = Vec::new();
        let mut estimated_cost = 0.0;
        let mut uses_index = false;

        match query.operation {
            QueryOperation::Select => {
                // Determine scan strategy
                let (scan_step, scan_cost, found_index) =
                    self.plan_scan(&query.target, &query.conditions);
                steps.push(scan_step);
                estimated_cost += scan_cost;
                uses_index = found_index;

                // Add remaining filters
                let index_conditions: HashSet<String> = if uses_index {
                    query
                        .conditions
                        .iter()
                        .filter(|c| {
                            self.indexed_fields
                                .contains(&(query.target.clone(), c.column.clone()))
                        })
                        .map(|c| c.column.clone())
                        .collect()
                } else {
                    HashSet::new()
                };

                let remaining_conditions: Vec<Condition> = query
                    .conditions
                    .iter()
                    .filter(|c| !index_conditions.contains(&c.column))
                    .cloned()
                    .collect();

                if !remaining_conditions.is_empty() {
                    steps.push(PlanStep::Filter {
                        conditions: remaining_conditions,
                    });
                    estimated_cost += 0.1; // Filter cost per row
                }

                // Add sorting
                for order in &query.order_by {
                    steps.push(PlanStep::Sort {
                        field: order.column.clone(),
                        descending: order.descending,
                    });
                    estimated_cost += 0.5; // Sort cost (n log n)
                }

                // Add limit
                if let Some(limit) = query.limit {
                    steps.push(PlanStep::Limit {
                        count: limit,
                        offset: query.offset.unwrap_or(0),
                    });
                }

                // Add projection if specific columns requested
                if !query.columns.is_empty() {
                    steps.push(PlanStep::Project {
                        columns: query.columns.clone(),
                    });
                }
            }

            QueryOperation::Insert => {
                if let Some(data) = &query.data {
                    steps.push(PlanStep::InsertNode {
                        node_type: query.target.clone(),
                        data: data.clone(),
                    });
                    estimated_cost = 1.0; // Insert is constant time
                }
            }

            QueryOperation::Update => {
                // First find nodes to update
                let (scan_step, scan_cost, found_index) =
                    self.plan_scan(&query.target, &query.conditions);
                steps.push(scan_step);
                estimated_cost += scan_cost;
                uses_index = found_index;

                // Filter
                if !query.conditions.is_empty() {
                    steps.push(PlanStep::Filter {
                        conditions: query.conditions.clone(),
                    });
                }

                // Update
                if let Some(data) = &query.data {
                    steps.push(PlanStep::UpdateNodes { data: data.clone() });
                    estimated_cost += 0.5; // Update cost per row
                }
            }

            QueryOperation::Delete => {
                // First find nodes to delete
                let (scan_step, scan_cost, found_index) =
                    self.plan_scan(&query.target, &query.conditions);
                steps.push(scan_step);
                estimated_cost += scan_cost;
                uses_index = found_index;

                // Filter
                if !query.conditions.is_empty() {
                    steps.push(PlanStep::Filter {
                        conditions: query.conditions.clone(),
                    });
                }

                // Delete
                steps.push(PlanStep::DeleteNodes);
                estimated_cost += 0.5; // Delete cost per row
            }

            QueryOperation::Traverse => {
                steps.push(PlanStep::Traverse {
                    start_node: query.target.clone(),
                    depth: query.limit.unwrap_or(2) as u32,
                    edge_types: None,
                });
                estimated_cost = 10.0; // Traversal is expensive
            }

            QueryOperation::CreateSchema | QueryOperation::DropSchema => {
                // Schema operations are handled separately
                estimated_cost = 1.0;
            }

            QueryOperation::VectorSearch => {
                estimated_cost = 5.0;
            }

            QueryOperation::CreateIndex
            | QueryOperation::DropIndex
            | QueryOperation::CreateFullTextIndex => {
                estimated_cost = 1.0;
            }

            QueryOperation::FullTextSearch => {
                estimated_cost = 3.0;
            }
        }

        Ok(QueryPlan {
            steps,
            estimated_cost,
            uses_index,
        })
    }

    /// Plan the scan strategy
    fn plan_scan(&self, node_type: &str, conditions: &[Condition]) -> (PlanStep, f64, bool) {
        // Check if any condition can use an index
        for condition in conditions {
            if self
                .indexed_fields
                .contains(&(node_type.to_string(), condition.column.clone()))
            {
                // Found an indexed field - use index lookup
                return (
                    PlanStep::IndexLookup {
                        node_type: node_type.to_string(),
                        field: condition.column.clone(),
                        value: condition.value.clone(),
                    },
                    0.1, // Index lookup is O(log n)
                    true,
                );
            }
        }

        // No index available - full scan
        (
            PlanStep::FullScan {
                node_type: node_type.to_string(),
            },
            1.0, // Full scan is O(n)
            false,
        )
    }

    /// Explain the query plan as a string
    pub fn explain(&self, plan: &QueryPlan) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "Query Plan (estimated cost: {:.2})",
            plan.estimated_cost
        ));
        lines.push(format!("Uses index: {}", plan.uses_index));
        lines.push("Steps:".to_string());

        for (i, step) in plan.steps.iter().enumerate() {
            let step_str = match step {
                PlanStep::FullScan { node_type } => {
                    format!("  {}. Full Scan on '{}'", i + 1, node_type)
                }
                PlanStep::IndexLookup {
                    node_type,
                    field,
                    value,
                } => {
                    format!(
                        "  {}. Index Lookup on '{}.{}' = {:?}",
                        i + 1,
                        node_type,
                        field,
                        value
                    )
                }
                PlanStep::Filter { conditions } => {
                    let cond_str: Vec<String> = conditions
                        .iter()
                        .map(|c| format!("{} {:?} {:?}", c.column, c.operator, c.value))
                        .collect();
                    format!("  {}. Filter: {}", i + 1, cond_str.join(" AND "))
                }
                PlanStep::Sort { field, descending } => {
                    let dir = if *descending { "DESC" } else { "ASC" };
                    format!("  {}. Sort by '{}' {}", i + 1, field, dir)
                }
                PlanStep::Limit { count, offset } => {
                    format!("  {}. Limit {} offset {}", i + 1, count, offset)
                }
                PlanStep::Project { columns } => {
                    format!("  {}. Project: {}", i + 1, columns.join(", "))
                }
                PlanStep::Traverse {
                    start_node,
                    depth,
                    edge_types,
                } => {
                    let edges = edge_types
                        .as_ref()
                        .map(|e| e.join(", "))
                        .unwrap_or_else(|| "all".to_string());
                    format!(
                        "  {}. Traverse from '{}' depth {} edges [{}]",
                        i + 1,
                        start_node,
                        depth,
                        edges
                    )
                }
                PlanStep::InsertNode { node_type, .. } => {
                    format!("  {}. Insert into '{}'", i + 1, node_type)
                }
                PlanStep::UpdateNodes { .. } => {
                    format!("  {}. Update nodes", i + 1)
                }
                PlanStep::DeleteNodes => {
                    format!("  {}. Delete nodes", i + 1)
                }
            };
            lines.push(step_str);
        }

        lines.join("\n")
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Operator;
    use crate::storage::Value;

    #[test]
    fn test_plan_select() {
        let planner = QueryPlanner::new();

        let query = ParsedQuery {
            operation: QueryOperation::Select,
            target: "users".to_string(),
            columns: vec!["name".to_string()],
            conditions: vec![Condition {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: Value::Int(25),
            }],
            order_by: vec![],
            limit: Some(10),
            offset: None,
            data: None,
            vector_search: None,
            fulltext_search: None,
        };

        let plan = planner.plan(&query).unwrap();
        assert!(!plan.uses_index); // No index defined
        assert!(plan.steps.len() >= 2); // At least scan + filter
    }

    #[test]
    fn test_plan_with_index() {
        let mut planner = QueryPlanner::new();
        planner
            .indexed_fields
            .insert(("users".to_string(), "email".to_string()));

        let query = ParsedQuery {
            operation: QueryOperation::Select,
            target: "users".to_string(),
            columns: vec![],
            conditions: vec![Condition {
                column: "email".to_string(),
                operator: Operator::Eq,
                value: Value::String("test@example.com".to_string()),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
            data: None,
            vector_search: None,
            fulltext_search: None,
        };

        let plan = planner.plan(&query).unwrap();
        assert!(plan.uses_index);
    }
}
