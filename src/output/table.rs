//! Table Renderer
//!
//! Beautiful table output for query results.

#![allow(dead_code)]

use anyhow::Result;
use colored::Colorize;

use crate::query::QueryResult;
use crate::storage::Value;

/// Table renderer
pub struct TableRenderer {
    max_width: usize,
    max_rows: usize,
}

impl TableRenderer {
    /// Create a new table renderer
    pub fn new() -> Self {
        Self {
            max_width: 50,
            max_rows: 1000,
        }
    }

    /// Set maximum column width
    pub fn max_width(mut self, width: usize) -> Self {
        self.max_width = width;
        self
    }

    /// Set maximum rows to display
    pub fn max_rows(mut self, rows: usize) -> Self {
        self.max_rows = rows;
        self
    }

    /// Render query results as a table
    pub fn render(&self, results: &QueryResult) -> Result<()> {
        if results.is_empty() {
            println!("{}", "(no results)".bright_black());
            return Ok(());
        }

        // Convert to tabled-compatible format
        let display_rows = results.rows.len().min(self.max_rows);
        let truncated = results.rows.len() > self.max_rows;

        // Build table data
        let mut table_data: Vec<Vec<String>> = Vec::new();

        for row in results.rows.iter().take(display_rows) {
            let row_strs: Vec<String> = row.iter().map(|v| self.format_value(v)).collect();
            table_data.push(row_strs);
        }

        // Create table
        println!();

        if table_data.is_empty() {
            println!("{}", "(no results)".bright_black());
            return Ok(());
        }

        // Print using manual formatting for more control
        self.print_manual_table(&results.columns, &table_data)?;

        // Show truncation notice
        if truncated {
            println!(
                "{}",
                format!(
                    "... showing {} of {} rows",
                    display_rows,
                    results.rows.len()
                )
                .bright_black()
            );
        }

        println!();

        Ok(())
    }

    /// Format a value for display
    fn format_value(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format!("{:.4}", f),
            Value::String(s) => {
                if s.len() > self.max_width {
                    format!("{}...", &s[..self.max_width - 3])
                } else {
                    s.clone()
                }
            }
            Value::Bytes(b) => format!("<{} bytes>", b.len()),
            Value::Array(arr) => {
                let preview: Vec<String> =
                    arr.iter().take(3).map(|v| self.format_value(v)).collect();
                let suffix = if arr.len() > 3 { "..." } else { "" };
                format!("[{}{}]", preview.join(", "), suffix)
            }
            Value::Object(obj) => {
                let preview: Vec<String> = obj
                    .iter()
                    .take(3)
                    .map(|(k, v)| format!("{}: {}", k, self.format_value(v)))
                    .collect();
                let suffix = if obj.len() > 3 { "..." } else { "" };
                format!("{{{}{}}}", preview.join(", "), suffix)
            }
            Value::Vector(v) => {
                format!("<vector dim={}>", v.len())
            }
        }
    }

    /// Print table manually for better control
    fn print_manual_table(&self, columns: &[String], data: &[Vec<String>]) -> Result<()> {
        // Calculate column widths
        let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

        for row in data {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.len());
                }
            }
        }

        // Cap widths
        for w in &mut widths {
            *w = (*w).min(self.max_width);
        }

        // Print header
        let header: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let width = widths.get(i).copied().unwrap_or(10);
                format!("{:width$}", c, width = width)
            })
            .collect();

        println!("{}", header.join(" │ ").bright_cyan().bold());

        // Print separator
        let sep: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
        println!("{}", sep.join("─┼─").bright_black());

        // Print rows
        for row in data {
            let row_str: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, cell)| {
                    let width = widths.get(i).copied().unwrap_or(10);
                    let truncated = if cell.len() > width {
                        format!("{}...", &cell[..width - 3])
                    } else {
                        cell.clone()
                    };
                    format!("{:width$}", truncated, width = width)
                })
                .collect();
            println!("{}", row_str.join(" │ "));
        }

        Ok(())
    }
}

impl Default for TableRenderer {
    fn default() -> Self {
        Self::new()
    }
}
