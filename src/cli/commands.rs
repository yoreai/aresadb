//! CLI Commands
//!
//! Command handlers for the CLI.

#![allow(dead_code)]

use crate::query::QueryResult;
use anyhow::Result;

/// Output format for CLI
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Render as a human-readable table.
    #[default]
    Table,
    /// Render as JSON.
    Json,
    /// Render as comma-separated values.
    Csv,
}

/// Handle a CLI command
pub async fn handle_command(_command: &str, _args: &[&str]) -> Result<()> {
    // Command handling is done in main.rs
    // This module provides shared utilities
    Ok(())
}

/// Format a result based on output format
pub fn format_result(result: &QueryResult, format: OutputFormat) -> String {
    match format {
        OutputFormat::Table => format_as_table(result),
        OutputFormat::Json => format_as_json(result),
        OutputFormat::Csv => format_as_csv(result),
    }
}

fn format_as_table(result: &QueryResult) -> String {
    if result.is_empty() {
        return "(empty result)".to_string();
    }

    // Calculate column widths
    let mut widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();

    for row in &result.rows {
        for (i, value) in row.iter().enumerate() {
            let len = format!("{}", value).len();
            if i < widths.len() && len > widths[i] {
                widths[i] = len;
            }
        }
    }

    // Cap widths at 50
    for w in &mut widths {
        if *w > 50 {
            *w = 50;
        }
    }

    let mut lines = Vec::new();

    // Header
    let header: Vec<String> = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths.get(i).copied().unwrap_or(10)))
        .collect();
    lines.push(header.join(" | "));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    lines.push(sep.join("-+-"));

    // Rows
    for row in &result.rows {
        let row_str: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let s = format!("{}", v);
                let width = widths.get(i).copied().unwrap_or(10);
                if s.len() > width {
                    format!("{}...", &s[..width - 3])
                } else {
                    format!("{:width$}", s, width = width)
                }
            })
            .collect();
        lines.push(row_str.join(" | "));
    }

    lines.join("\n")
}

fn format_as_json(result: &QueryResult) -> String {
    serde_json::to_string_pretty(&result.to_json()).unwrap_or_else(|_| "{}".to_string())
}

fn format_as_csv(result: &QueryResult) -> String {
    let mut lines = Vec::new();

    // Header
    lines.push(result.columns.join(","));

    // Rows
    for row in &result.rows {
        let row_str: Vec<String> = row
            .iter()
            .map(|v| {
                let s = format!("{}", v);
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s
                }
            })
            .collect();
        lines.push(row_str.join(","));
    }

    lines.join("\n")
}
